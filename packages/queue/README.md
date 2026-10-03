# @rhythmjs/queue

Typed job queues for [Rhythm](https://github.com/rhythmjs/rhythm), the Bun-native backend framework: background work with retries, delays, priorities, and repeatable schedules, declared through a typed job map (the same discipline as `@rhythmjs/events`), served by a kernel module, processed by lifecycle-managed workers. In-house engines, no queue library: **in-memory by default**, and **Bun's native redis client** (`Bun.redis`) when you need distribution. If you want full BullMQ instead (stalled-job recovery, Job Schedulers, its ecosystem), [`@rhythmjs/bullmq`](../bullmq) serves the same typed surface on it.

Queues and events are complementary, not interchangeable: an event **announces** (ephemeral broadcast to current listeners), a queue job **obligates** (at-least-once, processed by exactly one worker).

## Install

```sh
bun add @rhythmjs/queue
```

## The job map is the contract

```ts
// jobs/index.ts, one centralized place
import type { QueueContext } from "@rhythmjs/queue";

export interface AppJobs {
  "email.send": { to: string; subject: string };
  "order.process": { orderId: string };
}
export type AppQueueContext = QueueContext<AppJobs>;
```

```ts
import { Rhythm } from "@rhythmjs/rhythm";
import { queueModule } from "@rhythmjs/queue";
import type { AppJobs } from "./jobs";

const app = new Rhythm().register(
  queueModule.forRoot<AppJobs>({
    name: "app",
    redis: process.env.REDIS_URL, // omit for the in-memory engine
    defaultJobOptions: { attempts: 3, backoff: { type: "exponential", delay: 1000 } },
  }),
  ({ queueService }) => ({ queueService }),
);
```

`forRoot` builds the service eagerly and puts it on the module's `context`. Rhythm has no lifecycle, so you close it: `await queueService.close()` on shutdown stops the workers and releases the engine.

## Producing

```ts
await queueService.add("email.send", { to: "a@b.c", subject: "hi" }, { delay: 5000 });
// queueService.add("email.snd", …) ← compile error
// queueService.add("order.process", {to}) ← compile error: wrong payload

await queueService.addBulk([
  { name: "email.send", payload: { to: "x@y.z", subject: "s" } },
  { name: "order.process", payload: { orderId: "o1" }, options: { priority: 1 } },
]);
```

`JobOptions`: `delay` (ms), `attempts` (total, default 1), `backoff` (fixed ms, or `{ type: "fixed" | "exponential", delay }`), `priority` (higher runs sooner), `jobId` (custom id; pending duplicates are dropped).

## Processing

```ts
const worker = queueService.process(
  {
    "email.send": async (payload, job) => sendMail(payload), // payload typed per name
    "order.process": async (payload, job) => fulfil(payload.orderId),
  },
  {
    concurrency: 4,
    onCompleted: (name, id) => metrics.increment(`jobs.${name}.ok`),
    onFailed: (name, id, error) => log.error(`job ${name}#${id} exhausted retries`, error), // fires once, after the last attempt
    onError: (error) => log.warn("queue engine hiccup", error), // engine/backend failures, not job failures
  },
);
await worker.close(); // drains in-flight jobs
```

One queue, jobs dispatched to processors by name; a job with no registered processor fails loudly and lands in the retry/failed flow like any other error.

Worker loops survive their backend: an engine error (a Redis connection reset, a corrupted repeat
spec) is reported to `onError`, the loop sleeps one `pollInterval`, and polling resumes — it never
kills the worker.

## Repeatable schedules

```ts
await queueService.schedule("order.process", { pattern: "0 3 * * *", timezone: "UTC" }, { orderId: "nightly" });
await queueService.schedule("email.send", { every: 60_000, limit: 10 }, { to: "digest@x", subject: "digest" });
await queueService.unschedule("order.process");
```

`pattern` runs through `@rhythmjs/schedule`'s in-house, timezone-aware cron engine. On the redis engine, due repeats are claimed with an atomic `ZREM`, so a schedule **fires once across any number of app replicas**.

## Engines

- **`memoryEngine()`** (default): in-process, zero dependencies. Perfect for tests, CLIs, and single-instance apps.
- **`redisEngine(connection?, { prefix? })`**: distributed, on Bun's native redis client. pass a `redis://` URL (the engine owns and closes the client), an existing `Bun.RedisClient`, or nothing to use `Bun.redis` (`REDIS_URL`). Ready work lives in a list, delayed and repeating work in sorted sets scored by readiness; claims go through `ZREM` so concurrent workers never double-take.

Both implement the structural `QueueEngine` contract (`add`/`take`/`requeue`/`record`/repeat methods/`counts`); tests can substitute their own, and the redis engine itself accepts any `RedisLike` (`send`/`close`).

## Introspection

```ts
await queueService.counts(); // { waiting, delayed, completed, failed, repeats, active }
```

## Development

```sh
bun install
bun test # bun test runner; in-memory and redis-command-surface suites, no server needed
bun run typecheck # tsc --noEmit
bun run build # bun build + tsc declarations
```
