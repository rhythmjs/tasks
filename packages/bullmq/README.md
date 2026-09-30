# @rhythmjs/bullmq

[BullMQ](https://docs.bullmq.io)-backed job queues for [Rhythm](https://github.com/rhythmjs/rhythm), the Bun-native backend framework: the same typed job map and kernel-module discipline as [`@rhythmjs/queue`](../queue), on full BullMQ. Choose this package when you want BullMQ's production machinery (Redis-durable jobs, stalled-job recovery with lock renewal, Lua-scripted atomic state transitions, Job Schedulers, rate limiting, the Bull Board / Taskforce ecosystem) and accept its ioredis dependency. Choose `@rhythmjs/queue` when you want zero queue dependencies on Bun natives.

The two packages share the same service shape, so switching is a one-line change at the registration site; handlers and producers do not move.

## Install

```sh
bun add @rhythmjs/bullmq
```

Requires Redis: `docker run --rm -p 6379:6379 redis`.

## The job map is the contract

```ts
// jobs/index.ts, one centralized place
import type { QueueContext } from "@rhythmjs/bullmq";

export interface AppJobs {
  "email.send": { to: string; subject: string };
  "order.process": { orderId: string };
}
export type AppQueueContext = QueueContext<AppJobs>;
```

```ts
import { Rhythm } from "@rhythmjs/rhythm";
import { bullmqModule } from "@rhythmjs/bullmq";
import type { AppJobs } from "./jobs";

const app = new Rhythm().register(
  bullmqModule.forRoot<AppJobs>({
    name: "app",
    connection: { host: "127.0.0.1", port: 6379 }, // ioredis ConnectionOptions
    defaultJobOptions: { attempts: 3, backoff: { type: "exponential", delay: 1000 }, removeOnComplete: 100 },
  }),
  ({ queueService }) => ({ queueService }),
);
```

The module owns the BullMQ lifecycle: workers and the queue close on the kernel's `teardown()` (reverse provider order, so it plays with `gracefulShutdown` automatically).

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

Job options are BullMQ's own `JobsOptions`, passed through untouched: `delay`, `attempts`, `backoff`, `priority`, `removeOnComplete`/`removeOnFail`, `jobId`, `lifo`, deduplication, and everything else BullMQ accepts.

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
    onFailed: (name, id, error) => log.error(`job ${name}#${id} failed`, error),
  },
);
await worker.close();
```

One BullMQ queue, jobs dispatched to processors by name; a job with no registered processor fails loudly and lands in BullMQ's retry/failed flow like any other error. Workers get BullMQ's stalled-job recovery: a job whose worker dies is picked up again.

## Repeatable schedules

```ts
await queueService.schedule("order.process", { pattern: "0 3 * * *", tz: "UTC" }, { orderId: "nightly" });
await queueService.unschedule("order.process");
```

Backed by BullMQ Job Schedulers (Redis-coordinated), this **fires once across any number of app replicas**. `repeat` is BullMQ's `RepeatOptions` (`pattern`, `every`, `tz`, `limit`, …).

## The escape hatch

`queueService.queue` is the underlying BullMQ `Queue`: pause/resume, `getJobs`, flows, metrics, and anything else this facade doesn't wrap:

```ts
await queueService.queue.pause();
```

## Development

```sh
bun install
bun test # wiring suite runs mocked; live suite self-skips without REDIS_URL
REDIS_URL=redis://localhost:6379 bun test # + live integration against real Redis
bun run typecheck # tsc --noEmit
bun run build # bun build + tsc declarations
```
