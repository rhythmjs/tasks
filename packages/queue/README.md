# @rhythmjs/queue

Typed, [BullMQ](https://docs.bullmq.io)-backed job queues for
[Rhythm](https://github.com/rhythmjs/rhythm): durable background work with retries, delays,
priorities, and repeatable schedules — declared through a typed job map (the same discipline as
`@rhythmjs/events`), served by a kernel module, processed by lifecycle-managed workers. Runs on Node
and Bun; requires Redis.

Queues and events are complementary, not interchangeable: an event **announces** (ephemeral
broadcast to current listeners), a queue job **obligates** (durable, at-least-once, processed by
exactly one worker).

## Install

```sh
pnpm add @rhythmjs/queue
```

## The job map is the contract

```ts
// jobs/index.ts — one centralized place
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
    connection: { host: "127.0.0.1", port: 6379 },
    defaultJobOptions: { attempts: 3, backoff: { type: "exponential", delay: 1000 } },
  }),
  ({ queueService }) => ({ queueService }),
);
```

The module owns the BullMQ lifecycle: workers and the queue close on the kernel's `teardown()`
(reverse provider order — plays with `gracefulShutdown` automatically).

## Producing

```ts
await queueService.add("email.send", { to: "a@b.c", subject: "hi" }, { delay: 5000 });
// queueService.add("email.snd", …)         ← compile error
// queueService.add("order.process", {to})  ← compile error: wrong payload

await queueService.addBulk([
  { name: "email.send", payload: { to: "x@y.z", subject: "s" } },
  { name: "order.process", payload: { orderId: "o1" }, options: { priority: 1 } },
]);
```

`JobOptions` passes through to BullMQ: `delay`, `attempts`, `backoff`, `priority`,
`removeOnComplete`/`removeOnFail`, `jobId`, and anything else BullMQ accepts.

## Processing

```ts
const worker = queueService.process(
  {
    "email.send": async (payload, job) => {
      await mailer.send(payload.to, payload.subject); // payload fully typed per job name
    },
    "order.process": async (payload) => { ... },
  },
  {
    concurrency: 8,
    onCompleted: (name, id) => log.info({ name, id }, "job done"),
    onFailed: (name, id, error) => log.error({ name, id, error }, "job failed"),
  },
);

await worker.close(); // or let service/module teardown do it
```

One BullMQ queue, jobs dispatched to processors by name; a job with no registered processor fails
loudly (and lands in BullMQ's retry/failed flow like any other error).

## Repeatable schedules

```ts
await queueService.schedule("order.process", { pattern: "0 3 * * *", timezone: "UTC" }, { orderId: "nightly" });
await queueService.unschedule("order.process");
```

Backed by BullMQ Job Schedulers (Redis-coordinated), this **fires once across any number of app
replicas** — the distributed-cron answer that `@rhythmjs/schedule`'s in-process timer adapter
deliberately leaves out. Use `@rhythmjs/schedule` for single-instance and platform-scheduled
(Cloudflare/Deno/HTTP) cron; use this for replica-safe recurring jobs with retry semantics.

## Bridging from events

The `(payload, event)` listener signature maps directly onto job names:

```ts
eventBus.on("order.**", (payload, event) => void queueService.add(event, payload));
```

Payloads cross Redis as JSON — keep them serializable (no functions, class instances, or cycles).

## Testing without Redis

The BullMQ classes sit behind a structural `QueueEngine` (`createQueue`/`createWorker`), injectable
like the proxy middleware's `fetch`:

```ts
queueModule.forRoot<AppJobs>({ engine: fakeEngine }); // in-memory fake, no Redis — see queue.test.ts
```

`createQueueService(options)` builds the service without the module. Types
(`QueueService`, `JobMap`, `JobOptions`, `JobProcessors`, `QueueContext`, the engine contract) also
ship type-only from `@rhythmjs/queue/types`.

## Development

```sh
pnpm install
pnpm test       # vp test — no Redis needed
pnpm typecheck  # tsc --noEmit
pnpm build      # vp pack
```
