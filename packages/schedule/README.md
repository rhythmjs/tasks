# @rhythmjs/schedule

Task scheduling for [Rhythm](https://github.com/rhythmjs/rhythm) on Bun: cron, interval, and
timeout jobs declared as plain values, served by a kernel module, and driven by an in-process
scheduler backed by a dependency-free, timezone-aware cron engine.

The design splits _what runs_ from _who decides when_: every trigger — the scheduler or your own
code — reduces to `scheduleService.run(name)`, so job semantics (overlap skipping, error capture,
per-job state) live in one place and the scheduler stays thin.

## Install

```sh
bun add @rhythmjs/schedule
```

## Defining jobs and the module

```ts
import { Rhythm } from "@rhythmjs/rhythm";
import { scheduleModule, cronJob, intervalJob, timeoutJob, cronPatterns } from "@rhythmjs/schedule";

const jobs = [
  cronJob("cleanup", "0 3 * * *", async () => { ... }, { timezone: "Europe/Paris" }),
  cronJob("report", cronPatterns.hourly, async () => { ... }),
  intervalJob("heartbeat", 30_000, () => { ... }),
  timeoutJob("warmup", 5_000, async () => { ... }),
];

const app = new Rhythm().register(scheduleModule.forRoot(...jobs), ({ scheduleService }) => ({
  scheduleService,
}));
```

Job options: `timezone` (cron only, DST-correct), `overlap` (`"skip"` — the default — or
`"allow"`), `disabled`. `cronPatterns` covers the common expressions (`everyMinute`, `hourly`,
`daily`, …); patterns take 5 fields, an optional leading seconds field (6 fields), names
(`jan-dec`, `sun-sat`), lists, ranges, steps, and `@daily`-style aliases.

## `scheduleService`

- `run(name)` — execute one job now: the universal entry point. Skips when `disabled` or already
  `running` (with `overlap: "skip"`), catches errors into the result and state, returns
  `{ name, ran, durationMs?, error? }`.
- `runDue(date?)` — run every enabled cron job due at that minute.
- `state(name)` — `{ running, runs, lastRun?, lastError?, nextRun? }` — pairs naturally with a
  `HealthIndicator`.
- `nextRun(name, from?)` — next occurrence (cron jobs; `null` otherwise).
- `add(job)` / `remove(name)` — dynamic registry, NestJS `SchedulerRegistry`-style. A scheduler
  started earlier keeps its snapshot; restart it to pick up changes.
- `jobs` — the registry.

## Running the scheduler

```ts
import { startScheduler } from "@rhythmjs/schedule/scheduler";

const scheduler = startScheduler(scheduleService); // cron timers + intervals + timeouts
scheduler.stop(); // tie into gracefulShutdown / provider dispose
```

## The cron engine

The engine behind `nextRun` is exported on its own:

```ts
import { Cron } from "@rhythmjs/schedule/cron";

new Cron("30 2 * * *", { timezone: "Europe/Paris" }).nextRun(); // Date | null, DST gaps skipped
```

## Out of scope, by design

Distributed one-instance locking across replicas (a pluggable lock-store follow-up, à la
`@nestjs/locks`), persistent job queues (BullMQ territory), and platform-specific schedulers —
this package is coupled to Bun on purpose.

## Development

```sh
bun install
bun test
bun run typecheck  # tsc --noEmit
bun run build      # bun build + tsc declarations
```
