# @rhythmjs/schedule

Task scheduling for [Rhythm](https://github.com/rhythmjs/rhythm): cron, interval, and timeout jobs
declared as plain values, served by a kernel module, and driven by whichever adapter matches your
runtime — an in-process scheduler (powered by [croner](https://github.com/hexagon/croner)) for
long-lived servers, `Deno.cron` for Deno Deploy, a `scheduled` handler for Cloudflare Cron Triggers,
and authenticated HTTP routes for Vercel, Netlify, Kubernetes CronJobs, or anything else that can
call an endpoint.

The design splits _what runs_ from _who decides when_: every trigger — timer, platform event, HTTP
call, or your own code — reduces to `scheduleService.run(name)`, so job semantics (overlap skipping,
error capture, per-job state) live in one place and every adapter stays thin.

## Install

```sh
pnpm add @rhythmjs/schedule
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

Job options: `timezone` (cron only, DST-correct via croner), `overlap` (`"skip"` — the default — or
`"allow"`), `disabled`. `cronPatterns` covers the common expressions (`everyMinute`, `hourly`,
`daily`, …); croner's extended syntax (seconds field, `L`, `W`, `#`) is available too.

## `scheduleService`

- `run(name)` — execute one job now: the universal entry point every adapter calls. Skips when
  `disabled` or already `running` (with `overlap: "skip"`), catches errors into the result and
  state, returns `{ name, ran, durationMs?, error? }`.
- `runDue(date?)` — run every enabled cron job due at that minute; what HTTP-triggered platforms
  call.
- `state(name)` — `{ running, runs, lastRun?, lastError?, nextRun? }` — pairs naturally with a
  `HealthIndicator`.
- `nextRun(name, from?)` — next occurrence (cron jobs; `null` otherwise).
- `add(job)` / `remove(name)` — dynamic registry, NestJS `SchedulerRegistry`-style. Adapters started
  earlier keep their snapshot; restart them to pick up changes.
- `jobs` — the registry.

## Adapters

**Long-lived servers (Node, Bun, self-hosted Deno)** — the process owns the clock:

```ts
import { startScheduler } from "@rhythmjs/schedule/adapters/timer";

const scheduler = startScheduler(scheduleService); // croner cron timers + intervals + timeouts
scheduler.stop(); // tie into gracefulShutdown / provider dispose
```

**Deno Deploy** — hand jobs to the runtime's own scheduler:

```ts
import { registerDenoCron } from "@rhythmjs/schedule/adapters/deno";

registerDenoCron(scheduleService); // one Deno.cron entry per enabled cron job
```

**Cloudflare Workers** — the platform clock calls `scheduled()`; declare the same expressions in
`wrangler.toml`:

```ts
import { toScheduledHandler } from "@rhythmjs/schedule/adapters/cloudflare";

export default {
  fetch: toFetchHandler(app),
  scheduled: toScheduledHandler(scheduleService), // matches event.cron, falls back to runDue()
};
```

**Vercel / Netlify / Kubernetes / EventBridge→API** — anything that can call an endpoint
(`@rhythmjs/router` optional peer):

```ts
import { cronRoutes } from "@rhythmjs/schedule/adapters/http";

router.use(cronRoutes(scheduleService, { secret: process.env.CRON_SECRET }).middleware());
// GET  /cron            → job list with state
// GET|POST /cron/due    → run everything due this minute (one catch-all platform schedule)
// GET|POST /cron/jobs/:name → run one job (one platform schedule per job)
```

All routes require `Authorization: Bearer <secret>` when `secret` is set — the header Vercel Cron
sends from `CRON_SECRET`.

On serverless platforms the schedule is declared twice by nature (in code and in
`wrangler.toml`/`vercel.json` — the platform reads config, not code); the Cloudflare adapter's
exact-match-then-`runDue` dispatch tolerates drift between the two.

## Out of scope, by design

Distributed one-instance locking across replicas (a pluggable lock-store follow-up, à la
`@nestjs/locks`), persistent job queues (BullMQ territory), and generating platform schedule config.

## Development

```sh
pnpm install
pnpm test       # vp test
pnpm typecheck  # tsc --noEmit
pnpm build      # vp pack
```
