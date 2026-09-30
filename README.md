# tasks

Background work for [Rhythm](https://github.com/rhythmjs/rhythm), the Bun-native backend framework — everything that runs outside the request/response path:

- **[`packages/events`](./packages/events)** — `@rhythmjs/events`: typed in-process event bus
  (announce). In-house dispatch, zero dependencies.
- **[`packages/queue`](./packages/queue)** — `@rhythmjs/queue`: typed job queues — in-memory by
  default, Bun's native redis client for distribution (obligate). Zero queue dependencies.
- **[`packages/bullmq`](./packages/bullmq)** — `@rhythmjs/bullmq`: the same typed queue surface on
  full [BullMQ](https://docs.bullmq.io) (obligate, production-grade) — stalled-job recovery, Job
  Schedulers, the BullMQ ecosystem. Requires Redis.
- **[`packages/schedule`](./packages/schedule)** — `@rhythmjs/schedule`: cron/interval/timeout jobs
  with an in-process scheduler and an in-house timezone-aware cron engine (recur).

The packages integrate deliberately: events bridge into queue jobs
(`eventBus.on("order.**", (p, e) => queueService.add(e, p))`), queue job schedulers cover
distributed cron, and all share the typed-map + kernel-module conventions.

## Examples

Small runnable programs, one per package, in [`examples/`](./examples) (workspace members using
`workspace:*`):

```sh
bun examples/events/index.ts     # typed bus: wildcards, once-promise, AbortSignal, emitAsync errors
bun examples/schedule/index.ts   # cron/interval/timeout via the timer adapter; runs ~3.5s, prints state
bun examples/queue/index.ts      # produce/process/schedule — in-memory; set REDIS_URL to distribute via Bun.redis
bun examples/bullmq/index.ts     # same flow on BullMQ — needs Redis (docker run --rm -p 6379:6379 redis)
```

## Tooling

This is a **Bun workspace** — no pnpm, no vite-plus:

```sh
bun install          # workspace install (bun.lock)
bun test             # all packages, bun:test
bun run build        # per package: bun build (ESM) + tsc (declarations)
bun run typecheck    # tsc --noEmit per package
bun run lint         # oxlint
bun run fmt          # prettier
bun run check        # fmt:check + lint + typecheck
```

`tsc` remains for type checking and `.d.ts` emission (Bun does not emit declarations), and
oxlint/prettier are root devDependencies executed by Bun — Bun has no built-in linter or formatter
yet.

## Publishing

Each package publishes independently to npm from its own directory
(`cd packages/<name> && bun publish`); `prepublishOnly` runs typecheck, tests, and build.

## License

ISC
