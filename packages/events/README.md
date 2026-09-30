# @rhythmjs/events

A fully typed event bus for [Rhythm](https://github.com/rhythmjs/rhythm): the NestJS events feature
rebuilt on the kernel with the type safety string-based emitters can't offer, and zero dependencies.
Event names, payloads, and even **wildcard subscriptions** are checked at compile time via
template-literal types.

The dispatch underneath is in-house and dependency-free — a pattern registry over Bun's
natively-optimized showcase modules, equally available on Node and Deno — configured with
`captureRejections` for async error routing and channel-prefixed internally so a user event named
`error` carries no special semantics. The typed facade (event map,
wildcards, dual emit semantics) is entirely this package; the hot dispatch path is the platform's.
On edge runtimes this requires Node compat (e.g. Cloudflare's `nodejs_compat`).

## Install

```sh
bun add @rhythmjs/events
```

## The event map is the contract

```ts
// events/index.ts — one centralized place, like config/index.ts
import type { EventsContext } from "@rhythmjs/events";

export interface AppEvents {
  "order.created": { orderId: string; total: number };
  "order.shipped": { orderId: string };
  "user.registered": { userId: string };
}
export type AppEventsContext = EventsContext<AppEvents>;
```

```ts
import { Rhythm } from "@rhythmjs/rhythm";
import { eventsModule } from "@rhythmjs/events";
import type { AppEvents, AppEventsContext } from "./events";

const app = new Rhythm().register(eventsModule.forRoot<AppEvents>(), ({ eventBus }) => ({ eventBus }));

// any child module, type-safe via the exported context type:
const orderModule = new Rhythm<AppEventsContext>().use(async (ctx, next) => {
  ctx.eventBus.emit("order.created", { orderId: "o1", total: 99 });
  // ctx.eventBus.emit("order.craeted", …)  ← compile error
  await next();
});
```

## Listening

```ts
const unsubscribe = eventBus.on("order.created", (payload) => {
  payload.total; // number — exact payload type
});

eventBus.on("order.*", (payload, event) => {
  // payload: OrderCreated | OrderShipped, event: "order.created" | "order.shipped"
});
eventBus.on("**", handler); // everything; "*" matches one segment, terminal "**" matches the rest

eventBus.once("order.created", handler); // one-shot
const payload = await eventBus.once("order.created"); // promise form — great in tests

eventBus.on("order.created", handler, { signal: controller.signal }); // AbortSignal unsubscription
eventBus.off("order.created", handler);
```

Listeners registered by a module belong in a provider, with the unsubscribes (or one
`AbortController`) returned to `dispose` — lifecycle-correct teardown through the kernel, no
decorators.

## Emitting — two semantics, two error contracts

- `emit(event, payload): void` — fire-and-forget, synchronous dispatch. Every listener failure (sync
  throw or async rejection) routes to the module's `onError(error, event, payload)` hook; the
  default rethrows in a microtask so nothing is silently swallowed. Emitters are never broken by
  listeners.
- `await emitAsync(event, payload)` — awaits all listeners in parallel; failures are collected into
  one `AggregateError` that the caller owns. Successful listeners always complete even when others
  fail.

## API

- `eventsModule.forRoot<TEvents>(options?)` — the kernel module, exporting `eventBus` via
  `register`'s second argument. `options.onError` sets the fire-and-forget error hook.
- `createEventBus<TEvents>(options?)` — the bus without the module (tests, standalone use).
- `EventBus<TEvents>` — `on` / `once` / `off` / `emit` / `emitAsync` / `listenerCount`.
- `EventsContext<TEvents>` — the context slice a child module declares as its input.
- `matchesPattern(pattern, event)` — the runtime matcher, exported for reuse.
- Types on `@rhythmjs/events/types`: `EventMap`, `EventPattern`, `MatchingEvents` /
  `MatchingNames` / `MatchingPayload` (the template-literal wildcard machinery), `EventHandler`,
  `OnOptions`, `EventBusOptions`.

The delimiter is fixed to `.` — it is what makes the wildcard types possible.

## Toward durable queues

The bus is deliberately in-process: no persistence, no retries, no cross-instance delivery. Those
belong to `@rhythmjs/queue`, which reuses the same event-map
discipline. Two conventions now keep that path smooth:

- Keep payloads **JSON-serializable** for any event you may later bridge to a queue — a queue job
  crosses Redis, so functions, class instances, and cyclic structures won't survive the trip.
- The `(payload, event)` handler signature makes bridging a one-liner, with the event name becoming
  the job name:

```ts
eventBus.on("order.**", (payload, event) => void queue.add(event, payload));
```

## Development

```sh
bun install
bun test           # bun test runner
bun run typecheck  # tsc --noEmit
bun run build      # bun build + tsc declarations
```
