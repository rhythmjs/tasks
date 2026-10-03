import { describe, expect, test } from "bun:test";
import { Rhythm } from "@rhythmjs/rhythm";
import { createEventBus, eventsModule, matchesPattern } from "./events";
import type { EventsContext } from "./types";

interface AppEvents {
  "order.created": { orderId: string; total: number };
  "order.shipped": { orderId: string };
  "order.item.added": { orderId: string; sku: string };
  "user.registered": { userId: string };
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("matchesPattern", () => {
  test("matches exact names, single-segment stars, and terminal globstars", () => {
    expect(matchesPattern("order.created", "order.created")).toBe(true);
    expect(matchesPattern("order.created", "order.shipped")).toBe(false);
    expect(matchesPattern("order.*", "order.created")).toBe(true);
    expect(matchesPattern("order.*", "order.item.added")).toBe(false);
    expect(matchesPattern("*.created", "order.created")).toBe(true);
    expect(matchesPattern("order.**", "order.item.added")).toBe(true);
    expect(matchesPattern("order.**", "order.created")).toBe(true);
    expect(matchesPattern("**", "user.registered")).toBe(true);
    expect(matchesPattern("user.*", "order.created")).toBe(false);
  });
});

describe("on / emit", () => {
  test("delivers typed payloads with the concrete event name", () => {
    const bus = createEventBus<AppEvents>();
    const seen: [string, unknown][] = [];

    bus.on("order.created", (payload, event) => {
      const total: number = payload.total;
      seen.push([event, total]);
    });

    bus.emit("order.created", { orderId: "o1", total: 42 });
    bus.emit("order.shipped", { orderId: "o1" });

    expect(seen).toEqual([["order.created", 42]]);
  });

  test("wildcard subscriptions receive every matching event", () => {
    const bus = createEventBus<AppEvents>();
    const names: string[] = [];

    bus.on("order.*", (_payload, event) => void names.push(event));
    bus.on("**", (_payload, event) => void names.push(`all:${event}`));

    bus.emit("order.created", { orderId: "o1", total: 1 });
    bus.emit("order.item.added", { orderId: "o1", sku: "s" });
    bus.emit("user.registered", { userId: "u1" });

    expect(names).toEqual(["order.created", "all:order.created", "all:order.item.added", "all:user.registered"]);
  });

  test("compile-time safety: unknown events and wrong payloads are rejected", () => {
    const bus = createEventBus<AppEvents>();

    // @ts-expect-error unknown event name
    void (() => bus.emit("order.craeted", { orderId: "o1", total: 1 }));
    // @ts-expect-error wrong payload shape
    void (() => bus.emit("order.shipped", { userId: "u1" }));
    // @ts-expect-error unknown exact subscription
    void (() => bus.on("order.deleted", () => {}));

    expect(true).toBe(true);
  });
});

describe("unsubscription", () => {
  test("returned unsubscribe and off both remove the handler", () => {
    const bus = createEventBus<AppEvents>();
    let calls = 0;
    const handler = (): void => void calls++;

    const unsubscribe = bus.on("order.created", handler);
    bus.on("order.shipped", handler);

    bus.emit("order.created", { orderId: "o1", total: 1 });
    unsubscribe();
    bus.emit("order.created", { orderId: "o1", total: 1 });

    bus.off("order.shipped", handler);
    bus.emit("order.shipped", { orderId: "o1" });

    expect(calls).toBe(1);
    expect(bus.listenerCount()).toBe(0);
  });

  test("an AbortSignal unsubscribes, and an aborted signal never subscribes", () => {
    const bus = createEventBus<AppEvents>();
    let calls = 0;
    const controller = new AbortController();

    bus.on("order.created", () => void calls++, { signal: controller.signal });
    bus.emit("order.created", { orderId: "o1", total: 1 });
    controller.abort();
    bus.emit("order.created", { orderId: "o1", total: 1 });

    const dead = new AbortController();
    dead.abort();
    bus.on("order.created", () => void calls++, { signal: dead.signal });
    bus.emit("order.created", { orderId: "o1", total: 1 });

    expect(calls).toBe(1);
  });
});

describe("once", () => {
  test("handler form fires exactly once", () => {
    const bus = createEventBus<AppEvents>();
    let calls = 0;

    bus.once("order.*", () => void calls++);
    bus.emit("order.created", { orderId: "o1", total: 1 });
    bus.emit("order.shipped", { orderId: "o1" });

    expect(calls).toBe(1);
    expect(bus.listenerCount()).toBe(0);
  });

  test("promise form resolves with the payload", async () => {
    const bus = createEventBus<AppEvents>();

    const waiting = bus.once("order.created");
    bus.emit("order.created", { orderId: "o9", total: 7 });

    expect(await waiting).toEqual({ orderId: "o9", total: 7 });
    expect(bus.listenerCount()).toBe(0);
  });

  test("promise form rejects on abort", async () => {
    const bus = createEventBus<AppEvents>();
    const controller = new AbortController();

    const waiting = bus.once("order.created", { signal: controller.signal });
    controller.abort(new Error("shutting down"));

    await expect(waiting).rejects.toThrow("shutting down");
  });
});

describe("error contracts", () => {
  test("emit routes sync throws and async rejections to onError and never breaks the emitter", async () => {
    const errors: [string, string][] = [];
    const bus = createEventBus<AppEvents>({
      onError: (error, event) => void errors.push([event, (error as Error).message]),
    });
    let delivered = 0;

    bus.on("order.created", () => {
      throw new Error("sync boom");
    });
    bus.on("order.created", () => Promise.reject(new Error("async boom")));
    bus.on("order.created", () => void delivered++);

    bus.emit("order.created", { orderId: "o1", total: 1 });
    await tick();

    expect(delivered).toBe(1);
    expect(errors).toEqual([
      ["order.created", "sync boom"],
      ["order.created", "async boom"],
    ]);
  });

  test("without onError, listener failures are logged and never crash the process", async () => {
    const logged: unknown[][] = [];
    const original = console.error;
    console.error = (...args: unknown[]) => void logged.push(args);
    try {
      const bus = createEventBus<AppEvents>();
      bus.on("order.created", () => {
        throw new Error("sync boom");
      });
      bus.on("order.created", () => Promise.reject(new Error("async boom")));

      bus.emit("order.created", { orderId: "o1", total: 1 });
      await tick();
    } finally {
      console.error = original;
    }

    expect(logged).toHaveLength(2);
    expect(String(logged[0]?.[0])).toContain("order.created");
    expect((logged[1]?.[1] as Error).message).toBe("async boom");
  });

  test("emitAsync awaits all listeners and aggregates failures", async () => {
    const bus = createEventBus<AppEvents>();
    const done: string[] = [];

    bus.on("order.created", async () => {
      await tick();
      done.push("slow");
    });
    bus.on("order.created", () => {
      throw new Error("one");
    });
    bus.on("order.created", () => Promise.reject(new Error("two")));

    const failure = await bus
      .emitAsync("order.created", { orderId: "o1", total: 1 })
      .then(() => null)
      .catch((error: unknown) => error as AggregateError);

    expect(done).toEqual(["slow"]);
    expect(failure).toBeInstanceOf(AggregateError);
    expect(failure!.errors.map((error: Error) => error.message)).toEqual(["one", "two"]);
  });

  test("emitAsync resolves cleanly when all listeners succeed", async () => {
    const bus = createEventBus<AppEvents>();
    bus.on("user.registered", async () => {});

    await expect(bus.emitAsync("user.registered", { userId: "u1" })).resolves.toBeUndefined();
  });
});

describe("eventsModule", () => {
  test("exports a typed bus through register, consumed by a child module via EventsContext", async () => {
    const seen: string[] = [];

    const orderModule = new Rhythm<EventsContext<AppEvents>>().use(async (ctx, next) => {
      ctx.eventBus.emit("order.created", { orderId: "o1", total: 10 });
      await next();
    });

    const app = new Rhythm()
      .register(eventsModule.forRoot<AppEvents>(), (m) => ({ eventBus: m.eventBus }))
      .register(orderModule);

    const ctx = await app.run({});
    ctx.eventBus.on("order.*", (_payload, event) => void seen.push(event));
    await app.run({});

    expect(seen).toEqual(["order.created"]);
  });
});
