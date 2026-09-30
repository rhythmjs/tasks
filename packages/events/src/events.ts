import { EventEmitter, captureRejectionSymbol, once as onceEvent } from "node:events";
import { Rhythm } from "@rhythmjs/rhythm";
import type { EventBus, EventBusOptions, EventMap } from "./types";

export type {
  EventBus,
  EventBusOptions,
  EventHandler,
  EventMap,
  EventPattern,
  EventsContext,
  MatchingEvents,
  MatchingNames,
  MatchingPayload,
  OnOptions,
} from "./types";

export function matchesPattern(pattern: string, event: string): boolean {
  if (pattern === event) return true;
  if (!pattern.includes("*")) return false;
  const patternSegments = pattern.split(".");
  const eventSegments = event.split(".");
  let index = 0;
  for (; index < patternSegments.length; index++) {
    const segment = patternSegments[index]!;
    if (segment === "**") return index === patternSegments.length - 1;
    if (index >= eventSegments.length) return false;
    if (segment !== "*" && segment !== eventSegments[index]) return false;
  }
  return index === eventSegments.length;
}

type AnyHandler = (payload: unknown, event: string) => void | Promise<void>;
type OnErrorHook = (error: unknown, event: string, payload: unknown) => void;

// Channels are prefixed so a user event named "error" never collides with
// EventEmitter's special error-event semantics.
const CHANNEL_PREFIX = "e:";

class BusEmitter extends EventEmitter {
  onErrorHook: OnErrorHook = () => {};

  [captureRejectionSymbol](error: unknown, channel: string | symbol, payload?: unknown, event?: unknown): void {
    const name = typeof event === "string" ? event : String(channel).slice(CHANNEL_PREFIX.length);
    this.onErrorHook(error, name, payload);
  }
}

interface Subscription {
  wrapped: AnyHandler;
  once: boolean;
  unsubscribe: () => void;
}

export function createEventBus<TEvents extends EventMap>(options: EventBusOptions = {}): EventBus<TEvents> {
  const onError: OnErrorHook =
    options.onError ??
    ((error: unknown) => {
      queueMicrotask(() => {
        throw error;
      });
    });

  const emitter = new BusEmitter({ captureRejections: true });
  emitter.setMaxListeners(0);
  emitter.onErrorHook = onError;

  const registry = new Map<string, Map<AnyHandler, Subscription>>();

  const subscribe = (pattern: string, handler: AnyHandler, once: boolean, signal?: AbortSignal): (() => void) => {
    if (signal?.aborted) return () => {};

    const subscription: Subscription = {
      once,
      // A returned promise is watched by captureRejections, which routes its
      // rejection to onError; sync throws are caught here — either way the
      // remaining listeners always run.
      wrapped: (payload, event) => {
        if (once) subscription.unsubscribe();
        try {
          return handler(payload, event);
        } catch (error) {
          onError(error, event, payload);
          return undefined;
        }
      },
      unsubscribe: () => {
        const handlers = registry.get(pattern);
        if (handlers === undefined || !handlers.has(handler)) return;
        handlers.delete(handler);
        if (handlers.size === 0) registry.delete(pattern);
        emitter.off(CHANNEL_PREFIX + pattern, subscription.wrapped);
      },
    };

    let handlers = registry.get(pattern);
    if (handlers === undefined) {
      handlers = new Map();
      registry.set(pattern, handlers);
    }
    handlers.set(handler, subscription);
    emitter.on(CHANNEL_PREFIX + pattern, subscription.wrapped);
    signal?.addEventListener("abort", subscription.unsubscribe, { once: true });
    return subscription.unsubscribe;
  };

  const bus = {
    on(pattern: string, handler: AnyHandler, onOptions: { signal?: AbortSignal } = {}) {
      return subscribe(pattern, handler, false, onOptions.signal);
    },
    once(
      pattern: string,
      handlerOrOptions?: AnyHandler | { signal?: AbortSignal },
      onOptions?: { signal?: AbortSignal },
    ) {
      if (typeof handlerOrOptions === "function") {
        return subscribe(pattern, handlerOrOptions, true, onOptions?.signal);
      }
      const signal = handlerOrOptions?.signal;
      return (async () => {
        try {
          const [payload] = (await onceEvent(emitter, CHANNEL_PREFIX + pattern, { signal })) as [unknown, string];
          return payload;
        } catch (error) {
          if (signal?.aborted) throw (signal.reason as Error | undefined) ?? error;
          throw error;
        }
      })();
    },
    off(pattern: string, handler: AnyHandler) {
      registry.get(pattern)?.get(handler)?.unsubscribe();
    },
    emit(event: string, payload: unknown) {
      emitter.emit(CHANNEL_PREFIX + event, payload, event);
      // Snapshot: handlers may unsubscribe (mutating the registry) mid-dispatch.
      const patterns = Array.from(registry.keys());
      for (const pattern of patterns) {
        if (pattern !== event && matchesPattern(pattern, event)) {
          emitter.emit(CHANNEL_PREFIX + pattern, payload, event);
        }
      }
    },
    async emitAsync(event: string, payload: unknown) {
      const handlers: AnyHandler[] = [];
      for (const [pattern, entries] of Array.from(registry)) {
        if (!matchesPattern(pattern, event)) continue;
        const snapshot = Array.from(entries);
        for (const [handler, subscription] of snapshot) {
          if (subscription.once) subscription.unsubscribe();
          handlers.push(handler);
        }
      }
      const settled = await Promise.allSettled(handlers.map(async (handler) => handler(payload, event)));
      const errors = settled
        .filter((result): result is PromiseRejectedResult => result.status === "rejected")
        .map((result) => result.reason as unknown);
      if (errors.length > 0) {
        throw new AggregateError(errors, `${errors.length} listener(s) failed for event "${event}"`);
      }
    },
    listenerCount(pattern?: string) {
      if (pattern !== undefined) return registry.get(pattern)?.size ?? 0;
      let total = 0;
      for (const handlers of registry.values()) total += handlers.size;
      return total;
    },
  };

  return bus as EventBus<TEvents>;
}

export const eventsModule = {
  forRoot<TEvents extends EventMap>(options: EventBusOptions = {}) {
    return new Rhythm({ type: "module", name: "events" }).provide(() => ({
      eventBus: createEventBus<TEvents>(options),
    }));
  },
};
