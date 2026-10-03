import { Rhythm } from "@rhythmjs/rhythm";
import type { EventBus, EventBusOptions, EventMap, EventsContext } from "./types";

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

interface Subscription {
  once: boolean;
  unsubscribe: () => void;
}

export function createEventBus<TEvents extends EventMap>(options: EventBusOptions = {}): EventBus<TEvents> {
  const onError: OnErrorHook =
    options.onError ??
    ((error: unknown, event: string) => {
      console.error(`[events] unhandled error in listener for "${event}":`, error);
    });

  const registry = new Map<string, Map<AnyHandler, Subscription>>();

  const subscribe = (pattern: string, handler: AnyHandler, once: boolean, signal?: AbortSignal): (() => void) => {
    if (signal?.aborted) return () => {};

    const subscription: Subscription = {
      once,
      unsubscribe: () => {
        const handlers = registry.get(pattern);
        if (handlers === undefined || !handlers.has(handler)) return;
        handlers.delete(handler);
        if (handlers.size === 0) registry.delete(pattern);
      },
    };

    let handlers = registry.get(pattern);
    if (handlers === undefined) {
      handlers = new Map();
      registry.set(pattern, handlers);
    }
    handlers.set(handler, subscription);
    signal?.addEventListener("abort", subscription.unsubscribe, { once: true });
    return subscription.unsubscribe;
  };

  const invoke = (handler: AnyHandler, payload: unknown, event: string): void => {
    try {
      const out = handler(payload, event);
      if (out instanceof Promise) {
        out.catch((error: unknown) => onError(error, event, payload));
      }
    } catch (error) {
      onError(error, event, payload);
    }
  };

  const dispatchPattern = (pattern: string, payload: unknown, event: string): void => {
    const entries = registry.get(pattern);
    if (entries === undefined) return;
    for (const [handler, subscription] of Array.from(entries)) {
      if (subscription.once) subscription.unsubscribe();
      invoke(handler, payload, event);
    }
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
      return new Promise((resolve, reject) => {
        if (signal?.aborted) {
          reject((signal.reason as Error | undefined) ?? new Error("aborted"));
          return;
        }
        const unsubscribe = subscribe(
          pattern,
          (payload) => {
            signal?.removeEventListener("abort", onAbort);
            resolve(payload);
          },
          true,
        );
        const onAbort = (): void => {
          unsubscribe();
          reject((signal?.reason as Error | undefined) ?? new Error("aborted"));
        };
        signal?.addEventListener("abort", onAbort, { once: true });
      });
    },
    off(pattern: string, handler: AnyHandler) {
      registry.get(pattern)?.get(handler)?.unsubscribe();
    },
    emit(event: string, payload: unknown) {
      dispatchPattern(event, payload, event);
      for (const pattern of Array.from(registry.keys())) {
        if (pattern !== event && matchesPattern(pattern, event)) dispatchPattern(pattern, payload, event);
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
    const module = new Rhythm<{}, EventsContext<TEvents>>({ type: "module", name: "events" });
    module.context.eventBus = createEventBus<TEvents>(options);
    return module;
  },
};
