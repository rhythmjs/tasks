export type EventMap = object;

type Split<S extends string> = S extends `${infer Head}.${infer Rest}` ? [Head, ...Split<Rest>] : [S];

type SegmentsMatch<P extends readonly string[], E extends readonly string[]> = P extends readonly [
  infer PHead extends string,
  ...infer PRest extends readonly string[],
]
  ? PHead extends "**"
    ? true
    : E extends readonly [infer EHead extends string, ...infer ERest extends readonly string[]]
      ? PHead extends "*"
        ? SegmentsMatch<PRest, ERest>
        : PHead extends EHead
          ? SegmentsMatch<PRest, ERest>
          : false
      : false
  : E extends readonly []
    ? true
    : false;

export type MatchingEvents<TEvents extends EventMap, P extends string> = {
  [K in keyof TEvents & string as SegmentsMatch<Split<P>, Split<K>> extends true ? K : never]: TEvents[K];
};

export type MatchingNames<TEvents extends EventMap, P extends string> = keyof MatchingEvents<TEvents, P> & string;

export type MatchingPayload<TEvents extends EventMap, P extends string> = MatchingEvents<TEvents, P>[MatchingNames<
  TEvents,
  P
>];

export type EventPattern<TEvents extends EventMap> = (keyof TEvents & string) | `${string}*${string}`;

export type EventHandler<TPayload, TName extends string = string> = (
  payload: TPayload,
  event: TName,
) => void | Promise<void>;

export interface OnOptions {
  signal?: AbortSignal;
}

export interface EventBusOptions {
  onError?: (error: unknown, event: string, payload: unknown) => void;
}

export interface EventBus<TEvents extends EventMap> {
  on<P extends EventPattern<TEvents>>(
    pattern: P,
    handler: EventHandler<MatchingPayload<TEvents, P>, MatchingNames<TEvents, P>>,
    options?: OnOptions,
  ): () => void;
  once<P extends EventPattern<TEvents>>(
    pattern: P,
    handler: EventHandler<MatchingPayload<TEvents, P>, MatchingNames<TEvents, P>>,
    options?: OnOptions,
  ): () => void;
  once<P extends EventPattern<TEvents>>(pattern: P, options?: OnOptions): Promise<MatchingPayload<TEvents, P>>;
  off<P extends EventPattern<TEvents>>(
    pattern: P,
    handler: EventHandler<MatchingPayload<TEvents, P>, MatchingNames<TEvents, P>>,
  ): void;
  emit<K extends keyof TEvents & string>(event: K, payload: TEvents[K]): void;
  emitAsync<K extends keyof TEvents & string>(event: K, payload: TEvents[K]): Promise<void>;
  listenerCount(pattern?: string): number;
}

export type EventsContext<TEvents extends EventMap> = {
  eventBus: EventBus<TEvents>;
};
