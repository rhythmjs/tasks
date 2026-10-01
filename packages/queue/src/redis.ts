import type { QueueEngine, RepeatSpec, StoredJob } from "./types";

export interface RedisEngineOptions {
  prefix?: string;
}

export interface RedisLike {
  send(command: string, args: string[]): Promise<unknown>;
  close(): void;
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const isNumber = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const isOptional = (value: unknown, check: (v: unknown) => boolean): boolean => value === undefined || check(value);

function parseStoredJob(raw: string): StoredJob {
  const job: unknown = JSON.parse(raw);
  if (
    !isObject(job) ||
    typeof job.id !== "string" ||
    typeof job.name !== "string" ||
    !isNumber(job.priority) ||
    !isNumber(job.attemptsMade) ||
    !Number.isInteger(job.maxAttempts) ||
    (job.maxAttempts as number) < 1 ||
    (job.backoffType !== "fixed" && job.backoffType !== "exponential") ||
    !isNumber(job.backoffDelay) ||
    job.backoffDelay < 0 ||
    !isNumber(job.readyAt)
  ) {
    throw new Error("invalid job payload in redis queue");
  }
  return job as unknown as StoredJob;
}

function parseRepeatSpec(raw: string): RepeatSpec {
  const spec: unknown = JSON.parse(raw);
  if (
    !isObject(spec) ||
    typeof spec.name !== "string" ||
    !isNumber(spec.nextAt) ||
    !isOptional(spec.every, (v) => isNumber(v) && v > 0) ||
    !isOptional(spec.pattern, (v) => typeof v === "string") ||
    !isOptional(spec.timezone, (v) => typeof v === "string") ||
    !isOptional(spec.remaining, (v) => isNumber(v) && v >= 0) ||
    !isOptional(spec.options, isObject)
  ) {
    throw new Error("invalid repeat spec in redis queue");
  }
  return spec as unknown as RepeatSpec;
}

export function redisEngine(
  connection?: string | Bun.RedisClient | RedisLike,
  options: RedisEngineOptions = {},
): QueueEngine {
  const owned = typeof connection === "string" || connection === undefined;
  const client: RedisLike =
    connection === undefined
      ? Bun.redis
      : typeof connection === "string"
        ? new Bun.RedisClient(connection)
        : connection;
  const prefix = options.prefix ?? "rhythm:queue";
  const key = (suffix: string): string => `${prefix}:${suffix}`;

  const push = (job: StoredJob, now: number): Promise<unknown> =>
    job.readyAt > now
      ? client.send("ZADD", [key("delayed"), String(job.readyAt), JSON.stringify(job)])
      : client.send("RPUSH", [key("waiting"), JSON.stringify(job)]);

  const promote = async (now: number): Promise<void> => {
    const due = (await client.send("ZRANGEBYSCORE", [
      key("delayed"),
      "0",
      String(now),
      "LIMIT",
      "0",
      "32",
    ])) as string[];
    for (const member of due) {
      const claimed = (await client.send("ZREM", [key("delayed"), member])) as number;
      if (claimed === 1) await client.send("RPUSH", [key("waiting"), member]);
    }
  };

  return {
    add: async (job) => {
      await push(job, Date.now());
    },
    addBulk: async (jobs) => {
      const now = Date.now();
      for (const job of jobs) await push(job, now);
    },
    take: async (now) => {
      await promote(now);
      const raw = (await client.send("LPOP", [key("waiting")])) as string | null;
      return raw === null ? null : parseStoredJob(raw);
    },
    requeue: async (job, readyAt) => {
      await client.send("ZADD", [key("delayed"), String(readyAt), JSON.stringify({ ...job, readyAt })]);
    },
    record: async (kind) => {
      await client.send("INCR", [key(kind)]);
    },
    setRepeat: async (spec) => {
      await client.send("HSET", [key("repeat:data"), spec.name, JSON.stringify(spec)]);
      await client.send("ZADD", [key("repeat"), String(spec.nextAt), spec.name]);
    },
    clearRepeat: async (name) => {
      await client.send("ZREM", [key("repeat"), name]);
      await client.send("HDEL", [key("repeat:data"), name]);
    },
    claimDueRepeats: async (now) => {
      const due = (await client.send("ZRANGEBYSCORE", [key("repeat"), "0", String(now)])) as string[];
      const claimed: RepeatSpec[] = [];
      for (const name of due) {
        if (((await client.send("ZREM", [key("repeat"), name])) as number) !== 1) continue;
        const raw = (await client.send("HGET", [key("repeat:data"), name])) as string | null;
        await client.send("HDEL", [key("repeat:data"), name]);
        if (raw !== null) claimed.push(parseRepeatSpec(raw));
      }
      return claimed;
    },
    counts: async () => ({
      waiting: (await client.send("LLEN", [key("waiting")])) as number,
      delayed: (await client.send("ZCARD", [key("delayed")])) as number,
      completed: Number((await client.send("GET", [key("completed")])) ?? 0),
      failed: Number((await client.send("GET", [key("failed")])) ?? 0),
      repeats: (await client.send("ZCARD", [key("repeat")])) as number,
    }),
    close: () => {
      if (owned) client.close();
      return Promise.resolve();
    },
  };
}
