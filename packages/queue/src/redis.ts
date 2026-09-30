import type { QueueEngine, RepeatSpec, StoredJob } from "./types";

export interface RedisEngineOptions {
  prefix?: string;
}

export interface RedisLike {
  send(command: string, args: string[]): Promise<unknown>;
  close(): void;
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
      return raw === null ? null : (JSON.parse(raw) as StoredJob);
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
        if (raw !== null) claimed.push(JSON.parse(raw) as RepeatSpec);
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
