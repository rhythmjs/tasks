import { describe, expect, test } from "bun:test";
import { Rhythm } from "@rhythmjs/rhythm";
import { createQueueService, memoryEngine, queueModule, redisEngine, type QueueEngine, type RedisLike } from "./queue";

interface Jobs {
  "email.send": { to: string };
  "report.build": { day: string };
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function until(check: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("condition never became true");
    await sleep(10);
  }
}

describe("memory engine service", () => {
  test("dispatches typed payloads to named processors", async () => {
    const service = createQueueService<Jobs>();
    const sent: string[] = [];

    const id = await service.add("email.send", { to: "ada@example.com" });
    expect(typeof id).toBe("string");

    service.process({ "email.send": (payload, job) => void sent.push(`${payload.to}:${job.attemptsMade}`) });
    await until(() => sent.length === 1);
    await service.close();

    expect(sent).toEqual(["ada@example.com:1"]);
  });

  test("delay holds a job until it is ready", async () => {
    const service = createQueueService<Jobs>();
    const stamps: number[] = [];
    const started = Date.now();

    await service.add("email.send", { to: "x" }, { delay: 80 });
    service.process({ "email.send": () => void stamps.push(Date.now() - started) });

    await sleep(40);
    expect(stamps).toHaveLength(0);
    await until(() => stamps.length === 1);
    expect(stamps[0]!).toBeGreaterThanOrEqual(70);
    await service.close();
  });

  test("retries with backoff, then fails once attempts are exhausted", async () => {
    const service = createQueueService<Jobs>();
    const failures: { name: string; error: unknown }[] = [];
    let runs = 0;

    await service.add("email.send", { to: "x" }, { attempts: 3, backoff: 20 });
    service.process(
      {
        "email.send": () => {
          runs++;
          throw new Error("smtp down");
        },
      },
      { onFailed: (name, _id, error) => void failures.push({ name, error }) },
    );

    await until(() => failures.length === 1);
    await service.close();

    expect(runs).toBe(3);
    expect(failures[0]!.name).toBe("email.send");
    expect((failures[0]!.error as Error).message).toBe("smtp down");
    expect((await service.counts()).failed).toBe(1);
  });

  test("higher priority runs sooner", async () => {
    const service = createQueueService<Jobs>();
    const order: string[] = [];

    await service.addBulk([
      { name: "email.send", payload: { to: "low" } },
      { name: "email.send", payload: { to: "high" }, options: { priority: 10 } },
      { name: "email.send", payload: { to: "mid" }, options: { priority: 5 } },
    ]);
    service.process({ "email.send": (payload) => void order.push(payload.to) });

    await until(() => order.length === 3);
    await service.close();
    expect(order).toEqual(["high", "mid", "low"]);
  });

  test("a custom jobId deduplicates pending jobs", async () => {
    const service = createQueueService<Jobs>();
    let runs = 0;

    await service.add("email.send", { to: "x" }, { jobId: "once", delay: 40 });
    await service.add("email.send", { to: "x" }, { jobId: "once", delay: 40 });
    expect((await service.counts()).delayed).toBe(1);

    service.process({ "email.send": () => void runs++ });
    await until(() => runs === 1);
    await sleep(60);
    await service.close();
    expect(runs).toBe(1);
  });

  test("schedule repeats with every + limit, and unschedule stops early", async () => {
    const service = createQueueService<Jobs>();
    let built = 0;
    let mailed = 0;

    await service.schedule("report.build", { every: 40, limit: 2 }, { day: "mon" });
    await service.schedule("email.send", { every: 30 }, { to: "digest" });
    service.process({
      "report.build": () => void built++,
      "email.send": () => void mailed++,
    });

    await until(() => built === 2 && mailed >= 2);
    await service.unschedule("email.send");
    const mailedAtStop = mailed;
    await sleep(120);
    await service.close();

    expect(built).toBe(2);
    expect(mailed - mailedAtStop).toBeLessThanOrEqual(1);
  });

  test("cron-pattern repeats run through the schedule engine", async () => {
    const service = createQueueService<Jobs>();
    let ticks = 0;

    await service.schedule("report.build", { pattern: "* * * * * *", limit: 1 }, { day: "tick" });
    service.process({ "report.build": () => void ticks++ });

    await until(() => ticks === 1, 3000);
    await service.close();
    expect(ticks).toBe(1);
  });

  test("concurrency runs jobs in parallel and close drains in-flight work", async () => {
    const service = createQueueService<Jobs>();
    let concurrent = 0;
    let peak = 0;
    let done = 0;

    await service.addBulk(
      Array.from({ length: 4 }, (_, i) => ({ name: "email.send" as const, payload: { to: String(i) } })),
    );
    const worker = service.process(
      {
        "email.send": async () => {
          concurrent++;
          peak = Math.max(peak, concurrent);
          await sleep(40);
          concurrent--;
          done++;
        },
      },
      { concurrency: 2 },
    );

    await until(() => done === 4);
    await worker.close();
    expect(peak).toBe(2);
    expect((await service.counts()).completed).toBe(4);
    await service.close();
  });

  test("a job with no registered processor counts as failed", async () => {
    const service = createQueueService<Jobs>();
    const failures: unknown[] = [];

    await service.add("report.build", { day: "tue" });
    service.process({}, { onFailed: (_name, _id, error) => void failures.push(error) });

    await until(() => failures.length === 1);
    await service.close();
    expect(String(failures[0])).toContain('no processor registered for job "report.build"');
  });

  test("a job named after an inherited object key is not dispatched to Object.prototype", async () => {
    const service = createQueueService<Record<string, unknown>>();
    const failures: unknown[] = [];

    await service.add("constructor", {});
    service.process({}, { onFailed: (_name, _id, error) => void failures.push(error) });

    await until(() => failures.length === 1);
    await service.close();
    expect(String(failures[0])).toContain('no processor registered for job "constructor"');
  });

  test("a worker loop survives transient engine failures and keeps processing", async () => {
    const inner = memoryEngine();
    let flaky = 2;
    const engine: QueueEngine = {
      ...inner,
      take: (now) => {
        if (flaky > 0) {
          flaky--;
          throw new Error("connection reset");
        }
        return inner.take(now);
      },
    };
    const service = createQueueService<Jobs>({ engine });
    const sent: string[] = [];
    const errors: unknown[] = [];

    await service.add("email.send", { to: "ada@example.com" });
    service.process(
      { "email.send": (payload) => void sent.push(payload.to) },
      { pollInterval: 10, onError: (error) => void errors.push(error) },
    );

    await until(() => sent.length === 1);
    await service.close();
    expect(errors.length).toBeGreaterThanOrEqual(2);
    expect(String(errors[0])).toContain("connection reset");
  });

  test("a corrupted repeat spec is dropped instead of killing the worker", async () => {
    const inner = memoryEngine();
    let corrupted = true;
    const engine: QueueEngine = {
      ...inner,
      claimDueRepeats: (now) => {
        if (corrupted) {
          corrupted = false;
          return Promise.resolve([{ name: "report.build", payload: { day: "?" }, pattern: "60 * * * *", nextAt: 0 }]);
        }
        return inner.claimDueRepeats(now);
      },
    };
    const service = createQueueService<Jobs>({ engine });
    const sent: string[] = [];
    const errors: unknown[] = [];

    await service.add("email.send", { to: "ada@example.com" });
    service.process(
      { "email.send": (payload) => void sent.push(payload.to), "report.build": () => {} },
      { pollInterval: 10, onError: (error) => void errors.push(error) },
    );

    await until(() => sent.length === 1);
    await service.close();
    expect(errors.length).toBeGreaterThanOrEqual(1);
  });
});

describe("queueModule", () => {
  test("provides queueService on the kernel and closes it on teardown", async () => {
    const app = new Rhythm().register(queueModule.forRoot<Jobs>(), ({ queueService }) => ({ queueService }));
    await app.setup();
    const { queueService } = await app.run({});

    const handled: string[] = [];
    await queueService.add("email.send", { to: "kernel" });
    queueService.process({ "email.send": (payload) => void handled.push(payload.to) });
    await until(() => handled.length === 1);

    await app.teardown();
    expect(handled).toEqual(["kernel"]);
  });
});

describe("redisEngine payload validation", () => {
  const stub = (replies: Record<string, unknown>): RedisLike => ({
    close: () => {},
    send: (command) => Promise.resolve(command === "ZRANGEBYSCORE" && !("ZRANGEBYSCORE" in replies) ? [] : replies[command] ?? null),
  });
  const validJob = {
    id: "1",
    name: "a",
    payload: {},
    priority: 0,
    attemptsMade: 0,
    maxAttempts: 3,
    backoffType: "fixed" as const,
    backoffDelay: 10,
    readyAt: 0,
  };

  test("take() returns a well-formed stored job", async () => {
    const engine = redisEngine(stub({ LPOP: JSON.stringify(validJob) }));

    expect(await engine.take(Date.now())).toEqual(validJob);
  });

  test("take() rejects jobs with the wrong shape or types", async () => {
    const bad = [
      "[]",
      "null",
      JSON.stringify({ ...validJob, maxAttempts: "1e9" }),
      JSON.stringify({ ...validJob, maxAttempts: 0 }),
      JSON.stringify({ ...validJob, maxAttempts: 1.5 }),
      JSON.stringify({ ...validJob, backoffType: "none" }),
      JSON.stringify({ ...validJob, backoffDelay: -1 }),
      JSON.stringify({ ...validJob, name: 7 }),
      JSON.stringify({ ...validJob, readyAt: null }),
    ];
    for (const raw of bad) {
      await expect(redisEngine(stub({ LPOP: raw })).take(Date.now())).rejects.toThrow("invalid job payload");
    }
  });

  test("claimDueRepeats() rejects malformed repeat specs", async () => {
    const claim = (spec: unknown) =>
      redisEngine(stub({ ZRANGEBYSCORE: ["r"], ZREM: 1, HGET: JSON.stringify(spec) })).claimDueRepeats(Date.now());

    expect(await claim({ name: "r", payload: {}, nextAt: 5, every: 1000 })).toHaveLength(1);
    await expect(claim({ name: "r", nextAt: "soon" })).rejects.toThrow("invalid repeat spec");
    await expect(claim({ name: "r", nextAt: 5, every: 0 })).rejects.toThrow("invalid repeat spec");
    await expect(claim({ name: "r", nextAt: 5, pattern: 3 })).rejects.toThrow("invalid repeat spec");
  });
});

function fakeRedis(): RedisLike & { closed: boolean } {
  const lists = new Map<string, string[]>();
  const zsets = new Map<string, Map<string, number>>();
  const hashes = new Map<string, Map<string, string>>();
  const strings = new Map<string, number>();
  const zset = (key: string) => zsets.get(key) ?? zsets.set(key, new Map()).get(key)!;
  const fake = {
    closed: false,
    close: () => void (fake.closed = true),
    send(command: string, args: string[]): Promise<unknown> {
      const [key = "", ...rest] = args;
      switch (command) {
        case "RPUSH": {
          const list = lists.get(key) ?? lists.set(key, []).get(key)!;
          list.push(...rest);
          return Promise.resolve(list.length);
        }
        case "LPOP":
          return Promise.resolve(lists.get(key)?.shift() ?? null);
        case "LLEN":
          return Promise.resolve(lists.get(key)?.length ?? 0);
        case "ZADD": {
          zset(key).set(rest[1]!, Number(rest[0]));
          return Promise.resolve(1);
        }
        case "ZREM": {
          const had = zset(key).delete(rest[0]!);
          return Promise.resolve(had ? 1 : 0);
        }
        case "ZCARD":
          return Promise.resolve(zset(key).size);
        case "ZRANGEBYSCORE": {
          const max = Number(rest[1]);
          const limit = rest[2] === "LIMIT" ? Number(rest[4]) : Infinity;
          const members = [...zset(key)]
            .filter(([, score]) => score <= max)
            .sort((a, b) => a[1] - b[1])
            .slice(0, limit)
            .map(([member]) => member);
          return Promise.resolve(members);
        }
        case "HSET": {
          const hash = hashes.get(key) ?? hashes.set(key, new Map()).get(key)!;
          hash.set(rest[0]!, rest[1]!);
          return Promise.resolve(1);
        }
        case "HGET":
          return Promise.resolve(hashes.get(key)?.get(rest[0]!) ?? null);
        case "HDEL":
          return Promise.resolve(hashes.get(key)?.delete(rest[0]!) ? 1 : 0);
        case "INCR": {
          const next = (strings.get(key) ?? 0) + 1;
          strings.set(key, next);
          return Promise.resolve(next);
        }
        case "GET":
          return Promise.resolve(strings.has(key) ? String(strings.get(key)) : null);
        default:
          return Promise.reject(new Error(`fakeRedis: unhandled command ${command}`));
      }
    },
  };
  return fake;
}

describe("redis engine (Bun.redis command surface)", () => {
  test("runs the full lifecycle against the redis command set", async () => {
    const redis = fakeRedis();
    const service = createQueueService<Jobs>({ engine: redisEngine(redis) });
    const sent: string[] = [];
    const failures: string[] = [];

    await service.add("email.send", { to: "now" });
    await service.add("email.send", { to: "later" }, { delay: 50 });
    await service.add("report.build", { day: "boom" }, { attempts: 2, backoff: 20 });
    await service.schedule("email.send", { every: 40, limit: 1 }, { to: "repeat" });

    expect(await service.counts()).toMatchObject({ waiting: 2, delayed: 1, repeats: 1 });

    service.process(
      {
        "email.send": (payload) => void sent.push(payload.to),
        "report.build": () => {
          throw new Error("nope");
        },
      },
      { onFailed: (name) => void failures.push(name) },
    );

    await until(() => sent.length === 3 && failures.length === 1);
    await service.close();

    expect(sent.sort()).toEqual(["later", "now", "repeat"]);
    expect(failures).toEqual(["report.build"]);
    const counts = await service.counts();
    expect(counts.completed).toBe(3);
    expect(counts.failed).toBe(1);
  });

  test("an owned string connection is closed with the engine, a passed client is not", async () => {
    const redis = fakeRedis();
    const engine = redisEngine(redis);
    await engine.close();
    expect(redis.closed).toBe(false);
  });
});
