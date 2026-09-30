import { describe, expect, test } from "bun:test";
import { Rhythm } from "@rhythmjs/rhythm";
import { createQueueService, queueModule } from "./queue";
import type { EngineJob, EngineQueue, EngineWorker, QueueEngine } from "./types";

interface AppJobs {
  "email.send": { to: string; subject: string };
  "order.process": { orderId: string };
}

interface FakeEngine extends QueueEngine {
  queues: {
    name: string;
    options: Record<string, unknown>;
    added: { name: string; data: unknown; options?: Record<string, unknown> }[];
    schedulers: Map<string, { repeat: Record<string, unknown>; template?: Record<string, unknown> }>;
    closed: boolean;
  }[];
  workers: {
    name: string;
    options: Record<string, unknown>;
    processor: (job: EngineJob) => Promise<unknown>;
    listeners: Map<string, (...args: never[]) => void>;
    closed: boolean;
  }[];
}

const fakeEngine = (): FakeEngine => {
  const engine: FakeEngine = {
    queues: [],
    workers: [],
    createQueue: (name, options) => {
      const state: FakeEngine["queues"][number] = {
        name,
        options,
        added: [],
        schedulers: new Map(),
        closed: false,
      };
      engine.queues.push(state);
      let nextId = 1;
      const queue: EngineQueue = {
        add: async (jobName, data, jobOptions) => {
          state.added.push({ name: jobName, data, ...(jobOptions === undefined ? {} : { options: jobOptions }) });
          return { id: String(nextId++) };
        },
        addBulk: async (jobs) => {
          return jobs.map((job) => {
            state.added.push({
              name: job.name,
              data: job.data,
              ...(job.opts === undefined ? {} : { options: job.opts }),
            });
            return { id: String(nextId++) };
          });
        },
        upsertJobScheduler: async (schedulerId, repeat, template) => {
          state.schedulers.set(schedulerId, { repeat, ...(template === undefined ? {} : { template }) });
        },
        removeJobScheduler: async (schedulerId) => {
          state.schedulers.delete(schedulerId);
        },
        getJobCounts: async () => ({ waiting: state.added.length, active: 0 }),
        close: async () => {
          state.closed = true;
        },
      };
      return queue;
    },
    createWorker: (name, processor, options) => {
      const state: FakeEngine["workers"][number] = {
        name,
        options,
        processor,
        listeners: new Map(),
        closed: false,
      };
      engine.workers.push(state);
      const worker: EngineWorker = {
        on: (event, listener) => state.listeners.set(event, listener),
        close: async () => {
          state.closed = true;
        },
      };
      return worker;
    },
  };
  return engine;
};

describe("createQueueService", () => {
  test("add enqueues typed jobs on the configured queue and returns the id", async () => {
    const engine = fakeEngine();
    const service = createQueueService<AppJobs>({
      name: "app",
      engine,
      connection: { host: "localhost" },
      prefix: "rj",
      defaultJobOptions: { attempts: 3 },
    });

    const id = await service.add("email.send", { to: "a@b.c", subject: "hi" }, { delay: 500 });

    expect(id).toBe("1");
    const queue = engine.queues[0]!;
    expect(queue.name).toBe("app");
    expect(queue.options).toMatchObject({
      connection: { host: "localhost" },
      prefix: "rj",
      defaultJobOptions: { attempts: 3 },
    });
    expect(queue.added).toEqual([
      { name: "email.send", data: { to: "a@b.c", subject: "hi" }, options: { delay: 500 } },
    ]);

    // @ts-expect-error unknown job name
    void (() => service.add("email.snd", { to: "a@b.c", subject: "hi" }));
    // @ts-expect-error wrong payload shape
    void (() => service.add("order.process", { to: "a@b.c" }));
  });

  test("addBulk maps names, payloads, and options", async () => {
    const engine = fakeEngine();
    const service = createQueueService<AppJobs>({ engine });

    const ids = await service.addBulk([
      { name: "email.send", payload: { to: "x@y.z", subject: "s" } },
      { name: "order.process", payload: { orderId: "o1" }, options: { priority: 1 } },
    ]);

    expect(ids).toEqual(["1", "2"]);
    expect(engine.queues[0]!.added.map((job) => job.name)).toEqual(["email.send", "order.process"]);
    expect(engine.queues[0]!.added[1]).toMatchObject({ options: { priority: 1 } });
  });

  test("schedule upserts a job scheduler keyed by job name, unschedule removes it", async () => {
    const engine = fakeEngine();
    const service = createQueueService<AppJobs>({ engine });

    await service.schedule("order.process", { pattern: "0 3 * * *", timezone: "UTC" }, { orderId: "nightly" });

    const scheduler = engine.queues[0]!.schedulers.get("order.process");
    expect(scheduler?.repeat).toEqual({ pattern: "0 3 * * *", timezone: "UTC" });
    expect(scheduler?.template).toEqual({ name: "order.process", data: { orderId: "nightly" } });

    await service.unschedule("order.process");
    expect(engine.queues[0]!.schedulers.size).toBe(0);
  });

  test("process dispatches by job name to the typed processor", async () => {
    const engine = fakeEngine();
    const service = createQueueService<AppJobs>({ engine });
    const handled: string[] = [];

    service.process(
      {
        "email.send": (payload, job) => {
          handled.push(`${payload.to}#${job.id ?? "?"}`);
          return "sent";
        },
      },
      { concurrency: 4 },
    );

    const worker = engine.workers[0]!;
    expect(worker.name).toBe("rhythm");
    expect(worker.options).toMatchObject({ concurrency: 4 });

    const result = await worker.processor({ name: "email.send", data: { to: "a@b.c", subject: "s" }, id: "7" });
    expect(result).toBe("sent");
    expect(handled).toEqual(["a@b.c#7"]);

    await expect(worker.processor({ name: "order.process", data: { orderId: "o1" } })).rejects.toThrow(
      'no processor registered for job "order.process"',
    );
  });

  test("wires lifecycle hooks to worker events", async () => {
    const engine = fakeEngine();
    const service = createQueueService<AppJobs>({ engine });
    const seen: string[] = [];

    service.process(
      { "email.send": () => {} },
      {
        onCompleted: (name, id) => void seen.push(`ok:${name}:${id ?? "?"}`),
        onFailed: (name, id, error) => void seen.push(`err:${name}:${id ?? "?"}:${(error as Error).message}`),
      },
    );

    const worker = engine.workers[0]!;
    (worker.listeners.get("completed") as (job: EngineJob) => void)({ name: "email.send", data: {}, id: "3" });
    (worker.listeners.get("failed") as (job: EngineJob | undefined, error: unknown) => void)(
      { name: "email.send", data: {}, id: "4" },
      new Error("smtp down"),
    );

    expect(seen).toEqual(["ok:email.send:3", "err:email.send:4:smtp down"]);
  });

  test("close shuts down workers then the queue; worker handles close individually", async () => {
    const engine = fakeEngine();
    const service = createQueueService<AppJobs>({ engine });

    const handle = service.process({ "email.send": () => {} });
    service.process({ "order.process": () => {} });

    await handle.close();
    expect(engine.workers[0]!.closed).toBe(true);
    expect(engine.workers[1]!.closed).toBe(false);

    await service.close();
    expect(engine.workers[1]!.closed).toBe(true);
    expect(engine.queues[0]!.closed).toBe(true);
  });

  test("counts delegates to the queue", async () => {
    const engine = fakeEngine();
    const service = createQueueService<AppJobs>({ engine });
    await service.add("email.send", { to: "a@b.c", subject: "s" });

    expect(await service.counts()).toEqual({ waiting: 1, active: 0 });
  });
});

describe("queueModule", () => {
  test("exports the service through register and closes it on teardown", async () => {
    const engine = fakeEngine();
    const app = new Rhythm().register(queueModule.forRoot<AppJobs>({ engine }), (m) => ({
      queueService: m.queueService,
    }));

    await app.setup();
    const ctx = await app.run({});
    await ctx.queueService.add("order.process", { orderId: "o1" });
    expect(engine.queues[0]!.added).toHaveLength(1);

    await app.teardown();
    expect(engine.queues[0]!.closed).toBe(true);
  });
});
