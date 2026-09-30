import { beforeEach, describe, expect, mock, test } from "bun:test";

type AnyRecord = Record<string, unknown>;

let seq = 0;

class FakeQueue {
  static instances: FakeQueue[] = [];
  name: string;
  opts: AnyRecord;
  added: { name: string; data: unknown; opts: AnyRecord | undefined; id: string }[] = [];
  schedulers = new Map<string, { repeat: AnyRecord; template: AnyRecord | undefined }>();
  closed = false;

  constructor(name: string, opts: AnyRecord) {
    this.name = name;
    this.opts = opts;
    FakeQueue.instances.push(this);
  }

  add(name: string, data: unknown, opts?: AnyRecord) {
    const job = { name, data, opts, id: String(++seq) };
    this.added.push(job);
    return Promise.resolve(job);
  }

  addBulk(jobs: { name: string; data: unknown; opts?: AnyRecord }[]) {
    return Promise.all(jobs.map((job) => this.add(job.name, job.data, job.opts)));
  }

  upsertJobScheduler(id: string, repeat: AnyRecord, template?: AnyRecord) {
    this.schedulers.set(id, { repeat, template });
    return Promise.resolve({ id });
  }

  removeJobScheduler(id: string) {
    return Promise.resolve(this.schedulers.delete(id));
  }

  getJobCounts() {
    return Promise.resolve({ waiting: this.added.length });
  }

  close() {
    this.closed = true;
    return Promise.resolve();
  }
}

type Listener = (...args: never[]) => void;

class FakeWorker {
  static instances: FakeWorker[] = [];
  name: string;
  processor: (job: unknown) => Promise<unknown>;
  opts: AnyRecord;
  listeners = new Map<string, Listener>();
  closed = false;

  constructor(name: string, processor: (job: unknown) => Promise<unknown>, opts: AnyRecord) {
    this.name = name;
    this.processor = processor;
    this.opts = opts;
    FakeWorker.instances.push(this);
  }

  on(event: string, listener: Listener): this {
    this.listeners.set(event, listener);
    return this;
  }

  emit(event: string, ...args: unknown[]): void {
    (this.listeners.get(event) as ((...a: unknown[]) => void) | undefined)?.(...args);
  }

  close() {
    this.closed = true;
    return Promise.resolve();
  }
}

mock.module("bullmq", () => ({ Queue: FakeQueue, Worker: FakeWorker }));

const { bullmqModule, createQueueService } = await import("./bullmq");
const { Rhythm } = await import("@rhythmjs/rhythm");

interface Jobs {
  "email.send": { to: string };
  "order.process": { orderId: string };
}

beforeEach(() => {
  FakeQueue.instances.length = 0;
  FakeWorker.instances.length = 0;
});

describe("createQueueService wiring", () => {
  test("passes name, connection, prefix, and defaultJobOptions to the Queue", () => {
    createQueueService<Jobs>({
      name: "app",
      connection: { host: "redis.internal", port: 6380 },
      prefix: "{tenant}",
      defaultJobOptions: { attempts: 3, removeOnComplete: true },
    });

    const queue = FakeQueue.instances[0]!;
    expect(queue.name).toBe("app");
    expect(queue.opts).toEqual({
      connection: { host: "redis.internal", port: 6380 },
      prefix: "{tenant}",
      defaultJobOptions: { attempts: 3, removeOnComplete: true },
    });
  });

  test("defaults to the rhythm queue on localhost", () => {
    createQueueService<Jobs>();
    const queue = FakeQueue.instances[0]!;
    expect(queue.name).toBe("rhythm");
    expect(queue.opts).toEqual({ connection: { host: "127.0.0.1", port: 6379 } });
  });

  test("add and addBulk map payloads onto BullMQ jobs and return ids", async () => {
    const service = createQueueService<Jobs>();
    const queue = FakeQueue.instances[0]!;

    const id = await service.add("email.send", { to: "a@b.c" }, { delay: 500 });
    const ids = await service.addBulk([
      { name: "email.send", payload: { to: "x" } },
      { name: "order.process", payload: { orderId: "o1" }, options: { priority: 1 } },
    ]);

    expect(id).toBe(queue.added[0]!.id);
    expect(ids).toHaveLength(2);
    expect(queue.added.map((job) => ({ name: job.name, data: job.data, opts: job.opts }))).toEqual([
      { name: "email.send", data: { to: "a@b.c" }, opts: { delay: 500 } },
      { name: "email.send", data: { to: "x" }, opts: undefined },
      { name: "order.process", data: { orderId: "o1" }, opts: { priority: 1 } },
    ]);
  });

  test("schedule upserts a Job Scheduler keyed by job name; unschedule removes it", async () => {
    const service = createQueueService<Jobs>();
    const queue = FakeQueue.instances[0]!;

    await service.schedule("order.process", { pattern: "0 3 * * *", tz: "UTC" }, { orderId: "n" }, { attempts: 2 });
    expect(queue.schedulers.get("order.process")).toEqual({
      repeat: { pattern: "0 3 * * *", tz: "UTC" },
      template: { name: "order.process", data: { orderId: "n" }, opts: { attempts: 2 } },
    });

    await service.unschedule("order.process");
    expect(queue.schedulers.size).toBe(0);
  });

  test("process creates a Worker on the queue with shared options and concurrency", () => {
    const service = createQueueService<Jobs>({ name: "app", prefix: "p", connection: { host: "h" } });
    service.process({}, { concurrency: 7 });

    const worker = FakeWorker.instances[0]!;
    expect(worker.name).toBe("app");
    expect(worker.opts).toEqual({ connection: { host: "h" }, prefix: "p", concurrency: 7 });
  });

  test("the worker processor dispatches by job name with typed JobInfo, and throws for unknown names", async () => {
    const service = createQueueService<Jobs>();
    const seen: unknown[] = [];
    service.process({ "email.send": (payload, job) => void seen.push([payload, job]) });

    const worker = FakeWorker.instances[0]!;
    await worker.processor({ name: "email.send", data: { to: "a@b.c" }, id: "42", attemptsMade: 1 });
    expect(seen).toEqual([[{ to: "a@b.c" }, { id: "42", name: "email.send", attemptsMade: 1 }]]);

    await expect(worker.processor({ name: "order.process", data: {}, id: "43", attemptsMade: 0 })).rejects.toThrow(
      'no processor registered for job "order.process"',
    );
  });

  test("completed and failed events reach the callbacks", () => {
    const service = createQueueService<Jobs>();
    const completed: unknown[] = [];
    const failed: unknown[] = [];
    service.process(
      {},
      {
        onCompleted: (name, id) => void completed.push([name, id]),
        onFailed: (name, id, error) => void failed.push([name, id, (error as Error).message]),
      },
    );

    const worker = FakeWorker.instances[0]!;
    worker.emit("completed", { name: "email.send", id: "1" });
    worker.emit("failed", { name: "order.process", id: "2" }, new Error("boom"));
    worker.emit("failed", undefined, new Error("orphaned"));

    expect(completed).toEqual([["email.send", "1"]]);
    expect(failed).toEqual([
      ["order.process", "2", "boom"],
      ["unknown", undefined, "orphaned"],
    ]);
  });

  test("exposes the underlying queue and forwards counts", async () => {
    const service = createQueueService<Jobs>();
    expect(service.queue).toBe(FakeQueue.instances[0] as never);
    await service.add("email.send", { to: "x" });
    expect(await service.counts()).toEqual({ waiting: 1 });
  });

  test("close closes workers then the queue; a closed worker handle deregisters", async () => {
    const service = createQueueService<Jobs>();
    const first = service.process({});
    service.process({});

    await first.close();
    expect(FakeWorker.instances[0]!.closed).toBe(true);

    await service.close();
    expect(FakeWorker.instances[1]!.closed).toBe(true);
    expect(FakeQueue.instances[0]!.closed).toBe(true);
  });
});

describe("bullmqModule", () => {
  test("provides queueService on the kernel and closes everything on teardown", async () => {
    const app = new Rhythm().register(bullmqModule.forRoot<Jobs>({ name: "mod" }), ({ queueService }) => ({
      queueService,
    }));
    await app.setup();
    const { queueService } = await app.run({});

    queueService.process({});
    expect(FakeQueue.instances[0]!.name).toBe("mod");

    await app.teardown();
    expect(FakeWorker.instances[0]!.closed).toBe(true);
    expect(FakeQueue.instances[0]!.closed).toBe(true);
  });
});
