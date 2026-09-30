import { Queue, Worker } from "bullmq";
import { Rhythm } from "@rhythmjs/rhythm";
import type {
  EngineJob,
  EngineQueue,
  EngineWorker,
  JobInfo,
  JobMap,
  JobProcessors,
  ProcessOptions,
  QueueEngine,
  QueueModuleOptions,
  QueueService,
} from "./types";

export type {
  BulkJobInput,
  EngineJob,
  EngineQueue,
  EngineWorker,
  JobInfo,
  JobMap,
  JobOptions,
  JobProcessors,
  ProcessOptions,
  QueueContext,
  QueueEngine,
  QueueModuleOptions,
  QueueService,
  QueueWorkerHandle,
  RepeatOptions,
} from "./types";

function bullmqEngine(): QueueEngine {
  return {
    createQueue: (name, options) => new Queue(name, options as never) as unknown as EngineQueue,
    createWorker: (name, processor, options) =>
      new Worker(name, processor as never, options as never) as unknown as EngineWorker,
  };
}

export function createQueueService<TJobs extends JobMap>(options: QueueModuleOptions = {}): QueueService<TJobs> {
  const queueName = options.name ?? "rhythm";
  const engine = options.engine ?? bullmqEngine();
  const shared = {
    ...(options.connection === undefined ? {} : { connection: options.connection }),
    ...(options.prefix === undefined ? {} : { prefix: options.prefix }),
  };
  const queue = engine.createQueue(queueName, {
    ...shared,
    ...(options.defaultJobOptions === undefined ? {} : { defaultJobOptions: options.defaultJobOptions }),
  });
  const workers: EngineWorker[] = [];

  const service: QueueService<TJobs> = {
    add: async (name, payload, jobOptions) => {
      const job = await queue.add(name, payload, jobOptions);
      return job.id;
    },
    addBulk: async (jobs) => {
      const added = await queue.addBulk(
        jobs.map((job) => ({
          name: job.name,
          data: job.payload,
          ...(job.options === undefined ? {} : { opts: job.options }),
        })),
      );
      return added.map((job) => job.id);
    },
    schedule: async (name, repeat, payload, jobOptions) => {
      await queue.upsertJobScheduler(name, repeat as Record<string, unknown>, {
        name,
        data: payload,
        ...(jobOptions === undefined ? {} : { opts: jobOptions }),
      });
    },
    unschedule: async (name) => {
      await queue.removeJobScheduler(name);
    },
    process: (processors: JobProcessors<TJobs>, processOptions: ProcessOptions = {}) => {
      const dispatch = async (job: EngineJob): Promise<unknown> => {
        const handler = (processors as Record<string, ((payload: unknown, job: JobInfo) => unknown) | undefined>)[
          job.name
        ];
        if (handler === undefined) throw new Error(`no processor registered for job "${job.name}"`);
        return handler(job.data, { name: job.name, id: job.id, attemptsMade: job.attemptsMade });
      };

      const worker = engine.createWorker(queueName, dispatch, {
        ...shared,
        ...(processOptions.concurrency === undefined ? {} : { concurrency: processOptions.concurrency }),
      });

      if (processOptions.onCompleted !== undefined) {
        const onCompleted = processOptions.onCompleted;
        worker.on("completed", ((job: EngineJob) => onCompleted(job.name, job.id)) as never);
      }
      if (processOptions.onFailed !== undefined) {
        const onFailed = processOptions.onFailed;
        worker.on("failed", ((job: EngineJob | undefined, error: unknown) =>
          onFailed(job?.name ?? "unknown", job?.id, error)) as never);
      }

      workers.push(worker);
      return {
        close: async () => {
          await worker.close();
          const index = workers.indexOf(worker);
          if (index !== -1) workers.splice(index, 1);
        },
      };
    },
    counts: () => queue.getJobCounts(),
    close: async () => {
      await Promise.all(workers.splice(0).map((worker) => worker.close()));
      await queue.close();
    },
  };

  return service;
}

export const queueModule = {
  forRoot<TJobs extends JobMap>(options: QueueModuleOptions = {}) {
    return new Rhythm({ type: "module", name: "queue" }).provide(
      () => ({ queueService: createQueueService<TJobs>(options) }),
      (value) => value.queueService.close(),
    );
  },
};
