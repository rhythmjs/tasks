import { Queue, Worker, type Job } from "bullmq";
import { Rhythm } from "@rhythmjs/rhythm";
import type {
  BullmqModuleOptions,
  JobInfo,
  JobMap,
  JobProcessors,
  ProcessOptions,
  QueueService,
  QueueWorkerHandle,
} from "./types";

export type {
  BulkJobInput,
  BullmqModuleOptions,
  ConnectionOptions,
  JobInfo,
  JobMap,
  JobProcessors,
  JobsOptions,
  ProcessOptions,
  QueueContext,
  QueueService,
  QueueWorkerHandle,
  RepeatOptions,
} from "./types";

export function createQueueService<TJobs extends JobMap>(options: BullmqModuleOptions = {}): QueueService<TJobs> {
  const queueName = options.name ?? "rhythm";
  const connection = options.connection ?? { host: "127.0.0.1", port: 6379 };
  const shared = {
    connection,
    ...(options.prefix === undefined ? {} : { prefix: options.prefix }),
  };
  const queue = new Queue(queueName, {
    ...shared,
    ...(options.defaultJobOptions === undefined ? {} : { defaultJobOptions: options.defaultJobOptions }),
  });
  const workers: Worker[] = [];

  const service: QueueService<TJobs> = {
    queue,
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
      await queue.upsertJobScheduler(name, repeat, {
        name,
        data: payload,
        ...(jobOptions === undefined ? {} : { opts: jobOptions }),
      });
    },
    unschedule: async (name) => {
      await queue.removeJobScheduler(name);
    },
    process: (processors: JobProcessors<TJobs>, processOptions: ProcessOptions = {}) => {
      const dispatch = async (job: Job): Promise<unknown> => {
        const handler = Object.hasOwn(processors, job.name)
          ? (processors as Record<string, (payload: unknown, job: JobInfo) => unknown>)[job.name]
          : undefined;
        if (handler === undefined) throw new Error(`no processor registered for job "${job.name}"`);
        return handler(job.data, { id: job.id, name: job.name, attemptsMade: job.attemptsMade });
      };

      const worker = new Worker(queueName, dispatch, {
        ...shared,
        ...(processOptions.concurrency === undefined ? {} : { concurrency: processOptions.concurrency }),
      });

      if (processOptions.onCompleted !== undefined) {
        const onCompleted = processOptions.onCompleted;
        worker.on("completed", (job: Job) => onCompleted(job.name, job.id));
      }
      if (processOptions.onFailed !== undefined) {
        const onFailed = processOptions.onFailed;
        worker.on("failed", (job: Job | undefined, error: Error) => onFailed(job?.name ?? "unknown", job?.id, error));
      }

      workers.push(worker);
      const handle: QueueWorkerHandle = {
        close: async () => {
          await worker.close();
          const index = workers.indexOf(worker);
          if (index !== -1) workers.splice(index, 1);
        },
      };
      return handle;
    },
    counts: () => queue.getJobCounts() as Promise<Record<string, number>>,
    close: async () => {
      await Promise.all(workers.splice(0).map((worker) => worker.close()));
      await queue.close();
    },
  };

  return service;
}

export const bullmqModule = {
  forRoot<TJobs extends JobMap>(options: BullmqModuleOptions = {}) {
    return new Rhythm({ type: "module", name: "bullmq" }).provide(
      () => ({ queueService: createQueueService<TJobs>(options) }),
      (value) => value.queueService.close(),
    );
  },
};
