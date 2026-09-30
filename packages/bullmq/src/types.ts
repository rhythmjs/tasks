import type { ConnectionOptions, JobsOptions, Queue, RepeatOptions } from "bullmq";

export type { ConnectionOptions, JobsOptions, RepeatOptions } from "bullmq";

export type JobMap = object;

export interface JobInfo {
  id: string | undefined;
  name: string;
  attemptsMade: number;
}

export type JobProcessors<TJobs extends JobMap> = {
  [K in keyof TJobs & string]?: (payload: TJobs[K], job: JobInfo) => unknown;
};

export interface ProcessOptions {
  /** Parallel job slots in this worker (BullMQ `concurrency`, default 1). */
  concurrency?: number;
  onCompleted?: (name: string, jobId: string | undefined) => void;
  onFailed?: (name: string, jobId: string | undefined, error: unknown) => void;
}

export interface QueueWorkerHandle {
  close(): Promise<void>;
}

export type BulkJobInput<TJobs extends JobMap> = {
  [K in keyof TJobs & string]: { name: K; payload: TJobs[K]; options?: JobsOptions };
}[keyof TJobs & string];

export interface QueueService<TJobs extends JobMap> {
  add<K extends keyof TJobs & string>(name: K, payload: TJobs[K], options?: JobsOptions): Promise<string | undefined>;
  addBulk(jobs: BulkJobInput<TJobs>[]): Promise<(string | undefined)[]>;
  schedule<K extends keyof TJobs & string>(
    name: K,
    repeat: RepeatOptions,
    payload: TJobs[K],
    options?: JobsOptions,
  ): Promise<void>;
  unschedule(name: keyof TJobs & string): Promise<void>;
  process(processors: JobProcessors<TJobs>, options?: ProcessOptions): QueueWorkerHandle;
  counts(): Promise<Record<string, number>>;
  close(): Promise<void>;
  /** The underlying BullMQ Queue, for everything this facade does not cover. */
  readonly queue: Queue;
}

export type QueueContext<TJobs extends JobMap> = {
  queueService: QueueService<TJobs>;
};

export interface BullmqModuleOptions {
  /** Queue name (default "rhythm"). */
  name?: string;
  /** ioredis connection options or instance (default localhost:6379). */
  connection?: ConnectionOptions;
  /** Redis key prefix (BullMQ `prefix`). */
  prefix?: string;
  /** BullMQ defaults for every job: attempts, backoff, removeOnComplete, … */
  defaultJobOptions?: JobsOptions;
}
