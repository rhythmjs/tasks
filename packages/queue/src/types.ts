export type JobMap = object;

export interface JobOptions {
  delay?: number;
  attempts?: number;
  priority?: number;
  backoff?: number | { type: "fixed" | "exponential"; delay: number };
  removeOnComplete?: boolean | number | { age?: number; count?: number };
  removeOnFail?: boolean | number | { age?: number; count?: number };
  jobId?: string;
  [key: string]: unknown;
}

export interface RepeatOptions {
  pattern?: string;
  every?: number;
  timezone?: string;
  limit?: number;
}

export interface JobInfo {
  id?: string;
  name: string;
  attemptsMade?: number;
}

export type JobProcessors<TJobs extends JobMap> = {
  [K in keyof TJobs & string]?: (payload: TJobs[K], job: JobInfo) => unknown;
};

export interface ProcessOptions {
  concurrency?: number;
  onCompleted?: (name: string, jobId: string | undefined) => void;
  onFailed?: (name: string, jobId: string | undefined, error: unknown) => void;
}

export interface QueueWorkerHandle {
  close(): Promise<void>;
}

export type BulkJobInput<TJobs extends JobMap> = {
  [K in keyof TJobs & string]: { name: K; payload: TJobs[K]; options?: JobOptions };
}[keyof TJobs & string];

export interface QueueService<TJobs extends JobMap> {
  add<K extends keyof TJobs & string>(name: K, payload: TJobs[K], options?: JobOptions): Promise<string | undefined>;
  addBulk(jobs: BulkJobInput<TJobs>[]): Promise<(string | undefined)[]>;
  schedule<K extends keyof TJobs & string>(
    name: K,
    repeat: RepeatOptions,
    payload: TJobs[K],
    options?: JobOptions,
  ): Promise<void>;
  unschedule(name: keyof TJobs & string): Promise<void>;
  process(processors: JobProcessors<TJobs>, options?: ProcessOptions): QueueWorkerHandle;
  counts(): Promise<Record<string, number>>;
  close(): Promise<void>;
}

export type QueueContext<TJobs extends JobMap> = {
  queueService: QueueService<TJobs>;
};

// Structural engine contract: the BullMQ classes satisfy it, and tests can
// substitute an in-memory fake so no Redis is needed.
export interface EngineJob {
  name: string;
  data: unknown;
  id?: string;
  attemptsMade?: number;
}

export interface EngineQueue {
  add(name: string, data: unknown, options?: Record<string, unknown>): Promise<{ id?: string }>;
  addBulk(jobs: { name: string; data: unknown; opts?: Record<string, unknown> }[]): Promise<{ id?: string }[]>;
  upsertJobScheduler(
    schedulerId: string,
    repeat: Record<string, unknown>,
    template?: Record<string, unknown>,
  ): Promise<unknown>;
  removeJobScheduler(schedulerId: string): Promise<unknown>;
  getJobCounts(...types: string[]): Promise<Record<string, number>>;
  close(): Promise<void>;
}

export interface EngineWorker {
  on(event: string, listener: (...args: never[]) => void): unknown;
  close(): Promise<void>;
}

export interface QueueEngine {
  createQueue(name: string, options: Record<string, unknown>): EngineQueue;
  createWorker(
    name: string,
    processor: (job: EngineJob) => Promise<unknown>,
    options: Record<string, unknown>,
  ): EngineWorker;
}

export interface QueueModuleOptions {
  name?: string;
  connection?: Record<string, unknown>;
  prefix?: string;
  defaultJobOptions?: JobOptions;
  engine?: QueueEngine;
}
