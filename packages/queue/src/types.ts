export type JobMap = object;

export interface BackoffOptions {
  type: "fixed" | "exponential";
  delay: number;
}

export interface JobOptions {
  delay?: number;
  attempts?: number;
  priority?: number;
  backoff?: number | BackoffOptions;
  jobId?: string;
}

export interface RepeatOptions {
  pattern?: string;
  every?: number;
  timezone?: string;
  limit?: number;
}

export interface JobInfo {
  id: string;
  name: string;
  attemptsMade: number;
}

export type JobProcessors<TJobs extends JobMap> = {
  [K in keyof TJobs & string]?: (payload: TJobs[K], job: JobInfo) => unknown;
};

export interface ProcessOptions {
  concurrency?: number;
  pollInterval?: number;
  onCompleted?: (name: string, jobId: string) => void;
  onError?: (error: unknown) => void;
  onFailed?: (name: string, jobId: string, error: unknown) => void;
}

export interface QueueWorkerHandle {
  close(): Promise<void>;
}

export type BulkJobInput<TJobs extends JobMap> = {
  [K in keyof TJobs & string]: { name: K; payload: TJobs[K]; options?: JobOptions };
}[keyof TJobs & string];

export interface QueueService<TJobs extends JobMap> {
  add<K extends keyof TJobs & string>(name: K, payload: TJobs[K], options?: JobOptions): Promise<string>;
  addBulk(jobs: BulkJobInput<TJobs>[]): Promise<string[]>;
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

export interface StoredJob {
  id: string;
  name: string;
  payload: unknown;
  priority: number;
  attemptsMade: number;
  maxAttempts: number;
  backoffType: "fixed" | "exponential";
  backoffDelay: number;
  readyAt: number;
}

export interface RepeatSpec {
  name: string;
  payload: unknown;
  options?: JobOptions;
  every?: number;
  pattern?: string;
  timezone?: string;
  remaining?: number;
  nextAt: number;
}

export interface QueueEngine {
  add(job: StoredJob): Promise<void>;
  addBulk(jobs: StoredJob[]): Promise<void>;
  take(now: number): Promise<StoredJob | null>;
  requeue(job: StoredJob, readyAt: number): Promise<void>;
  record(kind: "completed" | "failed"): Promise<void>;
  setRepeat(spec: RepeatSpec): Promise<void>;
  clearRepeat(name: string): Promise<void>;
  claimDueRepeats(now: number): Promise<RepeatSpec[]>;
  counts(): Promise<Record<string, number>>;
  close(): Promise<void>;
}

export interface QueueModuleOptions {
  name?: string;
  prefix?: string;
  defaultJobOptions?: JobOptions;
  redis?: string | Bun.RedisClient;
  engine?: QueueEngine;
}
