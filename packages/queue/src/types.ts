export type JobMap = object;

export interface BackoffOptions {
  type: "fixed" | "exponential";
  delay: number;
}

export interface JobOptions {
  /** Milliseconds to wait before the job becomes ready. */
  delay?: number;
  /** Total attempts including the first (default 1). */
  attempts?: number;
  /** Higher runs sooner (default 0). */
  priority?: number;
  /** Retry backoff: a fixed delay in ms, or fixed/exponential with a base delay. */
  backoff?: number | BackoffOptions;
  /** Custom id; pending jobs with the same id are deduplicated. */
  jobId?: string;
}

export interface RepeatOptions {
  /** Cron pattern (5 or 6 fields), evaluated by @rhythmjs/schedule's engine. */
  pattern?: string;
  /** Fixed interval in milliseconds. */
  every?: number;
  /** IANA timezone for `pattern`. */
  timezone?: string;
  /** Stop after this many runs. */
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
  /** Parallel job slots in this worker (default 1). */
  concurrency?: number;
  /** How often an idle worker polls the engine, in ms (default 20). */
  pollInterval?: number;
  onCompleted?: (name: string, jobId: string) => void;
  /** Fires once per job, after its attempts are exhausted. */
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

// ---------------------------------------------------------------------------
// Engine contract: how the service stores and pulls work. Two in-house
// implementations ship — memoryEngine (default) and redisEngine (Bun.redis) —
// and tests can substitute their own.

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
  /** Runs left; undefined means unlimited. */
  remaining?: number;
  nextAt: number;
}

export interface QueueEngine {
  add(job: StoredJob): Promise<void>;
  addBulk(jobs: StoredJob[]): Promise<void>;
  /** The next ready job, or null. The engine promotes due delayed jobs itself. */
  take(now: number): Promise<StoredJob | null>;
  /** Put a failed job back for another attempt at `readyAt`. */
  requeue(job: StoredJob, readyAt: number): Promise<void>;
  record(kind: "completed" | "failed"): Promise<void>;
  setRepeat(spec: RepeatSpec): Promise<void>;
  clearRepeat(name: string): Promise<void>;
  /** Atomically claim (remove) every repeat due at `now`; the service re-arms survivors. */
  claimDueRepeats(now: number): Promise<RepeatSpec[]>;
  counts(): Promise<Record<string, number>>;
  close(): Promise<void>;
}

export interface QueueModuleOptions {
  name?: string;
  prefix?: string;
  defaultJobOptions?: JobOptions;
  /** A redis URL or a Bun.RedisClient: selects the Bun-native redis engine. */
  redis?: string | Bun.RedisClient;
  engine?: QueueEngine;
}
