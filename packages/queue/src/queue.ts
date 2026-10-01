import { Rhythm } from "@rhythmjs/rhythm";
import { Cron } from "@rhythmjs/schedule/cron";
import { memoryEngine } from "./memory";
import { redisEngine } from "./redis";
import type {
  JobInfo,
  JobMap,
  JobOptions,
  JobProcessors,
  ProcessOptions,
  QueueModuleOptions,
  QueueService,
  QueueWorkerHandle,
  RepeatOptions,
  RepeatSpec,
  StoredJob,
} from "./types";

export type {
  BackoffOptions,
  BulkJobInput,
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
  RepeatSpec,
  StoredJob,
} from "./types";
export { memoryEngine } from "./memory";
export { redisEngine, type RedisEngineOptions, type RedisLike } from "./redis";

function toStored(name: string, payload: unknown, options: JobOptions = {}): StoredJob {
  const backoff = options.backoff ?? 0;
  return {
    id: options.jobId ?? crypto.randomUUID(),
    name,
    payload,
    priority: options.priority ?? 0,
    attemptsMade: 0,
    maxAttempts: Math.max(options.attempts ?? 1, 1),
    backoffType: typeof backoff === "number" ? "fixed" : backoff.type,
    backoffDelay: typeof backoff === "number" ? backoff : backoff.delay,
    readyAt: Date.now() + (options.delay ?? 0),
  };
}

function retryDelay(job: StoredJob): number {
  if (job.backoffType === "exponential") return job.backoffDelay * 2 ** (job.attemptsMade - 1);
  return job.backoffDelay;
}

function nextOccurrence(spec: Pick<RepeatSpec, "every" | "pattern" | "timezone">, from: number): number | null {
  if (spec.every !== undefined) return from + spec.every;
  if (spec.pattern !== undefined) {
    const next = new Cron(spec.pattern, spec.timezone === undefined ? {} : { timezone: spec.timezone }).nextRun(
      new Date(from),
    );
    return next === null ? null : next.getTime();
  }
  return null;
}

export function createQueueService<TJobs extends JobMap>(options: QueueModuleOptions = {}): QueueService<TJobs> {
  const engine =
    options.engine ??
    (options.redis !== undefined
      ? redisEngine(options.redis, { prefix: `${options.prefix ?? "rhythm"}:${options.name ?? "queue"}` })
      : memoryEngine());
  const defaults = options.defaultJobOptions;
  const workers: QueueWorkerHandle[] = [];
  let active = 0;

  const service: QueueService<TJobs> = {
    add: async (name, payload, jobOptions) => {
      const job = toStored(name, payload, { ...defaults, ...jobOptions });
      await engine.add(job);
      return job.id;
    },
    addBulk: async (jobs) => {
      const stored = jobs.map((job) => toStored(job.name, job.payload, { ...defaults, ...job.options }));
      await engine.addBulk(stored);
      return stored.map((job) => job.id);
    },
    schedule: async (name, repeat: RepeatOptions, payload, jobOptions) => {
      const nextAt = nextOccurrence(repeat, Date.now());
      if (nextAt === null) throw new Error(`repeat for "${name}" needs \`every\` or a cron \`pattern\``);
      await engine.setRepeat({
        name,
        payload,
        ...(jobOptions === undefined ? {} : { options: jobOptions }),
        ...(repeat.every === undefined ? {} : { every: repeat.every }),
        ...(repeat.pattern === undefined ? {} : { pattern: repeat.pattern }),
        ...(repeat.timezone === undefined ? {} : { timezone: repeat.timezone }),
        ...(repeat.limit === undefined ? {} : { remaining: repeat.limit }),
        nextAt,
      });
    },
    unschedule: (name) => engine.clearRepeat(name),
    process: (processors: JobProcessors<TJobs>, processOptions: ProcessOptions = {}) => {
      const concurrency = Math.max(processOptions.concurrency ?? 1, 1);
      const pollInterval = processOptions.pollInterval ?? 20;
      let stopped = false;

      const dispatch = async (job: StoredJob): Promise<unknown> => {
        const handler = Object.hasOwn(processors, job.name)
          ? (processors as Record<string, (payload: unknown, job: JobInfo) => unknown>)[job.name]
          : undefined;
        if (handler === undefined) throw new Error(`no processor registered for job "${job.name}"`);
        return handler(job.payload, { id: job.id, name: job.name, attemptsMade: job.attemptsMade });
      };

      const armRepeats = async (): Promise<void> => {
        const now = Date.now();
        for (const spec of await engine.claimDueRepeats(now)) {
          await engine.add(toStored(spec.name, spec.payload, { ...defaults, ...spec.options }));
          const remaining = spec.remaining === undefined ? undefined : spec.remaining - 1;
          if (remaining !== undefined && remaining <= 0) continue;
          const nextAt = nextOccurrence(spec, now);
          if (nextAt === null) continue;
          await engine.setRepeat({ ...spec, ...(remaining === undefined ? {} : { remaining }), nextAt });
        }
      };

      const loop = async (): Promise<void> => {
        while (!stopped) {
          try {
            await armRepeats();
            const job = await engine.take(Date.now());
            if (job === null) {
              await Bun.sleep(pollInterval);
              continue;
            }
            active++;
            job.attemptsMade++;
            try {
              await dispatch(job);
              await engine.record("completed");
              processOptions.onCompleted?.(job.name, job.id);
            } catch (error) {
              if (job.attemptsMade < job.maxAttempts) {
                await engine.requeue(job, Date.now() + retryDelay(job));
              } else {
                await engine.record("failed");
                processOptions.onFailed?.(job.name, job.id, error);
              }
            } finally {
              active--;
            }
          } catch (error) {
            try {
              processOptions.onError?.(error);
            } catch {}
            await Bun.sleep(pollInterval);
          }
        }
      };

      const loops = Array.from({ length: concurrency }, () => loop());
      const handle: QueueWorkerHandle = {
        close: async () => {
          stopped = true;
          await Promise.all(loops);
          const index = workers.indexOf(handle);
          if (index !== -1) workers.splice(index, 1);
        },
      };
      workers.push(handle);
      return handle;
    },
    counts: async () => ({ ...(await engine.counts()), active }),
    close: async () => {
      await Promise.all(workers.splice(0).map((worker) => worker.close()));
      await engine.close();
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
