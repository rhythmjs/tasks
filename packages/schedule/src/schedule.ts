import { Rhythm } from "@rhythmjs/rhythm";
import { Cron } from "./cron";
import type {
  JobHandler,
  JobOptions,
  JobRunResult,
  JobState,
  ScheduleContext,
  ScheduleJob,
  ScheduleService,
} from "./types";

export type {
  JobHandler,
  JobKind,
  JobOptions,
  JobRunResult,
  JobState,
  OverlapPolicy,
  ScheduleContext,
  ScheduleJob,
  ScheduleService,
} from "./types";

export const cronPatterns = {
  everySecond: "* * * * * *",
  everyMinute: "* * * * *",
  every5Minutes: "*/5 * * * *",
  every10Minutes: "*/10 * * * *",
  every30Minutes: "*/30 * * * *",
  hourly: "0 * * * *",
  daily: "0 0 * * *",
  weekly: "0 0 * * 0",
  monthly: "0 0 1 * *",
  yearly: "0 0 1 1 *",
} as const;

function job(
  kind: ScheduleJob["kind"],
  name: string,
  schedule: string | number,
  handler: JobHandler,
  options: JobOptions,
): ScheduleJob {
  if (typeof handler !== "function") throw new TypeError("job handler must be a function!");
  return {
    name,
    kind,
    schedule,
    handler,
    ...(options.timezone === undefined ? {} : { timezone: options.timezone }),
    overlap: options.overlap ?? "skip",
    disabled: options.disabled ?? false,
  };
}

export function cronJob(name: string, schedule: string, handler: JobHandler, options: JobOptions = {}): ScheduleJob {
  return job("cron", name, schedule, handler, options);
}

export function intervalJob(name: string, ms: number, handler: JobHandler, options: JobOptions = {}): ScheduleJob {
  return job("interval", name, ms, handler, options);
}

export function timeoutJob(name: string, ms: number, handler: JobHandler, options: JobOptions = {}): ScheduleJob {
  return job("timeout", name, ms, handler, options);
}

function cronOf(scheduleJob: ScheduleJob): Cron {
  return new Cron(
    scheduleJob.schedule as string,
    scheduleJob.timezone === undefined ? {} : { timezone: scheduleJob.timezone },
  );
}

export function createScheduleService(...jobs: ScheduleJob[]): ScheduleService {
  const byName = new Map<string, ScheduleJob>();
  const states = new Map<string, JobState>();

  const register = (scheduleJob: ScheduleJob): void => {
    if (byName.has(scheduleJob.name)) throw new Error(`duplicate job "${scheduleJob.name}"`);
    byName.set(scheduleJob.name, scheduleJob);
    states.set(scheduleJob.name, { running: false, runs: 0 });
  };
  for (const scheduleJob of jobs) register(scheduleJob);

  const jobOrThrow = (name: string): ScheduleJob => {
    const scheduleJob = byName.get(name);
    if (scheduleJob === undefined) throw new Error(`unknown job "${name}"`);
    return scheduleJob;
  };

  const service: ScheduleService = {
    get jobs() {
      return [...byName.values()];
    },
    add: (scheduleJob) => register(scheduleJob),
    remove: (name) => {
      jobOrThrow(name);
      byName.delete(name);
      states.delete(name);
    },
    run: async (name) => {
      const scheduleJob = jobOrThrow(name);
      const state = states.get(name)!;
      if (scheduleJob.disabled) return { name, ran: false };
      if (state.running && scheduleJob.overlap === "skip") return { name, ran: false };

      state.running = true;
      const start = performance.now();
      try {
        await scheduleJob.handler();
        state.lastRun = new Date();
        state.runs += 1;
        delete state.lastError;
        return { name, ran: true, durationMs: Math.round(performance.now() - start) };
      } catch (error) {
        state.lastRun = new Date();
        state.runs += 1;
        state.lastError = error instanceof Error ? error.message : String(error);
        return { name, ran: true, durationMs: Math.round(performance.now() - start), error: state.lastError };
      } finally {
        state.running = false;
      }
    },
    runDue: async (date = new Date()) => {
      const minuteStart = new Date(Math.floor(date.getTime() / 60_000) * 60_000);
      const results: JobRunResult[] = [];
      for (const scheduleJob of byName.values()) {
        if (scheduleJob.kind !== "cron" || scheduleJob.disabled) continue;
        const next = cronOf(scheduleJob).nextRun(new Date(minuteStart.getTime() - 1));
        if (next !== null && next.getTime() >= minuteStart.getTime() && next.getTime() <= date.getTime()) {
          results.push(await service.run(scheduleJob.name));
        }
      }
      return results;
    },
    nextRun: (name, from = new Date()) => {
      const scheduleJob = jobOrThrow(name);
      if (scheduleJob.kind !== "cron") return null;
      return cronOf(scheduleJob).nextRun(from);
    },
    state: (name) => {
      jobOrThrow(name);
      const state = states.get(name)!;
      const next = byName.get(name)!.kind === "cron" ? service.nextRun(name) : null;
      return { ...state, ...(next === null ? {} : { nextRun: next }) };
    },
  };

  return service;
}

export const scheduleModule = {
  forRoot(...jobs: ScheduleJob[]) {
    const module = new Rhythm<{}, ScheduleContext>({ type: "module", name: "schedule" });
    module.context.scheduleService = createScheduleService(...jobs);
    return module;
  },
};
