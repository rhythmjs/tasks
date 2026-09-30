import { Cron } from "./cron";
import type { ScheduleService } from "./types";

export interface Scheduler {
  stop(): void;
}

// setTimeout delays above 2^31 - 1 ms overflow, so far-future cron runs
// re-arm in chunks instead of firing early.
const MAX_DELAY = 2_147_483_647;

export function startScheduler(service: ScheduleService): Scheduler {
  const timeouts = new Set<ReturnType<typeof setTimeout>>();
  const intervals: ReturnType<typeof setInterval>[] = [];
  let stopped = false;

  const arm = (name: string, cron: Cron): void => {
    if (stopped) return;
    const next = cron.nextRun();
    if (next === null) return;
    const delay = next.getTime() - Date.now();
    const timer =
      delay > MAX_DELAY
        ? setTimeout(() => {
            timeouts.delete(timer);
            arm(name, cron);
          }, MAX_DELAY)
        : setTimeout(
            () => {
              timeouts.delete(timer);
              void service.run(name).finally(() => arm(name, cron));
            },
            Math.max(delay, 0),
          );
    timeouts.add(timer);
  };

  for (const job of service.jobs) {
    if (job.disabled) continue;
    if (job.kind === "cron") {
      arm(job.name, new Cron(job.schedule as string, job.timezone === undefined ? {} : { timezone: job.timezone }));
    } else if (job.kind === "interval") {
      intervals.push(setInterval(() => void service.run(job.name), job.schedule as number));
    } else {
      const timer = setTimeout(() => {
        timeouts.delete(timer);
        void service.run(job.name);
      }, job.schedule as number);
      timeouts.add(timer);
    }
  }

  return {
    stop() {
      stopped = true;
      for (const timer of timeouts) clearTimeout(timer);
      timeouts.clear();
      for (const interval of intervals) clearInterval(interval);
    },
  };
}
