import { Cron } from "./cron";
import type { ScheduleService } from "./types";

export interface Scheduler {
  stop(): void;
}

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
              service.run(name).then(
                () => arm(name, cron),
                () => {},
              );
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
      const interval: ReturnType<typeof setInterval> = setInterval(
        () => service.run(job.name).catch(() => clearInterval(interval)),
        job.schedule as number,
      );
      intervals.push(interval);
    } else {
      const timer = setTimeout(() => {
        timeouts.delete(timer);
        service.run(job.name).catch(() => {});
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
