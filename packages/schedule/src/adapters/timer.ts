import { Cron } from "croner";
import type { ScheduleService } from "../types";

export interface Scheduler {
  stop(): void;
}

export function startScheduler(service: ScheduleService): Scheduler {
  const crons: Cron[] = [];
  const intervals: ReturnType<typeof setInterval>[] = [];
  const timeouts: ReturnType<typeof setTimeout>[] = [];

  for (const job of service.jobs) {
    if (job.disabled) continue;
    if (job.kind === "cron") {
      crons.push(
        new Cron(
          job.schedule as string,
          { protect: job.overlap === "skip", ...(job.timezone === undefined ? {} : { timezone: job.timezone }) },
          () => void service.run(job.name),
        ),
      );
    } else if (job.kind === "interval") {
      intervals.push(setInterval(() => void service.run(job.name), job.schedule as number));
    } else {
      timeouts.push(setTimeout(() => void service.run(job.name), job.schedule as number));
    }
  }

  return {
    stop() {
      for (const cron of crons) cron.stop();
      for (const interval of intervals) clearInterval(interval);
      for (const timeout of timeouts) clearTimeout(timeout);
    },
  };
}
