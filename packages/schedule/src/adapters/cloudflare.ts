import type { JobRunResult, ScheduleService } from "../types";

export interface ScheduledController {
  cron: string;
  scheduledTime?: number;
}

export type ScheduledHandler = (controller: ScheduledController) => Promise<JobRunResult[]>;

export function toScheduledHandler(service: ScheduleService): ScheduledHandler {
  return async (controller) => {
    const results: JobRunResult[] = [];
    for (const job of service.jobs) {
      if (job.kind !== "cron" || job.disabled) continue;
      if (job.schedule === controller.cron) results.push(await service.run(job.name));
    }
    if (results.length > 0) return results;
    return service.runDue(controller.scheduledTime === undefined ? new Date() : new Date(controller.scheduledTime));
  };
}
