import type { ScheduleService } from "../types";

export type DenoCronFn = (name: string, schedule: string, handler: () => void | Promise<void>) => unknown;

export function registerDenoCron(service: ScheduleService, denoCron?: DenoCronFn): string[] {
  const cron = denoCron ?? (globalThis as { Deno?: { cron?: DenoCronFn } }).Deno?.cron;
  if (cron === undefined) throw new Error("Deno.cron is not available in this runtime!");

  const registered: string[] = [];
  for (const job of service.jobs) {
    if (job.kind !== "cron" || job.disabled) continue;
    cron(job.name, job.schedule as string, async () => {
      await service.run(job.name);
    });
    registered.push(job.name);
  }
  return registered;
}
