import { RhythmRouter, type RhythmRouterContext } from "@rhythmjs/router";
import type { RhythmHttpContext } from "@rhythmjs/router/adapters/context";
import type { ScheduleService } from "../types";

export interface CronRoutesOptions {
  path?: string;
  secret?: string;
}

export function cronRoutes(service: ScheduleService, options: CronRoutesOptions = {}): RhythmRouter {
  const router = new RhythmRouter({ prefix: options.path ?? "/cron" });

  if (options.secret !== undefined) {
    const expected = `Bearer ${options.secret}`;
    router.use(async (ctx, next) => {
      if (ctx.request.headers.get("authorization") !== expected) {
        ctx.json({ success: false, status: 401, message: "Unauthorized" }, 401);
        return;
      }
      await next();
    });
  }

  const list = (ctx: RhythmHttpContext): void => {
    ctx.json(
      service.jobs.map((job) => ({
        name: job.name,
        kind: job.kind,
        schedule: job.schedule,
        disabled: job.disabled,
        state: service.state(job.name),
      })),
    );
  };

  const runDue = async (ctx: RhythmHttpContext): Promise<void> => {
    ctx.json(await service.runDue());
  };

  const runOne = async (ctx: RhythmHttpContext & Partial<RhythmRouterContext>): Promise<void> => {
    const name = ctx.params?.name ?? "";
    try {
      ctx.json(await service.run(name));
    } catch {
      ctx.json({ success: false, status: 404, message: `unknown job "${name}"` }, 404);
    }
  };

  return router
    .get("/", list)
    .get("/due", runDue)
    .post("/due", runDue)
    .get("/jobs/:name", runOne)
    .post("/jobs/:name", runOne);
}
