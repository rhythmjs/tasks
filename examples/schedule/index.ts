import { Rhythm } from "@rhythmjs/rhythm";
import { cronJob, cronPatterns, intervalJob, scheduleModule, timeoutJob } from "@rhythmjs/schedule";
import { startScheduler } from "@rhythmjs/schedule/scheduler";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const app = new Rhythm().register(
  scheduleModule.forRoot(
    cronJob("tick", cronPatterns.everySecond, () => console.log("[cron] tick")),
    intervalJob("heartbeat", 700, () => console.log("[interval] heartbeat")),
    timeoutJob("warmup", 300, () => console.log("[timeout] warmed up (runs once)")),
    cronJob("flaky", cronPatterns.everySecond, () => {
      throw new Error("db unreachable");
    }),
  ),
  ({ scheduleService }) => ({ scheduleService }),
);

const { scheduleService } = await app.run({});

await scheduleService.run("warmup");

console.log("[nextRun] tick fires next at", scheduleService.nextRun("tick")?.toISOString());
console.log(
  "[runDue] due this minute:",
  (await scheduleService.runDue()).map((result) => result.name),
);

const scheduler = startScheduler(scheduleService);
await sleep(3500);
scheduler.stop();

for (const job of scheduleService.jobs) {
  const state = scheduleService.state(job.name);
  console.log(
    `[state] ${job.name}: runs=${state.runs} lastError=${state.lastError ?? "none"} nextRun=${state.nextRun?.toISOString() ?? "-"}`,
  );
}

console.log("stopped cleanly");
