import { describe, expect, test } from "bun:test";
import { Rhythm } from "@rhythmjs/rhythm";
import { createScheduleService, cronJob, cronPatterns, intervalJob, scheduleModule, timeoutJob } from "./schedule";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("job factories", () => {
  test("apply defaults and carry options", () => {
    const basic = cronJob("a", "* * * * *", () => {});
    expect(basic).toMatchObject({ kind: "cron", overlap: "skip", disabled: false });

    const tuned = cronJob("b", cronPatterns.daily, () => {}, {
      timezone: "Europe/Paris",
      overlap: "allow",
      disabled: true,
    });
    expect(tuned).toMatchObject({ schedule: "0 0 * * *", timezone: "Europe/Paris", overlap: "allow", disabled: true });

    expect(intervalJob("c", 30_000, () => {}).kind).toBe("interval");
    expect(timeoutJob("d", 5_000, () => {}).kind).toBe("timeout");
  });
});

describe("createScheduleService", () => {
  test("run executes the handler and records state", async () => {
    let runs = 0;
    const service = createScheduleService(cronJob("tick", "* * * * *", () => void runs++));

    const result = await service.run("tick");

    expect(result).toMatchObject({ name: "tick", ran: true });
    expect(result.durationMs).toBeGreaterThanOrEqual(0);
    expect(runs).toBe(1);
    const state = service.state("tick");
    expect(state.runs).toBe(1);
    expect(state.lastRun).toBeInstanceOf(Date);
    expect(state.lastError).toBeUndefined();
  });

  test("captures errors into the result and state, then clears on success", async () => {
    let fail = true;
    const service = createScheduleService(
      cronJob("flaky", "* * * * *", () => {
        if (fail) throw new Error("boom");
      }),
    );

    const failed = await service.run("flaky");
    expect(failed).toMatchObject({ ran: true, error: "boom" });
    expect(service.state("flaky").lastError).toBe("boom");

    fail = false;
    await service.run("flaky");
    expect(service.state("flaky").lastError).toBeUndefined();
  });

  test("skips disabled jobs and overlapping runs", async () => {
    let concurrent = 0;
    const service = createScheduleService(
      cronJob("off", "* * * * *", () => {}, { disabled: true }),
      cronJob("slow", "* * * * *", async () => {
        concurrent++;
        await sleep(40);
      }),
    );

    expect(await service.run("off")).toEqual({ name: "off", ran: false });

    const first = service.run("slow");
    const second = await service.run("slow");
    expect(second).toEqual({ name: "slow", ran: false });
    await first;
    expect(concurrent).toBe(1);
  });

  test("overlap allow runs concurrently", async () => {
    let concurrent = 0;
    let peak = 0;
    const service = createScheduleService(
      cronJob(
        "par",
        "* * * * *",
        async () => {
          concurrent++;
          peak = Math.max(peak, concurrent);
          await sleep(30);
          concurrent--;
        },
        { overlap: "allow" },
      ),
    );

    await Promise.all([service.run("par"), service.run("par")]);
    expect(peak).toBe(2);
  });

  test("throws for unknown and duplicate names, supports add and remove", async () => {
    const service = createScheduleService(cronJob("a", "* * * * *", () => {}));

    await expect(service.run("nope")).rejects.toThrow('unknown job "nope"');
    expect(() => service.add(cronJob("a", "* * * * *", () => {}))).toThrow('duplicate job "a"');

    service.add(intervalJob("b", 1000, () => {}));
    expect(service.jobs.map((job) => job.name)).toEqual(["a", "b"]);
    service.remove("a");
    expect(service.jobs.map((job) => job.name)).toEqual(["b"]);
  });

  test("nextRun computes the next cron occurrence and null for non-cron jobs", () => {
    const service = createScheduleService(
      cronJob("nightly", "0 3 * * *", () => {}),
      intervalJob("beat", 1000, () => {}),
    );

    const from = new Date("2026-01-10T10:00:00Z");
    const next = service.nextRun("nightly", from)!;
    expect(next.getTime()).toBeGreaterThan(from.getTime());
    expect(next.getMinutes()).toBe(0);
    expect(service.nextRun("beat")).toBeNull();
  });

  test("runDue runs exactly the cron jobs due at the given minute", async () => {
    const ran: string[] = [];
    const service = createScheduleService(
      cronJob("five", "*/5 * * * *", () => void ran.push("five")),
      cronJob("hourly", "0 * * * *", () => void ran.push("hourly")),
      intervalJob("beat", 1000, () => void ran.push("beat")),
    );

    const due = await service.runDue(new Date("2026-01-10T10:05:30Z"));
    expect(due.map((result) => result.name)).toEqual(["five"]);
    expect(ran).toEqual(["five"]);

    expect(await service.runDue(new Date("2026-01-10T10:07:00Z"))).toEqual([]);

    const topOfHour = await service.runDue(new Date("2026-01-10T11:00:10Z"));
    expect(topOfHour.map((result) => result.name).sort()).toEqual(["five", "hourly"]);
  });
});

describe("scheduleModule", () => {
  test("exports the service through register", async () => {
    let runs = 0;
    const app = new Rhythm().register(scheduleModule.forRoot(cronJob("tick", "* * * * *", () => void runs++)), (m) => ({
      scheduleService: m.scheduleService,
    }));

    const ctx = await app.run({});

    await ctx.scheduleService.run("tick");
    expect(runs).toBe(1);
    expect(ctx.scheduleService.state("tick").runs).toBe(1);
  });
});
