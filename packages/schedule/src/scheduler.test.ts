import { describe, expect, test } from "bun:test";
import { createScheduleService, cronJob, intervalJob, timeoutJob } from "./schedule";
import { startScheduler } from "./scheduler";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("startScheduler", () => {
  test("drives interval and timeout jobs, and stop halts them", async () => {
    let beats = 0;
    let warmed = 0;
    const service = createScheduleService(
      intervalJob("beat", 25, () => void beats++),
      timeoutJob("warmup", 20, () => void warmed++),
      intervalJob(
        "off",
        10,
        () => {
          throw new Error("should not run");
        },
        { disabled: true },
      ),
    );

    const scheduler = startScheduler(service);
    await sleep(90);
    scheduler.stop();

    expect(beats).toBeGreaterThanOrEqual(2);
    expect(warmed).toBe(1);
    expect(service.state("beat").lastError).toBeUndefined();

    const frozen = beats;
    await sleep(60);
    expect(beats).toBe(frozen);
  });

  test("drives cron jobs with the in-house cron engine", async () => {
    let ticks = 0;
    const service = createScheduleService(cronJob("everysec", "* * * * * *", () => void ticks++));

    const scheduler = startScheduler(service);
    await sleep(1200);
    scheduler.stop();

    expect(ticks).toBeGreaterThanOrEqual(1);
    expect(service.state("everysec").runs).toBe(ticks);
  });

  test("removing an armed cron job stops it firing, with no unhandled rejections", async () => {
    let ticks = 0;
    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => void rejections.push(reason);
    process.on("unhandledRejection", onRejection);
    const service = createScheduleService(cronJob("doomed", "* * * * * *", () => void ticks++));

    const scheduler = startScheduler(service);
    service.remove("doomed");
    try {
      await sleep(2200);
    } finally {
      scheduler.stop();
      process.off("unhandledRejection", onRejection);
    }

    expect(ticks).toBe(0);
    expect(rejections).toHaveLength(0);
  });

  test("removing an interval job clears its timer instead of rejecting every tick", async () => {
    let ticks = 0;
    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => void rejections.push(reason);
    process.on("unhandledRejection", onRejection);
    const service = createScheduleService(
      intervalJob("doomed", 20, () => void ticks++),
      timeoutJob("gone", 20, () => void ticks++),
    );

    const scheduler = startScheduler(service);
    service.remove("doomed");
    service.remove("gone");
    try {
      await sleep(120);
    } finally {
      scheduler.stop();
      process.off("unhandledRejection", onRejection);
    }

    expect(ticks).toBe(0);
    expect(rejections).toHaveLength(0);
  });

  test("stop() before a cron fire prevents the run and the re-arm", async () => {
    let ticks = 0;
    const service = createScheduleService(cronJob("everysec", "* * * * * *", () => void ticks++));

    const scheduler = startScheduler(service);
    scheduler.stop();
    await sleep(1100);

    expect(ticks).toBe(0);
  });
});
