import { describe, expect, test } from "bun:test";
import { Rhythm } from "@rhythmjs/rhythm";
import { toFetchHandler } from "@rhythmjs/router/fetch";
import type { RhythmHttpContext } from "@rhythmjs/router/adapters/context";
import { createScheduleService, cronJob, intervalJob, timeoutJob } from "../schedule";
import { startScheduler } from "./timer";
import { toScheduledHandler } from "./cloudflare";
import { registerDenoCron, type DenoCronFn } from "./deno";
import { cronRoutes } from "./http";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("timer adapter", () => {
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

  test("drives cron jobs through croner", async () => {
    let ticks = 0;
    const service = createScheduleService(cronJob("everysec", "* * * * * *", () => void ticks++));

    const scheduler = startScheduler(service);
    await sleep(1200);
    scheduler.stop();

    expect(ticks).toBeGreaterThanOrEqual(1);
    expect(service.state("everysec").runs).toBe(ticks);
  });
});

describe("cloudflare adapter", () => {
  test("dispatches jobs whose expression matches event.cron exactly", async () => {
    const ran: string[] = [];
    const service = createScheduleService(
      cronJob("a", "*/5 * * * *", () => void ran.push("a")),
      cronJob("b", "0 3 * * *", () => void ran.push("b")),
    );

    const results = await toScheduledHandler(service)({ cron: "*/5 * * * *" });

    expect(results.map((result) => result.name)).toEqual(["a"]);
    expect(ran).toEqual(["a"]);
  });

  test("falls back to runDue when no expression matches exactly", async () => {
    const ran: string[] = [];
    const service = createScheduleService(cronJob("five", "*/5 * * * *", () => void ran.push("five")));

    const results = await toScheduledHandler(service)({
      cron: "5 * * * *",
      scheduledTime: new Date("2026-01-10T10:05:00Z").getTime(),
    });

    expect(results.map((result) => result.name)).toEqual(["five"]);
  });
});

describe("deno adapter", () => {
  test("registers enabled cron jobs and wires handlers to the service", async () => {
    const registrations: { name: string; schedule: string; handler: () => void | Promise<void> }[] = [];
    const denoCron: DenoCronFn = (name, schedule, handler) => void registrations.push({ name, schedule, handler });

    let ran = 0;
    const service = createScheduleService(
      cronJob("sync", "*/10 * * * *", () => void ran++),
      cronJob("off", "* * * * *", () => {}, { disabled: true }),
      intervalJob("beat", 1000, () => {}),
    );

    const registered = registerDenoCron(service, denoCron);

    expect(registered).toEqual(["sync"]);
    expect(registrations.map((r) => ({ name: r.name, schedule: r.schedule }))).toEqual([
      { name: "sync", schedule: "*/10 * * * *" },
    ]);

    await registrations[0]!.handler();
    expect(ran).toBe(1);
  });

  test("throws when Deno.cron is unavailable", () => {
    expect(() => registerDenoCron(createScheduleService())).toThrow("Deno.cron is not available");
  });
});

describe("http adapter", () => {
  const serve = (service = createScheduleService(cronJob("tick", "*/5 * * * *", () => {})), secret?: string) =>
    toFetchHandler(
      new Rhythm<RhythmHttpContext>().use(cronRoutes(service, secret === undefined ? {} : { secret }).middleware()),
    );

  test("lists jobs with their state", async () => {
    const handler = serve();

    const res = await handler(new Request("http://localhost/cron/"));
    const body = (await res.json()) as { name: string; kind: string; state: { runs: number } }[];

    expect(res.status).toBe(200);
    expect(body).toHaveLength(1);
    expect(body[0]).toMatchObject({ name: "tick", kind: "cron", state: { runs: 0 } });
  });

  test("runs a single job by name and 404s unknown names", async () => {
    let ran = 0;
    const service = createScheduleService(cronJob("tick", "*/5 * * * *", () => void ran++));
    const handler = serve(service);

    const ok = await handler(new Request("http://localhost/cron/jobs/tick", { method: "POST" }));
    expect((await ok.json()) as object).toMatchObject({ name: "tick", ran: true });
    expect(ran).toBe(1);

    const missing = await handler(new Request("http://localhost/cron/jobs/nope", { method: "POST" }));
    expect(missing.status).toBe(404);
  });

  test("run-due endpoint evaluates due jobs", async () => {
    const handler = serve();

    const res = await handler(new Request("http://localhost/cron/due"));
    expect(res.status).toBe(200);
    expect(Array.isArray(await res.json())).toBe(true);
  });

  test("guards every route with the bearer secret", async () => {
    const handler = serve(undefined, "s3cret");

    const denied = await handler(new Request("http://localhost/cron/"));
    expect(denied.status).toBe(401);

    const wrong = await handler(new Request("http://localhost/cron/", { headers: { authorization: "Bearer nope" } }));
    expect(wrong.status).toBe(401);

    const allowed = await handler(
      new Request("http://localhost/cron/", { headers: { authorization: "Bearer s3cret" } }),
    );
    expect(allowed.status).toBe(200);
  });
});
