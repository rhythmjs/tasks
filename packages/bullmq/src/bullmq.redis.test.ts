// Live integration against a real Redis. Skips itself unless REDIS_URL is
// set, e.g.:  docker run --rm -p 6379:6379 redis  &&  REDIS_URL=redis://localhost:6379 bun test
//
// This file sorts before bullmq.test.ts on purpose: it must import the real
// bullmq module before that file replaces it with mock.module().
import { afterAll, describe, expect, test } from "bun:test";
import { createQueueService } from "./bullmq";

interface Jobs {
  "email.send": { to: string };
  "order.process": { orderId: string };
}

const url = process.env.REDIS_URL;
const live = describe.skipIf(url === undefined);

function parseConnection(redisUrl: string): { host: string; port: number } {
  const parsed = new URL(redisUrl);
  return { host: parsed.hostname, port: Number(parsed.port || 6379) };
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function until(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("condition never became true");
    await sleep(25);
  }
}

const services: { close(): Promise<void> }[] = [];
afterAll(async () => {
  for (const service of services) await service.close();
});

live("bullmq live", () => {
  const options = () => ({
    name: `rhythm-test-${crypto.randomUUID().slice(0, 8)}`,
    connection: parseConnection(url!),
  });

  test("add → process → completed roundtrip with retries", async () => {
    const service = createQueueService<Jobs>(options());
    services.push(service);
    const sent: string[] = [];
    const completions: string[] = [];
    let flaky = 0;

    await service.add("email.send", { to: "ada@example.com" });
    await service.add("order.process", { orderId: "o-1" }, { attempts: 2, backoff: { type: "fixed", delay: 50 } });

    service.process(
      {
        "email.send": (payload) => void sent.push(payload.to),
        "order.process": () => {
          if (flaky++ === 0) throw new Error("first attempt fails");
        },
      },
      { onCompleted: (name) => void completions.push(name) },
    );

    await until(() => sent.length === 1 && flaky === 2);
    expect(sent).toEqual(["ada@example.com"]);
    expect(completions.sort()).toEqual(["email.send", "order.process"]);
  });

  test("job schedulers register and remove", async () => {
    const service = createQueueService<Jobs>(options());
    services.push(service);

    await service.schedule("order.process", { pattern: "0 3 * * *", tz: "UTC" }, { orderId: "nightly" });
    const schedulers = await service.queue.getJobSchedulers();
    expect(schedulers.map((s) => s.key ?? s.name)).toContain("order.process");

    await service.unschedule("order.process");
    expect(await service.queue.getJobSchedulers()).toHaveLength(0);
  });

  test("counts reflect queue state", async () => {
    const service = createQueueService<Jobs>(options());
    services.push(service);

    await service.add("email.send", { to: "x" }, { delay: 60_000 });
    const counts = await service.counts();
    expect(counts.delayed).toBe(1);
  });
});
