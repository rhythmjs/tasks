import { Rhythm } from "@rhythmjs/rhythm";
import { bullmqModule } from "@rhythmjs/bullmq";

interface AppJobs {
  "email.send": { to: string; subject: string };
  "order.process": { orderId: string };
}

const host = process.env.REDIS_HOST ?? "127.0.0.1";
const port = Number(process.env.REDIS_PORT ?? 6379);

try {
  const socket = await Bun.connect({ hostname: host, port, socket: { data() {} } });
  socket.end();
} catch {
  console.error(`No Redis at ${host}:${port}. Start one with: docker run --rm -p 6379:6379 redis`);
  process.exit(1);
}

const app = new Rhythm().register(
  bullmqModule.forRoot<AppJobs>({
    name: "example",
    connection: { host, port },
    defaultJobOptions: { attempts: 2, removeOnComplete: true, removeOnFail: true },
  }),
  ({ queueService }) => ({ queueService }),
);

const { queueService } = await app.run({});

const done = new Set<string>();
queueService.process(
  {
    "email.send": async (payload) => {
      console.log(`[worker] sending mail to ${payload.to}: "${payload.subject}"`);
    },
    "order.process": async (payload, job) => {
      console.log(`[worker] processing order ${payload.orderId} (job ${job.id ?? "?"})`);
    },
  },
  {
    concurrency: 4,
    onCompleted: (name, id) => {
      console.log(`[completed] ${name} #${id ?? "?"}`);
      done.add(`${name}:${id ?? "?"}`);
    },
    onFailed: (name, id, error) => console.error(`[failed] ${name} #${id ?? "?"}:`, error),
  },
);

await queueService.add("email.send", { to: "ada@example.com", subject: "welcome" });
await queueService.add("order.process", { orderId: "o-1" }, { priority: 1 });
await queueService.addBulk([
  { name: "email.send", payload: { to: "grace@example.com", subject: "digest" } },
  { name: "order.process", payload: { orderId: "o-2" } },
]);

await queueService.schedule("order.process", { pattern: "0 3 * * *", tz: "UTC" }, { orderId: "nightly" });
console.log("[schedule] nightly order.process registered");
await queueService.unschedule("order.process");

const deadline = Date.now() + 5000;
while (done.size < 4 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 100));
console.log("[counts]", await queueService.counts());

await queueService.close();
console.log("closed cleanly");
process.exit(0);
