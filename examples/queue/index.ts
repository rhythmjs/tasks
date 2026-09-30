import { Rhythm } from "@rhythmjs/rhythm";
import { queueModule } from "@rhythmjs/queue";

interface AppJobs {
  "email.send": { to: string; subject: string };
  "order.process": { orderId: string };
}

const app = new Rhythm().register(
  queueModule.forRoot<AppJobs>({
    name: "example",
    defaultJobOptions: { attempts: 2, backoff: 100 },
    ...(process.env.REDIS_URL === undefined ? {} : { redis: process.env.REDIS_URL }),
  }),
  ({ queueService }) => ({ queueService }),
);

await app.setup();
const { queueService } = await app.run({});

const done = new Set<string>();
queueService.process(
  {
    "email.send": async (payload) => {
      console.log(`[worker] sending mail to ${payload.to}: "${payload.subject}"`);
    },
    "order.process": async (payload, job) => {
      console.log(`[worker] processing order ${payload.orderId} (job ${job.id}, attempt ${job.attemptsMade})`);
    },
  },
  {
    concurrency: 4,
    onCompleted: (name, id) => {
      console.log(`[completed] ${name} #${id.slice(0, 8)}`);
      done.add(`${name}:${id}`);
    },
    onFailed: (name, id, error) => console.error(`[failed] ${name} #${id.slice(0, 8)}:`, error),
  },
);

await queueService.add("email.send", { to: "ada@example.com", subject: "welcome" });
await queueService.add("order.process", { orderId: "o-1" }, { priority: 1 });
await queueService.addBulk([
  { name: "email.send", payload: { to: "grace@example.com", subject: "digest" } },
  { name: "order.process", payload: { orderId: "o-2" } },
]);

await queueService.schedule("order.process", { pattern: "0 3 * * *", timezone: "UTC" }, { orderId: "nightly" });
console.log("[schedule] nightly order.process registered");
await queueService.unschedule("order.process");

const deadline = Date.now() + 5000;
while (done.size < 4 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 50));
console.log("[counts]", await queueService.counts());

await app.teardown();
console.log("closed cleanly");
process.exit(0);
