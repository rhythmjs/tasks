import { Rhythm } from "@rhythmjs/rhythm";
import { eventsModule, type EventsContext } from "@rhythmjs/events";

interface AppEvents {
  "order.created": { orderId: string; total: number };
  "order.shipped": { orderId: string };
  "user.registered": { userId: string };
}

const app = new Rhythm().register(
  eventsModule.forRoot<AppEvents>({
    onError: (error, event) => console.error(`[onError] listener failed for "${event}":`, (error as Error).message),
  }),
  ({ eventBus }) => ({ eventBus }),
);

const orderModule = new Rhythm<EventsContext<AppEvents>>().use(async (ctx, next) => {
  ctx.eventBus.emit("order.created", { orderId: "o-1", total: 99 });
  await next();
});
app.register(orderModule);

await app.setup();
const { eventBus } = await app.run({});

const controller = new AbortController();
eventBus.on("order.*", (payload, event) => console.log(`[order.*] ${event}`, payload), {
  signal: controller.signal,
});
eventBus.on("**", (_payload, event) => console.log(`[audit] ${event}`));

const firstUser = eventBus.once("user.registered");

eventBus.emit("order.shipped", { orderId: "o-1" });
eventBus.emit("user.registered", { userId: "u-1" });
console.log("[once] first user:", await firstUser);

controller.abort();
eventBus.emit("order.shipped", { orderId: "o-2" });

eventBus.on("order.created", () => {
  throw new Error("projection out of date");
});
eventBus.emit("order.created", { orderId: "o-3", total: 12 });

try {
  await eventBus.emitAsync("order.created", { orderId: "o-4", total: 7 });
} catch (error) {
  const aggregate = error as AggregateError;
  console.log(`[emitAsync] ${aggregate.message}`);
}

await app.teardown();
