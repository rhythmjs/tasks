import type { QueueEngine, RepeatSpec, StoredJob } from "./types";

/** The default engine: in-process, zero dependencies, timers and arrays. */
export function memoryEngine(): QueueEngine {
  const waiting: StoredJob[] = [];
  const delayed: StoredJob[] = [];
  const repeats = new Map<string, RepeatSpec>();
  const pending = new Set<string>();
  let completed = 0;
  let failed = 0;

  const enqueue = (job: StoredJob): void => {
    if (pending.has(job.id)) return;
    pending.add(job.id);
    (job.readyAt > Date.now() ? delayed : waiting).push(job);
  };

  const promote = (now: number): void => {
    for (let i = delayed.length - 1; i >= 0; i--) {
      if (delayed[i]!.readyAt <= now) waiting.push(...delayed.splice(i, 1));
    }
  };

  return {
    add: (job) => {
      enqueue(job);
      return Promise.resolve();
    },
    addBulk: (jobs) => {
      for (const job of jobs) enqueue(job);
      return Promise.resolve();
    },
    take: (now) => {
      promote(now);
      if (waiting.length === 0) return Promise.resolve(null);
      let best = 0;
      for (let i = 1; i < waiting.length; i++) {
        if (waiting[i]!.priority > waiting[best]!.priority) best = i;
      }
      const job = waiting.splice(best, 1)[0]!;
      pending.delete(job.id);
      return Promise.resolve(job);
    },
    requeue: (job, readyAt) => {
      pending.add(job.id);
      delayed.push({ ...job, readyAt });
      return Promise.resolve();
    },
    record: (kind) => {
      if (kind === "completed") completed++;
      else failed++;
      return Promise.resolve();
    },
    setRepeat: (spec) => {
      repeats.set(spec.name, spec);
      return Promise.resolve();
    },
    clearRepeat: (name) => {
      repeats.delete(name);
      return Promise.resolve();
    },
    claimDueRepeats: (now) => {
      const due: RepeatSpec[] = [];
      for (const [name, spec] of repeats) {
        if (spec.nextAt <= now) {
          repeats.delete(name);
          due.push(spec);
        }
      }
      return Promise.resolve(due);
    },
    counts: () =>
      Promise.resolve({
        waiting: waiting.length,
        delayed: delayed.length,
        completed,
        failed,
        repeats: repeats.size,
      }),
    close: () => {
      waiting.length = 0;
      delayed.length = 0;
      repeats.clear();
      pending.clear();
      return Promise.resolve();
    },
  };
}
