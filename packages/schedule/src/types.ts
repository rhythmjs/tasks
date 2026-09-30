export type JobHandler = () => void | Promise<void>;

export type JobKind = "cron" | "interval" | "timeout";

export type OverlapPolicy = "skip" | "allow";

export interface JobOptions {
  timezone?: string;
  overlap?: OverlapPolicy;
  disabled?: boolean;
}

export interface ScheduleJob {
  readonly name: string;
  readonly kind: JobKind;
  readonly schedule: string | number;
  readonly handler: JobHandler;
  readonly timezone?: string;
  readonly overlap: OverlapPolicy;
  readonly disabled: boolean;
}

export interface JobState {
  running: boolean;
  runs: number;
  lastRun?: Date;
  lastError?: string;
  nextRun?: Date;
}

export interface JobRunResult {
  name: string;
  ran: boolean;
  durationMs?: number;
  error?: string;
}

export interface ScheduleService {
  readonly jobs: readonly ScheduleJob[];
  add(job: ScheduleJob): void;
  remove(name: string): void;
  run(name: string): Promise<JobRunResult>;
  runDue(date?: Date): Promise<JobRunResult[]>;
  nextRun(name: string, from?: Date): Date | null;
  state(name: string): JobState;
}

export type ScheduleContext = {
  scheduleService: ScheduleService;
};
