export interface CronOptions {
  timezone?: string;
}

interface DateParts {
  year: number;
  month: number; // 1-12
  day: number; // 1-31
  hour: number; // 0-23
  minute: number; // 0-59
  second: number; // 0-59
  dow: number; // 0-6, 0 = Sunday
}

interface Clock {
  parts(ts: number): DateParts;
  make(year: number, month: number, day: number, hour: number, minute: number, second: number): number;
}

const localClock: Clock = {
  parts(ts) {
    const d = new Date(ts);
    return {
      year: d.getFullYear(),
      month: d.getMonth() + 1,
      day: d.getDate(),
      hour: d.getHours(),
      minute: d.getMinutes(),
      second: d.getSeconds(),
      dow: d.getDay(),
    };
  },
  make(year, month, day, hour, minute, second) {
    return new Date(year, month - 1, day, hour, minute, second).getTime();
  },
};

const WEEKDAYS: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };

function timezoneClock(timezone: string): Clock {
  let format: Intl.DateTimeFormat;
  try {
    format = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone,
      hourCycle: "h23",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      weekday: "short",
    });
  } catch {
    throw new Error(`unknown timezone "${timezone}"`);
  }

  const parts = (ts: number): DateParts => {
    const fields: Partial<Record<Intl.DateTimeFormatPartTypes, string>> = {};
    for (const part of format.formatToParts(ts)) fields[part.type] = part.value;
    return {
      year: Number(fields.year),
      month: Number(fields.month),
      day: Number(fields.day),
      hour: Number(fields.hour) % 24,
      minute: Number(fields.minute),
      second: Number(fields.second),
      dow: WEEKDAYS[fields.weekday!]!,
    };
  };

  return {
    parts,
    make(year, month, day, hour, minute, second) {
      // Guess the UTC instant, then correct it by however far the guess lands
      // from the requested wall-clock time in the target zone. Two rounds
      // converge everywhere except inside a DST gap, where the requested time
      // does not exist and the caller re-validates via parts().
      const target = Date.UTC(year, month - 1, day, hour, minute, second);
      let ts = target;
      for (let i = 0; i < 3; i++) {
        const p = parts(ts);
        const actual = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second);
        if (actual === target) break;
        ts += target - actual;
      }
      return ts;
    },
  };
}

const MONTH_NAMES: Record<string, number> = {
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  may: 5,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  oct: 10,
  nov: 11,
  dec: 12,
};

const DOW_NAMES: Record<string, number> = { sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6 };

const ALIASES: Record<string, string> = {
  "@yearly": "0 0 1 1 *",
  "@annually": "0 0 1 1 *",
  "@monthly": "0 0 1 * *",
  "@weekly": "0 0 * * 0",
  "@daily": "0 0 * * *",
  "@hourly": "0 * * * *",
};

function parseValue(token: string, field: string, names: Record<string, number>, pattern: string): number {
  const named = names[token.toLowerCase()];
  if (named !== undefined) return named;
  if (!/^\d+$/.test(token)) throw new Error(`invalid ${field} "${token}" in cron pattern "${pattern}"`);
  return Number(token);
}

function parseField(
  expr: string,
  field: string,
  min: number,
  max: number,
  names: Record<string, number>,
  pattern: string,
): boolean[] {
  const flags: boolean[] = Array.from({ length: max + 1 }, () => false);
  const fail = (): never => {
    throw new Error(`invalid ${field} "${expr}" in cron pattern "${pattern}"`);
  };

  for (const part of expr.split(",")) {
    const [rangeExpr, stepExpr, ...rest] = part.split("/");
    if (rangeExpr === undefined || rangeExpr === "" || rest.length > 0 || stepExpr === "") fail();
    const step = stepExpr === undefined ? 1 : parseValue(stepExpr, field, {}, pattern);
    if (step < 1) fail();

    let from: number;
    let to: number;
    if (rangeExpr === "*") {
      from = min;
      to = max;
    } else if (rangeExpr.includes("-")) {
      const [a, b, ...tail] = rangeExpr.split("-");
      if (a === undefined || b === undefined || a === "" || b === "" || tail.length > 0) fail();
      from = parseValue(a!, field, names, pattern);
      to = parseValue(b!, field, names, pattern);
    } else {
      from = parseValue(rangeExpr, field, names, pattern);
      to = stepExpr === undefined ? from : max;
    }

    if (from < min || to > max || from > to) fail();
    for (let value = from; value <= to; value += step) flags[value] = true;
  }
  return flags;
}

export class Cron {
  readonly pattern: string;
  readonly #clock: Clock;
  readonly #seconds: boolean[];
  readonly #minutes: boolean[];
  readonly #hours: boolean[];
  readonly #days: boolean[];
  readonly #months: boolean[];
  readonly #dows: boolean[];
  readonly #dayRestricted: boolean;
  readonly #dowRestricted: boolean;

  constructor(pattern: string, options: CronOptions = {}) {
    this.pattern = pattern;
    const source = ALIASES[pattern.trim().toLowerCase()] ?? pattern.trim();
    const fields = source.split(/\s+/);
    if (fields.length < 5 || fields.length > 6) {
      throw new Error(`cron pattern "${pattern}" must have 5 or 6 fields`);
    }
    const [second, minute, hour, day, month, dow] = fields.length === 6 ? fields : ["0", ...fields];
    this.#seconds = parseField(second!, "second", 0, 59, {}, pattern);
    this.#minutes = parseField(minute!, "minute", 0, 59, {}, pattern);
    this.#hours = parseField(hour!, "hour", 0, 23, {}, pattern);
    this.#days = parseField(day!, "day-of-month", 1, 31, {}, pattern);
    this.#months = parseField(month!, "month", 1, 12, MONTH_NAMES, pattern);
    // Day-of-week 7 is an alias for Sunday.
    const dows = parseField(dow!, "day-of-week", 0, 7, DOW_NAMES, pattern);
    if (dows[7] === true) dows[0] = true;
    this.#dows = dows;
    this.#clock = options.timezone === undefined ? localClock : timezoneClock(options.timezone);
    // Standard cron: when both day fields are restricted, a date matching
    // either one is due; a field is unrestricted when it starts with "*".
    this.#dayRestricted = !day!.startsWith("*");
    this.#dowRestricted = !dow!.startsWith("*");
  }

  #dayMatches(parts: DateParts): boolean {
    const dayHit = this.#days[parts.day] === true;
    const dowHit = this.#dows[parts.dow] === true;
    if (this.#dayRestricted && this.#dowRestricted) return dayHit || dowHit;
    if (this.#dayRestricted) return dayHit;
    if (this.#dowRestricted) return dowHit;
    return true;
  }

  nextRun(from: Date = new Date()): Date | null {
    const clock = this.#clock;
    let ts = Math.floor(from.getTime() / 1000) * 1000 + 1000;
    const limit = from.getTime() + 8 * 366 * 86_400_000;

    while (ts <= limit) {
      const p = clock.parts(ts);
      let jump: number;
      if (this.#months[p.month] !== true) jump = clock.make(p.year, p.month + 1, 1, 0, 0, 0);
      else if (!this.#dayMatches(p)) jump = clock.make(p.year, p.month, p.day + 1, 0, 0, 0);
      else if (this.#hours[p.hour] !== true) jump = clock.make(p.year, p.month, p.day, p.hour + 1, 0, 0);
      else if (this.#minutes[p.minute] !== true) jump = clock.make(p.year, p.month, p.day, p.hour, p.minute + 1, 0);
      else if (this.#seconds[p.second] !== true) jump = ts + 1000;
      else return new Date(ts);
      // Clamp forward so DST gaps and overlaps can never stall the search.
      ts = Math.max(jump, ts + 1000);
    }
    return null;
  }
}
