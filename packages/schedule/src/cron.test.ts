import { describe, expect, test } from "bun:test";
import { Cron } from "./cron";

const at = (iso: string) => new Date(iso);
const next = (pattern: string, from: string, timezone?: string) =>
  new Cron(pattern, timezone === undefined ? {} : { timezone }).nextRun(at(from))?.toISOString();

describe("Cron parsing", () => {
  test("accepts 5-field, 6-field, and @alias patterns", () => {
    expect(next("* * * * *", "2026-01-10T10:00:30.000Z", "UTC")).toBe("2026-01-10T10:01:00.000Z");
    expect(next("* * * * * *", "2026-01-10T10:00:30.000Z", "UTC")).toBe("2026-01-10T10:00:31.000Z");
    expect(next("@daily", "2026-01-10T10:00:00.000Z", "UTC")).toBe("2026-01-11T00:00:00.000Z");
    expect(next("@hourly", "2026-01-10T10:00:00.000Z", "UTC")).toBe("2026-01-10T11:00:00.000Z");
  });

  test("rejects malformed patterns", () => {
    expect(() => new Cron("* * * *")).toThrow("5 or 6 fields");
    expect(() => new Cron("constructor")).toThrow("5 or 6 fields");
    expect(() => new Cron("* * * * constructor")).toThrow('invalid day-of-week "constructor"');
    expect(() => new Cron("* * * constructor *")).toThrow('invalid month "constructor"');
    expect(() => new Cron("61 * * * *")).toThrow('invalid minute "61"');
    expect(() => new Cron("* * * * mars *")).toThrow('invalid month "mars"');
    expect(() => new Cron("*/0 * * * *")).toThrow("invalid minute");
    expect(() => new Cron("5-1 * * * *")).toThrow("invalid minute");
    expect(() => new Cron("a * * * *")).toThrow('invalid minute "a"');
    expect(() => new Cron("* * * * *", { timezone: "Mars/Olympus" })).toThrow('unknown timezone "Mars/Olympus"');
  });
});

describe("Cron.nextRun", () => {
  test("is strictly after `from`, at second resolution", () => {
    expect(next("0 * * * *", "2026-01-10T10:00:00.000Z", "UTC")).toBe("2026-01-10T11:00:00.000Z");
    expect(next("0 * * * *", "2026-01-10T09:59:59.999Z", "UTC")).toBe("2026-01-10T10:00:00.000Z");
  });

  test("steps, ranges, and lists", () => {
    expect(next("*/15 * * * *", "2026-01-10T10:03:00.000Z", "UTC")).toBe("2026-01-10T10:15:00.000Z");
    expect(next("0 9-17 * * *", "2026-01-10T18:30:00.000Z", "UTC")).toBe("2026-01-11T09:00:00.000Z");
    expect(next("0 0,12 * * *", "2026-01-10T01:00:00.000Z", "UTC")).toBe("2026-01-10T12:00:00.000Z");
    expect(next("5/10 * * * *", "2026-01-10T10:06:00.000Z", "UTC")).toBe("2026-01-10T10:15:00.000Z");
  });

  test("month and weekday names", () => {
    expect(next("0 0 1 jan *", "2026-03-01T00:00:00.000Z", "UTC")).toBe("2027-01-01T00:00:00.000Z");
    expect(next("0 9 * * mon-fri", "2026-01-10T00:00:00.000Z", "UTC")).toBe("2026-01-12T09:00:00.000Z");
    expect(next("0 0 * * sun", "2026-01-10T01:00:00.000Z", "UTC")).toBe("2026-01-11T00:00:00.000Z");
  });

  test("day-of-week 7 means Sunday", () => {
    expect(next("0 0 * * 7", "2026-01-10T01:00:00.000Z", "UTC")).toBe("2026-01-11T00:00:00.000Z");
  });

  test("restricted day-of-month OR day-of-week, like standard cron", () => {
    expect(next("0 0 13 * fri", "2026-01-08T00:00:00.000Z", "UTC")).toBe("2026-01-09T00:00:00.000Z");
    expect(next("0 0 13 * fri", "2026-01-10T00:00:00.000Z", "UTC")).toBe("2026-01-13T00:00:00.000Z");
  });

  test("an impossible date returns null", () => {
    expect(new Cron("0 0 30 2 *", { timezone: "UTC" }).nextRun(at("2026-01-01T00:00:00Z"))).toBeNull();
  });

  test("leap-day schedules wait for a leap year", () => {
    expect(next("0 0 29 2 *", "2026-01-01T00:00:00.000Z", "UTC")).toBe("2028-02-29T00:00:00.000Z");
  });

  test("timezone-aware occurrences", () => {
    expect(next("0 0 * * *", "2026-01-10T10:00:00.000Z", "America/New_York")).toBe("2026-01-11T05:00:00.000Z");
    expect(next("0 0 * * *", "2026-07-10T10:00:00.000Z", "America/New_York")).toBe("2026-07-11T04:00:00.000Z");
    expect(next("30 9 * * *", "2026-01-10T10:00:00.000Z", "Asia/Karachi")).toBe("2026-01-11T04:30:00.000Z");
  });

  test("a wall-clock time erased by a DST spring-forward gap is skipped", () => {
    expect(next("30 2 * * *", "2026-03-28T12:00:00.000Z", "Europe/Paris")).toBe("2026-03-30T00:30:00.000Z");
  });

  test("without a timezone the host's local clock is used", () => {
    const nightly = new Cron("0 3 * * *").nextRun(at("2026-01-10T10:00:00Z"))!;
    expect(nightly.getHours()).toBe(3);
    expect(nightly.getMinutes()).toBe(0);
    expect(nightly.getTime()).toBeGreaterThan(at("2026-01-10T10:00:00Z").getTime());
  });
});
