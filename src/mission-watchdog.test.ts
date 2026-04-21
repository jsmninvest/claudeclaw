/**
 * Unit tests for the mission-watchdog scheduled_task failure predicate.
 *
 * Regression motivation: the watchdog used to match last_result with
 * LIKE '%Failed%' / LIKE '%Error%', which escalated healthy zero-count
 * summaries like 'Jobs Failed:     0'. We now only treat a row as a
 * failure when last_status='failed' OR last_result contains a real
 * error shape.
 *
 * These tests exercise the SAME SQL clause the watchdog uses
 * (SCHEDULED_TASK_FAILURE_CLAUSE), fed through an in-memory SQLite
 * database — so if someone tweaks the clause, this test catches
 * regressions without needing to spin up the full watchdog pipeline.
 */
import Database from 'better-sqlite3';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { SCHEDULED_TASK_FAILURE_CLAUSE } from './mission-watchdog.js';

describe('SCHEDULED_TASK_FAILURE_CLAUSE', () => {
  let db: Database.Database;

  beforeEach(() => {
    db = new Database(':memory:');
    db.exec(`
      CREATE TABLE scheduled_tasks (
        id          TEXT PRIMARY KEY,
        last_status TEXT,
        last_result TEXT
      );
    `);
  });

  afterEach(() => {
    db.close();
  });

  /** Returns true if the row would be escalated by the watchdog. */
  function matches(last_status: string | null, last_result: string | null): boolean {
    db.prepare('DELETE FROM scheduled_tasks').run();
    db.prepare('INSERT INTO scheduled_tasks (id, last_status, last_result) VALUES (?, ?, ?)').run(
      't1',
      last_status,
      last_result,
    );
    const row = db
      .prepare(`SELECT id FROM scheduled_tasks WHERE ${SCHEDULED_TASK_FAILURE_CLAUSE}`)
      .get();
    return row !== undefined;
  }

  // ── Regression: the exact false-positive that motivated this fix ──

  it("does NOT escalate a healthy 'Jobs Failed:     0' summary", () => {
    // Real last_result from scheduled_task 020c38b1 (s2l, */10 * * * *)
    const healthy =
      'Jobs Failed:     0\n' +
      'Queue was empty — no transcript jobs pending at run time.';
    expect(matches('success', healthy)).toBe(false);
  });

  it("does NOT escalate 'Failed: 0' variants", () => {
    expect(matches('success', 'Failed: 0')).toBe(false);
    expect(matches('success', 'Jobs Failed: 0')).toBe(false);
    expect(matches('success', 'Failed:     0 errors across run')).toBe(false);
    expect(matches(null, 'Failed: 0')).toBe(false);
  });

  // ── Primary signal: last_status='failed' always escalates ─────────

  it("escalates when last_status='failed' (even with empty last_result)", () => {
    expect(matches('failed', null)).toBe(true);
    expect(matches('failed', '')).toBe(true);
    expect(matches('failed', 'All good')).toBe(true);
  });

  // ── Content signal: real error shapes still trigger ───────────────

  it("escalates when last_result contains a non-zero 'Failed: N' count", () => {
    expect(matches('success', 'Failed: 1')).toBe(true);
    expect(matches('success', 'Failed: 5 of 10')).toBe(true);
    expect(matches('success', 'Jobs Failed: 3')).toBe(true);
  });

  it("escalates when last_result contains 'Error:'", () => {
    expect(matches('success', 'Error: something broke')).toBe(true);
    // LIKE is ASCII-case-insensitive in SQLite, so lowercase also matches.
    expect(matches('success', 'error: network timeout')).toBe(true);
  });

  it('escalates on FATAL / Exception / Traceback markers', () => {
    expect(matches('success', 'FATAL: disk full')).toBe(true);
    expect(matches('success', 'java.lang.NullPointerException')).toBe(true);
    expect(matches('success', 'Traceback (most recent call last):\n  File "x.py"')).toBe(true);
  });

  // ── Non-failures that used to false-positive under LIKE '%Error%' ──

  it("does NOT escalate benign mentions of 'error' without a colon", () => {
    // LIKE '%Error:%' requires the colon — plain "error rate" is fine.
    expect(matches('success', 'Error rate: 0.0%')).toBe(false);
    expect(matches('success', 'no errors detected')).toBe(false);
  });

  it("does NOT escalate summaries that merely mention the word 'Failed'", () => {
    expect(matches('success', 'Nothing Failed today.')).toBe(false);
    expect(matches('success', 'Failed count is within SLO.')).toBe(false);
  });

  // ── Null/empty safety ─────────────────────────────────────────────

  it('does not escalate null/empty rows', () => {
    expect(matches(null, null)).toBe(false);
    expect(matches('success', null)).toBe(false);
    expect(matches('success', '')).toBe(false);
  });
});
