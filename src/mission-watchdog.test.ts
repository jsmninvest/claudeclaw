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
 *
 * The second block (auto-retry lane) exercises the full runMissionWatchdog()
 * pipeline against a temp file-backed SQLite so we cover the SQL eligibility
 * predicate + the createRetryMission transaction + the Telegram dispatch
 * ping in one integration-ish test.
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// ── Module-level test harness for the auto-retry lane ───────────────
// runMissionWatchdog() opens its own better-sqlite3 connection via
// path.join(STORE_DIR, 'claudeclaw.db'). To drive it from a test without
// clobbering the dev DB, we redirect STORE_DIR to a pid-scoped tmp dir
// and call initDatabase() so db.ts's module-level `db` (used by
// createAutoTriageMission + createRetryMission) opens the SAME file.
//
// vi.hoisted runs before vi.mock so the env vars + tmp path are in place
// when config.js is first imported by the mock factory.
const { TEST_STORE_DIR } = vi.hoisted(() => {
  process.env.TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || 'test-token-xyz';
  process.env.ALLOWED_CHAT_ID = process.env.ALLOWED_CHAT_ID || '7678675171';
  process.env.DB_ENCRYPTION_KEY = process.env.DB_ENCRYPTION_KEY || '0'.repeat(64);
  return { TEST_STORE_DIR: `/tmp/claudeclaw-watchdog-test-${process.pid}` };
});

vi.mock('./config.js', async () => {
  const actual = await vi.importActual<typeof import('./config.js')>('./config.js');
  return { ...actual, STORE_DIR: TEST_STORE_DIR };
});

import { SCHEDULED_TASK_FAILURE_CLAUSE, runMissionWatchdog } from './mission-watchdog.js';
import { initDatabase } from './db.js';
import { _resetSendFnForTest, _setSendFnForTest } from './mission-autopush.js';

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

  // ── Regression: JSON fail-closed payload with last_status='success' ──
  // Motivation: scheduled_tasks row 83a91d04 (rate-update-v1, Tue/Fri 07:00)
  // had last_status='success' with a body of
  //   {"ok":false,"error":"Missing MBSLIVE_USERNAME..."}
  // because the agent-layer wrapper reports the ok:false payload under a
  // successful mission run. Watchdog missed it for 10 days (Apr 14/17/21).

  it("escalates JSON payload with ok:false even when last_status='success'", () => {
    expect(
      matches(
        'success',
        '{"ok":false,"error":"Missing MBSLIVE_USERNAME or MBSLIVE_PASSWORD"}',
      ),
    ).toBe(true);
    // Pretty-printed variant (what scheduled-preview-run.mjs emits on fail).
    expect(matches('success', '{\n  "ok": false,\n  "error": "boom"\n}')).toBe(false);
    // ^ note: `"ok": false` (with a space) does NOT match the literal
    // `"ok":false` shape — that's intentional. The compact no-space form is
    // what the agent wrapper captures; pretty-printed is rare in last_result
    // and is left to the other shape matchers ('Error:', etc.).
    // Inside a fenced markdown code block (exactly how 83a91d04 was stored).
    expect(
      matches(
        'success',
        '```json\n{"ok":false,"error":"Missing MBSLIVE_USERNAME or MBSLIVE_PASSWORD — credentials not found in env cache or fallback sources"}\n```',
      ),
    ).toBe(true);
  });

  it("escalates JSON payload with an \"error\":\"...\" field", () => {
    expect(matches('success', '{"error":"something broke"}')).toBe(true);
    expect(matches('success', '{"ok":true,"error":"non-fatal warning"}')).toBe(true);
  });

  // ── Regression: healthy JSON that merely mentions 'error'/'ok'/'false' ──

  it("does NOT escalate healthy ok:true JSON that mentions 'error' in text", () => {
    // No "error":"..." key and ok is true — benign.
    expect(matches('success', '{"ok":true,"notes":"no errors today"}')).toBe(false);
    expect(matches('success', '{"ok":true,"summary":"0 errors, 0 warnings"}')).toBe(false);
    // Mentions the word "false" but not as an ok:false payload.
    expect(matches('success', '{"ok":true,"was_dry_run":false}')).toBe(false);
  });

  // ── Null/empty safety ─────────────────────────────────────────────

  it('does not escalate null/empty rows', () => {
    expect(matches(null, null)).toBe(false);
    expect(matches('success', null)).toBe(false);
    expect(matches('success', '')).toBe(false);
  });
});

// ─────────────────────────────────────────────────────────────────────
//  Auto-retry lane — end-to-end over runMissionWatchdog()
// ─────────────────────────────────────────────────────────────────────
// Covers the spec contract from src/mission-watchdog.ts lane 0:
//   - Eligibility SQL: status='failed' AND escalated_at IS NULL
//     AND retry_attempt=0 AND created_by='main'
//     AND title NOT LIKE 'Auto-triage:%'
//     AND error matches turn-cap OR timeout shape.
//   - Hard cap at 1 retry (retry_attempt=1 parent → no further retry).
//   - Fall-through: ineligible failures still flow to createAutoTriageMission.
//   - Telegram dispatch ping fires via notifyRetryDispatch.

const TURN_CAP_ERR =
  'Claude Code returned an error result: Reached maximum number of turns (60)';

interface MissionRow {
  id: string;
  title: string;
  assigned_agent: string | null;
  status: string;
  created_by: string;
  retry_attempt: number;
  retried_from: string | null;
  escalated_at: number | null;
}

describe('auto-retry lane', () => {
  let rawDb: Database.Database;
  let sent: Array<{ token: string; chatId: string; text: string }>;

  beforeEach(() => {
    // Fresh DB file per test so parent mission rows don't leak across cases.
    fs.rmSync(TEST_STORE_DIR, { recursive: true, force: true });
    fs.mkdirSync(TEST_STORE_DIR, { recursive: true });
    initDatabase(); // populates db.ts's module-level `db` at STORE_DIR/claudeclaw.db
    rawDb = new Database(path.join(TEST_STORE_DIR, 'claudeclaw.db'));
    rawDb.pragma('journal_mode = WAL');
    rawDb.pragma('busy_timeout = 5000');

    sent = [];
    _setSendFnForTest(async (token, chatId, text) => {
      sent.push({ token, chatId, text });
    });
    delete process.env.MISSION_AUTOPUSH_DISABLED;
  });

  afterEach(() => {
    rawDb.close();
    _resetSendFnForTest();
  });

  afterAll(() => {
    fs.rmSync(TEST_STORE_DIR, { recursive: true, force: true });
  });

  /** Insert a failed mission directly — bypasses createMissionTask/completeMissionTask
   *  so we can control every column the watchdog eligibility clause reads. */
  function seedFailedMission(opts: {
    id: string;
    title?: string;
    error: string;
    createdBy?: string;
    retryAttempt?: number;
    assignedAgent?: string | null;
  }): void {
    const {
      id,
      title = 'Test mission ' + id,
      error,
      createdBy = 'main',
      retryAttempt = 0,
      assignedAgent = 'builder',
    } = opts;
    const now = Math.floor(Date.now() / 1000);
    rawDb
      .prepare(
        `INSERT INTO mission_tasks
           (id, title, prompt, assigned_agent, status, result, error, created_by,
            priority, created_at, started_at, completed_at, acceptance_criteria,
            timeout_ms, autopushed_at, escalated_at, retry_attempt, retried_from)
         VALUES (?, ?, ?, ?, 'failed', NULL, ?, ?, 5, ?, ?, ?, NULL, NULL, NULL, NULL, ?, NULL)`,
      )
      .run(id, title, 'prompt body', assignedAgent, error, createdBy, now, now, now, retryAttempt);
  }

  function getChildOf(parentId: string): MissionRow | undefined {
    return rawDb
      .prepare(`SELECT * FROM mission_tasks WHERE retried_from = ?`)
      .get(parentId) as MissionRow | undefined;
  }

  function getMission(id: string): MissionRow | undefined {
    return rawDb
      .prepare(`SELECT * FROM mission_tasks WHERE id = ?`)
      .get(id) as MissionRow | undefined;
  }

  // ── 1. Happy path ──────────────────────────────────────────────────

  it('turn-cap + retry_attempt=0 + created_by=main → spawns retry child with retry_attempt=1', async () => {
    seedFailedMission({ id: 'parent01', error: TURN_CAP_ERR });

    const res = await runMissionWatchdog();

    expect(res.autoRetried).toBe(1);
    expect(res.retriedMissions).toHaveLength(1);

    const child = getChildOf('parent01');
    expect(child).toBeDefined();
    expect(child!.retry_attempt).toBe(1);
    expect(child!.retried_from).toBe('parent01');
    expect(child!.title.startsWith('[retry] ')).toBe(true);
    expect(child!.created_by).toBe('main');
    expect(child!.status).toBe('queued');

    // Parent must be stamped so the @main triage lane skips it.
    const parent = getMission('parent01');
    expect(parent!.escalated_at).not.toBeNull();
  });

  // ── 2. Hard cap: retry_attempt=1 means we already retried once ─────

  it('retry_attempt=1 parent → NO retry, falls through to @main triage', async () => {
    seedFailedMission({ id: 'parent02', error: TURN_CAP_ERR, retryAttempt: 1 });

    const res = await runMissionWatchdog();

    expect(res.autoRetried).toBe(0);
    expect(getChildOf('parent02')).toBeUndefined();

    // Fall-through lane fires: parent gets escalated_at + an Auto-triage mission exists.
    expect(res.failedEscalated).toBe(1);
    const triage = rawDb
      .prepare(
        `SELECT id FROM mission_tasks WHERE assigned_agent = 'main' AND title LIKE 'Auto-triage:%'`,
      )
      .get() as { id: string } | undefined;
    expect(triage).toBeDefined();
  });

  // ── 3. Only main-dispatched work is eligible ───────────────────────

  it("created_by='dashboard' → NO retry even on turn-cap", async () => {
    seedFailedMission({ id: 'parent03', error: TURN_CAP_ERR, createdBy: 'dashboard' });

    const res = await runMissionWatchdog();

    expect(res.autoRetried).toBe(0);
    expect(getChildOf('parent03')).toBeUndefined();
  });

  // ── 4. Never retry the watchdog's own triage missions ──────────────

  it("title starting with 'Auto-triage:' → NO retry", async () => {
    seedFailedMission({
      id: 'parent04',
      title: 'Auto-triage: mission_failed abc123',
      error: TURN_CAP_ERR,
    });

    const res = await runMissionWatchdog();

    expect(res.autoRetried).toBe(0);
    expect(getChildOf('parent04')).toBeUndefined();
  });

  // ── 5. Error shape must match turn-cap OR timeout ──────────────────

  it('acceptance-failure error → NO retry, falls through to escalation', async () => {
    seedFailedMission({
      id: 'parent05',
      error: 'Acceptance FAIL: criterion X not met',
    });

    const res = await runMissionWatchdog();

    expect(res.autoRetried).toBe(0);
    expect(getChildOf('parent05')).toBeUndefined();

    // Regular failed-mission lane still handles it.
    expect(res.failedEscalated).toBe(1);
    const parent = getMission('parent05');
    expect(parent!.escalated_at).not.toBeNull();
  });

  // ── 6. Telegram dispatch ping shape ────────────────────────────────

  it('fires Telegram ping with 🔄 Auto-retry + parent id + attempt 2/2', async () => {
    seedFailedMission({ id: 'parent06', error: TURN_CAP_ERR });

    await runMissionWatchdog();
    // notifyRetryDispatch is fire-and-forget (void) — yield the microtask
    // queue so the awaited sendFn resolves before we inspect `sent`.
    await new Promise((resolve) => setTimeout(resolve, 50));

    const ping = sent.find((s) => s.text.includes('🔄 Auto-retry'));
    expect(ping).toBeDefined();
    // formatRetryDispatch slices the parent id to 8 chars — 'parent06' is 8.
    expect(ping!.text).toContain('parent06');
    expect(ping!.text).toContain('attempt 2/2');
  });
});
