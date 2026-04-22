/**
 * Mission watchdog — surfaces silent failures by queueing auto-triage
 * missions on @main (opus). Covers three signals:
 *
 *   1. Failed missions (last 24h) that haven't been escalated.
 *   2. Stuck missions: status='running' for > 1 hour.
 *   3. Failed scheduled_tasks (last 1h): last_status='failed' OR last_result
 *      contains a real error shape (Error:, FATAL, Exception, Traceback, or
 *      a non-zero "Failed: N" count). A 1-hour de-dupe window keeps a cron
 *      that fails every 10 min from spamming triage missions.
 *
 *      Why the shape check: healthy pretty-printed output like
 *      "Jobs Failed:     0" used to match a naive LIKE '%Failed%' and
 *      trigger false escalations. The patterns below require a non-zero
 *      digit or an explicit error prefix to avoid that.
 *
 * Escalation = create a priority-9 mission on @main (NOT a Telegram ping).
 * Main owns the triage → dispatches a reduced-scope muscle fix to the right
 * spoke, and only bothers Rudy if a permission/decision is needed.
 *
 * Idempotent: each source row gets escalated_at stamped after triage so the
 * same failure never triggers twice (within the 1h window for scheduled_tasks).
 */

import Database from 'better-sqlite3';
import path from 'path';

import { STORE_DIR } from './config.js';
import { logger } from './logger.js';
import { createAutoTriageMission } from './auto-triage.js';
import { createRetryMission, logToHiveMind, RetryReason } from './db.js';
import { notifyRetryDispatch } from './mission-autopush.js';

const SCHEDULED_TASK_DEDUP_SECONDS = 3600; // 1 hour

/**
 * SQL predicate used to decide whether a scheduled_task run should be
 * treated as a failure worth escalating. Exported so the unit test can
 * feed real rows through the same clause the watchdog uses.
 *
 * - `last_status = 'failed'` is the primary signal (set by the scheduler).
 * - The content checks guard against silent failures where a worker wrote
 *   an error to last_result but never flipped last_status. We only match
 *   real error shapes — never bare words like "error" or "failed" that
 *   show up in healthy summaries:
 *     * `Failed: [1-9]`   — non-zero failure count (via GLOB character class).
 *                           Healthy summaries like "Jobs Failed:     0" do
 *                           NOT match because the char after the single space
 *                           must be in [1-9].
 *     * `Error:`          — typical error prefix (LIKE is ASCII-case-insensitive).
 *     * `FATAL`           — log-level marker.
 *     * `Exception`       — thrown/raised exception in output.
 *     * `Traceback`       — Python-style stack trace.
 *     * `"ok":false`      — JSON fail-closed payload shape. Added 2026-04-21
 *                           after rate-update-v1 row 83a91d04 went silent:
 *                           the agent-layer wrapper reported ok:false JSON
 *                           under a successful mission run (last_status='success'),
 *                           so the status signal missed it. Matching the
 *                           canonical JSON shape catches this even when the
 *                           exit code is lost in translation. Same reasoning
 *                           as `Failed: [1-9]` — we require the explicit JSON
 *                           key, not just the word "ok" or "false".
 *     * `"error":"`       — JSON error field shape. Matches any payload that
 *                           builds a JSON error result (e.g. {"error":"..."}
 *                           or {"ok":false,"error":"..."}). Requires the
 *                           trailing quote so healthy JSON like
 *                           {"notes":"no errors today"} does NOT match.
 *     * `CLI failed`      — explicit CLI-failure banner used by one-shot
 *                           wrapper scripts (e.g. mission-watchdog-cli's
 *                           stderr line). Added 2026-04-21 after the
 *                           watchdog itself DOA'd under launchd and the
 *                           scheduler kept marking last_status='success'.
 */
export const SCHEDULED_TASK_FAILURE_CLAUSE = `(
  last_status = 'failed'
  OR last_result GLOB '*Failed: [1-9]*'
  OR last_result LIKE '%Error:%'
  OR last_result LIKE '%FATAL%'
  OR last_result LIKE '%Exception%'
  OR last_result LIKE '%Traceback%'
  OR last_result LIKE '%"ok":false%'
  OR last_result LIKE '%"error":"%'
  OR last_result LIKE '%CLI failed%'
)`;

interface FailedMissionRow {
  id: string;
  title: string;
  assigned_agent: string | null;
  error: string | null;
  prompt: string;
  result: string | null;
}

interface StuckMissionRow {
  id: string;
  title: string;
  assigned_agent: string | null;
  prompt: string;
  started_at: number;
}

interface FailedScheduledRow {
  id: string;
  prompt: string;
  schedule: string;
  agent_id: string | null;
  last_status: string | null;
  last_result: string | null;
  last_run: number;
}

export interface WatchdogResult {
  failedEscalated: number;
  stuckEscalated: number;
  scheduledEscalated: number;
  /** How many obvious-transient failures (turn-cap / timeout) the watchdog
   *  auto-retried on this tick. Separate from failedEscalated — these rows
   *  bypass the @main triage lane entirely. */
  autoRetried: number;
  errors: number;
  triagedMissions: string[];
  /** IDs of the child retry missions spawned on this tick. */
  retriedMissions: string[];
}

interface RetryCandidateRow {
  id: string;
  title: string;
  assigned_agent: string | null;
  error: string | null;
}

/**
 * Classify the parent's error string into an auto-retry reason.
 * Mirrors the SQL eligibility clause — anything the SQL matches, this
 * function must classify. Returns null if the error shape isn't one of
 * the two we retry on.
 */
function classifyRetryReason(error: string | null): RetryReason | null {
  if (!error) return null;
  if (error.includes('Reached maximum number of turns')) return 'turn_cap';
  if (error.includes('Timed out after')) return 'timeout';
  return null;
}

function getDb(): Database.Database {
  const dbPath = path.join(STORE_DIR, 'claudeclaw.db');
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  return db;
}

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((r) => r.name === column);
}

/**
 * Run the watchdog once. Returns a structured result — the caller (CLI or
 * scheduler) decides how to surface it.
 *
 * NOTE: deliberately takes no chatId / no Telegram routing. All escalation
 * is in-band via mission_tasks. That makes the watchdog resilient to a
 * missing ALLOWED_CHAT_ID / bot-token misconfig — its whole job is to catch
 * failures, so it must not itself fail on an env-var issue.
 */
export async function runMissionWatchdog(): Promise<WatchdogResult> {
  const db = getDb();
  const result: WatchdogResult = {
    failedEscalated: 0,
    stuckEscalated: 0,
    scheduledEscalated: 0,
    autoRetried: 0,
    errors: 0,
    triagedMissions: [],
    retriedMissions: [],
  };

  try {
    // ── 0. Auto-retry lane (obvious transient failures only) ─────────
    // Hard-scoped slice of the failed-mission set that we retry ONCE
    // without bothering @main. Eligibility is deliberately narrow:
    //   - status='failed' AND escalated_at IS NULL (same as main lane)
    //   - retry_attempt=0              (single-budget, hard cap)
    //   - created_by='main'            (only Aditya-dispatched work)
    //   - title NOT LIKE 'Auto-triage:%' (never retry watchdog meta-work)
    //   - error matches turn-cap OR timeout shape (classifier below verifies
    //     the exact match so the SQL clause stays readable)
    // Everything else falls through to the existing @main triage lane.
    //
    // Column-existence guard: retry_attempt is added in v1.7.3. On older
    // DBs the column is missing, so we skip the lane entirely rather than
    // throw. Same forward-compat pattern used by the scheduled_tasks lane
    // below for its own post-migration column.
    if (hasColumn(db, 'mission_tasks', 'retry_attempt')) {
      const retryCandidates = db
        .prepare(
          `SELECT id, title, assigned_agent, error
             FROM mission_tasks
            WHERE status = 'failed'
              AND escalated_at IS NULL
              AND retry_attempt = 0
              AND created_by = 'main'
              AND title NOT LIKE 'Auto-triage:%'
              AND (
                error LIKE '%Reached maximum number of turns%'
                OR error LIKE '%Timed out after%'
              )
              AND created_at > strftime('%s','now','-24 hours')`,
        )
        .all() as RetryCandidateRow[];

      for (const row of retryCandidates) {
        const reason = classifyRetryReason(row.error);
        // Belt-and-suspenders: SQL already filtered to turn-cap/timeout.
        // If classifier disagrees, skip and let @main triage handle it.
        if (!reason) continue;
        try {
          const retry = createRetryMission(row.id, reason);
          if (!retry) {
            // Parent disappeared or preconditions failed between SELECT and
            // INSERT — let the regular escalation lane handle it.
            continue;
          }
          // Ping Aditya out-of-band (not batched with completion pushes).
          // Fire-and-forget: notifyRetryDispatch fall-opens on any error.
          void notifyRetryDispatch({
            childId: retry.childId,
            parentId: retry.parentId,
            assignedAgent: retry.assignedAgent,
            title: retry.title,
            reason: retry.reason,
          });
          // Log to hive_mind so the other agents see the auto-retry event
          // alongside regular completions. Swallow DB errors here — we
          // don't want a hive_mind write issue to undo the retry.
          try {
            logToHiveMind(
              'watchdog',
              '',
              'mission_auto_retry',
              `Auto-retry ${retry.childId} for ${retry.parentId}: ${reason}`,
            );
          } catch (err) {
            logger.warn({ err, childId: retry.childId }, 'mission-watchdog: hive_mind log failed');
          }
          result.autoRetried += 1;
          result.retriedMissions.push(retry.childId);
        } catch (err) {
          logger.error(
            { err, missionId: row.id },
            'mission-watchdog: auto-retry dispatch failed, falling through to triage',
          );
          result.errors += 1;
          // Intentionally do NOT stamp escalated_at here — leaving it NULL
          // lets the @main triage lane below pick up the row as a safety net.
        }
      }
    }

    // ── 1. Failed missions (last 24h, not yet escalated) ──────────────
    const failedRows = db
      .prepare(
        `SELECT id, title, assigned_agent, error, prompt, result
           FROM mission_tasks
          WHERE status = 'failed'
            AND escalated_at IS NULL
            AND created_at > strftime('%s','now','-24 hours')`,
      )
      .all() as FailedMissionRow[];

    const stampFailed = db.prepare(
      `UPDATE mission_tasks SET escalated_at = unixepoch() WHERE id = ?`,
    );

    for (const row of failedRows) {
      try {
        const triageId = createAutoTriageMission({
          kind: 'mission_failed',
          sourceId: row.id,
          title: row.title,
          assignedAgent: row.assigned_agent,
          prompt: row.prompt,
          error: row.error,
          lastOutput: row.result,
        });
        stampFailed.run(row.id);
        result.failedEscalated += 1;
        result.triagedMissions.push(triageId);
      } catch (err) {
        logger.error(
          { err, missionId: row.id },
          'mission-watchdog: failed to queue auto-triage for failed mission',
        );
        result.errors += 1;
      }
    }

    // ── 2. Stuck missions (running > 1h, not yet escalated) ──────────
    const stuckRows = db
      .prepare(
        `SELECT id, title, assigned_agent, prompt, started_at
           FROM mission_tasks
          WHERE status = 'running'
            AND escalated_at IS NULL
            AND started_at IS NOT NULL
            AND started_at < (unixepoch() - 3600)`,
      )
      .all() as StuckMissionRow[];

    const stampStuck = db.prepare(
      `UPDATE mission_tasks SET escalated_at = unixepoch() WHERE id = ?`,
    );

    for (const row of stuckRows) {
      const ageMin = Math.floor((Date.now() / 1000 - row.started_at) / 60);
      try {
        const triageId = createAutoTriageMission({
          kind: 'mission_stuck',
          sourceId: row.id,
          title: row.title,
          assignedAgent: row.assigned_agent,
          prompt: row.prompt,
          error: `Mission has been status='running' for ${ageMin} min with no completion.`,
          stuckMinutes: ageMin,
        });
        stampStuck.run(row.id);
        result.stuckEscalated += 1;
        result.triagedMissions.push(triageId);
      } catch (err) {
        logger.error(
          { err, missionId: row.id },
          'mission-watchdog: failed to queue auto-triage for stuck mission',
        );
        result.errors += 1;
      }
    }

    // ── 3. Failed scheduled_tasks (last 1h, not escalated recently) ──
    // Only run this if the column has been migrated — keeps the watchdog
    // forward-compatible with older DBs.
    if (hasColumn(db, 'scheduled_tasks', 'escalated_at')) {
      const failedScheduled = db
        .prepare(
          `SELECT id, prompt, schedule, agent_id, last_status, last_result, last_run
             FROM scheduled_tasks
            WHERE last_run IS NOT NULL
              AND last_run > strftime('%s','now','-1 hour')
              AND ${SCHEDULED_TASK_FAILURE_CLAUSE}
              AND (escalated_at IS NULL OR escalated_at < unixepoch() - ?)`,
        )
        .all(SCHEDULED_TASK_DEDUP_SECONDS) as FailedScheduledRow[];

      const stampScheduled = db.prepare(
        `UPDATE scheduled_tasks SET escalated_at = unixepoch() WHERE id = ?`,
      );

      for (const row of failedScheduled) {
        // Defensive coercion: scheduled_tasks.prompt is declared TEXT but
        // SQLite's dynamic typing lets BLOBs slip in (seen in 264aeba5 where
        // a Buffer got written). Buffer.split() throws, so coerce every
        // text-ish column we downstream-consume.
        const promptStr =
          typeof row.prompt === 'string'
            ? row.prompt
            : row.prompt == null
              ? ''
              : Buffer.isBuffer(row.prompt)
                ? (row.prompt as Buffer).toString('utf8')
                : String(row.prompt);
        const lastResultStr =
          typeof row.last_result === 'string'
            ? row.last_result
            : row.last_result == null
              ? null
              : Buffer.isBuffer(row.last_result)
                ? (row.last_result as Buffer).toString('utf8')
                : String(row.last_result);
        // Derive a usable title from the first line of the prompt — scheduled_tasks
        // don't have a dedicated title column.
        const firstLine = (promptStr || '').split('\n')[0].trim();
        const title = firstLine.length > 100 ? firstLine.slice(0, 99) + '…' : firstLine;
        const errBlurb =
          row.last_status === 'failed'
            ? `scheduled_task last_status='failed'`
            : `scheduled_task last_result contains a failure marker`;
        try {
          const triageId = createAutoTriageMission({
            kind: 'scheduled_task_failed',
            sourceId: row.id,
            title: title || `scheduled_task ${row.id}`,
            assignedAgent: row.agent_id,
            schedule: row.schedule,
            prompt: promptStr,
            error: errBlurb + ` (last_run=${row.last_run})`,
            lastOutput: lastResultStr,
          });
          stampScheduled.run(row.id);
          result.scheduledEscalated += 1;
          result.triagedMissions.push(triageId);
        } catch (err) {
          logger.error(
            { err, scheduledTaskId: row.id },
            'mission-watchdog: failed to queue auto-triage for scheduled_task',
          );
          result.errors += 1;
        }
      }
    } else {
      logger.warn(
        'mission-watchdog: scheduled_tasks.escalated_at column missing — skipping scheduled_tasks scan. Run `npm run migrate`.',
      );
    }
  } finally {
    db.close();
  }

  return result;
}
