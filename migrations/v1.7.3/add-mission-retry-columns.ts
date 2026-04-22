import path from 'path';
import Database from 'better-sqlite3';

export const description =
  'Add mission_tasks.retry_attempt (INTEGER NOT NULL DEFAULT 0) and ' +
  'mission_tasks.retried_from (TEXT, nullable pointer to parent mission id) ' +
  'so src/mission-watchdog.ts can run a single-budget auto-retry lane for ' +
  'obvious transient failures (turn-cap exhaustion, timeout). ' +
  '0 = original, 1 = first auto-retry. retried_from points back to the parent ' +
  'failed mission. Additive-only + idempotent — running twice is safe.';

const DB_PATH = path.join('store', 'claudeclaw.db');

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((r) => r.name === column);
}

export async function run(): Promise<void> {
  const db = new Database(DB_PATH);

  try {
    if (!hasColumn(db, 'mission_tasks', 'retry_attempt')) {
      // NOT NULL DEFAULT 0 backfills existing rows to 0 (the "original"
      // attempt slot) so the watchdog's retry_attempt=0 eligibility check
      // doesn't mis-classify pre-migration rows as already-retried.
      db.exec(`ALTER TABLE mission_tasks ADD COLUMN retry_attempt INTEGER NOT NULL DEFAULT 0`);
    }

    if (!hasColumn(db, 'mission_tasks', 'retried_from')) {
      db.exec(`ALTER TABLE mission_tasks ADD COLUMN retried_from TEXT`);
    }

    // Partial index on the exact predicate the watchdog's auto-retry lane
    // hits every 10 min:
    //   SELECT ... WHERE status='failed' AND retry_attempt=0 AND escalated_at IS NULL ...
    // Partial-on-(failed, 0) keeps the index tiny — completed rows and
    // already-retried rows drop out.
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_mission_retry_candidates
        ON mission_tasks(status, retry_attempt)
        WHERE status = 'failed' AND retry_attempt = 0;
    `);
  } finally {
    db.close();
  }
}
