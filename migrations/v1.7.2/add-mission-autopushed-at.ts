import path from 'path';
import Database from 'better-sqlite3';

export const description =
  'Add mission_tasks.autopushed_at (unix ts, nullable) so the mission-autopush ' +
  'hook in src/mission-autopush.ts can CAS-claim a row and guarantee exactly-once ' +
  'Telegram notification per mission completion. NULL = not yet pushed. ' +
  'Additive-only; existing rows left at NULL so back-log completions can push ' +
  'on next scheduler tick if still relevant.';

const DB_PATH = path.join('store', 'claudeclaw.db');

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((r) => r.name === column);
}

export async function run(): Promise<void> {
  const db = new Database(DB_PATH);

  try {
    if (!hasColumn(db, 'mission_tasks', 'autopushed_at')) {
      db.exec(`ALTER TABLE mission_tasks ADD COLUMN autopushed_at INTEGER`);
    }

    // Partial index on the exact predicate the hook hits on every completion:
    //   UPDATE ... WHERE id = ? AND autopushed_at IS NULL
    // Partial-on-NULL keeps the index tiny — once a row is pushed it drops out.
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_mission_autopushed_pending
        ON mission_tasks(id)
        WHERE autopushed_at IS NULL;
    `);
  } finally {
    db.close();
  }
}
