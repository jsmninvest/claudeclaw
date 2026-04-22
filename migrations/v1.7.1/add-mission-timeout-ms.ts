import path from 'path';
import Database from 'better-sqlite3';

export const description =
  'Add mission_tasks.timeout_ms (milliseconds, nullable) so callers can ' +
  'override the per-turn agent timeout on a task-by-task basis. NULL means ' +
  '"use default" (AGENT_TURN_TIMEOUT_MS env var, or the scheduler default). ' +
  'Additive-only; existing rows left at NULL.';

const DB_PATH = path.join('store', 'claudeclaw.db');

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((r) => r.name === column);
}

export async function run(): Promise<void> {
  const db = new Database(DB_PATH);

  try {
    if (!hasColumn(db, 'mission_tasks', 'timeout_ms')) {
      db.exec(`ALTER TABLE mission_tasks ADD COLUMN timeout_ms INTEGER`);
    }
  } finally {
    db.close();
  }
}
