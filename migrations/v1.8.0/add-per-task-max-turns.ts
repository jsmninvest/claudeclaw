import path from 'path';
import Database from 'better-sqlite3';

export const description =
  'Add per-task max_turns override columns (INTEGER, nullable) to ' +
  'scheduled_tasks and mission_tasks. NULL means fall back to the global ' +
  'AGENT_MAX_TURNS env default (currently 60). Non-null values bypass that ' +
  'default for this specific task — used by known-expensive tasks (e.g. the ' +
  'DION planner at 4a50eb32) so we can raise their ceiling without lifting ' +
  'it globally for ad-hoc throwaway work. Additive-only + idempotent.';

const DB_PATH = path.join('store', 'claudeclaw.db');

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((r) => r.name === column);
}

export async function run(): Promise<void> {
  const db = new Database(DB_PATH);

  try {
    if (!hasColumn(db, 'scheduled_tasks', 'max_turns')) {
      db.exec(`ALTER TABLE scheduled_tasks ADD COLUMN max_turns INTEGER`);
    }

    if (!hasColumn(db, 'mission_tasks', 'max_turns')) {
      db.exec(`ALTER TABLE mission_tasks ADD COLUMN max_turns INTEGER`);
    }
  } finally {
    db.close();
  }
}
