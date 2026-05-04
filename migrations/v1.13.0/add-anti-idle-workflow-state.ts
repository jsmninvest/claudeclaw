import path from 'path';
import Database from 'better-sqlite3';

export const description =
  'Anti-Idle v5: add workflow_state columns to kanban_tasks so the new ' +
  'plan→codex→build chain (idle → main triage → domain expert → builder → ' +
  'verify) can carry per-task workflow context independently of column_id.\n' +
  '\n' +
  '  - workflow_state           — current state in the v5 chain (NULL = idle)\n' +
  '  - workflow_owner           — agent currently responsible for the task\n' +
  '  - workflow_correlation_id  — links every mission/event for this task\n' +
  '  - workflow_attempt         — count of plan→review cycles (cap = 2)\n' +
  '  - workflow_updated_at      — unix-seconds timestamp of last state move\n' +
  '\n' +
  'Index on (workflow_state, workflow_updated_at) supports the SLA + watchdog ' +
  'queries in the orchestrator (e.g. "triage_requested untouched > 30 min"). ' +
  'Additive-only; existing rows left untouched (workflow_state = NULL means ' +
  '"not yet entered v5 workflow"). Idempotent.';

const DB_PATH = path.join('store', 'claudeclaw.db');

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((r) => r.name === column);
}

export async function run(): Promise<void> {
  const db = new Database(DB_PATH);
  try {
    if (!hasColumn(db, 'kanban_tasks', 'workflow_state')) {
      db.exec(`ALTER TABLE kanban_tasks ADD COLUMN workflow_state TEXT DEFAULT NULL`);
    }
    if (!hasColumn(db, 'kanban_tasks', 'workflow_owner')) {
      db.exec(`ALTER TABLE kanban_tasks ADD COLUMN workflow_owner TEXT DEFAULT NULL`);
    }
    if (!hasColumn(db, 'kanban_tasks', 'workflow_correlation_id')) {
      db.exec(`ALTER TABLE kanban_tasks ADD COLUMN workflow_correlation_id TEXT DEFAULT NULL`);
    }
    if (!hasColumn(db, 'kanban_tasks', 'workflow_attempt')) {
      db.exec(`ALTER TABLE kanban_tasks ADD COLUMN workflow_attempt INTEGER DEFAULT 0`);
    }
    if (!hasColumn(db, 'kanban_tasks', 'workflow_updated_at')) {
      db.exec(`ALTER TABLE kanban_tasks ADD COLUMN workflow_updated_at INTEGER DEFAULT NULL`);
    }

    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_kanban_workflow
        ON kanban_tasks(workflow_state, workflow_updated_at);
    `);
  } finally {
    db.close();
  }
}
