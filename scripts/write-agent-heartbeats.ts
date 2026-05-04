#!/usr/bin/env tsx
/**
 * write-agent-heartbeats.ts — Lightweight heartbeat writer.
 *
 * Runs every 5 minutes via system cron. For each spoke agent:
 *   1. Reads the PID file (store/claudeclaw.pid for `main`,
 *      store/agent-<id>.pid for spokes).
 *   2. Probes liveness with `process.kill(pid, 0)`.
 *   3. Looks up any running scheduled_task / mission_task for that agent.
 *   4. Writes one row into agent_heartbeats:
 *        busy    = process alive  + a running task
 *        idle    = process alive  + no running task
 *        offline = PID missing or process dead
 *   5. Mirrors the row to Supabase via PostgREST upsert.
 *
 * Semantics:
 *   - last_heartbeat advances ONLY when the agent is alive. For offline
 *     agents we preserve the prior value so dashboards can see "last
 *     seen 4 hours ago".
 *   - updated_at is always set to "now" so we can prove the writer ran.
 *
 * Run manually:
 *   npx tsx scripts/write-agent-heartbeats.ts
 *
 * Cron:
 *   *\/5 * * * * /usr/local/bin/npx tsx scripts/write-agent-heartbeats.ts \
 *     >> /tmp/heartbeat-writer.log 2>&1
 */

import fs from 'fs';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import Database from 'better-sqlite3';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '..');
const DB_PATH = path.join(PROJECT_ROOT, 'store', 'claudeclaw.db');
const PID_DIR = path.join(PROJECT_ROOT, 'store');

interface AgentSpec {
  id: string;
  name: string;
  pidFile: string;
}

// The 9 real spoke agents Rudy coordinates.
const AGENTS: AgentSpec[] = [
  { id: 'main',      name: 'Main',          pidFile: 'claudeclaw.pid' },
  { id: 'ops',       name: 'Operations',    pidFile: 'agent-ops.pid' },
  { id: 'builder',   name: 'Builder',       pidFile: 'agent-builder.pid' },
  { id: 'content',   name: 'Content',       pidFile: 'agent-content.pid' },
  { id: 'research',  name: 'Research',      pidFile: 'agent-research.pid' },
  { id: 's2l',       name: 'Speed-to-Lead', pidFile: 'agent-s2l.pid' },
  { id: 'qa',        name: 'QA',            pidFile: 'agent-qa.pid' },
  { id: 'rainmaker', name: 'Rainmaker',     pidFile: 'agent-rainmaker.pid' },
  { id: 'trader',    name: 'Trader',        pidFile: 'agent-trader.pid' },
];

interface Liveness {
  alive: boolean;
  pid: number | null;
  reason: 'alive' | 'no-pid-file' | 'bad-pid-file' | 'dead-process';
}

function checkLiveness(pidFile: string): Liveness {
  const fullPath = path.join(PID_DIR, pidFile);
  let raw: string;
  try {
    raw = fs.readFileSync(fullPath, 'utf-8').trim();
  } catch {
    return { alive: false, pid: null, reason: 'no-pid-file' };
  }
  const pid = parseInt(raw, 10);
  if (!Number.isFinite(pid) || pid <= 0) {
    return { alive: false, pid: null, reason: 'bad-pid-file' };
  }
  try {
    // Signal 0 — existence probe, doesn't kill anything.
    process.kill(pid, 0);
    return { alive: true, pid, reason: 'alive' };
  } catch {
    return { alive: false, pid, reason: 'dead-process' };
  }
}

interface CurrentTask {
  description: string;
  startedAt: number | null;
  source: 'scheduled_tasks' | 'mission_tasks';
}

function findCurrentTask(db: Database.Database, agentId: string): CurrentTask | null {
  // Prefer an actively-running scheduled task.
  const sched = db
    .prepare(
      `SELECT id, prompt, started_at FROM scheduled_tasks
       WHERE agent_id = ? AND last_status = 'running'
       ORDER BY COALESCE(started_at, 0) DESC
       LIMIT 1`,
    )
    .get(agentId) as { id: string; prompt: string; started_at: number | null } | undefined;
  if (sched) {
    return {
      description: truncate(sched.prompt, 200),
      startedAt: sched.started_at ?? null,
      source: 'scheduled_tasks',
    };
  }
  // Fall back to a queued/running mission task.
  const mission = db
    .prepare(
      `SELECT id, title, started_at, status FROM mission_tasks
       WHERE assigned_agent = ? AND status IN ('queued','running')
       ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END,
                priority DESC,
                created_at ASC
       LIMIT 1`,
    )
    .get(agentId) as
    | { id: string; title: string; started_at: number | null; status: string }
    | undefined;
  if (mission) {
    return {
      description: truncate(mission.title, 200),
      startedAt: mission.started_at ?? null,
      source: 'mission_tasks',
    };
  }
  return null;
}

function truncate(s: string, n: number): string {
  if (!s) return s;
  return s.length > n ? `${s.slice(0, n - 1)}…` : s;
}

interface HeartbeatRow {
  agent_id: string;
  agent_name: string;
  status: 'busy' | 'idle' | 'offline';
  current_task: string | null;
  current_task_started_at: number | null;
  metadata: string;          // JSON string for SQLite
  metadataObj: Record<string, unknown>; // raw object for Supabase jsonb
  last_heartbeat: number;    // unix seconds; preserved when offline
  updated_at: number;        // always now
  created_at: number;        // preserved on update
}

function buildRow(
  spec: AgentSpec,
  live: Liveness,
  task: CurrentTask | null,
  prior: { last_heartbeat: number | null; created_at: number | null } | null,
  now: number,
): HeartbeatRow {
  const status: HeartbeatRow['status'] = !live.alive
    ? 'offline'
    : task != null
      ? 'busy'
      : 'idle';

  const metadataObj: Record<string, unknown> = {
    type: 'pid-watcher',
    pid: live.pid,
    pid_file: spec.pidFile,
    liveness: live.reason,
  };
  if (task) {
    metadataObj['task_source'] = task.source;
  }

  // Only advance last_heartbeat when alive. Otherwise keep prior. If we've
  // never seen this agent alive, seed with `now` so the row exists with
  // a sensible timestamp; future cycles preserve the real "last alive" value.
  const last_heartbeat = live.alive
    ? now
    : (prior?.last_heartbeat ?? now);

  return {
    agent_id: spec.id,
    agent_name: spec.name,
    status,
    current_task: task?.description ?? null,
    current_task_started_at: task?.startedAt ?? null,
    metadata: JSON.stringify(metadataObj),
    metadataObj,
    last_heartbeat,
    updated_at: now,
    created_at: prior?.created_at ?? now,
  };
}

function readEnvFiles(): { url: string; key: string } | null {
  const candidates = [
    path.join(PROJECT_ROOT, '.env'),
    path.join(os.homedir(), '.clawdbot', 'secrets', '.env'),
  ];
  let url: string | undefined;
  let key: string | undefined;
  for (const file of candidates) {
    let content: string;
    try {
      content = fs.readFileSync(file, 'utf-8');
    } catch {
      continue;
    }
    for (const line of content.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const eq = trimmed.indexOf('=');
      if (eq === -1) continue;
      const k = trimmed.slice(0, eq).trim();
      let v = trimmed.slice(eq + 1).trim();
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
        v = v.slice(1, -1);
      }
      if (!v) continue;
      if (k === 'SUPABASE_URL' && !url) url = v;
      if ((k === 'SUPABASE_SERVICE_KEY' || k === 'SUPABASE_SERVICE_ROLE_KEY') && !key) key = v;
    }
    if (url && key) break;
  }
  if (!url || !key) return null;
  return { url, key };
}

function unixToIso(s: number | null): string | null {
  if (s == null) return null;
  return new Date(s * 1000).toISOString();
}

async function pushToSupabase(
  rows: HeartbeatRow[],
  env: { url: string; key: string },
): Promise<void> {
  // Convert to Supabase shape: timestamptz columns get ISO strings,
  // jsonb columns get raw objects. Numeric/integer pass through.
  const payload = rows.map((r) => ({
    agent_id: r.agent_id,
    agent_name: r.agent_name,
    status: r.status,
    current_task: r.current_task,
    current_task_started_at: unixToIso(r.current_task_started_at),
    queue: [],
    metadata: r.metadataObj,
    last_heartbeat: unixToIso(r.last_heartbeat),
    updated_at: unixToIso(r.updated_at),
    // Don't overwrite created_at on upsert — let the column default win
    // for new inserts; merge-duplicates leaves existing rows alone.
  }));

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 10_000);
  try {
    const res = await fetch(
      `${env.url}/rest/v1/agent_heartbeats?on_conflict=agent_id`,
      {
        method: 'POST',
        headers: {
          apikey: env.key,
          Authorization: `Bearer ${env.key}`,
          'Content-Type': 'application/json',
          Prefer: 'resolution=merge-duplicates,return=minimal',
        },
        body: JSON.stringify(payload),
        signal: ctrl.signal,
      },
    );
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Supabase upsert failed: ${res.status} ${body}`);
    }
  } finally {
    clearTimeout(timer);
  }
}

async function main(): Promise<void> {
  const now = Math.floor(Date.now() / 1000);

  if (!fs.existsSync(DB_PATH)) {
    console.error(`✗ DB not found: ${DB_PATH}`);
    process.exit(1);
  }

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  // Pre-read existing rows so we can preserve last_heartbeat when offline
  // and created_at across writes.
  const priorRows = db
    .prepare(`SELECT agent_id, last_heartbeat, created_at FROM agent_heartbeats`)
    .all() as { agent_id: string; last_heartbeat: number | null; created_at: number | null }[];
  const priorByAgent = new Map(priorRows.map((r) => [r.agent_id, r]));

  const rows: HeartbeatRow[] = [];
  for (const spec of AGENTS) {
    const live = checkLiveness(spec.pidFile);
    const task = live.alive ? findCurrentTask(db, spec.id) : null;
    const prior = priorByAgent.get(spec.id) ?? null;
    rows.push(buildRow(spec, live, task, prior, now));
  }

  // SQLite upsert. ON CONFLICT preserves created_at via the prior read,
  // and we always overwrite the rest.
  const upsert = db.prepare(`
    INSERT INTO agent_heartbeats (
      agent_id, agent_name, status, model, current_task, current_task_started_at,
      session_id, tokens_used_session, tokens_used_today, cost_today_usd,
      queue, metadata, last_heartbeat, created_at, updated_at
    ) VALUES (
      @agent_id, @agent_name, @status, NULL, @current_task, @current_task_started_at,
      NULL, 0, 0, 0,
      '[]', @metadata, @last_heartbeat, @created_at, @updated_at
    )
    ON CONFLICT(agent_id) DO UPDATE SET
      agent_name              = excluded.agent_name,
      status                  = excluded.status,
      current_task            = excluded.current_task,
      current_task_started_at = excluded.current_task_started_at,
      metadata                = excluded.metadata,
      last_heartbeat          = excluded.last_heartbeat,
      updated_at              = excluded.updated_at
  `);
  const txn = db.transaction((batch: HeartbeatRow[]) => {
    for (const r of batch) upsert.run(r);
  });
  txn(rows);
  db.close();

  // Console summary — captured by /tmp/heartbeat-writer.log.
  console.log(`[${new Date().toISOString()}] heartbeat writer:`);
  for (const r of rows) {
    console.log(
      `  ${r.agent_id.padEnd(10)} ${r.status.padEnd(8)} ` +
        `pid=${(JSON.parse(r.metadata).pid ?? '-').toString().padEnd(6)} ` +
        `last_alive=${new Date(r.last_heartbeat * 1000).toISOString()}`,
    );
  }

  // Mirror to Supabase. SQLite is the source of truth — Supabase failure
  // must not crash the writer.
  const env = readEnvFiles();
  if (!env) {
    console.warn('  Supabase env missing (SUPABASE_URL / SERVICE_KEY); SQLite-only this cycle.');
    return;
  }
  try {
    await pushToSupabase(rows, env);
    console.log(`  ✓ Supabase upsert ok (${rows.length} rows)`);
  } catch (err) {
    console.warn(`  ✗ Supabase upsert failed: ${err instanceof Error ? err.message : err}`);
  }
}

main().catch((err) => {
  console.error('write-agent-heartbeats fatal:', err);
  process.exit(1);
});
