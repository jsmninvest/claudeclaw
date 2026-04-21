import { describe, it, expect, vi, beforeEach } from 'vitest';

import {
  scanLenderEmails,
  shouldSkipEmail,
  makeRecordId,
  slugify,
  type C21Client,
  type C21EmailFull,
  type C21EmailSummary,
  type CheckpointStore,
  type NormalizedLenderRecord,
  type UpsertSink,
  type EmailNormalizer,
} from './c21-scanner.js';

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

function makeEmail(
  id: string,
  overrides: Partial<C21EmailSummary> = {},
): C21EmailSummary {
  return {
    id,
    from: `ae-${id}@somelender.com`,
    subject: `Lender Program Update ${id}`,
    date: '2026-03-01T10:00:00Z',
    snippet: 'New DSCR program details enclosed.',
    ...overrides,
  };
}

function makeFull(id: string, overrides: Partial<C21EmailFull> = {}): C21EmailFull {
  return {
    ...makeEmail(id),
    body: 'Body of email describing a program',
    ...overrides,
  };
}

function makeProgramRecord(
  emailId: string,
  program: string,
  lender = 'AcmeLend',
): NormalizedLenderRecord {
  return {
    lender,
    program,
    ae_name: null,
    ae_email: null,
    min_fico: 680,
    max_ltv: 75,
    min_loan_amount: 150_000,
    accepts_modular: false,
    accepts_leased_land: false,
    ca_eligible: true,
    source_email_id: emailId,
    source_date: '2026-03-01',
    summary: `${program} at ${lender}`,
    kind: 'program',
  };
}

function makeAeRecord(
  emailId: string,
  ae_email: string,
  lender = 'AcmeLend',
): NormalizedLenderRecord {
  return {
    lender,
    program: null,
    ae_name: 'Jane AE',
    ae_email,
    min_fico: null,
    max_ltv: null,
    min_loan_amount: null,
    accepts_modular: null,
    accepts_leased_land: null,
    ca_eligible: true,
    source_email_id: emailId,
    source_date: '2026-03-01',
    summary: `${ae_email} covers CA`,
    kind: 'ae',
  };
}

function makeC21Client(pages: Array<{ emails: C21EmailSummary[]; nextPageToken?: string }>): C21Client {
  let call = 0;
  return {
    searchEmails: vi.fn(async () => {
      const page = pages[call] ?? { emails: [] };
      call += 1;
      return page;
    }),
    readEmail: vi.fn(async (id: string) => makeFull(id)),
  };
}

function makeCheckpointStore(): CheckpointStore & {
  rows: Map<string, {
    run_id: string;
    started_at: number;
    completed_at: number | null;
    namespace: string | null;
    emails_seen: number;
    emails_kept: number;
    emails_skipped: number;
    errors: number;
    last_email_id: string | null;
    status: string;
  }>;
} {
  const rows = new Map<string, {
    run_id: string;
    started_at: number;
    completed_at: number | null;
    namespace: string | null;
    emails_seen: number;
    emails_kept: number;
    emails_skipped: number;
    errors: number;
    last_email_id: string | null;
    status: string;
  }>();
  return {
    rows,
    beginRun(runId, namespace, startedAt) {
      rows.set(runId, {
        run_id: runId,
        started_at: startedAt,
        completed_at: null,
        namespace,
        emails_seen: 0,
        emails_kept: 0,
        emails_skipped: 0,
        errors: 0,
        last_email_id: null,
        status: 'running',
      });
    },
    updateProgress(runId, patch) {
      const row = rows.get(runId);
      if (!row) return;
      if (patch.emails_seen !== undefined) row.emails_seen = patch.emails_seen;
      if (patch.emails_kept !== undefined) row.emails_kept = patch.emails_kept;
      if (patch.emails_skipped !== undefined) row.emails_skipped = patch.emails_skipped;
      if (patch.errors !== undefined) row.errors = patch.errors;
      if (patch.last_email_id !== undefined) row.last_email_id = patch.last_email_id;
    },
    finishRun(runId, status, completedAt) {
      const row = rows.get(runId);
      if (!row) return;
      row.status = status;
      row.completed_at = completedAt;
    },
    loadRun(runId) {
      return rows.get(runId) ?? null;
    },
  };
}

const nowSec = () => 1_700_000_000;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('scanLenderEmails', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('batches across multiple c21 pages and checkpoints after each batch', async () => {
    const c21 = makeC21Client([
      { emails: [makeEmail('a'), makeEmail('b')], nextPageToken: 'p2' },
      { emails: [makeEmail('c')], nextPageToken: undefined },
    ]);
    const normalize: EmailNormalizer = vi.fn(async (email) => [
      makeProgramRecord(email.id, `Prog-${email.id}`),
    ]);
    const upsert: UpsertSink = vi.fn(async () => {});
    const checkpoint = makeCheckpointStore();

    const report = await scanLenderEmails(
      { batchSize: 25, runId: 'run-batch' },
      {
        c21,
        normalize,
        upsert,
        checkpoint,
        now: nowSec,
        sleep: async () => {},
      },
    );

    expect(c21.searchEmails).toHaveBeenCalledTimes(2);
    expect(upsert).toHaveBeenCalledTimes(2); // one upsert per non-empty page
    expect(report.emailsSeen).toBe(3);
    expect(report.emailsKept).toBe(3);
    expect(report.programsUpserted).toBe(3);
    expect(report.status).toBe('completed');
    const row = checkpoint.loadRun('run-batch')!;
    expect(row.status).toBe('completed');
    expect(row.emails_seen).toBe(3);
    expect(row.last_email_id).toBe('c');
  });

  it('skips heuristic-dropped emails without calling the normalizer', async () => {
    const emails = [
      makeEmail('keep-1'),
      makeEmail('ooo-1', { subject: 'Out of Office: back Monday' }),
      makeEmail('ndr-1', { subject: 'Undeliverable: your message' }),
      makeEmail('keep-2'),
    ];
    const c21 = makeC21Client([{ emails }]);
    const normalize: EmailNormalizer = vi.fn(async (email) => [
      makeProgramRecord(email.id, 'Prog'),
    ]);
    const upsert: UpsertSink = vi.fn(async () => {});
    const checkpoint = makeCheckpointStore();

    const report = await scanLenderEmails(
      { batchSize: 10, runId: 'run-skip' },
      { c21, normalize, upsert, checkpoint, now: nowSec, sleep: async () => {} },
    );

    expect(normalize).toHaveBeenCalledTimes(2);
    expect(report.emailsSeen).toBe(4);
    expect(report.emailsKept).toBe(2);
    expect(report.emailsSkipped).toBe(2);
    // Standalone unit assertion for the heuristic itself.
    expect(shouldSkipEmail(makeEmail('x', { subject: 'Out of Office' }))).toBe(true);
    expect(shouldSkipEmail(makeEmail('y', { subject: 'New DSCR Rates' }))).toBe(false);
  });

  it('produces deterministic ids — same (email,program) collapses', async () => {
    const emails = [makeEmail('email-1'), makeEmail('email-2')];
    const c21 = makeC21Client([{ emails }]);
    const normalize: EmailNormalizer = vi.fn(async (email) => [
      makeProgramRecord(email.id, 'DSCR Premier'),
    ]);
    const captured: Array<Array<{ id: string; namespace?: string }>> = [];
    const upsert: UpsertSink = vi.fn(async (records: Array<{ id: string; namespace?: string }>) => {
      captured.push(records.map((r) => ({ id: r.id, namespace: r.namespace })));
    });
    const checkpoint = makeCheckpointStore();

    await scanLenderEmails(
      { batchSize: 10, runId: 'run-dedup' },
      { c21, normalize, upsert, checkpoint, now: nowSec, sleep: async () => {} },
    );

    const flat = captured.flat();
    expect(flat).toHaveLength(2);
    // Direct-function confirmation: same inputs → same id.
    expect(makeRecordId('email-1', slugify('DSCR Premier'))).toBe(flat[0].id);
    expect(flat[0].id).not.toBe(flat[1].id);
    // And the id format is stable + prefixed.
    expect(flat[0].id).toMatch(/^lender-c21-[0-9a-f]{40}$/);
  });

  it('routes program records to lender-programs and AE records to lender-aes', async () => {
    const c21 = makeC21Client([{ emails: [makeEmail('multi-1')] }]);
    const normalize: EmailNormalizer = vi.fn(async (email) => [
      makeProgramRecord(email.id, 'DSCR A'),
      makeAeRecord(email.id, 'jane@acme.com'),
    ]);
    const captured: Array<Array<{ id: string; namespace?: string }>> = [];
    const upsert: UpsertSink = vi.fn(async (records: Array<{ id: string; namespace?: string }>) => {
      captured.push(records.map((r) => ({ id: r.id, namespace: r.namespace })));
    });
    const checkpoint = makeCheckpointStore();

    const report = await scanLenderEmails(
      { batchSize: 5, runId: 'run-route' },
      { c21, normalize, upsert, checkpoint, now: nowSec, sleep: async () => {} },
    );

    const flat = captured.flat();
    const programHit = flat.find((r) => r.namespace === 'lender-programs');
    const aeHit = flat.find((r) => r.namespace === 'lender-aes');
    expect(programHit).toBeDefined();
    expect(aeHit).toBeDefined();
    expect(report.programsUpserted).toBe(1);
    expect(report.aesUpserted).toBe(1);
  });

  it('dry-run mode never upserts and marks the run completed', async () => {
    const c21 = makeC21Client([{ emails: [makeEmail('dry-1'), makeEmail('dry-2')] }]);
    const normalize: EmailNormalizer = vi.fn(async (email) => [
      makeProgramRecord(email.id, 'Prog'),
    ]);
    const upsert: UpsertSink = vi.fn(async () => {});
    const checkpoint = makeCheckpointStore();

    const report = await scanLenderEmails(
      { batchSize: 10, dryRun: true, runId: 'run-dry' },
      { c21, normalize, upsert, checkpoint, now: nowSec, sleep: async () => {} },
    );

    expect(upsert).not.toHaveBeenCalled();
    expect(report.dryRun).toBe(true);
    expect(report.emailsKept).toBe(2);
    expect(report.programsUpserted).toBe(2); // counted as intended upserts
    expect(report.status).toBe('completed');
  });

  it('resumes from the saved last_email_id instead of re-processing it', async () => {
    const emails = [makeEmail('a'), makeEmail('b'), makeEmail('c'), makeEmail('d')];
    const c21 = makeC21Client([{ emails }]);
    const normalize: EmailNormalizer = vi.fn(async (email) => [
      makeProgramRecord(email.id, 'Prog'),
    ]);
    const upsert: UpsertSink = vi.fn(async () => {});
    const checkpoint = makeCheckpointStore();

    // Seed the checkpoint store as if a prior window processed a + b.
    checkpoint.beginRun('run-resume', 'lender-programs,lender-aes', nowSec());
    checkpoint.updateProgress('run-resume', {
      emails_seen: 2,
      emails_kept: 2,
      emails_skipped: 0,
      errors: 0,
      last_email_id: 'b',
    });

    const report = await scanLenderEmails(
      { batchSize: 10, resumeRunId: 'run-resume' },
      { c21, normalize, upsert, checkpoint, now: nowSec, sleep: async () => {} },
    );

    // Only c + d should have been processed; normalize called twice.
    expect(normalize).toHaveBeenCalledTimes(2);
    expect(report.emailsSeen).toBe(4); // 2 from prior window + 2 new
    expect(report.emailsKept).toBe(4);
    expect(report.lastEmailId).toBe('d');
    expect(report.status).toBe('completed');
    expect(checkpoint.loadRun('run-resume')!.status).toBe('completed');
  });

  it('rejects resume when the target run id is unknown', async () => {
    const c21 = makeC21Client([{ emails: [] }]);
    const normalize: EmailNormalizer = vi.fn(async () => []);
    const upsert: UpsertSink = vi.fn(async () => {});
    const checkpoint = makeCheckpointStore();

    await expect(
      scanLenderEmails(
        { resumeRunId: 'does-not-exist' },
        { c21, normalize, upsert, checkpoint, now: nowSec, sleep: async () => {} },
      ),
    ).rejects.toThrow(/not found/);
  });
});
