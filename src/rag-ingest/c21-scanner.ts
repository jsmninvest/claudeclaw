/**
 * C21 lender email scanner — ingests lender program / AE emails from the
 * Century 21 Outlook MCP into the loan-atlas Pinecone index.
 *
 * Pipeline:
 *   1. Iterate emails via c21 MCP (`c21_list_emails` / `c21_search_emails`),
 *      batchSize at a time, optionally filtered by `senderFilters`,
 *      `keywordFilters`, and `since`.
 *   2. Heuristic skip pass — drop emails that obviously are not lender
 *      program / AE content (out-of-office, internal noise, marketing
 *      newsletters without structured data).
 *   3. Sonnet-normalise the remaining emails into structured records:
 *      { lender, program, ae_name, ae_email, min_fico, max_ltv,
 *        min_loan_amount, accepts_modular, accepts_leased_land,
 *        ca_eligible, source_email_id, source_date, summary, kind }.
 *      `kind` routes the record to the `lender-programs` or `lender-aes`
 *      namespace.
 *   4. Deterministic id — `lender-c21-{sha1(email_id + ':' + program_slug)}`.
 *      This guarantees the same email + program collapses to the same
 *      Pinecone vector on re-ingest (dedup for free).
 *   5. Upsert via `upsertLoanAtlas`, batched by namespace.
 *   6. Checkpoint every batch to the `rag_ingest_runs` table so
 *      `--resume <run_id>` continues past the last successful email id.
 *
 * This module does NOT import the MCP directly — all I/O is injected via
 * `ScanDeps` so tests can stub c21, the normaliser, the upsert sink, and
 * the checkpoint store without touching the network.
 */

import crypto from 'crypto';

export interface C21EmailSummary {
  id: string;
  from: string;
  subject: string;
  date: string;     // ISO string
  snippet?: string;
}

export interface C21EmailFull extends C21EmailSummary {
  body: string;
}

export interface C21SearchParams {
  senderFilters?: string[];
  keywordFilters?: string[];
  since?: string;
  batchSize?: number;
  pageToken?: string;
}

export interface C21SearchPage {
  emails: C21EmailSummary[];
  nextPageToken?: string;
}

/**
 * Thin adapter over the c21 MCP tools. Production wiring calls the MCP via
 * the Claude agent SDK; tests inject a fake that returns fixture pages.
 */
export interface C21Client {
  searchEmails(params: C21SearchParams): Promise<C21SearchPage>;
  readEmail(id: string): Promise<C21EmailFull>;
}

/**
 * Record extracted from a single email. `kind` decides the target namespace.
 */
export interface NormalizedLenderRecord {
  lender: string;
  program: string | null;
  ae_name: string | null;
  ae_email: string | null;
  min_fico: number | null;
  max_ltv: number | null;
  min_loan_amount: number | null;
  accepts_modular: boolean | null;
  accepts_leased_land: boolean | null;
  ca_eligible: boolean | null;
  source_email_id: string;
  source_date: string;
  summary: string;
  kind: 'program' | 'ae';
}

/**
 * Sonnet-backed normaliser. Returns 0..N records per email (an email may
 * announce a program + an AE, so multiple records is normal).
 */
export interface EmailNormalizer {
  (email: C21EmailFull): Promise<NormalizedLenderRecord[]>;
}

export interface UpsertRecordShape {
  id: string;
  text: string;
  metadata: Record<string, unknown>;
  namespace?: string;
}

export interface UpsertSink {
  (records: UpsertRecordShape[]): Promise<void>;
}

/**
 * Heuristic skip — returns true if we should drop the email before
 * paying for Sonnet normalisation. Kept exported for testability.
 */
export function shouldSkipEmail(email: C21EmailSummary): boolean {
  const subj = (email.subject ?? '').toLowerCase();
  const from = (email.from ?? '').toLowerCase();
  const snippet = (email.snippet ?? '').toLowerCase();

  // Auto-replies and internal chatter.
  if (/^(re:\s*)*(out of office|ooo|automatic reply|auto[- ]reply)/i.test(subj)) return true;
  if (/no[- ]reply|donotreply|noreply/.test(from)) return false; // still ingest broadcast blasts
  if (/unsubscribe successful|delivery status notification|undeliverable/i.test(subj)) return true;
  if (subj.length === 0 && snippet.length === 0) return true;

  // Newsletters without structured rate/program content tend to lead with
  // purely marketing copy; skip when we see obvious non-lender signal.
  if (/webinar invite|free lunch|holiday greetings|happy (birthday|holidays)/.test(subj)) return true;

  return false;
}

export interface CheckpointStore {
  /** Create a new run row (status='running'). */
  beginRun(runId: string, namespace: string | null, startedAt: number): void;
  /** Persist batch-level progress + last processed email id. */
  updateProgress(
    runId: string,
    patch: Partial<{
      emails_seen: number;
      emails_kept: number;
      emails_skipped: number;
      errors: number;
      last_email_id: string;
    }>,
  ): void;
  /** Mark the run terminal. */
  finishRun(
    runId: string,
    status: 'completed' | 'failed' | 'partial',
    completedAt: number,
  ): void;
  /** Load an existing run so --resume can pick up where it stopped. */
  loadRun(runId: string): {
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
  } | null;
}

export interface ScanOptions {
  senderFilters?: string[];
  keywordFilters?: string[];
  since?: string;
  batchSize?: number;     // emails per c21 page. default 25.
  dryRun?: boolean;
  /** Resume an existing run instead of creating a new one. */
  resumeRunId?: string;
  /** Override the default namespace routing. */
  namespaces?: { programs: string; aes: string };
  /** Supply an explicit run id; otherwise one is generated. */
  runId?: string;
  /** Called after every batch for progress reporting. */
  onBatch?: (report: IngestReport) => void;
}

export interface ScanDeps {
  c21: C21Client;
  normalize: EmailNormalizer;
  upsert: UpsertSink;
  checkpoint: CheckpointStore;
  now?: () => number;    // epoch seconds, for deterministic tests
  makeRunId?: () => string;
  sleep?: (ms: number) => Promise<void>;
}

export interface IngestReport {
  runId: string;
  namespace: string;
  emailsSeen: number;
  emailsKept: number;
  emailsSkipped: number;
  programsUpserted: number;
  aesUpserted: number;
  errors: number;
  lastEmailId: string | null;
  status: 'running' | 'completed' | 'failed' | 'partial';
  dryRun: boolean;
}

const DEFAULT_NS = { programs: 'lender-programs', aes: 'lender-aes' };
const DEFAULT_BATCH = 25;
const MAX_EMAILS_PER_SEC = 10;

/** lowercase + whitespace→- + strip non-word. Stable across re-ingests. */
export function slugify(s: string | null | undefined): string {
  if (!s) return 'none';
  return s
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    || 'none';
}

/**
 * Deterministic Pinecone id. Same (email_id, program_slug) → same id.
 */
export function makeRecordId(
  emailId: string,
  programSlug: string,
): string {
  const hash = crypto
    .createHash('sha1')
    .update(`${emailId}:${programSlug}`)
    .digest('hex');
  return `lender-c21-${hash}`;
}

function recordText(r: NormalizedLenderRecord): string {
  // What gets embedded. Keep it dense with the fields a retrieval query
  // would key off of: lender + program + constraints + summary.
  const parts: string[] = [];
  parts.push(`Lender: ${r.lender}`);
  if (r.program) parts.push(`Program: ${r.program}`);
  if (r.ae_name) parts.push(`AE: ${r.ae_name}`);
  if (r.ae_email) parts.push(`AE email: ${r.ae_email}`);
  if (r.min_fico !== null) parts.push(`Min FICO: ${r.min_fico}`);
  if (r.max_ltv !== null) parts.push(`Max LTV: ${r.max_ltv}`);
  if (r.min_loan_amount !== null) parts.push(`Min loan: $${r.min_loan_amount}`);
  if (r.accepts_modular !== null) parts.push(`Modular: ${r.accepts_modular ? 'yes' : 'no'}`);
  if (r.accepts_leased_land !== null) parts.push(`Leased land: ${r.accepts_leased_land ? 'yes' : 'no'}`);
  if (r.ca_eligible !== null) parts.push(`CA eligible: ${r.ca_eligible ? 'yes' : 'no'}`);
  parts.push(`Summary: ${r.summary}`);
  return parts.join('\n');
}

function recordMetadata(r: NormalizedLenderRecord): Record<string, unknown> {
  // Pinecone metadata must be JSON-serialisable primitives or arrays of same.
  const m: Record<string, unknown> = {
    lender: r.lender,
    source_email_id: r.source_email_id,
    source_date: r.source_date,
    kind: r.kind,
    ingested_by: 'c21-scanner',
  };
  if (r.program) m.program = r.program;
  if (r.ae_name) m.ae_name = r.ae_name;
  if (r.ae_email) m.ae_email = r.ae_email;
  if (r.min_fico !== null) m.min_fico = r.min_fico;
  if (r.max_ltv !== null) m.max_ltv = r.max_ltv;
  if (r.min_loan_amount !== null) m.min_loan_amount = r.min_loan_amount;
  if (r.accepts_modular !== null) m.accepts_modular = r.accepts_modular;
  if (r.accepts_leased_land !== null) m.accepts_leased_land = r.accepts_leased_land;
  if (r.ca_eligible !== null) m.ca_eligible = r.ca_eligible;
  return m;
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function defaultNow(): number {
  return Math.floor(Date.now() / 1000);
}

function defaultMakeRunId(): string {
  const ts = Date.now();
  const rand = crypto.randomBytes(4).toString('hex');
  return `c21-${ts}-${rand}`;
}

/**
 * Scan lender emails in the c21 mailbox and upsert normalised records into
 * the loan-atlas Pinecone index. See module doc for the full pipeline.
 *
 * Returns an IngestReport with per-run counts. The run row in
 * rag_ingest_runs is updated after each batch so --resume is always safe.
 */
export async function scanLenderEmails(
  opts: ScanOptions,
  deps: ScanDeps,
): Promise<IngestReport> {
  const batchSize = opts.batchSize ?? DEFAULT_BATCH;
  const namespaces = opts.namespaces ?? DEFAULT_NS;
  const now = deps.now ?? defaultNow;
  const makeRunId = deps.makeRunId ?? defaultMakeRunId;
  const sleep = deps.sleep ?? defaultSleep;

  // Resolve run id + resume state.
  let runId: string;
  let resumeFromEmailId: string | null = null;
  let seen = 0;
  let kept = 0;
  let skipped = 0;
  let errors = 0;
  let lastEmailId: string | null = null;

  if (opts.resumeRunId) {
    const existing = deps.checkpoint.loadRun(opts.resumeRunId);
    if (!existing) {
      throw new Error(
        `Cannot resume: run ${opts.resumeRunId} not found in rag_ingest_runs.`,
      );
    }
    if (existing.status === 'completed') {
      throw new Error(
        `Cannot resume: run ${opts.resumeRunId} is already completed.`,
      );
    }
    runId = existing.run_id;
    resumeFromEmailId = existing.last_email_id;
    seen = existing.emails_seen;
    kept = existing.emails_kept;
    skipped = existing.emails_skipped;
    errors = existing.errors;
    lastEmailId = existing.last_email_id;
  } else {
    runId = opts.runId ?? makeRunId();
    const ns = `${namespaces.programs},${namespaces.aes}`;
    deps.checkpoint.beginRun(runId, ns, now());
  }

  let programsUpserted = 0;
  let aesUpserted = 0;
  let status: IngestReport['status'] = 'running';
  let pageToken: string | undefined;
  let skipUntilSeen = resumeFromEmailId !== null;

  const mkReport = (): IngestReport => ({
    runId,
    namespace: `${namespaces.programs},${namespaces.aes}`,
    emailsSeen: seen,
    emailsKept: kept,
    emailsSkipped: skipped,
    programsUpserted,
    aesUpserted,
    errors,
    lastEmailId,
    status,
    dryRun: !!opts.dryRun,
  });

  try {
    // One outer loop per c21 search page; inner loop processes the emails.
    // After every page we checkpoint + rate-limit so the watchdog can't
    // catch us mid-batch without a durable resume point.
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const page = await deps.c21.searchEmails({
        senderFilters: opts.senderFilters,
        keywordFilters: opts.keywordFilters,
        since: opts.since,
        batchSize,
        pageToken,
      });

      if (page.emails.length === 0) break;

      const recordsForBatch: UpsertRecordShape[] = [];

      for (const summary of page.emails) {
        // Resume: skip everything up to and including last_email_id.
        if (skipUntilSeen) {
          if (summary.id === resumeFromEmailId) skipUntilSeen = false;
          continue;
        }

        seen += 1;
        lastEmailId = summary.id;

        if (shouldSkipEmail(summary)) {
          skipped += 1;
          continue;
        }

        let full: C21EmailFull;
        try {
          full = await deps.c21.readEmail(summary.id);
        } catch (err) {
          errors += 1;
          continue;
        }

        let extracted: NormalizedLenderRecord[];
        try {
          extracted = await deps.normalize(full);
        } catch (err) {
          errors += 1;
          continue;
        }

        if (extracted.length === 0) {
          skipped += 1;
          continue;
        }

        kept += 1;
        for (const rec of extracted) {
          const id = makeRecordId(rec.source_email_id, slugify(rec.program ?? rec.ae_email ?? rec.lender));
          const ns = rec.kind === 'ae' ? namespaces.aes : namespaces.programs;
          if (rec.kind === 'ae') aesUpserted += 1;
          else programsUpserted += 1;
          recordsForBatch.push({
            id,
            text: recordText(rec),
            metadata: recordMetadata(rec),
            namespace: ns,
          });
        }

        // Rate limit ≤ MAX_EMAILS_PER_SEC between email reads.
        await sleep(Math.floor(1000 / MAX_EMAILS_PER_SEC));
      }

      // One upsert per batch (skipped entirely in dry-run).
      if (recordsForBatch.length > 0 && !opts.dryRun) {
        try {
          await deps.upsert(recordsForBatch);
        } catch (err) {
          errors += recordsForBatch.length;
          // Don't promote the last_email_id past a failed upsert — let
          // resume re-process this batch.
          status = 'partial';
        }
      }

      deps.checkpoint.updateProgress(runId, {
        emails_seen: seen,
        emails_kept: kept,
        emails_skipped: skipped,
        errors,
        last_email_id: lastEmailId ?? undefined,
      });

      if (opts.onBatch) opts.onBatch(mkReport());

      if (!page.nextPageToken) break;
      pageToken = page.nextPageToken;
    }

    status = status === 'partial' ? 'partial' : 'completed';
    deps.checkpoint.finishRun(runId, status as 'completed' | 'partial', now());
    return mkReport();
  } catch (err) {
    status = 'failed';
    deps.checkpoint.finishRun(runId, 'failed', now());
    const report = mkReport();
    // Re-throw with the report attached so the CLI can print it.
    (err as Error & { report?: IngestReport }).report = report;
    throw err;
  }
}
