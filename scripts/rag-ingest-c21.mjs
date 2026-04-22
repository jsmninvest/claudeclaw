#!/usr/bin/env node
/**
 * rag-ingest-c21 — CLI wrapper around scanLenderEmails().
 *
 * Ingests lender program / AE emails from the C21 Outlook MCP into the
 * loan-atlas Pinecone index (1024-dim, OpenAI text-embedding-3-large).
 * Safe for long runs because progress checkpoints after every batch to
 * the `rag_ingest_runs` table; pass --resume <run_id> to continue.
 *
 * Usage:
 *   node scripts/rag-ingest-c21.mjs --since 2026-02-20 --batch-size 25
 *   node scripts/rag-ingest-c21.mjs --resume c21-17000000000-abcd1234
 *   node scripts/rag-ingest-c21.mjs --dry-run --since 2026-03-01
 *
 * Flags:
 *   --since DATE       ISO date (YYYY-MM-DD) — only scan emails since this date
 *   --batch-size N     Emails per c21 search page (default 25)
 *   --dry-run          Run the pipeline but never upsert
 *   --resume RUN_ID    Continue an existing rag_ingest_runs row
 *   --help, -h         Show this message
 *
 * Exit codes:
 *   0  success
 *   1  bad flag / arg
 *   2  runtime error (ingest failure, missing build, missing env)
 *
 * NOTE: This module intentionally does NOT auto-wire the c21 MCP. Binding
 * the Claude agent SDK to the MCP tools (c21_search_emails, c21_read_email,
 * c21_list_emails) happens in a follow-up task. For now, running the CLI
 * without a wired adapter exits 2 with a helpful message.
 */
import { fileURLToPath } from 'node:url';
import path from 'node:path';

function parseArgs(argv) {
  const args = {
    since: undefined,
    batchSize: 25,
    dryRun: false,
    resume: undefined,
    help: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    switch (a) {
      case '--since':
        args.since = argv[++i];
        break;
      case '--batch-size':
      case '--batch': {
        const n = Number(argv[++i]);
        if (!Number.isFinite(n) || n <= 0) {
          console.error(`Error: --batch-size must be a positive integer.`);
          process.exit(1);
        }
        args.batchSize = n;
        break;
      }
      case '--dry-run':
        args.dryRun = true;
        break;
      case '--resume':
        args.resume = argv[++i];
        if (!args.resume) {
          console.error('Error: --resume requires a run id.');
          process.exit(1);
        }
        break;
      case '--help':
      case '-h':
        args.help = true;
        break;
      default:
        console.error(`Unknown flag: ${a}`);
        printUsage();
        process.exit(1);
    }
  }
  return args;
}

function printUsage() {
  process.stdout.write(
    [
      'Usage: node scripts/rag-ingest-c21.mjs [options]',
      '',
      'Options:',
      '  --since DATE        ISO date (YYYY-MM-DD); only ingest emails since this date',
      '  --batch-size N      Emails per c21 search page (default 25)',
      '  --dry-run           Run the pipeline but never upsert to Pinecone',
      '  --resume RUN_ID     Continue an existing rag_ingest_runs row',
      '  -h, --help          Show this message',
      '',
      'Examples:',
      '  node scripts/rag-ingest-c21.mjs --since 2026-02-20 --batch-size 25',
      '  node scripts/rag-ingest-c21.mjs --resume c21-1700000000-abcd1234',
      '  node scripts/rag-ingest-c21.mjs --dry-run --since 2026-03-01',
      '',
    ].join('\n'),
  );
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    process.exit(0);
  }

  // Lazy-import the compiled helpers. The helper paths resolve relative to
  // dist/ so the CLI can run without tsx.
  const here = path.dirname(fileURLToPath(import.meta.url));
  const distRoot = path.resolve(here, '..', 'dist');
  const scannerUrl = `file://${path.join(distRoot, 'rag-ingest', 'c21-scanner.js')}`;
  const ragUrl = `file://${path.join(distRoot, 'loan-atlas-rag.js')}`;
  const dbUrl = `file://${path.join(distRoot, 'db.js')}`;

  let scanner;
  let rag;
  let db;
  try {
    scanner = await import(scannerUrl);
    rag = await import(ragUrl);
    db = await import(dbUrl);
  } catch (err) {
    if (err && err.code === 'ERR_MODULE_NOT_FOUND') {
      console.error(
        'rag-ingest-c21: compiled helpers missing. Run `npm run build` first.',
      );
      process.exit(2);
    }
    throw err;
  }

  // Wire the SQLite-backed checkpoint store.
  db.initDatabase();
  const checkpoint = {
    beginRun: db.createRagIngestRun,
    updateProgress: db.updateRagIngestProgress,
    finishRun: db.finishRagIngestRun,
    loadRun: db.getRagIngestRun,
  };

  // The c21 MCP adapter is not wired into plain node yet — surface a
  // clear error so operators know what's missing instead of silently
  // looping. The follow-up task will inject a real adapter.
  const c21Unwired = {
    async searchEmails() {
      throw new Error(
        'c21 MCP adapter is not wired into the CLI yet. ' +
          'See `src/rag-ingest/c21-scanner.ts` and pass a C21Client to scanLenderEmails() ' +
          'programmatically, or wait for the follow-up task that binds the MCP tools.',
      );
    },
    async readEmail() {
      throw new Error('c21 MCP adapter is not wired into the CLI yet.');
    },
  };

  const normalize = async () => {
    throw new Error(
      'Sonnet normaliser is not wired into the CLI yet. ' +
        'Provide a normalize dep via scanLenderEmails({...}, { normalize, ... }).',
    );
  };

  const upsert = async (records) => {
    await rag.upsertLoanAtlas(records);
  };

  let report;
  try {
    report = await scanner.scanLenderEmails(
      {
        since: args.since,
        batchSize: args.batchSize,
        dryRun: args.dryRun,
        resumeRunId: args.resume,
      },
      {
        c21: c21Unwired,
        normalize,
        upsert,
        checkpoint,
        onBatch: (r) => {
          process.stdout.write(
            `[batch] seen=${r.emailsSeen} kept=${r.emailsKept} skipped=${r.emailsSkipped} ` +
              `programs=${r.programsUpserted} aes=${r.aesUpserted} errors=${r.errors} ` +
              `last=${r.lastEmailId ?? '-'} status=${r.status}\n`,
          );
        },
      },
    );
  } catch (err) {
    console.error(`rag-ingest-c21 failed: ${err?.message ?? err}`);
    if (err?.report) {
      console.error(JSON.stringify(err.report, null, 2));
    }
    process.exit(2);
  }

  process.stdout.write('\nFinal report:\n');
  process.stdout.write(JSON.stringify(report, null, 2) + '\n');
}

main().catch((err) => {
  console.error(err?.stack ?? err);
  process.exit(2);
});
