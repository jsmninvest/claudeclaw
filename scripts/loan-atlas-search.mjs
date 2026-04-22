#!/usr/bin/env node
/**
 * loan-atlas-search — CLI wrapper around searchLoanAtlas().
 *
 * Queries the loan-atlas Pinecone index (1024-dim, cosine, no embed block) by
 * embedding the query client-side with OpenAI text-embedding-3-large. Used by
 * the research + s2l agents when the Pinecone MCP "Integrated inference is not
 * configured" error blocks them.
 *
 * Usage:
 *   node scripts/loan-atlas-search.mjs "FHA one time close vacant land" \
 *     --namespace lender-programs --topK 10
 *
 * Flags:
 *   --namespace NAME   Pinecone namespace (e.g. lender-programs)
 *   --topK N           Number of hits to return (default 10)
 *   --filter JSON      Metadata filter, JSON object (Pinecone syntax)
 *   --json             Emit JSON lines instead of the formatted table
 *
 * Exit codes:
 *   0  success
 *   1  missing query arg or bad flag
 *   2  config error (missing .env keys) or upstream error
 */
import { fileURLToPath } from 'node:url';
import path from 'node:path';

function parseArgs(argv) {
  const args = { query: '', namespace: undefined, topK: 10, filter: undefined, json: false };
  const positional = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--namespace' || a === '-n') {
      args.namespace = argv[++i];
    } else if (a === '--topK' || a === '-k') {
      args.topK = Number(argv[++i]);
    } else if (a === '--filter' || a === '-f') {
      try {
        args.filter = JSON.parse(argv[++i]);
      } catch (err) {
        console.error(`Invalid --filter JSON: ${err.message}`);
        process.exit(1);
      }
    } else if (a === '--json') {
      args.json = true;
    } else if (a === '--help' || a === '-h') {
      printUsage();
      process.exit(0);
    } else if (a.startsWith('--')) {
      console.error(`Unknown flag: ${a}`);
      printUsage();
      process.exit(1);
    } else {
      positional.push(a);
    }
  }
  args.query = positional.join(' ').trim();
  if (!args.query) {
    console.error('Error: query string is required.');
    printUsage();
    process.exit(1);
  }
  if (!Number.isFinite(args.topK) || args.topK <= 0) {
    console.error('Error: --topK must be a positive integer.');
    process.exit(1);
  }
  return args;
}

function printUsage() {
  console.error(
    `Usage: node scripts/loan-atlas-search.mjs "<query>" [--namespace NAME] [--topK N] [--filter JSON] [--json]`,
  );
}

function truncate(str, max) {
  if (!str) return '';
  return str.length > max ? str.slice(0, max - 1) + '…' : str;
}

function pickTitle(metadata) {
  if (!metadata) return '';
  for (const key of ['title', 'name', 'program_name', 'heading', 'source', 'url']) {
    const v = metadata[key];
    if (typeof v === 'string' && v) return v;
  }
  const text = metadata.text ?? metadata.chunk_text ?? metadata.content;
  if (typeof text === 'string') return truncate(text.replace(/\s+/g, ' '), 80);
  return '';
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  // Load the helper from compiled dist/. If the repo has not been built yet,
  // tell the user exactly what to run. We cannot import the .ts source from
  // plain node.
  const here = path.dirname(fileURLToPath(import.meta.url));
  const helperUrl = new URL(
    `file://${path.resolve(here, '..', 'dist', 'loan-atlas-rag.js')}`,
  );

  let searchLoanAtlas;
  try {
    ({ searchLoanAtlas } = await import(helperUrl.href));
  } catch (err) {
    if (err && err.code === 'ERR_MODULE_NOT_FOUND') {
      console.error(
        'loan-atlas-rag helper not built. Run `npm run build` first, or use `tsx src/loan-atlas-rag.ts` programmatically.',
      );
      process.exit(2);
    }
    throw err;
  }

  let hits;
  try {
    hits = await searchLoanAtlas(args.query, {
      namespace: args.namespace,
      topK: args.topK,
      filter: args.filter,
    });
  } catch (err) {
    console.error(`loan-atlas search failed: ${err?.message ?? err}`);
    process.exit(2);
  }

  if (args.json) {
    for (const h of hits) {
      process.stdout.write(JSON.stringify(h) + '\n');
    }
    return;
  }

  if (hits.length === 0) {
    console.log('(no matches)');
    return;
  }

  // Formatted table. Keep it readable in a terminal without extra deps.
  const header = `score    id                                        title`;
  console.log(header);
  console.log('-'.repeat(header.length));
  for (const h of hits) {
    const score = h.score.toFixed(4).padEnd(8);
    const id = truncate(h.id, 40).padEnd(42);
    const title = truncate(pickTitle(h.metadata), 80);
    console.log(`${score} ${id} ${title}`);
  }
}

main().catch((err) => {
  console.error(err?.stack ?? err);
  process.exit(2);
});
