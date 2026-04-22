/**
 * loan-atlas RAG helper.
 *
 * The loan-atlas Pinecone index was ingested with OpenAI text-embedding-3-large
 * (dimensions=1024) from an external pipeline. The index has NO `embed` block,
 * so Pinecone's integrated inference / MCP search-records does not work.
 *
 * This module closes the gap by embedding queries client-side with OpenAI, then
 * calling Pinecone's low-level vector `query` API. Research, s2l, and other
 * agents use this to pull from the 7,288-record mortgage knowledge base.
 *
 * Usage:
 *   const hits = await searchLoanAtlas('FHA one time close vacant land', {
 *     namespace: 'lender-programs',
 *     topK: 10,
 *   });
 */
import { readEnvFile } from './env.js';

// Constants pinned to how the index was populated. Do NOT change these without
// re-ingesting — mismatched dims or models will silently return garbage.
export const LOAN_ATLAS_INDEX = 'loan-atlas';
export const EMBEDDING_MODEL = 'text-embedding-3-large';
export const EMBEDDING_DIMENSIONS = 1024;
const OPENAI_EMBEDDINGS_URL = 'https://api.openai.com/v1/embeddings';
const FETCH_TIMEOUT_MS = 10_000;

export interface LoanAtlasSearchOptions {
  namespace?: string;
  topK?: number;
  filter?: Record<string, unknown>;
}

export interface LoanAtlasHit {
  id: string;
  score: number;
  metadata: Record<string, unknown>;
}

/**
 * Shape of the Pinecone client we depend on. Defined as an interface so tests
 * can inject a fake without pulling the real SDK into the mock graph.
 */
export interface PineconeLike {
  index(name: string): {
    query(options: {
      vector: number[];
      topK: number;
      includeMetadata?: boolean;
      namespace?: string;
      filter?: object;
    }): Promise<{
      matches?: Array<{
        id: string;
        score?: number;
        metadata?: Record<string, unknown>;
      }>;
    }>;
    namespace?: (name: string) => {
      upsert(
        vectors: Array<{
          id: string;
          values: number[];
          metadata?: Record<string, unknown>;
        }>,
      ): Promise<unknown>;
    };
    upsert?(
      vectors: Array<{
        id: string;
        values: number[];
        metadata?: Record<string, unknown>;
      }>,
      opts?: { namespace?: string },
    ): Promise<unknown>;
  };
}

/**
 * Dependencies callers can override for testing. In production, leave empty.
 */
export interface SearchDeps {
  embed?: (query: string, apiKey: string) => Promise<number[]>;
  pineconeFactory?: (apiKey: string) => PineconeLike;
  readEnv?: (keys: string[]) => Record<string, string>;
}

/**
 * Embed a single query string with OpenAI text-embedding-3-large at 1024 dims.
 * Throws on non-200 responses or malformed payloads.
 */
export async function embedQueryOpenAI(
  query: string,
  apiKey: string,
): Promise<number[]> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(OPENAI_EMBEDDINGS_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: EMBEDDING_MODEL,
        input: query,
        dimensions: EMBEDDING_DIMENSIONS,
      }),
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(
      `OpenAI embeddings request failed: ${res.status} ${res.statusText} ${body}`,
    );
  }

  const payload = (await res.json()) as {
    data?: Array<{ embedding?: number[] }>;
  };
  const embedding = payload.data?.[0]?.embedding;
  if (!Array.isArray(embedding) || embedding.length !== EMBEDDING_DIMENSIONS) {
    throw new Error(
      `OpenAI embedding response malformed: expected ${EMBEDDING_DIMENSIONS}-dim vector, ` +
        `got ${Array.isArray(embedding) ? embedding.length : 'none'}`,
    );
  }
  return embedding;
}

async function defaultPineconeFactory(apiKey: string): Promise<PineconeLike> {
  const mod = await import('@pinecone-database/pinecone');
  return new mod.Pinecone({ apiKey }) as unknown as PineconeLike;
}

/**
 * Query the loan-atlas Pinecone index with client-side embedding.
 *
 * @param query   Natural-language query string.
 * @param opts    topK (default 10), namespace, metadata filter.
 * @returns       Matches sorted by cosine similarity (desc). Always includes
 *                metadata — call sites typically need it.
 *
 * @throws If OPENAI_API_KEY or PINECONE_API_KEY is missing from .env, with a
 *         message telling the user exactly what to add.
 */
export async function searchLoanAtlas(
  query: string,
  opts: LoanAtlasSearchOptions = {},
  deps: SearchDeps = {},
): Promise<LoanAtlasHit[]> {
  const readEnv = deps.readEnv ?? readEnvFile;
  const env = readEnv(['OPENAI_API_KEY', 'PINECONE_API_KEY']);
  const openaiKey = env.OPENAI_API_KEY;
  const pineconeKey = env.PINECONE_API_KEY;

  const missing: string[] = [];
  if (!openaiKey) missing.push('OPENAI_API_KEY');
  if (!pineconeKey) missing.push('PINECONE_API_KEY');
  if (missing.length > 0) {
    throw new Error(
      `loan-atlas RAG is not configured. Add the following to .env: ` +
        `${missing.join(', ')}. ` +
        `OPENAI_API_KEY is used to embed queries with text-embedding-3-large ` +
        `(dims=1024) to match how the index was ingested. PINECONE_API_KEY is ` +
        `used to query the "${LOAN_ATLAS_INDEX}" index.`,
    );
  }

  const embed = deps.embed ?? embedQueryOpenAI;
  const vector = await embed(query, openaiKey);

  const factory =
    deps.pineconeFactory ??
    ((key: string) => {
      // Lazy-resolve the real SDK so tests that inject a factory don't need it.
      let client: PineconeLike | null = null;
      return {
        index(name: string) {
          return {
            async query(queryOpts) {
              if (!client) client = await defaultPineconeFactory(key);
              return client.index(name).query(queryOpts);
            },
          };
        },
      } as PineconeLike;
    });

  const pc = factory(pineconeKey);
  const index = pc.index(LOAN_ATLAS_INDEX);

  const topK = opts.topK ?? 10;
  const response = await index.query({
    vector,
    topK,
    includeMetadata: true,
    ...(opts.namespace ? { namespace: opts.namespace } : {}),
    ...(opts.filter ? { filter: opts.filter } : {}),
  });

  const matches = response.matches ?? [];
  return matches.map((m) => ({
    id: m.id,
    score: typeof m.score === 'number' ? m.score : 0,
    metadata: m.metadata ?? {},
  }));
}

// ---------------------------------------------------------------------------
// Upsert path
// ---------------------------------------------------------------------------

export interface LoanAtlasUpsertRecord {
  id: string;
  text: string;
  metadata?: Record<string, unknown>;
  namespace?: string;
}

export interface UpsertDeps {
  embedBatch?: (inputs: string[], apiKey: string) => Promise<number[][]>;
  pineconeFactory?: (apiKey: string) => PineconeLike;
  readEnv?: (keys: string[]) => Record<string, string>;
  sleep?: (ms: number) => Promise<void>;
}

export interface UpsertOptions {
  /** Embeddings per OpenAI API call. Default 100 (the documented maximum). */
  batchSize?: number;
  /** Default namespace applied when a record does not set one. */
  namespace?: string;
  /**
   * Rate-limit records upserted per second. Defaults to 10. Applied by
   * pacing the batches — if a batch would complete sooner than the budget
   * allows, we sleep the remainder before issuing the next one.
   */
  maxRecordsPerSecond?: number;
}

const DEFAULT_UPSERT_BATCH = 100;
const DEFAULT_RATE_LIMIT = 10; // records/sec

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Embed up to `inputs.length` strings in a single OpenAI embeddings call.
 * OpenAI accepts up to 2048 inputs per call; we cap externally at 100 for
 * safety + easier retry granularity.
 */
export async function embedBatchOpenAI(
  inputs: string[],
  apiKey: string,
): Promise<number[][]> {
  if (inputs.length === 0) return [];
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(OPENAI_EMBEDDINGS_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: EMBEDDING_MODEL,
        input: inputs,
        dimensions: EMBEDDING_DIMENSIONS,
      }),
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(
      `OpenAI batch embeddings request failed: ${res.status} ${res.statusText} ${body}`,
    );
  }

  const payload = (await res.json()) as {
    data?: Array<{ embedding?: number[]; index?: number }>;
  };
  const data = payload.data ?? [];
  if (data.length !== inputs.length) {
    throw new Error(
      `OpenAI batch response count mismatch: expected ${inputs.length}, got ${data.length}`,
    );
  }
  // Sort by returned index to guarantee alignment with `inputs`.
  const sorted = [...data].sort(
    (a, b) => (a.index ?? 0) - (b.index ?? 0),
  );
  return sorted.map((row, i) => {
    const v = row.embedding;
    if (!Array.isArray(v) || v.length !== EMBEDDING_DIMENSIONS) {
      throw new Error(
        `OpenAI batch embedding #${i} malformed: expected ${EMBEDDING_DIMENSIONS}-dim vector, ` +
          `got ${Array.isArray(v) ? v.length : 'none'}`,
      );
    }
    return v;
  });
}

/**
 * Embed `records.text` client-side with OpenAI text-embedding-3-large and
 * upsert to the loan-atlas Pinecone index. Records are grouped by namespace
 * and embedded in batches of `batchSize` (default 100).
 *
 * Rate limit: records/sec is capped via `maxRecordsPerSecond` (default 10)
 * by pacing the batch cadence — we sleep after each batch so the average
 * throughput stays at or below the cap. Use 0 to disable pacing in tests.
 */
export async function upsertLoanAtlas(
  records: LoanAtlasUpsertRecord[],
  opts: UpsertOptions = {},
  deps: UpsertDeps = {},
): Promise<void> {
  if (records.length === 0) return;

  const readEnv = deps.readEnv ?? readEnvFile;
  const env = readEnv(['OPENAI_API_KEY', 'PINECONE_API_KEY']);
  const openaiKey = env.OPENAI_API_KEY;
  const pineconeKey = env.PINECONE_API_KEY;
  const missing: string[] = [];
  if (!openaiKey) missing.push('OPENAI_API_KEY');
  if (!pineconeKey) missing.push('PINECONE_API_KEY');
  if (missing.length > 0) {
    throw new Error(
      `upsertLoanAtlas requires ${missing.join(', ')} in .env.`,
    );
  }

  const embedBatch = deps.embedBatch ?? embedBatchOpenAI;
  const sleep = deps.sleep ?? defaultSleep;
  const batchSize = Math.max(1, opts.batchSize ?? DEFAULT_UPSERT_BATCH);
  const ratePerSec =
    opts.maxRecordsPerSecond === undefined
      ? DEFAULT_RATE_LIMIT
      : opts.maxRecordsPerSecond;

  const factory =
    deps.pineconeFactory ??
    ((key: string) => {
      let client: PineconeLike | null = null;
      const realFactory = async () => {
        if (!client) client = await defaultPineconeFactory(key);
        return client;
      };
      return {
        index(name: string) {
          return {
            async query(queryOpts) {
              const c = await realFactory();
              return c.index(name).query(queryOpts);
            },
            namespace(ns: string) {
              return {
                async upsert(vectors) {
                  const c = await realFactory();
                  const idx = c.index(name);
                  const nsScope = idx.namespace?.(ns);
                  if (nsScope) return nsScope.upsert(vectors);
                  if (idx.upsert) return idx.upsert(vectors, { namespace: ns });
                  throw new Error('Pinecone client exposes no upsert path.');
                },
              };
            },
          } as ReturnType<PineconeLike['index']>;
        },
      } as PineconeLike;
    });

  const pc = factory(pineconeKey);
  const index = pc.index(LOAN_ATLAS_INDEX);

  // Group records by namespace so we can route upserts correctly.
  const byNs = new Map<string, LoanAtlasUpsertRecord[]>();
  for (const r of records) {
    const ns = r.namespace ?? opts.namespace ?? '';
    if (!byNs.has(ns)) byNs.set(ns, []);
    byNs.get(ns)!.push(r);
  }

  for (const [ns, group] of byNs) {
    for (let i = 0; i < group.length; i += batchSize) {
      const slice = group.slice(i, i + batchSize);
      const batchStart = Date.now();
      const vectors = await embedBatch(
        slice.map((r) => r.text),
        openaiKey,
      );
      const payload = slice.map((r, k) => ({
        id: r.id,
        values: vectors[k],
        metadata: r.metadata ?? {},
      }));
      const nsScope = index.namespace?.(ns);
      if (nsScope) {
        await nsScope.upsert(payload);
      } else if (index.upsert) {
        await index.upsert(payload, ns ? { namespace: ns } : undefined);
      } else {
        throw new Error('Pinecone index has no upsert method.');
      }

      if (ratePerSec > 0) {
        const budgetMs = (slice.length / ratePerSec) * 1000;
        const elapsed = Date.now() - batchStart;
        const remaining = budgetMs - elapsed;
        if (remaining > 0) await sleep(remaining);
      }
    }
  }
}
