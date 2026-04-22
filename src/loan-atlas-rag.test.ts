import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import {
  searchLoanAtlas,
  embedQueryOpenAI,
  EMBEDDING_MODEL,
  EMBEDDING_DIMENSIONS,
  LOAN_ATLAS_INDEX,
  type PineconeLike,
  type SearchDeps,
} from './loan-atlas-rag.js';

// A deterministic 1024-dim vector for mock responses.
const fakeVector = Array.from({ length: EMBEDDING_DIMENSIONS }, (_, i) => i / 1024);

function fakeEnv(keys: Record<string, string>): SearchDeps['readEnv'] {
  return (requested: string[]) => {
    const out: Record<string, string> = {};
    for (const k of requested) if (keys[k]) out[k] = keys[k];
    return out;
  };
}

interface FakeIndex {
  query: ReturnType<typeof vi.fn>;
}

interface FakePinecone extends PineconeLike {
  index: ReturnType<typeof vi.fn>;
  __index: FakeIndex;
}

function makeFakePinecone(matches: unknown[] = []): FakePinecone {
  const queryFn = vi.fn().mockResolvedValue({ matches });
  const fakeIndex: FakeIndex = { query: queryFn };
  const indexFn = vi.fn().mockReturnValue(fakeIndex);
  return {
    index: indexFn,
    __index: fakeIndex,
  } as unknown as FakePinecone;
}

describe('searchLoanAtlas', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('throws a clear message when both env keys are missing', async () => {
    await expect(
      searchLoanAtlas('anything', {}, { readEnv: fakeEnv({}) }),
    ).rejects.toThrow(/OPENAI_API_KEY.*PINECONE_API_KEY/);
  });

  it('throws naming only the missing key when one is present', async () => {
    await expect(
      searchLoanAtlas(
        'anything',
        {},
        { readEnv: fakeEnv({ OPENAI_API_KEY: 'sk-test' }) },
      ),
    ).rejects.toThrow(/PINECONE_API_KEY/);
  });

  it('passes namespace, topK, and filter through to Pinecone', async () => {
    const pc = makeFakePinecone([
      { id: 'rec-1', score: 0.92, metadata: { title: 'FHA OTC' } },
      { id: 'rec-2', score: 0.81, metadata: { title: 'Non-QM BankStmt' } },
    ]);
    const embed = vi.fn().mockResolvedValue(fakeVector);

    const hits = await searchLoanAtlas(
      'FHA one time close vacant land',
      {
        namespace: 'lender-programs',
        topK: 5,
        filter: { loan_type: { $eq: 'FHA' } },
      },
      {
        readEnv: fakeEnv({ OPENAI_API_KEY: 'sk-x', PINECONE_API_KEY: 'pc-y' }),
        embed,
        pineconeFactory: () => pc,
      },
    );

    expect(embed).toHaveBeenCalledWith('FHA one time close vacant land', 'sk-x');
    expect(pc.index).toHaveBeenCalledWith(LOAN_ATLAS_INDEX);
    expect(pc.__index.query).toHaveBeenCalledWith({
      vector: fakeVector,
      topK: 5,
      includeMetadata: true,
      namespace: 'lender-programs',
      filter: { loan_type: { $eq: 'FHA' } },
    });
    expect(hits).toEqual([
      { id: 'rec-1', score: 0.92, metadata: { title: 'FHA OTC' } },
      { id: 'rec-2', score: 0.81, metadata: { title: 'Non-QM BankStmt' } },
    ]);
  });

  it('defaults topK to 10 and omits namespace/filter when not supplied', async () => {
    const pc = makeFakePinecone([]);
    await searchLoanAtlas(
      'general mortgage question',
      {},
      {
        readEnv: fakeEnv({ OPENAI_API_KEY: 'sk-x', PINECONE_API_KEY: 'pc-y' }),
        embed: async () => fakeVector,
        pineconeFactory: () => pc,
      },
    );
    const call = pc.__index.query.mock.calls[0][0];
    expect(call.topK).toBe(10);
    expect(call.includeMetadata).toBe(true);
    expect(call).not.toHaveProperty('namespace');
    expect(call).not.toHaveProperty('filter');
  });

  it('normalizes missing score and metadata to safe defaults', async () => {
    const pc = makeFakePinecone([
      { id: 'a' }, // no score, no metadata
      { id: 'b', score: 0.5 }, // no metadata
    ]);
    const hits = await searchLoanAtlas(
      'q',
      {},
      {
        readEnv: fakeEnv({ OPENAI_API_KEY: 'sk-x', PINECONE_API_KEY: 'pc-y' }),
        embed: async () => fakeVector,
        pineconeFactory: () => pc,
      },
    );
    expect(hits).toEqual([
      { id: 'a', score: 0, metadata: {} },
      { id: 'b', score: 0.5, metadata: {} },
    ]);
  });
});

describe('embedQueryOpenAI', () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('sends the correct model, dimensions, and auth header', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ data: [{ embedding: fakeVector }] }),
    });
    globalThis.fetch = mockFetch as unknown as typeof fetch;

    const vec = await embedQueryOpenAI('hello mortgage', 'sk-test-key');
    expect(vec).toHaveLength(EMBEDDING_DIMENSIONS);

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, init] = mockFetch.mock.calls[0];
    expect(url).toBe('https://api.openai.com/v1/embeddings');
    expect(init.method).toBe('POST');
    expect(init.headers['Authorization']).toBe('Bearer sk-test-key');
    const body = JSON.parse(init.body as string);
    expect(body).toEqual({
      model: EMBEDDING_MODEL,
      input: 'hello mortgage',
      dimensions: EMBEDDING_DIMENSIONS,
    });
  });

  it('throws on non-2xx OpenAI responses', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
      statusText: 'Unauthorized',
      text: async () => '{"error":"bad key"}',
    });
    globalThis.fetch = mockFetch as unknown as typeof fetch;

    await expect(embedQueryOpenAI('q', 'sk-bad')).rejects.toThrow(
      /OpenAI embeddings request failed: 401/,
    );
  });

  it('throws when the embedding has unexpected dimensions', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ data: [{ embedding: [0.1, 0.2, 0.3] }] }),
    });
    globalThis.fetch = mockFetch as unknown as typeof fetch;

    await expect(embedQueryOpenAI('q', 'sk-x')).rejects.toThrow(
      /malformed.*1024-dim.*got 3/,
    );
  });
});
