import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock the Claude Agent SDK BEFORE importing agent.ts so the module-under-test
// picks up our stub instead of spawning a real subprocess.
vi.mock('@anthropic-ai/claude-agent-sdk', () => ({
  query: vi.fn(),
}));

// Mock the logger so WARN/INFO lines don't pollute test output.
vi.mock('./logger.js', () => ({
  logger: {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  },
}));

import { query } from '@anthropic-ai/claude-agent-sdk';
import { runAgent } from './agent.js';

/**
 * Build an async-iterable that yields the given events in order, then — if
 * `hangAfter` is true — stalls forever. Mirrors the real SDK bug where a
 * `result` event is emitted but the stream never closes.
 */
function fakeStream(events: Array<Record<string, unknown>>, hangAfter: boolean) {
  return {
    async *[Symbol.asyncIterator]() {
      for (const ev of events) yield ev;
      if (hangAfter) {
        // Never-resolving promise — simulates SDK .next() stuck forever.
        await new Promise<never>(() => {});
      }
    },
  };
}

const mockedQuery = vi.mocked(query);

describe('runAgent — stream-tail hang regression (mission 81511133)', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    // Short grace window so the test runs fast.
    process.env.AGENT_STREAM_GRACE_MS = '150';
  });

  afterEach(() => {
    delete process.env.AGENT_STREAM_GRACE_MS;
  });

  it('resolves within the grace window when the SDK stalls after the result event', async () => {
    mockedQuery.mockImplementation(
      () =>
        fakeStream(
          [
            {
              type: 'system',
              subtype: 'init',
              session_id: 'sess-test',
              model: 'claude-opus-4-7',
              apiKeySource: 'none',
              permissionMode: 'bypassPermissions',
            },
            {
              type: 'result',
              subtype: 'success',
              result: 'done',
              total_cost_usd: 0.5,
              usage: {
                input_tokens: 30,
                output_tokens: 10,
                cache_read_input_tokens: 400_000,
              },
            },
          ],
          /* hangAfter */ true,
        ) as unknown as ReturnType<typeof query>,
    );

    const start = Date.now();
    const result = await runAgent('test prompt', undefined, () => {});
    const elapsed = Date.now() - start;

    // Result + usage must still be captured from the pre-hang `result` event
    expect(result.text).toBe('done');
    expect(result.aborted).toBeFalsy();
    expect(result.newSessionId).toBe('sess-test');
    expect(result.usage?.inputTokens).toBe(30);
    expect(result.usage?.totalCostUsd).toBe(0.5);

    // Must resolve promptly — grace = 150ms, buffer for scheduling.
    // Before the fix this would have sat until the outer 30-min turn timeout.
    expect(elapsed).toBeLessThan(3_000);
  }, 10_000);

  it('passes maxTurnsOverride through to the SDK options (per-task turn cap)', async () => {
    // Per-task max_turns override (v1.8.0). When the scheduler passes a
    // non-null max_turns from scheduled_tasks / mission_tasks, runAgent must
    // forward it to the SDK as `maxTurns` — overriding the AGENT_MAX_TURNS
    // env default. Regression guard for the DION planner 60-turn-cap bug.
    mockedQuery.mockImplementation(
      () =>
        fakeStream(
          [
            {
              type: 'system',
              subtype: 'init',
              session_id: 'sess-maxturns',
              model: 'claude-opus-4-7',
              apiKeySource: 'none',
              permissionMode: 'bypassPermissions',
            },
            {
              type: 'result',
              subtype: 'success',
              result: 'ok',
              total_cost_usd: 0.0,
              usage: { input_tokens: 1, output_tokens: 1, cache_read_input_tokens: 0 },
            },
          ],
          /* hangAfter */ false,
        ) as unknown as ReturnType<typeof query>,
    );

    await runAgent(
      'expensive task',
      undefined,   // sessionId
      () => {},    // onEvent
      undefined,   // cwd
      undefined,   // model
      undefined,   // abortController
      undefined,   // onStreamText
      undefined,   // mcpAllowlist
      120,         // maxTurnsOverride
    );

    expect(mockedQuery).toHaveBeenCalledTimes(1);
    const callArgs = mockedQuery.mock.calls[0][0] as { options: { maxTurns?: number } };
    expect(callArgs.options.maxTurns).toBe(120);
  });

  it('returns normally when the stream closes cleanly after the result event', async () => {
    mockedQuery.mockImplementation(
      () =>
        fakeStream(
          [
            {
              type: 'system',
              subtype: 'init',
              session_id: 'sess-clean',
              model: 'claude-haiku-4-5',
              apiKeySource: 'none',
              permissionMode: 'bypassPermissions',
            },
            {
              type: 'result',
              subtype: 'success',
              result: 'clean ok',
              total_cost_usd: 0.01,
              usage: {
                input_tokens: 5,
                output_tokens: 2,
                cache_read_input_tokens: 0,
              },
            },
          ],
          /* hangAfter */ false,
        ) as unknown as ReturnType<typeof query>,
    );

    const result = await runAgent('clean prompt', undefined, () => {});
    expect(result.text).toBe('clean ok');
    expect(result.aborted).toBeFalsy();
    expect(result.newSessionId).toBe('sess-clean');
  });
});
