import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Resolve the project root relative to this source file so .env loading
// works regardless of process.cwd(). At runtime this file lives at
// dist/env.js, so ../ = /<project-root>/. This is critical for CLIs
// (e.g. mission-watchdog-cli) spawned by schedulers from arbitrary cwds.
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '..');

/**
 * Parse the .env file and return values for the requested keys.
 * Does NOT load anything into process.env — callers decide what to
 * do with the values. This keeps secrets out of the process environment
 * so they don't leak to child processes.
 *
 * Looks up .env at PROJECT_ROOT (resolved from this file's location),
 * not process.cwd(), so CLIs spawned from any working directory work.
 */
export function readEnvFile(keys: string[]): Record<string, string> {
  const envFile = path.join(PROJECT_ROOT, '.env');
  let content: string;
  try {
    content = fs.readFileSync(envFile, 'utf-8');
  } catch {
    return {};
  }

  const result: Record<string, string> = {};
  const wanted = new Set(keys);

  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    if (!wanted.has(key)) continue;
    let value = trimmed.slice(eqIdx + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (value) result[key] = value;
  }

  return result;
}
