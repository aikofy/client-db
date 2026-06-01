import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Static guard: the slim Consumer entry must never reach the heavy storage/gossip/snapshot code
 * (or IndexedDB), or the bundle would balloon. We walk the relative-import graph from
 * src/consumer.ts and assert nothing forbidden is reachable. This catches a regression the
 * moment someone adds an errant import — no build step required.
 */

const SRC = dirname(fileURLToPath(import.meta.url));

const FORBIDDEN_PATH = /\/(storage)\/|\/sync\/(gossip|snapshot|webrtc-transport)\.ts$|\/db\.ts$/;
const FORBIDDEN_EXTERNAL = new Set(['idb', 'fake-indexeddb', 'uuid']);

function resolveRelative(fromFile: string, spec: string): string {
  // imports use '.js' (NodeNext-style); the source on disk is '.ts'
  const base = resolve(dirname(fromFile), spec);
  return base.replace(/\.js$/, '.ts');
}

function collectImports(file: string): string[] {
  const src = readFileSync(file, 'utf8');
  const specs: string[] = [];
  const re = /(?:import|export)\b[^'"]*?from\s*['"]([^'"]+)['"]/g;
  const bare = /\bimport\s*['"]([^'"]+)['"]/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(src))) specs.push(m[1]);
  while ((m = bare.exec(src))) specs.push(m[1]);
  return specs;
}

function reachableFrom(entry: string): { files: Set<string>; externals: Set<string> } {
  const files = new Set<string>();
  const externals = new Set<string>();
  const queue = [entry];
  while (queue.length) {
    const file = queue.pop()!;
    if (files.has(file)) continue;
    files.add(file);
    for (const spec of collectImports(file)) {
      if (spec.startsWith('.')) {
        const resolved = resolveRelative(file, spec);
        if (!files.has(resolved)) queue.push(resolved);
      } else {
        externals.add(spec);
      }
    }
  }
  return { files, externals };
}

describe('Consumer bundle hygiene', () => {
  const { files, externals } = reachableFrom(resolve(SRC, 'consumer.ts'));

  it('never reaches storage / gossip / snapshot / db modules', () => {
    const offenders = [...files].filter((f) => FORBIDDEN_PATH.test(f));
    expect(offenders).toEqual([]);
  });

  it('never pulls in heavy external deps (idb, uuid, fake-indexeddb)', () => {
    const offenders = [...externals].filter((e) => FORBIDDEN_EXTERNAL.has(e));
    expect(offenders).toEqual([]);
  });

  it('reaches only the rpc client + protocol layer', () => {
    const rel = [...files].map((f) => f.slice(SRC.length + 1)).sort();
    expect(rel).toEqual([
      'consumer.ts',
      'core/types.ts',
      'rpc/client.ts',
      'rpc/errors.ts',
      'rpc/protocol.ts',
    ]);
  });
});
