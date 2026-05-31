import { describe, it, expect, beforeEach } from 'vitest';
import 'fake-indexeddb/auto';
import { IDBFactory } from 'fake-indexeddb';
import { createDB } from './db.js';

const collections = { todos: { indexes: ['status'] } };

describe('createDB idempotency', () => {
  beforeEach(() => {
    globalThis.indexedDB = new IDBFactory();
  });

  it('concurrent calls with the same name return the same instance', async () => {
    const name = `db-${Math.random()}`;
    const [a, b] = await Promise.all([
      createDB({ name, version: 1, collections }),
      createDB({ name, version: 1, collections }),
    ]);
    expect(a).toBe(b);
  });

  it('sequential calls before close return the same instance', async () => {
    const name = `db-${Math.random()}`;
    const a = await createDB({ name, version: 1, collections });
    const b = await createDB({ name, version: 1, collections });
    expect(a).toBe(b);
    await a.close();
  });

  it('close clears the registry so reopen returns a fresh instance', async () => {
    const name = `db-${Math.random()}`;
    const a = await createDB({ name, version: 1, collections });
    await a.close();
    const b = await createDB({ name, version: 1, collections });
    expect(a).not.toBe(b);
    await b.close();
  });

  it('different names produce different instances', async () => {
    const a = await createDB({ name: `a-${Math.random()}`, version: 1, collections });
    const b = await createDB({ name: `b-${Math.random()}`, version: 1, collections });
    expect(a).not.toBe(b);
    await a.close();
    await b.close();
  });
});
