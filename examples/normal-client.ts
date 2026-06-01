/**
 * Example: a Normal Client that also serves Consumer Clients.
 *
 * A Normal Client is the standard full-replica client. Passing `rpc: { router }` additionally
 * turns it into an RPC server: it exposes named read/write/stream handlers over WebRTC. The
 * handler body is the access-control boundary — it reads the full local replica via `ctx.db`
 * but returns only what the authenticated `ctx.consumer` is allowed to see.
 *
 * Run this in a browser (it uses IndexedDB + WebRTC). Bundle with Vite/esbuild/etc.
 */
import { createDB, RpcRouter, RpcError, createTokenVerifier } from '@aikofy/client-db';

interface Todo {
  _id: string;
  title: string;
  ownerId: string;
  done: boolean;
}

// 1. Define the API surface consumers may call.
const router = new RpcRouter()
  .read('todos.listMine', {
    scopes: ['todos:read'],
    handler: async (ctx) => {
      const all = (await ctx.db.query('todos')) as unknown as Todo[];
      // Return only the caller's todos — ctx.consumer.id comes from the verified token.
      return all.filter((t) => t.ownerId === ctx.consumer.id).map((t) => ({ id: t._id, title: t.title, done: t.done }));
    },
  })
  .read('todos.get', {
    scopes: ['todos:read'],
    input: { parse: (v) => v as { id: string } },
    handler: async (ctx, { id }) => {
      const todo = (await ctx.db.get('todos', id)) as unknown as Todo | null;
      if (!todo || todo.ownerId !== ctx.consumer.id) throw new RpcError('NOT_FOUND', 'no such todo');
      return todo;
    },
  })
  .write('todos.create', {
    scopes: ['todos:write'],
    input: { parse: (v) => v as { title: string } },
    handler: (ctx, { title }) =>
      // The SDK auto-attaches an idempotency key for write methods; ctx.idempotent dedupes retries.
      ctx.idempotent(ctx.request.idempotencyKey, async () => {
        const doc = await ctx.db.put('todos', { title, ownerId: ctx.consumer.id, done: false });
        return { id: doc._id }; // replicates to other Normal Clients via gossip
      }),
  })
  .stream('todos.export', {
    scopes: ['todos:read'],
    handler: async function* (ctx) {
      const all = (await ctx.db.query('todos')) as unknown as Todo[];
      const mine = all.filter((t) => t.ownerId === ctx.consumer.id);
      for (let i = 0; i < mine.length; i += 100) {
        yield mine.slice(i, i + 100).map((t) => ({ id: t._id, title: t.title }));
      }
    },
  });

// 2. Open the DB with sync + the RPC server.
export async function startNormalClient(userId: string, token: string, idpPublicJwks: JsonWebKey[]) {
  const db = await createDB({
    name: userId, // DB name == sync room
    version: 1,
    collections: { todos: { indexes: ['ownerId'] } },
    sync: {
      signalingServer: `wss://signal.example.com/signal?token=${token}`,
      iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
    },
    rpc: {
      router,
      // Verify Consumer tokens locally (no per-request callout). Minted by your IdP; we only verify.
      verifyToken: createTokenVerifier({
        jwks: idpPublicJwks,
        issuer: 'https://idp.example.com',
        audience: 'client-db',
      }),
      // Optional: metrics + audit.
      onCall: (rec) => {
        console.log(`[rpc] ${rec.method} ${rec.status} ${rec.durationMs}ms by ${rec.identityId}`);
      },
    },
  });

  return db;
}
