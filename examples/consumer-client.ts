/**
 * Example: a Consumer Client.
 *
 * A Consumer holds NO database and never gossips. It connects to a Normal Client over a WebRTC
 * 'rpc' data channel and calls that client's handlers. Import from the slim `/consumer` entry
 * point so none of the storage/gossip code is pulled into your bundle.
 *
 * Run this in a browser (it uses WebRTC). Bundle with Vite/esbuild/etc.
 */
import { ConsumerClient, RpcError } from '@aikofy/client-db/consumer';

export async function runConsumer(targetUserId: string, getAccessToken: () => Promise<string>) {
  const consumer = new ConsumerClient({
    signalingServerUrl: 'wss://signal.example.com/signal',
    room: targetUserId, // the Normal Clients' DB name
    auth: { getToken: getAccessToken }, // your IdP / backend
    deadlineMs: 10_000,
  });

  // 1. Read — connects + authenticates on first call.
  const mine = await consumer.invoke<Array<{ id: string; title: string }>>('todos.listMine');
  console.log('my todos:', mine);

  // 2. Read a single item, handling a not-found status.
  try {
    const one = await consumer.invoke('todos.get', { id: 'abc' });
    console.log('got:', one);
  } catch (e) {
    if (e instanceof RpcError && e.status === 'NOT_FOUND') console.log('not found');
    else throw e;
  }

  // 3. Write — the SDK auto-attaches a stable idempotency key, so a retry is deduped, not
  //    duplicated. (By default writes are NOT replayed across a failover; pass { idempotent: true }
  //    to opt in.) A subsequent read carries the write's watermark for read-your-writes.
  const { id } = await consumer.invoke<{ id: string }>('todos.create', { title: 'Buy milk' });
  console.log('created:', id);

  // 4. Server-streaming — iterate chunks as they arrive; break (or an AbortSignal) cancels it.
  for await (const batch of consumer.stream<Array<{ id: string }>>('todos.export')) {
    console.log('export batch of', batch.length);
  }

  consumer.close();
}
