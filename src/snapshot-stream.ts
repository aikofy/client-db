import type { SnapshotChunk } from './core/types.js';

/**
 * Bridges between the in-memory `SnapshotChunk` stream (produced by
 * `db.exportStream()` / consumed by `db.importStream()`) and a byte stream of
 * newline-delimited JSON (NDJSON), suitable for uploading to / downloading from
 * a file, Blob, or cloud storage like Google Drive.
 *
 * Both directions are bounded-memory and honour backpressure, so a multi-GB DB
 * can be backed up and restored without ever materialising the whole dataset.
 *
 *   // Back up
 *   const body = snapshotChunksToNdjsonStream(db.exportStream());
 *   await fetch(uploadUrl, { method: 'POST', body, duplex: 'half' });
 *
 *   // Restore
 *   const res = await fetch(downloadUrl);
 *   await db.importStream(ndjsonStreamToSnapshotChunks(res.body!));
 */

/** Encode a chunk stream as NDJSON bytes (one JSON object per line). */
export function snapshotChunksToNdjsonStream(
  chunks: AsyncIterable<SnapshotChunk>,
): ReadableStream<Uint8Array> {
  const iterator = chunks[Symbol.asyncIterator]();
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    // pull() is called only when the consumer wants more data, so the source
    // generator is advanced lazily — peak memory stays at one chunk.
    async pull(controller) {
      try {
        const { value, done } = await iterator.next();
        if (done) {
          controller.close();
          return;
        }
        controller.enqueue(encoder.encode(JSON.stringify(value) + '\n'));
      } catch (err) {
        controller.error(err);
        await iterator.return?.(undefined as never).catch(() => undefined);
      }
    },
    async cancel() {
      await iterator.return?.(undefined as never).catch(() => undefined);
    },
  });
}

/** Decode an NDJSON byte stream back into snapshot chunks. */
export async function* ndjsonStreamToSnapshotChunks(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<SnapshotChunk> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let completed = false;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let nl = buffer.indexOf('\n');
      while (nl >= 0) {
        const line = buffer.slice(0, nl).trim(); // tolerate CRLF / blank lines
        buffer = buffer.slice(nl + 1);
        if (line.length > 0) yield JSON.parse(line) as SnapshotChunk;
        nl = buffer.indexOf('\n');
      }
    }
    buffer += decoder.decode();
    const tail = buffer.trim();
    if (tail.length > 0) yield JSON.parse(tail) as SnapshotChunk;
    completed = true;
  } finally {
    // On an abnormal exit (consumer break / import error / parse error) cancel
    // the upstream source so the fetch body / file handle is released promptly.
    // On normal completion the source already closed via `done`, so cancel() is
    // a harmless no-op.
    if (!completed) await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}
