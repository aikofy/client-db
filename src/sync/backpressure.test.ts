import { describe, it, expect } from 'vitest';
import { drainIfNeeded, BACKPRESSURE_HIGH_WATER } from './backpressure.js';

class MockChannel {
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  readyState: 'connecting' | 'open' | 'closing' | 'closed' = 'open';
  private listeners = new Map<string, Set<() => void>>();

  addEventListener(type: string, cb: () => void): void {
    let s = this.listeners.get(type);
    if (!s) this.listeners.set(type, (s = new Set()));
    s.add(cb);
  }
  removeEventListener(type: string, cb: () => void): void {
    this.listeners.get(type)?.delete(cb);
  }
  emit(type: string): void {
    for (const cb of [...(this.listeners.get(type) ?? [])]) cb();
  }
}

const asChannel = (m: MockChannel): RTCDataChannel => m as unknown as RTCDataChannel;
const settled = async (p: Promise<void>): Promise<boolean> => {
  let done = false;
  void p.then(() => {
    done = true;
  });
  await new Promise((r) => setTimeout(r, 0));
  return done;
};

describe('drainIfNeeded', () => {
  it('resolves immediately when below the high-water mark', async () => {
    const ch = new MockChannel();
    ch.bufferedAmount = 0;
    expect(await settled(drainIfNeeded(asChannel(ch)))).toBe(true);
  });

  it('waits while buffered, then resolves on bufferedamountlow', async () => {
    const ch = new MockChannel();
    ch.bufferedAmount = BACKPRESSURE_HIGH_WATER + 1;
    const p = drainIfNeeded(asChannel(ch));
    expect(await settled(p)).toBe(false); // parked

    ch.bufferedAmount = 0;
    ch.emit('bufferedamountlow');
    await expect(p).resolves.toBeUndefined();
  });

  it('resolves if the channel closes while waiting', async () => {
    const ch = new MockChannel();
    ch.bufferedAmount = BACKPRESSURE_HIGH_WATER + 1;
    const p = drainIfNeeded(asChannel(ch));
    expect(await settled(p)).toBe(false);

    ch.emit('close');
    await expect(p).resolves.toBeUndefined();
  });

  it('resolves on the timeout backstop', async () => {
    const ch = new MockChannel();
    ch.bufferedAmount = BACKPRESSURE_HIGH_WATER + 1;
    const p = drainIfNeeded(asChannel(ch), 20); // short timeout
    expect(await settled(p)).toBe(false);
    await expect(p).resolves.toBeUndefined(); // fires after ~20ms
  });

  it('resolves immediately if the channel is already closed (lost-event race guard)', async () => {
    const ch = new MockChannel();
    ch.bufferedAmount = BACKPRESSURE_HIGH_WATER + 1;
    ch.readyState = 'closed';
    expect(await settled(drainIfNeeded(asChannel(ch)))).toBe(true);
  });
});
