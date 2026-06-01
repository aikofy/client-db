/**
 * WebRTC data-channel backpressure. A fast producer can outrun a slow channel and balloon its
 * send buffer; `drainIfNeeded` parks until the buffer drains below the low-water mark (or the
 * channel closes, or a timeout backstop fires) so large/streamed payloads stay bounded in memory.
 * Shared by gossip pushes (`sendAsync`) and RPC streaming (`sendToConsumerAsync`).
 */
export const BACKPRESSURE_HIGH_WATER = 16 * 1024 * 1024; // 16 MB
export const BACKPRESSURE_LOW_WATER = 1 * 1024 * 1024; //  1 MB
export const BACKPRESSURE_TIMEOUT_MS = 30_000; // backstop so a stalled channel can't hang a send forever

/**
 * Resolve immediately if the channel's buffer is below the high-water mark; otherwise wait for
 * `bufferedamountlow` (or close/error, or `timeoutMs`). Never rejects.
 */
export function drainIfNeeded(
  channel: RTCDataChannel,
  timeoutMs: number = BACKPRESSURE_TIMEOUT_MS,
): Promise<void> {
  if (channel.bufferedAmount <= BACKPRESSURE_HIGH_WATER) return Promise.resolve();

  channel.bufferedAmountLowThreshold = BACKPRESSURE_LOW_WATER;
  return new Promise<void>((resolve) => {
    let settled = false;
    const finish = (): void => {
      if (settled) return;
      settled = true;
      channel.removeEventListener('bufferedamountlow', onSettle);
      channel.removeEventListener('close', onSettle);
      channel.removeEventListener('error', onSettle);
      clearTimeout(timer);
      resolve();
    };
    const onSettle = (): void => finish();
    const timer = setTimeout(finish, timeoutMs);
    channel.addEventListener('bufferedamountlow', onSettle);
    // If the channel closes/errors while we wait, resolve too — otherwise this await (and the
    // caller's send loop) would hang for the whole session.
    channel.addEventListener('close', onSettle);
    channel.addEventListener('error', onSettle);
    // Guard the lost-event race: the drain/close may have happened between the bufferedAmount
    // check and attaching the listeners.
    if (channel.readyState !== 'open' || channel.bufferedAmount <= BACKPRESSURE_LOW_WATER) {
      finish();
    }
  });
}
