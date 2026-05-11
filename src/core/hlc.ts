import type { HLCTimestamp } from './types.js';

const MS_DIGITS = 16;
const COUNTER_DIGITS = 6;

function padMs(ms: number): string {
  return ms.toString().padStart(MS_DIGITS, '0');
}

function padCounter(n: number): string {
  return n.toString().padStart(COUNTER_DIGITS, '0');
}

export interface HLCState {
  physicalMs: number;
  counter: number;
  nodeId: string;
}

export function parseHLC(ts: HLCTimestamp): HLCState {
  const dashIdx1 = ts.indexOf('-');
  const dashIdx2 = ts.indexOf('-', dashIdx1 + 1);
  return {
    physicalMs: parseInt(ts.slice(0, dashIdx1), 10),
    counter: parseInt(ts.slice(dashIdx1 + 1, dashIdx2), 10),
    nodeId: ts.slice(dashIdx2 + 1),
  };
}

function formatHLC(state: HLCState): HLCTimestamp {
  return `${padMs(state.physicalMs)}-${padCounter(state.counter)}-${state.nodeId}` as HLCTimestamp;
}

export class HLC {
  private physicalMs: number = 0;
  private counter: number = 0;
  readonly nodeId: string;

  constructor(nodeId: string, initial?: HLCTimestamp) {
    this.nodeId = nodeId;
    if (initial) {
      const parsed = parseHLC(initial);
      this.physicalMs = parsed.physicalMs;
      this.counter = parsed.counter;
    }
  }

  tick(): HLCTimestamp {
    const now = Date.now();
    if (now > this.physicalMs) {
      this.physicalMs = now;
      this.counter = 0;
    } else {
      this.counter += 1;
    }
    return formatHLC({ physicalMs: this.physicalMs, counter: this.counter, nodeId: this.nodeId });
  }

  /** Advance the clock when receiving a remote timestamp. */
  update(remote: HLCTimestamp): HLCTimestamp {
    const r = parseHLC(remote);
    const now = Date.now();

    if (now > this.physicalMs && now > r.physicalMs) {
      this.physicalMs = now;
      this.counter = 0;
    } else if (r.physicalMs > this.physicalMs) {
      this.physicalMs = r.physicalMs;
      this.counter = r.counter + 1;
    } else if (r.physicalMs === this.physicalMs) {
      this.counter = Math.max(this.counter, r.counter) + 1;
    } else {
      this.counter += 1;
    }

    return formatHLC({ physicalMs: this.physicalMs, counter: this.counter, nodeId: this.nodeId });
  }

  now(): HLCTimestamp {
    return formatHLC({ physicalMs: this.physicalMs, counter: this.counter, nodeId: this.nodeId });
  }
}
