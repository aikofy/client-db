import type { RpcStatus } from './protocol.js';

/**
 * Default `retryable` per status (the server may override per response). "Retryable" here
 * means a client MAY safely retry/fail over; for some statuses (DEADLINE_EXCEEDED, INTERNAL)
 * that is only true for idempotent calls — a decision the client makes, so the default is
 * conservative (false). See docs/rpc-protocol.md §4.
 */
const RETRYABLE_DEFAULT: Record<RpcStatus, boolean> = {
  OK: false,
  UNAUTHENTICATED: false,
  PERMISSION_DENIED: false,
  INVALID_ARGUMENT: false,
  NOT_FOUND: false,
  FAILED_PRECONDITION: false,
  ALREADY_EXISTS: false,
  RESOURCE_EXHAUSTED: true,
  UNAVAILABLE: true,
  DEADLINE_EXCEEDED: false,
  INTERNAL: false,
  CANCELLED: false,
};

export function defaultRetryable(status: RpcStatus): boolean {
  return RETRYABLE_DEFAULT[status];
}

/**
 * Throw from a handler to return a specific status to the caller. Anything else a handler
 * throws is mapped to INTERNAL.
 */
export class RpcError extends Error {
  readonly status: RpcStatus;
  readonly retryable: boolean;

  constructor(status: RpcStatus, message: string, retryable?: boolean) {
    super(message);
    this.name = 'RpcError';
    this.status = status;
    this.retryable = retryable ?? RETRYABLE_DEFAULT[status];
  }
}
