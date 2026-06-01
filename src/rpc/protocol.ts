/**
 * RPC wire protocol carried over the WebRTC 'rpc' data channel (Consumer ⇄ Normal Client).
 * Independent of the gossip `SyncMessage` union. See docs/rpc-protocol.md for the spec.
 */
import type { HLCTimestamp } from '../core/types.js';

export const PROTOCOL_VERSION = 1;

export type RpcStatus =
  | 'OK'
  | 'UNAUTHENTICATED'
  | 'PERMISSION_DENIED'
  | 'INVALID_ARGUMENT'
  | 'NOT_FOUND'
  | 'FAILED_PRECONDITION'
  | 'ALREADY_EXISTS'
  | 'RESOURCE_EXHAUSTED'
  | 'UNAVAILABLE'
  | 'DEADLINE_EXCEEDED'
  | 'INTERNAL'
  | 'CANCELLED';

export type MethodKind = 'read' | 'write' | 'stream';

export interface MethodInfo {
  name: string;
  kind: MethodKind;
  version: number;
}

export interface RpcLimits {
  maxPayloadBytes: number;
  defaultDeadlineMs: number;
  rateLimit: { perMin: number };
}

// ─── Client → server frames ─────────────────────────────────────────────────────

export interface AuthFrame {
  type: 'auth';
  token: string;
  protocolVersion: number;
}

export interface ReqFrame {
  type: 'req';
  id: string;
  method: string;
  params?: unknown;
  idempotencyKey?: string;
  deadlineMs?: number;
  readAfter?: HLCTimestamp;
}

export interface CancelFrame {
  type: 'cancel';
  id: string;
}

export interface PingFrame {
  type: 'ping';
  id: string;
  ts?: number;
}

export type ClientFrame = AuthFrame | ReqFrame | CancelFrame | PingFrame;

// ─── Server → client frames ─────────────────────────────────────────────────────

export interface AuthOkFrame {
  type: 'auth-ok';
  server: string;
  protocolVersion: number;
  methods: MethodInfo[];
  serverHlc: HLCTimestamp;
  limits: RpcLimits;
}

export interface AuthErrFrame {
  type: 'auth-err';
  status: RpcStatus;
  message: string;
}

export interface ResFrame {
  type: 'res';
  id: string;
  status: 'OK';
  body: unknown;
  hlc?: HLCTimestamp;
}

export interface ErrFrame {
  type: 'err';
  id: string;
  status: RpcStatus;
  message: string;
  retryable: boolean;
}

export interface StreamStartFrame {
  type: 'stream-start';
  id: string;
}

export interface StreamChunkFrame {
  type: 'stream-chunk';
  id: string;
  seq: number;
  chunk: unknown;
}

export interface StreamEndFrame {
  type: 'stream-end';
  id: string;
  status: 'OK';
  hlc?: HLCTimestamp;
}

export interface PongFrame {
  type: 'pong';
  id: string;
  ts?: number;
}

export type ServerFrame =
  | AuthOkFrame
  | AuthErrFrame
  | ResFrame
  | ErrFrame
  | StreamStartFrame
  | StreamChunkFrame
  | StreamEndFrame
  | PongFrame;

export type RpcMessage = ClientFrame | ServerFrame;

export const DEFAULT_LIMITS: RpcLimits = {
  maxPayloadBytes: 1 * 1024 * 1024, // 1 MB
  defaultDeadlineMs: 30_000,
  rateLimit: { perMin: 600 },
};
