import type { MethodInfo, MethodKind } from './protocol.js';
import type { RpcContext } from './context.js';

/** Any zod-compatible validator. `parse` returns the validated value or throws. */
export interface Validator<T = unknown> {
  parse(input: unknown): T;
}

export type RpcHandler<P = unknown, R = unknown> = (ctx: RpcContext, params: P) => R | Promise<R>;
export type StreamHandler<P = unknown, R = unknown> = (
  ctx: RpcContext,
  params: P,
) => AsyncIterable<R>;

export interface HandlerOptions<P = unknown, R = unknown> {
  /** Scopes the caller's token must carry. Declared now; enforced from Phase 4. */
  scopes?: string[];
  /** Optional input validator (zod schema or any `{ parse }`). */
  input?: Validator<P>;
  /** Method version for the catalog; defaults to 1. */
  version?: number;
  handler: RpcHandler<P, R>;
}

export interface StreamHandlerOptions<P = unknown, R = unknown> {
  scopes?: string[];
  input?: Validator<P>;
  version?: number;
  handler: StreamHandler<P, R>;
}

export interface HandlerDef {
  id: string;
  kind: MethodKind;
  version: number;
  scopes: string[];
  input?: Validator;
  handler: RpcHandler | StreamHandler;
}

/**
 * Registry of named handlers a Normal Client exposes to Consumers. Method ids are matched
 * exactly (use `name@vN` if you version). Reads and writes are unary; streams yield batches.
 */
export class RpcRouter {
  private handlers = new Map<string, HandlerDef>();

  read<P, R>(id: string, opts: HandlerOptions<P, R>): this {
    return this._add('read', id, opts as HandlerOptions);
  }

  write<P, R>(id: string, opts: HandlerOptions<P, R>): this {
    return this._add('write', id, opts as HandlerOptions);
  }

  stream<P, R>(id: string, opts: StreamHandlerOptions<P, R>): this {
    return this._add('stream', id, opts as StreamHandlerOptions);
  }

  private _add(
    kind: MethodKind,
    id: string,
    opts: HandlerOptions | StreamHandlerOptions,
  ): this {
    if (this.handlers.has(id)) throw new Error(`RpcRouter: duplicate handler "${id}"`);
    this.handlers.set(id, {
      id,
      kind,
      version: opts.version ?? 1,
      scopes: opts.scopes ?? [],
      input: opts.input,
      handler: opts.handler,
    });
    return this;
  }

  get(id: string): HandlerDef | undefined {
    return this.handlers.get(id);
  }

  /** Method catalog advertised to consumers in `auth-ok`. */
  catalog(): MethodInfo[] {
    return [...this.handlers.values()].map((h) => ({ name: h.id, kind: h.kind, version: h.version }));
  }
}
