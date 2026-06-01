/**
 * Token verification for the RPC server. Verifies short-lived JWS (JWT) tokens locally with
 * WebCrypto — no per-request callout, so any Normal Client can authenticate any Consumer offline
 * (preserves failover). The token is minted by an external IdP; we only verify. See
 * docs/signaling-protocol.md §6 and docs/consumer-client-plan.md §4.
 */
import type { ConsumerIdentity } from './context.js';
import type { TokenVerifier } from './server.js';

export type SupportedAlg = 'ES256' | 'RS256';

/** A JWK plus the optional `kid` (the DOM `JsonWebKey` type omits it). */
export type Jwk = JsonWebKey & { kid?: string };

export interface JwtClaims {
  sub?: string;
  iss?: string;
  aud?: string | string[];
  exp?: number;
  nbf?: number;
  iat?: number;
  jti?: string;
  /** OAuth-style space-delimited scopes. */
  scope?: string;
  /** Array form of scopes. */
  scopes?: string[];
  [k: string]: unknown;
}

export interface TokenVerifierOptions {
  /** Public keys to verify against (a JWKS). Selected by the token header `kid`; if the token
   *  has no `kid`, the first key is used. */
  jwks: Jwk[];
  /** Allowed algorithms. Default: both ES256 and RS256. `none` is never allowed. */
  algorithms?: SupportedAlg[];
  /** Expected `iss`, if set. */
  issuer?: string;
  /** Expected `aud` (matches a string or membership of a string[]), if set. */
  audience?: string;
  /** Leeway (seconds) for exp/nbf. Default 0. */
  clockToleranceSec?: number;
  /** Return true to reject a token (hook into the gossiped revocation set). */
  isRevoked?: (claims: JwtClaims) => boolean | Promise<boolean>;
  /** Map verified claims → identity. Default: id=sub, scopes from `scope`/`scopes`. */
  toIdentity?: (claims: JwtClaims, consumerId: string) => ConsumerIdentity;
}

interface AlgSpec {
  importParams: EcKeyImportParams | RsaHashedImportParams;
  verifyParams: EcdsaParams | Algorithm;
}

const ALGS: Record<SupportedAlg, AlgSpec> = {
  ES256: {
    importParams: { name: 'ECDSA', namedCurve: 'P-256' },
    verifyParams: { name: 'ECDSA', hash: 'SHA-256' },
  },
  RS256: {
    importParams: { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    verifyParams: { name: 'RSASSA-PKCS1-v1_5' },
  },
};

/**
 * Build a {@link TokenVerifier} for `RpcServer`/`createDB({ rpc: { verifyToken } })`.
 * Returns the verified identity, or `null` to reject (the server replies UNAUTHENTICATED).
 */
export function createTokenVerifier(opts: TokenVerifierOptions): TokenVerifier {
  const allowed = new Set<string>(opts.algorithms ?? ['ES256', 'RS256']);
  const tol = opts.clockToleranceSec ?? 0;
  const keyCache = new Map<string, CryptoKey>();

  async function resolveKey(
    header: { alg?: string; kid?: string },
  ): Promise<{ key: CryptoKey; spec: AlgSpec } | null> {
    const alg = header.alg;
    if (!alg || alg === 'none' || !allowed.has(alg)) return null;
    const spec = ALGS[alg as SupportedAlg];
    if (!spec) return null;

    const jwk = header.kid ? opts.jwks.find((k) => k.kid === header.kid) : opts.jwks[0];
    if (!jwk) return null;

    const cacheKey = `${jwk.kid ?? '0'}:${alg}`;
    let key = keyCache.get(cacheKey);
    if (!key) {
      try {
        key = await crypto.subtle.importKey('jwk', jwk, spec.importParams, false, ['verify']);
      } catch {
        return null;
      }
      keyCache.set(cacheKey, key);
    }
    return { key, spec };
  }

  return async (token: string, consumerId: string): Promise<ConsumerIdentity | null> => {
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    const [h, p, s] = parts;

    let header: { alg?: string; kid?: string };
    let claims: JwtClaims;
    try {
      header = JSON.parse(decodeText(b64urlToBytes(h))) as { alg?: string; kid?: string };
      claims = JSON.parse(decodeText(b64urlToBytes(p))) as JwtClaims;
    } catch {
      return null;
    }

    const resolved = await resolveKey(header);
    if (!resolved) return null;

    let signature: Uint8Array<ArrayBuffer>;
    try {
      signature = b64urlToBytes(s);
    } catch {
      return null;
    }
    // Wrap so the byte view is ArrayBuffer-backed (BufferSource), not ArrayBufferLike.
    const signed = new Uint8Array(new TextEncoder().encode(`${h}.${p}`));

    let valid = false;
    try {
      valid = await crypto.subtle.verify(resolved.spec.verifyParams, resolved.key, signature, signed);
    } catch {
      return null;
    }
    if (!valid) return null;

    const now = Math.floor(Date.now() / 1000);
    if (typeof claims.exp === 'number' && now > claims.exp + tol) return null;
    if (typeof claims.nbf === 'number' && now + tol < claims.nbf) return null;
    if (opts.issuer && claims.iss !== opts.issuer) return null;
    if (opts.audience) {
      const aud = claims.aud;
      const ok = Array.isArray(aud) ? aud.includes(opts.audience) : aud === opts.audience;
      if (!ok) return null;
    }
    if (opts.isRevoked && (await opts.isRevoked(claims))) return null;

    return opts.toIdentity ? opts.toIdentity(claims, consumerId) : defaultIdentity(claims, consumerId);
  };
}

function defaultIdentity(claims: JwtClaims, consumerId: string): ConsumerIdentity {
  const scopes = Array.isArray(claims.scopes)
    ? claims.scopes
    : typeof claims.scope === 'string'
      ? claims.scope.split(' ').filter(Boolean)
      : [];
  return { id: claims.sub ?? consumerId, scopes, claims };
}

function b64urlToBytes(s: string): Uint8Array<ArrayBuffer> {
  const b64 = s.replace(/-/g, '+').replace(/_/g, '/');
  const pad = b64.length % 4 === 0 ? '' : '='.repeat(4 - (b64.length % 4));
  const bin = atob(b64 + pad);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

function decodeText(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}
