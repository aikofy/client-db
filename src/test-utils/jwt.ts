/**
 * Test-only ES256 JWT signer built on WebCrypto, to exercise the RPC token verifier with real
 * signatures (valid / expired / forged / tampered). Not shipped (only src/index.ts is built).
 */

export interface TestSigner {
  /** Public key as a JWK, to feed `createTokenVerifier({ jwks: [jwk] })`. */
  jwk: JsonWebKey & { kid?: string };
  kid: string;
  /** Sign a JWT with the given claims. `alg` defaults to ES256. */
  sign(claims: Record<string, unknown>, opts?: { alg?: string; kid?: string }): Promise<string>;
}

export async function makeEs256Signer(kid = 'test-key'): Promise<TestSigner> {
  const pair = (await crypto.subtle.generateKey(
    { name: 'ECDSA', namedCurve: 'P-256' },
    true,
    ['sign', 'verify'],
  )) as CryptoKeyPair;

  const jwk = (await crypto.subtle.exportKey('jwk', pair.publicKey)) as JsonWebKey & { kid?: string };
  jwk.kid = kid;

  return {
    jwk,
    kid,
    async sign(claims, opts) {
      const header = { alg: opts?.alg ?? 'ES256', typ: 'JWT', kid: opts?.kid ?? kid };
      const h = b64url(new TextEncoder().encode(JSON.stringify(header)));
      const p = b64url(new TextEncoder().encode(JSON.stringify(claims)));
      const data = new Uint8Array(new TextEncoder().encode(`${h}.${p}`));
      const sig = await crypto.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, pair.privateKey, data);
      return `${h}.${p}.${b64url(new Uint8Array(sig))}`;
    },
  };
}

/** Build a token string with an arbitrary header (e.g. alg:'none') and an empty signature. */
export function unsignedToken(header: Record<string, unknown>, claims: Record<string, unknown>): string {
  const h = b64url(new TextEncoder().encode(JSON.stringify(header)));
  const p = b64url(new TextEncoder().encode(JSON.stringify(claims)));
  return `${h}.${p}.`;
}

function b64url(bytes: Uint8Array): string {
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
