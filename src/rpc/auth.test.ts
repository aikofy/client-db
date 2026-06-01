import { describe, it, expect } from 'vitest';
import { createTokenVerifier } from './auth.js';
import { makeEs256Signer, unsignedToken } from '../test-utils/jwt.js';

const NOW = () => Math.floor(Date.now() / 1000);

describe('createTokenVerifier (WebCrypto JWS)', () => {
  it('accepts a valid ES256 token and maps sub + scopes', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    const token = await signer.sign({ sub: 'patient-1', scope: 'todos:read todos:write', exp: NOW() + 60 });

    const identity = await verify(token, 'cons1');
    expect(identity).not.toBeNull();
    expect(identity!.id).toBe('patient-1');
    expect(identity!.scopes).toEqual(['todos:read', 'todos:write']);
  });

  it('accepts array-form scopes', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    const token = await signer.sign({ sub: 'p', scopes: ['a', 'b'], exp: NOW() + 60 });
    const id = await verify(token, 'cons1');
    expect(id!.scopes).toEqual(['a', 'b']);
  });

  it('rejects an expired token', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    const token = await signer.sign({ sub: 'p', exp: NOW() - 10 });
    expect(await verify(token, 'cons1')).toBeNull();
  });

  it('honors not-before (nbf)', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    const token = await signer.sign({ sub: 'p', nbf: NOW() + 60, exp: NOW() + 120 });
    expect(await verify(token, 'cons1')).toBeNull();
  });

  it('rejects a token signed by a different key (forged)', async () => {
    const real = await makeEs256Signer('real');
    const attacker = await makeEs256Signer('real'); // same kid, different key material
    const verify = createTokenVerifier({ jwks: [real.jwk] });
    const token = await attacker.sign({ sub: 'p', exp: NOW() + 60 });
    expect(await verify(token, 'cons1')).toBeNull();
  });

  it('rejects a tampered payload', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    const token = await signer.sign({ sub: 'p', scope: 'todos:read', exp: NOW() + 60 });
    const [h, , s] = token.split('.');
    const forgedPayload = btoa(JSON.stringify({ sub: 'admin', scope: 'admin:all', exp: NOW() + 60 }))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    expect(await verify(`${h}.${forgedPayload}.${s}`, 'cons1')).toBeNull();
  });

  it('rejects alg:none', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    const token = unsignedToken({ alg: 'none', typ: 'JWT' }, { sub: 'admin', exp: NOW() + 60 });
    expect(await verify(token, 'cons1')).toBeNull();
  });

  it('rejects a disallowed algorithm', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk], algorithms: ['RS256'] }); // ES256 not allowed
    const token = await signer.sign({ sub: 'p', exp: NOW() + 60 });
    expect(await verify(token, 'cons1')).toBeNull();
  });

  it('enforces issuer and audience when configured', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk], issuer: 'iss-1', audience: 'aud-1' });
    expect(await verify(await signer.sign({ sub: 'p', iss: 'iss-1', aud: 'aud-1', exp: NOW() + 60 }), 'c')).not.toBeNull();
    expect(await verify(await signer.sign({ sub: 'p', iss: 'other', aud: 'aud-1', exp: NOW() + 60 }), 'c')).toBeNull();
    expect(await verify(await signer.sign({ sub: 'p', iss: 'iss-1', aud: 'other', exp: NOW() + 60 }), 'c')).toBeNull();
    // aud as array membership
    expect(await verify(await signer.sign({ sub: 'p', iss: 'iss-1', aud: ['x', 'aud-1'], exp: NOW() + 60 }), 'c')).not.toBeNull();
  });

  it('rejects a revoked token', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({
      jwks: [signer.jwk],
      isRevoked: (claims) => claims.jti === 'revoked-1',
    });
    expect(await verify(await signer.sign({ sub: 'p', jti: 'revoked-1', exp: NOW() + 60 }), 'c')).toBeNull();
    expect(await verify(await signer.sign({ sub: 'p', jti: 'ok', exp: NOW() + 60 }), 'c')).not.toBeNull();
  });

  it('rejects malformed tokens', async () => {
    const signer = await makeEs256Signer();
    const verify = createTokenVerifier({ jwks: [signer.jwk] });
    expect(await verify('not-a-jwt', 'c')).toBeNull();
    expect(await verify('a.b', 'c')).toBeNull();
  });
});
