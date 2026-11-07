/* Exécution : node --test tests/jwt-analyzer.test.cjs (Node 20+) */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { webcrypto } = require("node:crypto");

const root = path.resolve(__dirname, "..");
const dir = path.join(root, "tools");
const coreCode = fs.readFileSync(path.join(dir, "jwt-analyzer-core.js"), "utf8");
const uiCode = fs.readFileSync(path.join(dir, "jwt-analyzer-ui.js"), "utf8");
const html = fs.readFileSync(path.join(dir, "decodeur_analyseur_jwt_standalone.html"), "utf8");
const css = fs.readFileSync(path.join(dir, "jwt-analyzer.css"), "utf8");

function loadCore(crypto = webcrypto) {
  const context = {
    window: { crypto },
    TextEncoder, TextDecoder,
    Uint8Array, btoa, atob
  };
  vm.runInNewContext(coreCode, context, { filename: "jwt-analyzer-core.js" });
  return context.window.JWTCore;
}
const core = loadCore();
const enc = new TextEncoder();

function joseBytes(value) {
  return core.encodeBase64Url(enc.encode(JSON.stringify(value)));
}

function jwt(header, payload, signature = "") {
  return joseBytes(header) + "." + joseBytes(payload) + "." + signature;
}

async function signedHmac(alg, secret = "secret-tres-long-et-aleatoire-pour-les-tests") {
  const hash = "SHA-" + alg.slice(2);
  const header = { alg, typ: "JWT" };
  const payload = { iss: "https://example.org", sub: "abc", aud: ["api", "cli"], exp: 2000000000 };
  const body = joseBytes(header) + "." + joseBytes(payload);
  const key = await webcrypto.subtle.importKey("raw", enc.encode(secret),
    { name: "HMAC", hash }, false, ["sign"]);
  const bytes = new Uint8Array(await webcrypto.subtle.sign("HMAC", key, enc.encode(body)));
  return { token: body + "." + core.encodeBase64Url(bytes), secret };
}

test("Base64URL strict : longueur, caractères et encodage canonique", () => {
  assert.equal(core.encodeBase64Url(new Uint8Array([255, 254, 253])), "__79");
  assert.deepEqual(Array.from(core.decodeBase64Url("__79")), [255, 254, 253]);
  for (const text of ["=", "+", "/", "a", "abc=", "à", "ab\nc", "_x"]) {
    assert.throws(() => core.decodeBase64Url(text), /Base64URL/);
  }
});

test("JWT compact : tolère Bearer, refuse cinq segments et payload JSON invalide", () => {
  const token = jwt({ alg: "HS256" }, { sub: "ok" }, "AQ");
  assert.equal(core.parseJwt(" Bearer " + token).payload.sub, "ok");
  assert.throws(() => core.parseJwt("a.b.c.d.e"), /JWE/);
  assert.throws(() => core.parseJwt("a.b"), /3 segments/);
  assert.throws(() => core.parseJwt(jwt({ alg: "HS256", b64: false }, { x: 1 }, "AQ")), /b64=false/);
  assert.throws(() => core.parseJwt(jwt({ alg: "" }, { x: 1 }, "AQ")), /alg/);
  assert.throws(() => core.parseJwt(jwt({ alg: "HS256" }, ["non objet"], "AQ")), /Payload/);
});

test("Analyse des dates : exp exactement égale à now est expirée", () => {
  const header = { alg: "HS256", typ: "at+jwt" };
  const report = core.checkClaims(header, {
    iss: "https://example.org", sub: "alice", aud: "api",
    exp: 1800000000, nbf: 1799999999, iat: 1799999998
  }, { iss: "https://example.org", aud: ["api"] }, 1800000000);
  assert.ok(report.checks.some(c => c.name === "Expiration" && c.state === "error"));
  assert.ok(report.checks.some(c => c.name === "Issuer attendu" && c.state === "ok"));
  assert.ok(report.checks.some(c => c.name === "Audience attendue" && c.state === "ok"));
  assert.ok(!report.checks.some(c => c.name === "Type déclaré" && c.state === "warning"));
});

test("La simple présence de iss, sub et aud n'est pas présentée comme une validation", () => {
  const report = core.checkClaims({ alg: "HS256" },
    { iss: "untrusted", sub: "user", aud: "api", exp: 2000000000 }, {}, 1900000000);
  for (const name of ["iss", "sub", "aud (format)"]) {
    const row = report.checks.find(c => c.name === name);
    assert.equal(row.state, "info", name);
  }
});

test("Les claims de type erroné, audiences attendues et incohérences sont détectés", () => {
  const report = core.checkClaims({ alg: "HS256" },
    { iss: 22, aud: [42], exp: "tomorrow", nbf: 2000, iat: 3000 },
    { aud: ["api", "cli"], audMode: "all", azp: "client", iss: "trusted" }, 1000);
  assert.ok(report.errors >= 4);
  assert.ok(report.checks.some(c => c.name === "Audience attendue" && c.state === "error"));
  assert.ok(report.checks.some(c => c.name === "Issuer attendu" && c.state === "error"));
  assert.ok(report.checks.some(c => c.name === "Authorized party" && c.state === "error"));
  assert.ok(report.checks.some(c => c.name === "exp" && c.state === "error"));
});

test("Tolérance d'horloge explicite et formats d'audience any/all", () => {
  const payload = { exp: 1000, nbf: 1002, aud: ["one"], iss: "issuer" };
  const strict = core.checkClaims({ alg: "HS256" }, payload, { skew: 0 }, 1001);
  assert.equal(strict.checks.find(c => c.name === "Expiration").state, "error");
  const tolerant = core.checkClaims({ alg: "HS256" }, payload, { skew: 60 }, 1001);
  assert.equal(tolerant.checks.find(c => c.name === "Expiration").state, "ok");
  assert.equal(tolerant.checks.find(c => c.name === "Not before").state, "ok");
  const any = core.checkClaims({ alg: "HS256" }, payload, { aud: "one,two", audMode: "any" }, 1001);
  const all = core.checkClaims({ alg: "HS256" }, payload, { aud: "one,two", audMode: "all" }, 1001);
  assert.equal(any.checks.find(c => c.name === "Audience attendue").state, "ok");
  assert.equal(all.checks.find(c => c.name === "Audience attendue").state, "error");
});

test("Signature non vérifiée ≠ signature invalide (sans clé)", async () => {
  const signed = await signedHmac("HS256");
  const parsed = core.parseJwt(signed.token);
  const noKey = await core.verifySignature(parsed, {});
  assert.equal(noKey.state, "missing");
  const invalid = await core.verifySignature(parsed, { secret: "wrong-secret" });
  assert.equal(invalid.state, "invalid");
  const ok = await core.verifySignature(parsed, { secret: signed.secret });
  assert.equal(ok.state, "verified");
});

for (const alg of ["HS256", "HS384", "HS512"]) {
  test("Vérifie une signature HMAC authentique : " + alg, async () => {
    const signed = await signedHmac(alg);
    const parsed = core.parseJwt(signed.token);
    assert.equal((await core.verifySignature(parsed, { secret: signed.secret })).state, "verified");
    assert.equal((await core.verifySignature(parsed, { expectedAlg: "RS256", secret: signed.secret })).state, "blocked");
  });
}

async function signAsymmetric(alg, kind) {
  const hash = "SHA-" + alg.slice(2);
  const isEc = kind === "ECDSA";
  const generated = await webcrypto.subtle.generateKey(
    isEc
      ? { name: "ECDSA", namedCurve: ({ ES256: "P-256", ES384: "P-384", ES512: "P-521" })[alg] }
      : { name: kind, hash, modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]) },
    true, ["sign", "verify"]
  );
  const h = joseBytes({ typ: "JWT", alg });
  const p = joseBytes({ sub: "test", exp: 2000000000 });
  const signingInput = enc.encode(h + "." + p);
  const signature = new Uint8Array(await webcrypto.subtle.sign(
    kind === "RSA-PSS"
      ? { name: kind, saltLength: Number(alg.slice(2)) / 8 }
      : kind === "ECDSA"
        ? { name: kind, hash }
        : { name: kind },
    generated.privateKey, signingInput
  ));
  const spki = Buffer.from(await webcrypto.subtle.exportKey("spki", generated.publicKey)).toString("base64");
  const pem = "-----BEGIN PUBLIC KEY-----\n" + spki.match(/.{1,64}/g).join("\n") +
    "\n-----END PUBLIC KEY-----";
  const jwk = await webcrypto.subtle.exportKey("jwk", generated.publicKey);
  return { token: h + "." + p + "." + core.encodeBase64Url(signature), pem, jwk };
}

for (const [alg, kind] of [
  ["RS256", "RSASSA-PKCS1-v1_5"], ["RS512", "RSASSA-PKCS1-v1_5"],
  ["PS256", "RSA-PSS"], ["PS384", "RSA-PSS"],
  ["ES256", "ECDSA"], ["ES384", "ECDSA"], ["ES512", "ECDSA"]
]) {
  test("Vérifie la signature " + alg + " en PEM et JWK", async () => {
    const { token, pem, jwk } = await signAsymmetric(alg, kind);
    const parsed = core.parseJwt(token);
    assert.equal((await core.verifySignature(parsed, { publicKey: pem })).state, "verified");
    assert.equal((await core.verifySignature(parsed, { publicKey: JSON.stringify(jwk) })).state, "verified");
    const badJwk = { ...jwk, d: "not-a-public-key" };
    assert.equal((await core.verifySignature(parsed, { publicKey: JSON.stringify(badJwk) })).state, "error");
  });
}

test("Bloque alg=none, alg inconnu et paramètre critique", async () => {
  const none = core.parseJwt(jwt({ alg: "none" }, { sub: "guest" }, ""));
  assert.equal((await core.verifySignature(none)).state, "blocked");
  const unknown = core.parseJwt(jwt({ alg: "EdDSA" }, { sub: "guest" }, "AQ"));
  assert.equal((await core.verifySignature(unknown)).state, "unsupported");
  const crit = core.parseJwt(jwt({ alg: "HS256", crit: ["new"] }, { sub: "guest" }, "AQ"));
  assert.equal((await core.verifySignature(crit, { secret: "x" })).state, "blocked");
});

test("Le rendu reste local ; tous les champs HTML attendus existent", () => {
  const ids = [...uiCode.matchAll(/\$\("([\w]+)"\)/g)].map(match => match[1]);
  for (const id of new Set(ids)) {
    assert.ok(html.includes('id="' + id + '"'), "ID non trouvé : " + id);
  }
  for (const file of ["jwt-analyzer-core.js", "jwt-analyzer-ui.js", "jwt-analyzer.css"]) {
    assert.ok(fs.existsSync(path.join(dir, file)));
    assert.ok(html.includes(file));
  }
  assert.match(html, /href="\.\.\/index\.html" aria-label="Retour à Toolbox"/);
  assert.match(html, /connect-src 'none'/);
  assert.doesNotMatch(html, /on(click|input|change)\s*=/i);
  assert.doesNotMatch(uiCode, /\.innerHTML\s*=/);
  assert.doesNotMatch(uiCode, /localStorage\.setItem\(.*(?:token|secret|history)/i);
  assert.ok(uiCode.includes('state.history'));
  assert.ok(css.includes("prefers-reduced-motion"));
  assert.ok(css.includes("@media (max-width: 480px)"));
});
