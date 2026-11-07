/* Exécution : node --test tests/jwt-analyzer-ui.test.cjs (Node 20+) */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const fs = require("node:fs");
const vm = require("node:vm");
const { webcrypto } = require("node:crypto");

const dir = path.join(__dirname, "..", "tools");
const coreSource = fs.readFileSync(path.join(dir, "jwt-analyzer-core.js"), "utf8");
const uiSource = fs.readFileSync(path.join(dir, "jwt-analyzer-ui.js"), "utf8");
const html = fs.readFileSync(path.join(dir, "decodeur_analyseur_jwt_standalone.html"), "utf8");
const encoder = new TextEncoder();

class Element {
  constructor(id = "") {
    this.id = id;
    this.events = new Map();
    this.attrs = {};
    this.children = [];
    this.dataset = {};
    this.textContent = "";
    this.value = "";
    this.type = "text";
    this.hidden = false;
    this.disabled = false;
    this.open = false;
    this.style = {};
  }
  addEventListener(name, cb) {
    const callbacks = this.events.get(name) || [];
    callbacks.push(cb);
    this.events.set(name, callbacks);
  }
  dispatch(name, payload = {}) {
    const callbacks = this.events.get(name) || [];
    assert.ok(callbacks.length, "Aucun listener " + name + " sur " + this.id);
    for (const callback of callbacks) callback({
      target: this, preventDefault() {}, ctrlKey: false, metaKey: false, ...payload
    });
  }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = [...nodes]; }
  focus() { this.focused = true; }
  scrollIntoView() { this.scrolled = true; }
}

function makePage(seed = {}) {
  const elements = Object.fromEntries(
    [...html.matchAll(/id="([A-Za-z0-9]+)"/g)].map(match => [match[1], new Element(match[1])])
  );
  const meta = new Element("meta");
  const doc = {
    documentElement: { dataset: {} },
    getElementById(id) {
      assert.ok(elements[id], "Élément absent : " + id);
      return elements[id];
    },
    querySelector(selector) {
      if (selector === 'meta[name="theme-color"]') return meta;
      throw new Error("Sélecteur inconnu : " + selector);
    },
    createElement(tag) { return new Element(tag); },
    createDocumentFragment() { return new Element("fragment"); }
  };

  const stored = new Map(Object.entries(seed));
  const localStorage = {
    getItem(key) { return stored.has(key) ? stored.get(key) : null; },
    setItem(key, value) { stored.set(key, String(value)); },
    removeItem(key) { stored.delete(key); }
  };

  const window = {
    crypto: webcrypto, matchMedia() { return { matches: false, addEventListener() {} }; },
    setTimeout() { return 1; }, clearTimeout() {}
  };

  const copied = [];
  const sandbox = {
    window, document: doc, localStorage, crypto: webcrypto,
    navigator: { clipboard: { async writeText(value) { copied.push(value); } } },
    TextEncoder, TextDecoder, Uint8Array, btoa, atob, Date
  };
  vm.runInNewContext(coreSource, sandbox, { filename: "jwt-analyzer-core.js" });
  vm.runInNewContext(uiSource, sandbox, { filename: "jwt-analyzer-ui.js" });

  return { elements, stored, window, copied, meta };
}

function encodeJson(value) {
  let str = "";
  const bytes = encoder.encode(JSON.stringify(value));
  for (const byte of bytes) str += String.fromCharCode(byte);
  return btoa(str).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

async function makeToken(secret = "une-cle-demo-solide") {
  const header = encodeJson({ alg: "HS256", typ: "JWT" });
  const payload = encodeJson({
    iss: "https://example.org", sub: "alice", aud: "my-api",
    exp: Math.floor(Date.now() / 1000) + 3600
  });
  const text = header + "." + payload;
  const key = await webcrypto.subtle.importKey("raw", encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const sig = await webcrypto.subtle.sign("HMAC", key, encoder.encode(text));
  const signature = Buffer.from(sig).toString("base64url");
  return text + "." + signature;
}

const tick = () => new Promise(resolve => setTimeout(resolve, 15));

test("Migration : supprime JWT, secret HMAC, PEM et payload de l'ancien localStorage", () => {
  const legacy = {
    theme: "dark",
    rules: { iss: "https://trusted.example", aud: ["api"], audMode: "any" },
    history: [{
      original: "SENSITIVE_JWT_SHOULD_NOT_PERSIST",
      payload: { identity: "SENSITIVE_PAYLOAD" },
      inputs: {
        hsSecret: "SENSITIVE_HMAC_SECRET",
        rsaPem: "SENSITIVE_PRIVATE_OR_PUBLIC_PEM"
      }
    }]
  };
  const page = makePage({ "jwt-decode-analyze": JSON.stringify(legacy) });
  const updated = page.stored.get("jwt-decode-analyze");
  for (const secret of [
    "SENSITIVE_JWT_SHOULD_NOT_PERSIST", "SENSITIVE_PAYLOAD",
    "SENSITIVE_HMAC_SECRET", "SENSITIVE_PRIVATE_OR_PUBLIC_PEM"
  ]) {
    assert.ok(!updated.includes(secret), "Secret restant en localStorage : " + secret);
  }
  assert.deepEqual(JSON.parse(updated).rules.aud, ["api"]);
  assert.equal(page.elements.expectedIss.value, "https://trusted.example");
  assert.equal(page.elements.themeSun.hidden, false);
  assert.match(page.elements.message.textContent, /supprimée/);
});

test("Analyse HMAC : états et historique en mémoire sans aucune persistance", async () => {
  const page = makePage();
  const token = await makeToken();
  page.elements.jwtInput.value = token;
  page.elements.jwtForm.dispatch("submit");
  await tick();
  assert.equal(page.elements.signatureBadge.dataset.state, "missing");
  assert.equal(page.elements.resultsSection.hidden, false);
  assert.equal(page.elements.clearHistory.disabled, false);

  page.elements.hmacSecret.value = "une-cle-demo-solide";
  page.elements.hmacSecret.dispatch("input");
  assert.equal(page.elements.resultsSection.hidden, true);
  page.elements.jwtForm.dispatch("submit");
  await tick();
  assert.equal(page.elements.signatureBadge.dataset.state, "verified");
  assert.equal(page.elements.claimsBadge.dataset.state, "ok");

  page.elements.hmacSecret.value = "wrong";
  page.elements.hmacSecret.dispatch("input");
  page.elements.jwtForm.dispatch("submit");
  await tick();
  assert.equal(page.elements.signatureBadge.dataset.state, "invalid");
  assert.ok(page.stored.size === 0, "Les analyses n'écrivent pas dans localStorage");

  page.elements.copyPayload.dispatch("click");
  await tick();
  assert.equal(page.copied.length, 1);
  assert.ok(page.copied[0].includes('"iss": "https://example.org"'));

  page.elements.clearHistory.dispatch("click");
  assert.equal(page.elements.clearHistory.disabled, true);
});

test("Modification du JWT : ne conserve pas de statut de vérification obsolète", async () => {
  const page = makePage();
  page.elements.jwtInput.value = await makeToken();
  page.elements.hmacSecret.value = "une-cle-demo-solide";
  page.elements.jwtForm.dispatch("submit");
  await tick();
  assert.equal(page.elements.signatureBadge.dataset.state, "verified");
  page.elements.jwtInput.value = "invalid";
  page.elements.jwtInput.dispatch("input");
  assert.equal(page.elements.signatureBadge.dataset.state, "idle");
  assert.equal(page.elements.resultsSection.hidden, true);
  page.elements.jwtForm.dispatch("submit");
  await tick();
  assert.match(page.elements.message.textContent, /3 segments/);
});

test("La préférence du thème est la seule valeur écrite hors des règles explicites", () => {
  const page = makePage();
  page.elements.themeToggle.dispatch("click");
  assert.equal(page.stored.get("password-studio-theme"), "dark");
  assert.equal(page.stored.has("jwt-decode-analyze"), false);
  page.elements.expectedIss.value = "https://issuer.example";
  page.elements.saveRules.dispatch("click");
  const rules = JSON.parse(page.stored.get("jwt-decode-analyze")).rules;
  assert.equal(rules.iss, "https://issuer.example");
  assert.ok(!("history" in JSON.parse(page.stored.get("jwt-decode-analyze"))));
});
