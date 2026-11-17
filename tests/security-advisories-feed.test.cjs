"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "tools/security-advisories.html"), "utf8");
const script = fs.readFileSync(path.join(root, "tools/security-advisories-feed.js"), "utf8");

function fakePage(snapshot, { fail = false } = {}) {
  class Element {
    constructor(tag = "DIV") {
      this.tagName = tag;
      this.children = [];
      this.events = {};
      this.textContent = "";
      this.value = "all";
      this.hidden = false;
    }
    addEventListener(name, fn) { this.events[name] = fn; }
    appendChild(child) { this.children.push(child); return child; }
    replaceChildren(...children) { this.children = children; }
    dispatch(name) { this.events[name](); }
  }
  const elements = Object.fromEntries(
    ["advisoryFeed", "feedState", "feedList", "feedFilters",
      "feedSeverity", "feedProduct", "feedCount"].map(id => [id, new Element()])
  );
  const doc = {
    getElementById(id) { return elements[id]; },
    createElement(tag) { return new Element(tag); },
  };
  const context = {
    document: doc,
    URL,
    Date,
    fetch: async (url, options) => {
      assert.equal(url, "./data/security-advisories.json");
      assert.equal(options.cache, "no-store");
      if (fail) throw new Error("offline");
      return { ok: true, json: async () => snapshot };
    },
  };
  vm.runInNewContext(script, context);
  return { elements };
}

async function settle() {
  await new Promise(resolve => setImmediate(resolve));
}

const ghsa = {
  id: "GHSA-aaaa-bbbb-cccc", product: "Argo CD", title: "<img src=x onerror=alert(1)>",
  source: "github:argoproj/argo-cd", published_at: "2026-11-16T00:00:00Z",
  severity: "high", cves: ["CVE-2026-12345"],
  affected_versions: ["example: < 2.1"], patched_versions: ["example: 2.1"],
  url: "https://github.com/argoproj/argo-cd/security/advisories/GHSA-aaaa-bbbb-cccc",
};
const aks = {
  id: "AKS-2026-0008", product: "AKS", title: "AKS bulletin",
  source: "aks", published_at: "2026-11-03T00:00:00Z",
  severity: "unknown", cves: [], affected_versions: [], patched_versions: [],
  url: "https://learn.microsoft.com/en-us/azure/aks/security-bulletins/overview#aks-2026-0008",
};

test("page retains the directory and uses only same-origin feed fetch", () => {
  assert.match(html, /href="service-status\.css"/);
  assert.match(html, /id="statusSections"/);
  assert.match(html, /connect-src 'self'/);
  assert.match(html, /src="security-advisories-feed\.js"/);
  assert.doesNotMatch(script, /\.innerHTML\s*=/);
});

test("shows publication time, partial source failures and safely renders text", async () => {
  const { elements } = fakePage({
    schema_version: 1, generated_at: new Date().toISOString(),
    sources: { aks: { status: "ok" }, github: { status: "error" } },
    alerts: [ghsa, aks],
  });
  await settle();
  assert.equal(elements.feedFilters.hidden, false);
  assert.equal(elements.feedCount.textContent, "2 avis affichés");
  assert.match(elements.feedState.textContent, /1 source\(s\) indisponible/);
  const first = elements.feedList.children[0];
  assert.equal(first.children[1].textContent, ghsa.title);
  assert.equal(first.children.at(-1).href, ghsa.url);
  assert.equal(first.children.at(-1).rel, "noopener noreferrer");
  elements.feedSeverity.value = "high";
  elements.feedSeverity.dispatch("change");
  assert.equal(elements.feedCount.textContent, "1 avis affiché");
});

test("unpublished snapshot never claims zero vulnerabilities", async () => {
  const { elements } = fakePage({ schema_version: 1, generated_at: null, sources: {}, alerts: [] });
  await settle();
  assert.match(elements.feedState.textContent, /Collecte initiale non publiée/);
  assert.equal(elements.feedFilters.hidden, true);
});

test("rejects untrusted URLs and keeps source links when offline", async () => {
  const { elements } = fakePage({
    schema_version: 1, generated_at: new Date().toISOString(),
    sources: { aks: { status: "ok" } },
    alerts: [{ ...ghsa, url: "https://evil.example/alert" }, aks],
  });
  await settle();
  assert.equal(elements.feedCount.textContent, "1 avis affiché");
  const offline = fakePage(null, { fail: true });
  await settle();
  assert.match(offline.elements.feedState.textContent, /Flux non disponible/);
});
