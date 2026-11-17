"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "tools/security-advisories.html"), "utf8");
const status = fs.readFileSync(path.join(root, "tools/service-status.html"), "utf8");
const portal = fs.readFileSync(path.join(root, "index.html"), "utf8");
const js = fs.readFileSync(path.join(root, "tools/service-status.js"), "utf8");
const css = fs.readFileSync(path.join(root, "tools/service-status.css"), "utf8");

const cards = [...html.matchAll(/<article class="status-link security-card" data-focus="(yes|no)" data-search="([^"]+)">([\s\S]*?)<\/article>/g)]
  .map(match => ({
    focus: match[1],
    search: match[2],
    body: match[3],
    links: [...match[3].matchAll(/<a href="([^"]+)" target="_blank" rel="noopener noreferrer">/g)].map(x => x[1]),
  }));

test("annuaire complet et navigation bidirectionnelle", () => {
  assert.equal(cards.length, 35);
  assert.equal(cards.filter(c => c.focus === "yes").length > 20, true);
  assert.match(portal, /href="tools\/security-advisories\.html"/);
  assert.match(html, /href="\.\.\/index\.html" aria-label="Retour à Toolbox"/);
  assert.match(html, /href="service-status\.html"/);
  assert.match(status, /href="security-advisories\.html"/);
  assert.match(html, /href="service-status\.css"/);
  assert.match(html, /src="service-status\.js"/);
  assert.match(html, /src="security-advisories-feed\.js"/);
});

test("liens officiels en HTTPS, pas de flux externe exécuté", () => {
  const allowed = [
    "github.com", "kubernetes.io", "learn.microsoft.com", "msrc.microsoft.com", "www.microsoft.com", "www.elastic.co",
    "grafana.com", "www.postgresql.org", "trino.io", "access.redhat.com",
    "ubuntu.com", "www.vmware.com", "support.broadcom.com",
    "www.synology.com", "www.debian.org", "security-tracker.debian.org"
  ];
  for (const card of cards) {
    assert.equal(card.links.length, 2);
    for (const link of card.links) {
      const url = new URL(link.replaceAll("&amp;", "&"));
      assert.equal(url.protocol, "https:");
      assert.ok(allowed.includes(url.hostname), url.hostname);
    }
  }
  assert.match(html, /connect-src 'self'/); // Only same-origin JSON snapshot is fetched.
  assert.match(html, /pas de veille en temps réel/i);
  assert.doesNotMatch(html, /<script(?![^>]*\bsrc=)[^>]*>[\s\S]*?<\/script>/i);
  assert.match(css, /prefers-reduced-motion/);
});

test("liens prioritaires AKS, Argo CD, Cilium, Kyverno et Harbor", () => {
  for (const url of [
    "https://learn.microsoft.com/en-us/azure/aks/security-bulletins/overview",
    "https://github.com/argoproj/argo-cd/security/advisories",
    "https://github.com/cilium/cilium/security/advisories",
    "https://github.com/kyverno/kyverno/security/advisories",
    "https://github.com/goharbor/harbor/security/advisories",
  ]) {
    assert.ok(cards.some(card => card.links.includes(url)), url);
  }
});

function createPage() {
  class Element {
    constructor() {
      this.dataset = {};
      this.value = "";
      this.textContent = "";
      this.hidden = false;
      this.attrs = {};
      this.events = new Map();
      this.classList = { toggle() {} };
    }
    addEventListener(name, fn) { this.events.set(name, fn); }
    setAttribute(name, value) { this.attrs[name] = value; }
    focus() {}
    blur() {}
    dispatch(name) {
      const fn = this.events.get(name);
      assert.ok(fn, name);
      fn({ target: this });
    }
  }
  const sections = [...html.matchAll(/<section class="status-section" data-group="([^"]+)"[\s\S]*?<\/section>/g)]
    .map(match => {
      const section = new Element();
      section.dataset.group = match[1].replaceAll("&amp;", "&");
      const localCards = [...match[0].matchAll(/<article class="status-link security-card" data-focus="(yes|no)" data-search="([^"]+)">/g)]
        .map(item => {
          const card = new Element();
          card.dataset.focus = item[1];
          card.dataset.search = item[2].replaceAll("&amp;", "&");
          return card;
        });
      section.querySelectorAll = () => localCards;
      return section;
    });
  const buttons = [...html.matchAll(/<button class="category-button[^"]*" type="button" data-category="([^"]+)" aria-pressed="[^"]+">/g)]
    .map(match => {
      const button = new Element();
      button.dataset.category = match[1].replaceAll("&amp;", "&");
      return button;
    });
  const elements = Object.fromEntries(
    ["statusSearch", "resultCount", "emptyState", "clearSearch", "themeToggle", "themeIcon"]
      .map(id => [id, new Element()])
  );
  elements.resultCount.dataset.singular = "produit";
  elements.resultCount.dataset.plural = "produits";
  const meta = new Element();
  const document = {
    documentElement: { dataset: {}, classList: { remove() {} } },
    getElementById: id => elements[id],
    querySelectorAll: selector => selector === ".status-section" ? sections : buttons,
    querySelector: () => meta,
    addEventListener() {},
  };
  vm.runInNewContext(js, {
    document, window: { matchMedia: () => ({ matches: false, addEventListener() {} }) },
    localStorage: { getItem: () => null, setItem() {} },
    HTMLElement: Element
  });
  return { sections, buttons, elements };
}

test("recherche, catégorie prioritaire, remise à zéro et compteur", () => {
  const page = createPage();
  assert.equal(page.elements.resultCount.textContent, "35 produits");
  page.elements.statusSearch.value = "kyverno";
  page.elements.statusSearch.dispatch("input");
  assert.equal(page.elements.resultCount.textContent, "1 produit");
  page.buttons.find(b => b.dataset.category === "priority").dispatch("click");
  assert.equal(page.elements.resultCount.textContent, "1 produit");
  page.buttons.find(b => b.dataset.category === "Conteneurs & CI/CD").dispatch("click");
  assert.equal(page.elements.resultCount.textContent, "0 produits");
  assert.equal(page.elements.emptyState.hidden, false);
  page.elements.clearSearch.dispatch("click");
  assert.equal(page.elements.resultCount.textContent, "35 produits");
  assert.equal(page.elements.emptyState.hidden, true);
});
