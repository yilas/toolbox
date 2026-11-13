/* Exécution : node --test tests/portal.test.cjs (Node 20+) */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
const css = fs.readFileSync(path.join(root, "portal.css"), "utf8");
const source = fs.readFileSync(path.join(root, "portal.js"), "utf8");

const cards = [...html.matchAll(/<a class="tool-card" href="([^"]+)" data-search="([^"]+)">[\s\S]*?<h2>([^<]+)<\/h2>[\s\S]*?<\/a>/g)]
  .map(match => ({ href: match[1], search: match[2], title: match[3] }));

function createPage({ darkSystem = false, storedTheme = null } = {}) {
  class Element {
    constructor(tagName = "DIV") {
      this.tagName = tagName;
      this.isContentEditable = false;
      this.dataset = {};
      this.hidden = false;
      this.value = "";
      this.textContent = "";
      this.attrs = {};
      this.events = new Map();
      this.focused = false;
      this.blurred = false;
    }
    addEventListener(type, handler) { this.events.set(type, handler); }
    setAttribute(name, value) { this.attrs[name] = value; }
    dispatch(type, event = {}) {
      const handler = this.events.get(type);
      assert.ok(handler, "Événement non câblé : " + type);
      handler({ target: this, ...event });
    }
    focus() { this.focused = true; doc.activeElement = this; }
    blur() { this.blurred = true; doc.activeElement = null; }
  }

  const names = [
    "toolSearch", "resultCount", "emptyState", "clearSearch",
    "upcomingSection", "themeToggle", "themeMoon", "themeSun"
  ];
  const elements = Object.fromEntries(names.map(id => [id, new Element(id === "toolSearch" ? "INPUT" : "DIV")]));
  const cardEls = cards.map(card => {
    const el = new Element("A");
    el.dataset.search = card.search;
    el.textContent = card.title;
    return el;
  });
  const bodyClass = new Set(["no-js"]);
  const htmlElement = { dataset: {}, classList: { remove(value) { bodyClass.delete(value); } } };
  const meta = new Element("META");
  let savedTheme = storedTheme;
  const docEvents = new Map();
  const doc = {
    documentElement: htmlElement,
    activeElement: null,
    getElementById(id) { return elements[id]; },
    querySelectorAll(selector) {
      if (selector === ".tool-card") return cardEls;
      throw new Error("Sélecteur inattendu : " + selector);
    },
    querySelector(selector) {
      if (selector === 'meta[name="theme-color"]') return meta;
      throw new Error("Sélecteur inattendu : " + selector);
    },
    addEventListener(type, callback) { docEvents.set(type, callback); }
  };
  const media = { matches: darkSystem, addEventListener() {} };
  const window = { matchMedia() { return media; } };
  const storage = {
    getItem(key) { assert.equal(key, "password-studio-theme"); return savedTheme; },
    setItem(key, value) { assert.equal(key, "password-studio-theme"); savedTheme = value; }
  };

  vm.runInNewContext(source, {
    document: doc, window, localStorage: storage, HTMLElement: Element
  }, { filename: "portal.js" });

  return {
    elements, cardEls, htmlElement, meta, bodyClass,
    get savedTheme() { return savedTheme; },
    keydown(event) {
      const handler = docEvents.get("keydown");
      assert.ok(handler);
      handler({ defaultPrevented: false, metaKey: false, ctrlKey: false, altKey: false, ...event });
    }
  };
}

test("Le catalogue présente les dix outils réellement présents, sans lien fictif", () => {
  assert.equal(cards.length, 10);
  assert.equal(new Set(cards.map(card => card.href)).size, cards.length);
  cards.forEach(card => {
    assert.match(card.href, /^tools\/[a-zA-Z0-9_.-]+\.html$/);
    assert.ok(fs.existsSync(path.join(root, card.href)), "Outil introuvable : " + card.href);
    assert.ok(card.search.trim(), "Mots-clés absents : " + card.title);
  });
  assert.doesNotMatch(html, /href="#"/);
});

test("Le portail fonctionne sans JS et ne dépend pas de CDN", () => {
  assert.ok(html.includes('class="no-js"'));
  assert.ok(html.includes('id="toolsGrid"'));
  assert.ok(html.includes('href="portal.css"'));
  assert.ok(html.includes('src="portal.js"'));
  assert.ok(html.includes("connect-src 'none'"));
  assert.doesNotMatch(html, /on(click|input|change)\s*=/i);
  assert.doesNotMatch(html, /https?:\/\/[^"]+\.js"/i);
  assert.ok(css.includes("@media (max-width: 570px)"));
  assert.ok(css.includes("prefers-reduced-motion"));
});

test("Recherche : accents, casse, plusieurs termes et aucun résultat", () => {
  const page = createPage();
  assert.equal(page.elements.resultCount.textContent, "10 outils");
  assert.equal(page.bodyClass.has("no-js"), false);

  page.elements.toolSearch.value = "reseaux";
  page.elements.toolSearch.dispatch("input");
  assert.equal(page.elements.resultCount.textContent, "1 outil");
  assert.equal(page.cardEls.filter(card => !card.hidden).length, 1);

  page.elements.toolSearch.value = "mot de passe cryptographie";
  page.elements.toolSearch.dispatch("input");
  assert.equal(page.elements.resultCount.textContent, "1 outil");

  page.elements.toolSearch.value = "rien-n-existe";
  page.elements.toolSearch.dispatch("input");
  assert.equal(page.elements.resultCount.textContent, "0 outils");
  assert.equal(page.elements.emptyState.hidden, false);
  assert.equal(page.elements.upcomingSection.hidden, true);

  page.elements.clearSearch.dispatch("click");
  assert.equal(page.elements.toolSearch.value, "");
  assert.equal(page.elements.emptyState.hidden, true);
  assert.equal(page.cardEls.filter(card => !card.hidden).length, 10);
});

test("Clavier : slash focalise la recherche, Escape la réinitialise", () => {
  const page = createPage();
  let prevented = false;
  page.keydown({ key: "/", target: new (page.elements.toolSearch.constructor)(), preventDefault() { prevented = true; } });
  assert.equal(prevented, true);
  assert.equal(page.elements.toolSearch.focused, true);

  page.elements.toolSearch.value = "jwt";
  page.elements.toolSearch.dispatch("input");
  page.keydown({ key: "Escape", target: page.elements.toolSearch });
  assert.equal(page.elements.toolSearch.value, "");
  assert.equal(page.elements.toolSearch.blurred, true);
  assert.equal(page.elements.resultCount.textContent, "10 outils");
});

test("Thème : partage de la préférence avec le générateur de mots de passe", () => {
  const page = createPage({ darkSystem: true });
  assert.equal(page.htmlElement.dataset.theme, "dark");
  assert.equal(page.elements.themeMoon.hidden, true);
  page.elements.themeToggle.dispatch("click");
  assert.equal(page.htmlElement.dataset.theme, "light");
  assert.equal(page.savedTheme, "light");
  assert.equal(page.meta.attrs.content, "#f5f7fc");

  const existing = createPage({ storedTheme: "dark", darkSystem: false });
  assert.equal(existing.htmlElement.dataset.theme, "dark");
});

test("Les deux futurs outils ne sont pas annoncés comme des liens accessibles", () => {
  assert.match(html, /En préparation/);
  assert.match(html, /Convertisseur de dates ISO/);
  assert.match(html, /Analyseur d'e-mails suspects/);
  assert.ok(html.includes('id="upcomingSection"'));
});
