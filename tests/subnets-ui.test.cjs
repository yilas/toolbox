/* node --test tests/subnets-ui.test.cjs */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const dir = path.resolve(__dirname, "..", "tools");
const html = fs.readFileSync(path.join(dir, "subnets_calculator.html"), "utf8");
const coreSource = fs.readFileSync(path.join(dir, "subnets-core.js"), "utf8");
const storageSource = fs.readFileSync(path.join(dir, "subnets-storage.js"), "utf8");
const uiSource = fs.readFileSync(path.join(dir, "subnets-ui.js"), "utf8");

class Element {
  constructor(tag = "div") {
    this.tagName = tag.toUpperCase();
    this.children = [];
    this.parent = null;
    this.events = new Map();
    this.dataset = {};
    this.attrs = {};
    this.style = {};
    this.hidden = false;
    this.disabled = false;
    this.checked = false;
    this.value = "";
    this.textContent = "";
    this.className = "";
    this.open = false;
    const classes = new Set();
    this.classList = {
      add(...names) { names.forEach(name => classes.add(name)); },
      remove(...names) { names.forEach(name => classes.delete(name)); },
      contains(name) { return classes.has(name); },
      toggle(name, force) {
        const on = force === undefined ? !classes.has(name) : Boolean(force);
        if (on) classes.add(name);
        else classes.delete(name);
        return on;
      }
    };
  }
  addEventListener(type, listener) {
    if (!this.events.has(type)) this.events.set(type, []);
    this.events.get(type).push(listener);
  }
  dispatch(type, args = {}) {
    const events = this.events.get(type) || [];
    assert.ok(events.length, "Listener manquant : " + type + " (" + this.tagName + ")");
    for (const fn of events) fn({ target: this, preventDefault() {}, ...args });
  }
  append(...children) {
    for (const child of children) {
      if (child.tagName === "#FRAGMENT") this.append(...child.children);
      else { child.parent = this; this.children.push(child); }
    }
  }
  replaceChildren(...children) { this.children = []; this.append(...children); }
  setAttribute(key, value) { this.attrs[key] = String(value); }
  closest(selector) {
    if (selector !== "[hidden]") throw Error("Selecteur non simule : " + selector);
    let node = this;
    while (node) {
      if (node.hidden) return node;
      node = node.parent;
    }
    return null;
  }
  showModal() { this.open = true; }
  close() {
    this.open = false;
    for (const fn of this.events.get("close") || []) fn({ target: this });
  }
  scrollIntoView() { this.scrolled = true; }
  focus() { this.focused = true; }
}

class MemoryStorage {
  constructor(initial = {}) { this.values = new Map(Object.entries(initial)); }
  get length() { return this.values.size; }
  key(i) { return [...this.values.keys()][i] || null; }
  getItem(key) { return this.values.get(key) ?? null; }
  setItem(key, val) { this.values.set(key, String(val)); }
  removeItem(key) { this.values.delete(key); }
}

function createPage(initial = {}) {
  const store = new MemoryStorage(initial);
  const elements = {};
  for (const match of html.matchAll(/id="([\w-]+)"/g)) {
    elements[match[1]] = new Element("div");
  }
  const body = new Element("body");
  const meta = new Element("meta");
  const document = {
    documentElement: { dataset: {} },
    body,
    getElementById(id) {
      if (!elements[id]) throw Error("Element absent : " + id);
      return elements[id];
    },
    createElement(tag) { return new Element(tag); },
    createDocumentFragment() { return new Element("#fragment"); },
    querySelector(selector) {
      if (selector === 'meta[name="theme-color"]') return meta;
      throw Error("Selecteur non simule : " + selector);
    },
    querySelectorAll(selector) {
      if (selector !== "[data-net-cidr]") throw Error("Selecteur non simule : " + selector);
      function walk(node) {
        return [node, ...node.children.flatMap(walk)];
      }
      return [elements.hierarchyTree, elements.subnetGrid].flatMap(walk)
        .filter(node => node.dataset.netCidr);
    }
  };
  const parentMap = [
    ["hierarchyTree","treeView"],
    ["subnetGrid","gridView"],
    ["treeView","workspace"],
    ["gridView","workspace"]
  ];
  for (const [id, parent] of parentMap) elements[parent].append(elements[id]);
  const window = {
    localStorage: store,
    matchMedia() { return { matches: false, addEventListener() {} }; },
    confirm() { return true; },
    setTimeout() { return 0; }
  };
  const sandbox = { window, document, localStorage: store };
  vm.runInNewContext(coreSource, sandbox, { filename: "subnets-core.js" });
  vm.runInNewContext(storageSource, sandbox, { filename: "subnets-storage.js" });
  vm.runInNewContext(uiSource, sandbox, { filename: "subnets-ui.js" });
  return { elements, store, window, document };
}

function click(element) { element.dispatch("click"); }
function submit(element) { element.dispatch("submit"); }

function findNode(page, cidr) {
  const nodes = page.document.querySelectorAll("[data-net-cidr]");
  return nodes.find(node => node.dataset.netCidr === cidr && !node.closest("[hidden]"));
}

function flatten(root) {
  return [root, ...root.children.flatMap(flatten)];
}

test("Chargement : normalisation canonique et arbre parent/enfant dépliable", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.13/24";
  submit(page.elements.networkForm);
  assert.equal(page.elements.activeNetwork.textContent, "10.0.0.0/24");
  assert.equal(page.elements.metricTotal.textContent, "256");
  assert.equal(page.elements.metricFree.textContent, "256");
  assert.equal(page.elements.workspace.hidden, false);
  assert.ok(findNode(page, "10.0.0.0/24"));
  assert.ok(findNode(page, "10.0.0.0/25"));
  assert.equal(page.store.getItem("toolbox-subnets-v2") !== null, true);
});

test("Réserver via l'arbre et garder l'inventaire actif malgré une saisie modifiée", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  page.elements.networkInput.value = "192.0.2.0/24";
  page.elements.networkInput.dispatch("input");
  const root = findNode(page, "10.0.0.0/24");
  const reserve = flatten(root).find(x => x.tagName === "BUTTON" && x.textContent === "Réserver");
  assert.ok(reserve);
  click(reserve);
  assert.equal(page.elements.allocationDialog.open, true);
  page.elements.allocationName.value = "Service API";
  submit(page.elements.allocationForm);
  assert.equal(page.elements.metricOccupied.textContent, "256");
  const saved = JSON.parse(page.store.getItem("toolbox-subnets-v2"));
  assert.equal(saved.active, "10.0.0.0/24");
  assert.equal(saved.networks["10.0.0.0/24"].allocations[0].name, "Service API");
  assert.equal(saved.networks["192.0.2.0/24"], undefined);
  assert.equal(findNode(page, "10.0.0.0/24").dataset.status, "reserved");
  assert.equal(findNode(page, "10.0.0.0/25").dataset.status, "inherited");
});

test("Libération : occupation recalculée et nœuds redevenus libres", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  const reserve = flatten(findNode(page, "10.0.0.0/24"))
    .find(x => x.tagName === "BUTTON" && x.textContent === "Réserver");
  click(reserve);
  submit(page.elements.allocationForm);
  const modify = flatten(findNode(page, "10.0.0.0/24"))
    .find(x => x.tagName === "BUTTON" && x.textContent === "Modifier");
  click(modify);
  assert.equal(page.elements.releaseButton.hidden, false);
  click(page.elements.releaseButton);
  assert.equal(page.elements.metricOccupied.textContent, "0");
  assert.equal(findNode(page, "10.0.0.0/24").dataset.status, "free");
});

test("Grille : préfixes /31 /32 et recherche IP vers la bonne page", () => {
  const page = createPage();
  page.elements.networkInput.value = "192.0.2.0/24";
  submit(page.elements.networkForm);
  click(page.elements.gridMode);
  page.elements.maskSelect.value = "31";
  page.elements.maskSelect.dispatch("change");
  assert.equal(page.elements.pageInfo.textContent.includes("128 sous-réseaux"), true);
  assert.equal(page.elements.nextPage.disabled, false);
  click(page.elements.nextPage);
  assert.equal(page.elements.pageInfo.textContent.includes("Page 2"), true);
  page.elements.searchInput.value = "192.0.2.255";
  submit(page.elements.searchForm);
  assert.equal(page.elements.maskSelect.value, "32");
  assert.ok(findNode(page, "192.0.2.255/32"));
});

test("Migration ancienne : l'occupation hiérarchique reste exacte", () => {
  const initial = {
    "snet_calc_inventory_10.0.0.0/24": JSON.stringify({
      "10.0.0.0/26": "taken", "10.0.0.0/25": "taken",
      "10.0.0.128/26": "taken"
    }),
    "snet_calc_last_active_network": "10.0.0.0/24"
  };
  const page = createPage(initial);
  assert.equal(page.elements.metricOccupied.textContent, "192");
  assert.equal(page.elements.overlapNotice.hidden, false);
  assert.equal(findNode(page, "10.0.0.0/24").dataset.status, "partial");
  assert.equal(page.store.getItem("snet_calc_inventory_10.0.0.0/24"),
    initial["snet_calc_inventory_10.0.0.0/24"]);
});
