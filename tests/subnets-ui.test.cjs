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
    this.style = { setProperty(name, value) { this[name] = value; } };
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
      return [elements.maskOverview, elements.subnetGrid].flatMap(walk)
        .filter(node => node.dataset.netCidr);
    }
  };
  const parentMap = [
    ["maskOverview","overviewView"],
    ["subnetGrid","gridView"],
    ["overviewView","workspace"],
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

test("Présentation d'origine : sections par masque et cartes CIDR lisibles", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.105.64.12/19";
  submit(page.elements.networkForm);
  assert.equal(page.elements.activeNetwork.textContent, "10.105.64.0/19");
  assert.equal(page.elements.metricTotal.textContent, "8 192".replace(/\u202f/g, " ")); // Locale FR
  assert.equal(page.elements.overviewView.hidden, false);
  const bands = page.elements.maskOverview.children;
  assert.equal(bands.length, 10); // /20 → /29 : comportement d'origine
  assert.deepEqual(bands.map(band => band.dataset.prefix),
    ["20","21","22","23","24","25","26","27","28","29"]);
  assert.deepEqual(bands.map(band => band.children[1].children.length),
    [2,4,8,16,32,64,128,256,512,1024]);
  const mask24 = bands[4];
  assert.equal(mask24.children[0].children[0].textContent, "Masque /24");
  const first = mask24.children[1].children[0];
  const last = mask24.children[1].children.at(-1);
  assert.equal(first.children[0].children[1].textContent, "10.105.64.0/24");
  assert.equal(last.children[0].children[1].textContent, "10.105.95.0/24");
  assert.equal(first.children[0].children[0].type, "checkbox");
  assert.equal(first.className, "network-item");
  assert.equal(page.elements.visibleMaskCount.textContent, "10 / 13 niveaux");
  assert.equal(page.store.getItem("toolbox-subnets-v2") !== null, true);
});

test("Réservation rapide : coche, pas de dialogue, héritage et inventaire correct", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.12/24";
  submit(page.elements.networkForm);
  page.elements.networkInput.value = "192.0.2.0/24";
  page.elements.networkInput.dispatch("input");
  const card = findNode(page, "10.0.0.0/25");
  assert.ok(card);
  const checkbox = card.children[0].children[0];
  checkbox.checked = true;
  checkbox.dispatch("change");
  assert.equal(page.elements.allocationDialog.open, false);
  assert.equal(page.elements.metricOccupied.textContent, "128");
  const saved = JSON.parse(page.store.getItem("toolbox-subnets-v2"));
  assert.equal(saved.active, "10.0.0.0/24");
  assert.equal(saved.networks["192.0.2.0/24"], undefined);
  assert.equal(saved.networks["10.0.0.0/24"].allocations[0].name, "");
  assert.equal(saved.networks["10.0.0.0/24"].allocations[0].description, "");
  assert.equal(findNode(page, "10.0.0.0/25").dataset.status, "reserved");
  assert.equal(findNode(page, "10.0.0.0/26").dataset.status, "inherited");
  assert.equal(findNode(page, "10.0.0.0/26").children[0].children[0].disabled, true);
});

test("Libérer par la case : recalcul du réseau et conservation de la présentation", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  let checkbox = findNode(page, "10.0.0.0/25").children[0].children[0];
  checkbox.checked = true;
  checkbox.dispatch("change");
  assert.equal(page.elements.metricOccupied.textContent, "128");
  checkbox = findNode(page, "10.0.0.0/25").children[0].children[0];
  assert.equal(checkbox.checked, true);
  checkbox.checked = false;
  checkbox.dispatch("change");
  assert.equal(page.elements.metricOccupied.textContent, "0");
  assert.equal(findNode(page, "10.0.0.0/25").dataset.status, "free");
});

test("Détails facultatifs : la case ne les exige pas, le menu les propose", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  const free = findNode(page, "10.0.0.0/25");
  const details = free.children.at(-1).children[0];
  assert.equal(details.textContent, "⋯");
  click(details);
  assert.equal(page.elements.allocationDialog.open, true);
  page.elements.allocationName.value = "Production";
  page.elements.allocationDescription.value = "Cluster applicatif";
  submit(page.elements.allocationForm);
  const saved = JSON.parse(page.store.getItem("toolbox-subnets-v2"));
  assert.equal(saved.networks["10.0.0.0/24"].allocations[0].name, "Production");
  assert.equal(saved.networks["10.0.0.0/24"].allocations[0].description, "Cluster applicatif");
  assert.equal(findNode(page, "10.0.0.0/25").dataset.status, "reserved");
  click(findNode(page, "10.0.0.0/25").children.at(-1).children[0]);
  assert.equal(page.elements.allocationDialog.open, true);
  assert.equal(page.elements.allocationName.value, "Production");
});

test("Réservation annotée : annuler la libération préserve les détails", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  click(findNode(page, "10.0.0.0/25").children.at(-1).children[0]);
  page.elements.allocationName.value = "Projet";
  submit(page.elements.allocationForm);
  page.window.confirm = () => false;
  const checkbox = findNode(page, "10.0.0.0/25").children[0].children[0];
  checkbox.checked = false;
  checkbox.dispatch("change");
  assert.equal(page.elements.metricOccupied.textContent, "128");
  assert.equal(checkbox.checked, true);
});

test("Filtres d'origine : masquer un masque entier, puis réafficher", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.105.64.0/19";
  submit(page.elements.networkForm);
  assert.equal(page.elements.maskFilterList.children.length, 13);
  const mask24 = page.elements.maskFilterList.children[4].children[0];
  assert.equal(mask24.checked, true);
  mask24.checked = false;
  mask24.dispatch("change");
  assert.equal(page.elements.maskOverview.children.length, 9);
  assert.ok(page.elements.maskOverview.children.every(block => block.dataset.prefix !== "24"));
  click(page.elements.showDefaultMasks);
  assert.equal(page.elements.maskOverview.children.length, 10);
  click(page.elements.hideAllMasks);
  assert.equal(page.elements.maskOverview.children.length, 1);
  click(page.elements.showDefaultMasks);
  assert.equal(page.elements.maskOverview.children.length, 10);
});

test("Masquage individuel : ne libère pas le CIDR et reste réversible", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  let card = findNode(page, "10.0.0.0/25");
  const checkbox = card.children[0].children[0];
  checkbox.checked = true;
  checkbox.dispatch("change");
  card = findNode(page, "10.0.0.0/25");
  const hide = card.children.at(-1).children.at(-1);
  assert.equal(hide.textContent, "×");
  click(hide);
  assert.equal(findNode(page, "10.0.0.0/25"), undefined);
  assert.equal(page.elements.restoreHidden.hidden, false);
  assert.equal(page.elements.metricOccupied.textContent, "128");
  assert.equal(JSON.parse(page.store.getItem("toolbox-subnets-v2")).networks["10.0.0.0/24"].allocations.length, 1);
  click(page.elements.restoreHidden);
  assert.equal(findNode(page, "10.0.0.0/25").dataset.status, "reserved");
});

test("Grands réseaux : pas de blocs anonymes ; notice explicite au-delà de 1024", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.105.64.0/19";
  submit(page.elements.networkForm);
  click(page.elements.showAllMasks);
  assert.equal(page.elements.maskOverview.children.length, 13);
  const mask29 = page.elements.maskOverview.children.find(b => b.dataset.prefix === "29");
  assert.equal(mask29.children[1].children.length, 1024);
  const mask30 = page.elements.maskOverview.children.find(b => b.dataset.prefix === "30");
  assert.match(mask30.children[1].children[0].textContent, /trop nombreux/);
  const jump = mask30.children[1].children[1];
  assert.match(jump.textContent, /Voir les \/30/);
  click(jump);
  assert.equal(page.elements.gridView.hidden, false);
  assert.equal(page.elements.maskSelect.value, "30");
  assert.ok(findNode(page, "10.105.64.0/30"));
});

test("Grille détaillée : sélection /31 et recherche d'une IP /32", () => {
  const page = createPage();
  page.elements.networkInput.value = "192.0.2.0/24";
  submit(page.elements.networkForm);
  click(page.elements.gridMode);
  page.elements.maskSelect.value = "31";
  page.elements.maskSelect.dispatch("change");
  assert.match(page.elements.pageInfo.textContent, /128 sous-réseaux/);
  click(page.elements.nextPage);
  assert.match(page.elements.pageInfo.textContent, /Page 2/);
  page.elements.searchInput.value = "192.0.2.255";
  submit(page.elements.searchForm);
  assert.equal(page.elements.maskSelect.value, "32");
  assert.ok(findNode(page, "192.0.2.255/32"));
});

test("Migration ancienne : l'occupation partielle est conservée", () => {
  const old = {
    "snet_calc_inventory_10.0.0.0/24": JSON.stringify({
      "10.0.0.0/26": "taken", "10.0.0.0/25": "taken",
      "10.0.0.128/26": "taken"
    }),
    "snet_calc_last_active_network": "10.0.0.0/24"
  };
  const page = createPage(old);
  assert.equal(page.elements.metricOccupied.textContent, "192");
  assert.equal(page.elements.overlapNotice.hidden, false);
  assert.equal(findNode(page, "10.0.0.0/25").dataset.status, "reserved");
  assert.equal(findNode(page, "10.0.0.128/25").dataset.status, "partial");
  assert.equal(page.store.getItem("snet_calc_inventory_10.0.0.0/24"), old["snet_calc_inventory_10.0.0.0/24"]);
});

test("Préférences de l'ancienne V2 compacte : retour à l'affichage historique par défaut", () => {
  const old = {
    "snet_calc_last_active_network": "10.105.64.0/19",
    "toolbox-subnets-view-10.105.64.0/19": JSON.stringify({
      masks: [19, 20, 21, 22, 23, 24],
      hidden: []
    })
  };
  const page = createPage(old);
  const bands = page.elements.maskOverview.children;
  assert.equal(bands.length, 10);
  assert.deepEqual(bands.map(b => b.dataset.prefix),
    ["20","21","22","23","24","25","26","27","28","29"]);
  assert.equal(page.elements.visibleMaskCount.textContent, "10 / 13 niveaux");
});

test("Rechargement : préférences d'affichage séparées de l'inventaire", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  const mask27 = page.elements.maskFilterList.children[2].children[0];
  mask27.checked = false;
  mask27.dispatch("change");
  const reloaded = createPage(Object.fromEntries(page.store.values.entries()));
  assert.equal(reloaded.elements.maskOverview.children.length, 5);
  assert.equal(reloaded.elements.visibleMaskCount.textContent, "5 / 8 niveaux");
});
