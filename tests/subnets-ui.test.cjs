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

test("Chargement : CIDR canonique et six niveaux lisibles par défaut", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.13/24";
  submit(page.elements.networkForm);
  assert.equal(page.elements.activeNetwork.textContent, "10.0.0.0/24");
  assert.equal(page.elements.metricTotal.textContent, "256");
  assert.equal(page.elements.metricFree.textContent, "256");
  assert.equal(page.elements.workspace.hidden, false);
  assert.ok(findNode(page, "10.0.0.0/24"));
  assert.ok(findNode(page, "10.0.0.0/25"));
  assert.equal(page.elements.maskOverview.children.length, 6); // /24 à /29
  assert.equal(page.elements.overviewView.hidden, false);
  assert.equal(page.elements.visibleMaskCount.textContent, "6 / 9 niveaux");
  assert.ok(page.store.getItem("toolbox-subnets-v2"));
});

test("Réservation immédiate sans dialogue ni métadonnées, même si la saisie change", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  page.elements.networkInput.value = "192.0.2.0/24";
  page.elements.networkInput.dispatch("input");
  click(findNode(page, "10.0.0.0/24"));
  assert.equal(page.elements.allocationDialog.open, false);
  assert.equal(page.elements.metricOccupied.textContent, "256");
  const saved = JSON.parse(page.store.getItem("toolbox-subnets-v2"));
  assert.equal(saved.active, "10.0.0.0/24");
  assert.equal(saved.networks["192.0.2.0/24"], undefined);
  const allocation = saved.networks["10.0.0.0/24"].allocations[0];
  assert.equal(allocation.cidr, "10.0.0.0/24");
  assert.equal(allocation.name, "");
  assert.equal(allocation.description, "");
  assert.equal(findNode(page, "10.0.0.0/24").parent.dataset.status, "reserved");
  assert.equal(findNode(page, "10.0.0.0/25").parent.dataset.status, "inherited");
});

test("Libération sans formulaire : occupation recalculée et inventaire vidé", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  click(findNode(page, "10.0.0.0/24"));
  assert.equal(page.elements.metricOccupied.textContent, "256");
  const reserved = findNode(page, "10.0.0.0/24").parent;
  const release = reserved.children[1].children[0];
  assert.equal(release.textContent, "Libérer");
  click(release);
  assert.equal(page.elements.allocationDialog.open, false);
  assert.equal(page.elements.metricOccupied.textContent, "0");
  assert.equal(findNode(page, "10.0.0.0/24").parent.dataset.status, "free");
  const stored = JSON.parse(page.store.getItem("toolbox-subnets-v2"));
  assert.equal(stored.networks["10.0.0.0/24"].allocations.length, 0);
});

test("Détails optionnels : réserver avec description, puis modifier une réservation directe", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  const emptyCard = findNode(page, "10.0.0.0/24").parent;
  const withDetails = emptyCard.children[1].children[0];
  assert.equal(withDetails.textContent, "Avec détails");
  click(withDetails);
  assert.equal(page.elements.allocationDialog.open, true);
  page.elements.allocationName.value = "Service API";
  page.elements.allocationDescription.value = "Plage réservée au service";
  submit(page.elements.allocationForm);
  let saved = JSON.parse(page.store.getItem("toolbox-subnets-v2"));
  assert.equal(saved.networks["10.0.0.0/24"].allocations[0].name, "Service API");
  assert.equal(saved.networks["10.0.0.0/24"].allocations[0].description, "Plage réservée au service");

  click(findNode(page, "10.0.0.0/24")); // CIDR réservé : éditer les détails.
  assert.equal(page.elements.allocationDialog.open, true);
  assert.equal(page.elements.allocationName.value, "Service API");
  page.elements.allocationName.value = "Service API v2";
  submit(page.elements.allocationForm);
  saved = JSON.parse(page.store.getItem("toolbox-subnets-v2"));
  assert.equal(saved.networks["10.0.0.0/24"].allocations[0].name, "Service API v2");
});

test("Libérer une réservation annotée demande confirmation, annuler conserve les données", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.0.0.0/24";
  submit(page.elements.networkForm);
  click(findNode(page, "10.0.0.0/24").parent.children[1].children[0]); // Avec détails
  page.elements.allocationName.value = "Production";
  submit(page.elements.allocationForm);
  page.window.confirm = () => false;
  click(findNode(page, "10.0.0.0/24").parent.children[1].children[0]); // Libérer
  assert.equal(page.elements.metricOccupied.textContent, "256");
  assert.equal(JSON.parse(page.store.getItem("toolbox-subnets-v2")).networks["10.0.0.0/24"].allocations[0].name, "Production");
  page.window.confirm = () => true;
  click(findNode(page, "10.0.0.0/24").parent.children[1].children[0]);
  assert.equal(page.elements.metricOccupied.textContent, "0");
});

test("Grille : la case réserve et libère sans ouvrir de dialogue, les détails restent accessibles", () => {
  const page = createPage();
  page.elements.networkInput.value = "192.0.2.0/24";
  submit(page.elements.networkForm);
  click(page.elements.gridMode);
  const firstCard = findNode(page, "192.0.2.0/26");
  assert.ok(firstCard); // La grille démarre au masque /26.
  const checkbox = firstCard.children[0].children[0];
  assert.equal(checkbox.checked, false);
  checkbox.checked = true;
  checkbox.dispatch("change");
  assert.equal(page.elements.allocationDialog.open, false);
  assert.equal(JSON.parse(page.store.getItem("toolbox-subnets-v2")).networks["192.0.2.0/24"].allocations.length, 1);

  const reserved = findNode(page, "192.0.2.0/26");
  const checkAgain = reserved.children[0].children[0];
  assert.equal(checkAgain.checked, true);
  checkAgain.checked = false;
  checkAgain.dispatch("change");
  assert.equal(JSON.parse(page.store.getItem("toolbox-subnets-v2")).networks["192.0.2.0/24"].allocations.length, 0);

  const free = findNode(page, "192.0.2.0/26");
  const buttons = free.children[2].children[1];
  assert.equal(buttons.children[0].textContent, "Réserver");
  assert.equal(buttons.children[1].textContent, "Avec détails");
  click(buttons.children[1]);
  assert.equal(page.elements.allocationDialog.open, true);
});

test("Régression capture : /19 affiche les 32 CIDR /24 entièrement lisibles", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.105.64.0/19";
  submit(page.elements.networkForm);
  const bands = page.elements.maskOverview.children;
  assert.equal(bands.length, 6); // /19 à /24 affichés immédiatement
  const counts = bands.map(b => b.children[1].children.length);
  assert.deepEqual(counts, [1, 2, 4, 8, 16, 32]);
  const cards = bands[5].children[1].children;
  assert.equal(cards[0].children[0].children[0].textContent, "10.105.64.0/24");
  assert.equal(cards[31].children[0].children[0].textContent, "10.105.95.0/24");
  assert.ok(cards.every(card => card.children[0].children[0].className === "overview-card-cidr"));
  assert.ok(cards.every(card => card.children[0].children[0].textContent.includes("/24")));
  assert.doesNotMatch(html, /id="treeView"|id="hierarchyTree"/);
  assert.doesNotMatch(uiSource, /state\.expanded|renderTree|createTreeRow/);
});

test("Masquer un niveau et un CIDR, puis restaurer sans effacer la réservation", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.105.64.0/19";
  submit(page.elements.networkForm);

  // Les filtres de niveaux s'appliquent indépendamment des réservations.
  const mask23 = page.elements.maskFilterList.children[4].children[0];
  assert.equal(mask23.checked, true);
  mask23.checked = false;
  mask23.dispatch("change");
  assert.equal(page.elements.maskOverview.children.length, 5);
  assert.ok(page.elements.maskOverview.children.every(band => band.dataset.prefix !== "23"));

  const band24 = page.elements.maskOverview.children.at(-1);
  const firstCard = band24.children[1].children[0];
  const hide = firstCard.children[1].children.at(-1);
  assert.equal(hide.textContent, "Masquer");
  click(hide);
  assert.equal(band24.children[1].children.length, 32); // Ancien DOM hors vue
  const current24 = page.elements.maskOverview.children.at(-1);
  assert.equal(current24.children[1].children.length, 31);
  assert.equal(findNode(page, "10.105.64.0/24"), undefined);
  assert.equal(page.elements.restoreHidden.hidden, false);
  assert.equal(JSON.parse(page.store.getItem("toolbox-subnets-v2")).networks["10.105.64.0/19"].allocations.length, 0);
  click(page.elements.restoreHidden);
  assert.equal(page.elements.maskOverview.children.at(-1).children[1].children.length, 32);
  assert.ok(findNode(page, "10.105.64.0/24"));
});

test("Les préférences de visibilité sont conservées par réseau, hors inventaire", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.105.64.0/19";
  submit(page.elements.networkForm);
  click(page.elements.hideAllMasks);
  assert.equal(page.elements.maskOverview.children.length, 1); // Message aucun masque
  assert.equal(page.elements.visibleMaskCount.textContent, "0 / 14 niveaux");
  click(page.elements.showDefaultMasks);
  assert.equal(page.elements.maskOverview.children.length, 6);
  const preference = page.store.getItem("toolbox-subnets-view-10.105.64.0/19");
  assert.equal(JSON.parse(preference).masks.length, 6);
  const reloaded = createPage(Object.fromEntries(page.store.values.entries()));
  assert.equal(reloaded.elements.maskOverview.children.length, 6);
  assert.equal(reloaded.elements.visibleMaskCount.textContent, "6 / 14 niveaux");
});

test("Au-delà de 128 CIDR : regroupements nommés avec accès au détail", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.105.196.0/22";
  submit(page.elements.networkForm);
  click(page.elements.showAllMasks);
  const bands = page.elements.maskOverview.children;
  assert.equal(bands.length, 11); // /22 à /32
  const counts = bands.map(band => band.children[1].children.length);
  assert.deepEqual(counts, [1, 2, 4, 8, 16, 32, 64, 128, 64, 64, 64]);
  const dense = bands.at(-1);
  const first = dense.children[1].children[0].children[0]; // Bouton principal
  assert.equal(first.children[0].className, "overview-card-cidr");
  assert.match(first.children[1].textContent, /sous-réseaux \/32/);
  click(first);
  assert.equal(page.elements.overviewView.hidden, true);
  assert.equal(page.elements.gridView.hidden, false);
  assert.equal(page.elements.maskSelect.value, "32");
  assert.equal(page.elements.pageInfo.textContent.includes("Page 1"), true);
  assert.ok(findNode(page, "10.105.196.0/32"));
});

test("Réseau /0 : les 33 niveaux restent disponibles avec un plafond de 128 cartes", () => {
  const page = createPage();
  page.elements.networkInput.value = "10.105.196.0/0";
  submit(page.elements.networkForm);
  assert.equal(page.elements.maskOverview.children.length, 6);
  click(page.elements.showAllMasks);
  const bands = page.elements.maskOverview.children;
  assert.equal(bands.length, 33);
  assert.ok(bands.every(band => band.children[1].children.length <= 128));
  assert.equal(page.elements.activeNetwork.textContent, "0.0.0.0/0");
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
  assert.equal(findNode(page, "10.0.0.0/24").parent.dataset.status, "partial");
  assert.equal(page.store.getItem("snet_calc_inventory_10.0.0.0/24"),
    initial["snet_calc_inventory_10.0.0.0/24"]);
});
