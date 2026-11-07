/* node --test tests/subnets-core.test.cjs */
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const root = path.resolve(__dirname, "..");
const dir = path.join(root, "tools");
const coreCode = fs.readFileSync(path.join(dir, "subnets-core.js"), "utf8");
const storageCode = fs.readFileSync(path.join(dir, "subnets-storage.js"), "utf8");
const uiCode = fs.readFileSync(path.join(dir, "subnets-ui.js"), "utf8");
const css = fs.readFileSync(path.join(dir, "subnets-calculator.css"), "utf8");
const html = fs.readFileSync(path.join(dir, "subnets_calculator.html"), "utf8");

class FakeStorage {
  constructor(initial = {}) { this.values = new Map(Object.entries(initial)); }
  get length() { return this.values.size; }
  key(index) { return [...this.values.keys()][index] ?? null; }
  getItem(key) { return this.values.has(key) ? this.values.get(key) : null; }
  setItem(key, value) { this.values.set(key, String(value)); }
  removeItem(key) { this.values.delete(key); }
}
function runtime(store = new FakeStorage()) {
  const window = { localStorage: store };
  vm.runInNewContext(coreCode, { window }, { filename: "subnets-core.js" });
  vm.runInNewContext(storageCode, { window }, { filename: "subnets-storage.js" });
  return { core: window.SubnetsCore, storage: window.SubnetsStorage, store };
}
const { core } = runtime();

test("Les bornes /0, /31 et /32 sont exactes", () => {
  const zero = core.parseCIDR("10.1.2.3/0");
  assert.equal(zero.cidr, "0.0.0.0/0");
  assert.equal(zero.start, 0);
  assert.equal(zero.end, 4294967295);
  assert.equal(zero.size, 4294967296);
  assert.equal(zero.last, "255.255.255.255");
  assert.equal(core.parseCIDR("198.51.100.17/31").cidr, "198.51.100.16/31");
  assert.equal(core.parseCIDR("198.51.100.17/32").cidr, "198.51.100.17/32");
  assert.deepEqual(Array.from(core.children("198.51.100.16/31"), item => item.cidr),
    ["198.51.100.16/32", "198.51.100.17/32"]);
  assert.equal(core.children("198.51.100.17/32").length, 0);
});

test("Les réseaux saisis par une adresse hôte sont normalisés", () => {
  const a = core.parseCIDR("10.105.196.10/22");
  const b = core.parseCIDR("10.105.196.0/22");
  assert.equal(a.cidr, b.cidr);
  assert.equal(a.cidr, "10.105.196.0/22");
  assert.equal(a.size, 1024);
  assert.equal(a.last, "10.105.199.255");
});

test("Les IPv4 et les préfixes invalides sont rejetés", () => {
  for (const value of [
    "10.0.0.1/33", "10.0.0.1/24abc", "300.1.2.3/24",
    "10.1.2/24", "10..2.3/24", "10.0.0.1/-1", "01.2.3.4/24",
    "10.1.2.3/032", "10.1.2.3/", "10.1.2.3/3.5", "1.2.3.4.5/24"
  ]) assert.throws(() => core.parseCIDR(value));
  assert.throws(() => core.parseIPv4("256.0.0.1"));
  assert.throws(() => core.formatIPv4(-1));
});

test("Union correcte des sous-réseaux imbriqués : 75 %, pas 100 %", () => {
  const coverage = core.prepareCoverage(
    ["10.0.0.0/26", "10.0.0.0/25", "10.0.0.128/26"], "10.0.0.0/24");
  assert.equal(coverage.occupied, 192);
  assert.equal(coverage.hasOverlap, true);
  const parent = core.describeNode("10.0.0.0/24", coverage);
  assert.equal(parent.occupied, 192);
  assert.equal(parent.free, 64);
  assert.equal(parent.percent, 75);
  assert.equal(parent.status, "partial");
  assert.equal(core.describeNode("10.0.0.64/26", coverage).status, "inherited");
  assert.equal(core.describeNode("10.0.0.192/26", coverage).status, "free");
});

test("Les cinq états de hiérarchie sont déterminés correctement", () => {
  const partial = core.prepareCoverage(["10.10.0.0/26"], "10.10.0.0/24");
  assert.equal(core.describeNode("10.10.0.0/24", partial).status, "partial");
  assert.equal(core.describeNode("10.10.0.0/26", partial).status, "reserved");
  const inherited = core.describeNode("10.10.0.0/27", partial);
  assert.equal(inherited.status, "inherited");
  assert.equal(inherited.inheritedFrom, "10.10.0.0/26");
  assert.equal(core.describeNode("10.10.0.64/26", partial).status, "free");
  const full = core.prepareCoverage(["10.10.0.0/25", "10.10.0.128/25"], "10.10.0.0/24");
  assert.equal(core.describeNode("10.10.0.0/24", full).status, "full");
  assert.equal(core.describeNode("10.10.0.0/25", full).status, "reserved");
  assert.equal(core.describeNode("10.10.0.0/26", full).status, "inherited");
  assert.equal(core.describeNode("10.10.0.0/24", full).occupied, 256);
});

test("Les nouvelles réservations chevauchantes sont refusées et les voisines acceptées", () => {
  const parent = "10.0.0.0/24";
  let allocations = core.addAllocation([], parent, "10.0.0.0/26", { name: "API" }, 10);
  assert.equal(allocations[0].name, "API");
  assert.throws(() => core.addAllocation(allocations, parent, "10.0.0.0/25"), /chevauche/);
  assert.throws(() => core.addAllocation(allocations, parent, "10.0.0.0/27"), /chevauche/);
  assert.throws(() => core.addAllocation(allocations, parent, "10.0.0.0/26"), /chevauche/);
  assert.throws(() => core.addAllocation(allocations, parent, "10.0.1.0/24"), /dehors/);
  allocations = core.addAllocation(allocations, parent, "10.0.0.64/26", { description: "B" }, 20);
  assert.equal(core.prepareCoverage(allocations, parent).occupied, 128);
  allocations = core.updateAllocation(allocations, parent, "10.0.0.0/26", { name: "API2" }, 30);
  assert.equal(allocations[0].name, "API2");
  allocations = core.removeAllocation(allocations, parent, "10.0.0.0/26");
  assert.equal(allocations.length, 1);
  assert.equal(allocations[0].cidr, "10.0.0.64/26");
});

test("Pagination /0 vers /32 : 2^32 sous-réseaux possibles sans les allouer", () => {
  const page = core.pageOfSubnets("0.0.0.0/0", 32, 67108863, 64);
  assert.equal(page.count, 4294967296);
  assert.equal(page.totalPages, 67108864);
  assert.equal(page.subnets.length, 64);
  assert.equal(page.subnets[0].cidr, "255.255.255.192/32");
  assert.equal(page.subnets[63].cidr, "255.255.255.255/32");
  const location = core.pageForCidr("0.0.0.0/0", "255.255.255.255/32");
  assert.equal(location.page, 67108863);
  assert.equal(location.index, 4294967295);
  assert.throws(() => core.pageOfSubnets("10.0.0.0/24", 23), /Préfixe/);
  assert.throws(() => core.pageOfSubnets("10.0.0.0/24", 30, 100000), /Page invalide/);
});

test("La navigation reconstruit la hiérarchie du parent vers /32", () => {
  const path = core.ancestors("10.105.197.123/32", "10.105.196.0/22");
  assert.equal(path.length, 11);
  assert.equal(path[0].cidr, "10.105.196.0/22");
  assert.equal(path.at(-1).cidr, "10.105.197.123/32");
  assert.throws(() => core.ancestors("192.0.2.0/24", "10.105.196.0/22"), /n'appartient pas/);
});

test("CSV Excel : UTF-8 BOM, séparateur ;, neutralisation des formules", () => {
  const csv = core.inventoryCSV("10.0.0.0/24", [{
    cidr: "10.0.0.0/25", name: "=2+2", description: '@SUM("A1")'
  }]);
  assert.ok(csv.startsWith("\ufeff"));
  assert.ok(csv.includes(";'=2+2") || csv.includes("'=2+2"));
  assert.ok(csv.includes("'@SUM"));
  assert.ok(csv.includes("10.0.0.0/25"));
});

test("Migration : anciens inventaires préservés, réseau canonique et occupation exacte", () => {
  const old = {
    "snet_calc_inventory_10.0.0.10/24": JSON.stringify({
      "10.0.0.0/26": "taken", "10.0.0.0/25": "taken"
    }),
    "snet_calc_inventory_10.0.0.0/24": JSON.stringify({ "10.0.0.128/26": "taken" }),
    "snet_calc_last_active_network": "10.0.0.10/24"
  };
  const rt = runtime(new FakeStorage(old));
  const loaded = rt.storage.load(rt.store);
  assert.equal(loaded.migrated, true);
  assert.equal(loaded.legacyCount, 2);
  assert.equal(loaded.data.active, "10.0.0.0/24");
  const allocations = loaded.data.networks["10.0.0.0/24"].allocations;
  assert.equal(allocations.length, 3);
  assert.equal(rt.core.prepareCoverage(allocations, "10.0.0.0/24").occupied, 192);
  assert.equal(rt.store.getItem("snet_calc_inventory_10.0.0.10/24"),
    old["snet_calc_inventory_10.0.0.10/24"]);
  assert.equal(rt.storage.load(rt.store).migrated, false);
});

test("Stockage V2 corrompu : aucune suppression ni écriture automatique", () => {
  const rt = runtime(new FakeStorage({ "toolbox-subnets-v2": "{unclosed" }));
  const result = rt.storage.load(rt.store);
  assert.equal(result.blocked, true);
  assert.equal(rt.store.getItem("toolbox-subnets-v2"), "{unclosed");
  assert.equal(Object.keys(result.data.networks).length, 0);
});

test("Import JSON : fusion sans écrasement des réservations existantes", () => {
  const rt = runtime();
  const first = rt.storage.withAllocations(rt.storage.emptyState(), "192.0.2.0/24",
    [{ cidr: "192.0.2.0/26", name: "Original" }]);
  const incoming = rt.storage.withAllocations(rt.storage.emptyState(), "192.0.2.0/24",
    [{ cidr: "192.0.2.0/26", name: "Distant" },
     { cidr: "192.0.2.128/26", name: "Ajout" }]);
  const merged = rt.storage.importJSON(rt.storage.exportJSON(incoming), first);
  assert.equal(merged.importedAllocations, 1);
  assert.equal(merged.data.networks["192.0.2.0/24"].allocations.length, 2);
  assert.equal(merged.data.networks["192.0.2.0/24"].allocations[0].name, "Original");
  assert.throws(() => rt.storage.importJSON("{}", first), /format d'inventaire V2/);
  assert.equal(rt.storage.save(merged.data, rt.store).ok, true);
  assert.equal(rt.storage.load(rt.store).data.networks["192.0.2.0/24"].allocations.length, 2);
});

test("HTML et UI : retour Toolbox, deux vues, aucune injection HTML dynamique", () => {
  assert.ok(html.includes('href="../index.html" aria-label="Retour à Toolbox"'));
  for (const file of ["subnets-core.js", "subnets-storage.js", "subnets-ui.js", "subnets-calculator.css"]) {
    assert.ok(fs.existsSync(path.join(dir, file)));
    assert.ok(html.includes(file));
  }
  for (const id of ["hierarchyTree", "subnetGrid", "maskSelect", "statusFilter",
    "networkInput", "savedNetworks", "importFile", "exportJson", "exportCsv",
    "allocationDialog", "summarySection", "occupationBar"]) {
    assert.ok(html.includes('id="' + id + '"'));
  }
  assert.match(html, /connect-src 'none'/);
  assert.doesNotMatch(html, /on(?:click|change|input)\s*=/i);
  assert.doesNotMatch(uiCode, /\.innerHTML\s*=/);
  assert.ok(css.includes("prefers-reduced-motion"));
  assert.ok(css.includes("@media (max-width: 580px)"));
});
