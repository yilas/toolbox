/* Exécuter : node --test tests/password-generator.test.cjs (Node 20+) */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { webcrypto } = require("node:crypto");

const toolsDir = path.join(__dirname, "..", "tools");
const wordlistsSource = fs.readFileSync(path.join(toolsDir, "password-wordlists.js"), "utf8");
const coreSource = fs.readFileSync(path.join(toolsDir, "password-generator-core.js"), "utf8");
const uiSource = fs.readFileSync(path.join(toolsDir, "password-generator-ui.js"), "utf8");
const htmlSource = fs.readFileSync(path.join(toolsDir, "random-numbers-generator.html"), "utf8");

function loadCore(crypto = webcrypto) {
  const sandbox = { window: { crypto } };
  vm.runInNewContext(wordlistsSource, sandbox, { filename: "password-wordlists.js" });
  vm.runInNewContext(coreSource, sandbox, { filename: "password-generator-core.js" });
  return sandbox.window;
}

const balanced = {
  length: 18, upper: true, lower: true, digits: true,
  symbols: true, guarantee: true, excludeAmbiguous: false
};

test("Les dictionnaires FR et EN contiennent chacun 2 048 mots uniques", () => {
  const { PASSWORD_WORDLISTS } = loadCore();
  for (const language of ["fr", "en"]) {
    const words = PASSWORD_WORDLISTS[language];
    assert.equal(words.length, 2048);
    assert.equal(new Set(words).size, 2048);
    assert.ok(words.every(word => word && word === word.normalize("NFC")));
  }
});

test("Tirage uniforme par rejet : la valeur hors limite est ignorée", () => {
  const numbers = [4294967295, 17];
  const root = loadCore({
    getRandomValues(array) {
      assert.ok(numbers.length, "Le générateur doit s'arrêter après une valeur acceptée.");
      array[0] = numbers.shift();
      return array;
    }
  });
  assert.equal(root.PasswordCore.randomInt(10), 7);
  assert.equal(numbers.length, 0);
});

test("Les bornes invalides sont rejetées", () => {
  const { PasswordCore: core } = loadCore();
  for (const max of [0, -1, 1.5, NaN, 2 ** 32 + 1]) {
    assert.throws(() => core.randomInt(max), /Borne aléatoire invalide/);
  }
});

test("Le moteur ne retombe jamais sur Math.random()", () => {
  assert.doesNotMatch(coreSource, /Math\.random\s*\(/);
  const root = loadCore(undefined);
  assert.equal(typeof root.PasswordCore.generatePassword, "function");
});

test("Génération sécurisée : longueur, groupes et entropie exacte", () => {
  const { PasswordCore: core } = loadCore();
  for (let i = 0; i < 80; i++) {
    const result = core.generatePassword({ ...balanced, length: 4 });
    assert.equal(result.value.length, 4);
    assert.match(result.value, /[A-Z]/);
    assert.match(result.value, /[a-z]/);
    assert.match(result.value, /[0-9]/);
    assert.match(result.value, /[!@#$%^&*()\-_=+\[\]{}|;:,.<>?/]/);
  }
  assert.equal(core.countValidPasswords(2, [2, 2]), 8n);
  const short = core.generatePassword({ ...balanced, length: 4 });
  assert.ok(Math.abs(short.entropy - Math.log2(Number(core.countValidPasswords(4, [26, 26, 10, 27])))) < 1e-10);
});

test("Catégories non garanties : échantillonnage uniforme dans l'alphabet autorisé", () => {
  const { PasswordCore: core } = loadCore();
  const result = core.generatePassword({
    length: 32, upper: false, lower: false, digits: true,
    symbols: false, guarantee: false, excludeAmbiguous: false
  });
  assert.match(result.value, /^[0-9]{32}$/);
  assert.ok(Math.abs(result.entropy - 32 * Math.log2(10)) < 1e-10);
});

test("Exclusion des caractères ambigus", () => {
  const { PasswordCore: core } = loadCore();
  const result = core.generatePassword({
    ...balanced, length: 128, excludeAmbiguous: true
  });
  assert.doesNotMatch(result.value, /[0O1Il|]/);
});

test("Options incohérentes : erreur claire, aucun ancien secret réutilisé", () => {
  const { PasswordCore: core } = loadCore();
  assert.throws(() => core.generatePassword({
    ...balanced, upper: false, lower: false, digits: false, symbols: false
  }), /Sélectionnez au moins une catégorie/);
  for (const length of [3, 129, 2.5]) {
    assert.throws(() => core.generatePassword({ ...balanced, length }), /longueur/);
  }
});

test("Phrases de passe FR et EN : nombre de mots et calcul d'entropie", () => {
  const { PasswordCore: core } = loadCore();
  for (const language of ["fr", "en"]) {
    const result = core.generatePassphrase({
      words: 7, language, separator: "-", capitalize: false, appendDigit: false
    });
    assert.equal(result.value.split("-").length, 7);
    assert.equal(result.entropy, 77);
    const withDigit = core.generatePassphrase({
      words: 6, language, separator: ".", capitalize: true, appendDigit: true
    });
    assert.equal(withDigit.value.split(".").length, 6);
    assert.match(withDigit.value, /[0-9]$/);
    assert.ok(Math.abs(withDigit.entropy - (66 + Math.log2(10))) < 1e-10);
  }
});

test("Les scripts locaux déclarés existent et tous les sélecteurs d'ID de l'UI sont présents", () => {
  const scripts = [...htmlSource.matchAll(/<script[^>]+src="([^"]+)"/g)].map(match => match[1]);
  assert.equal(scripts.length, 3);
  scripts.forEach(src => {
    assert.ok(src.startsWith("./"));
    assert.ok(fs.existsSync(path.join(toolsDir, src)));
  });
  const ids = [...uiSource.matchAll(/\$\("([a-zA-Z0-9]+)"\)/g)].map(match => match[1]);
  for (const id of new Set(ids)) {
    assert.ok(htmlSource.includes('id="' + id + '"'), "ID absent dans le HTML : " + id);
  }
  assert.ok(htmlSource.includes("connect-src 'none'"));
  assert.doesNotMatch(htmlSource, /on(click|change|input)\s*=/i);
});
