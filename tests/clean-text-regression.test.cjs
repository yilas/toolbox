"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const html = fs.readFileSync(path.join(__dirname, "../tools/clean-text.html"), "utf8");
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
test("Complete Clean Text script parses", () => { new vm.Script(script); });
test("Cleanup options are connected", () => {
  const prefix = script.slice(0, script.indexOf('    $("runBtn").addEventListener'));
  const { processText } = vm.runInNewContext(prefix + "\n({processText});", {document:{getElementById:()=>({})}});
  assert.equal(processText("Voici une version corrigée :\nBonjour!!!!", {aiCleanup:true}).cleaned, "Bonjour!\n");
  assert.equal(processText("# **Titre**\n---\nTexte", {markdownCleanup:true}).cleaned, "# Titre\nTexte\n");
  assert.equal(processText("# Titre\n**Bonjour**", {format:"plain"}).cleaned, "Titre\nBonjour\n");
});
