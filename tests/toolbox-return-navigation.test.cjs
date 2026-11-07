/* Exécution : node --test tests/toolbox-return-navigation.test.cjs */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const toolsDirectory = path.join(root, "tools");
const portal = fs.readFileSync(path.join(root, "index.html"), "utf8");
const toolFiles = fs.readdirSync(toolsDirectory)
  .filter(filename => filename.toLowerCase().endsWith(".html"))
  .sort();

function findBackLinks(html) {
  return [...html.matchAll(
    /<a\b[^>]*href=["']\.\.\/index\.html["'][^>]*>([\s\S]*?)<\/a>/gi
  )];
}

test("Tous les outils HTML publiés disposent d'un lien unique vers Toolbox", () => {
  assert.ok(toolFiles.length > 0, "Aucun outil à vérifier");
  for (const filename of toolFiles) {
    const source = fs.readFileSync(path.join(toolsDirectory, filename), "utf8");
    const links = findBackLinks(source);
    assert.equal(
      links.length,
      1,
      filename + " : exactement un lien vers ../index.html est requis"
    );
    const visibleText = links[0][1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    assert.match(visibleText, /Retour à Toolbox/i, filename + " : intitulé visible manquant");
    assert.match(
      links[0][0],
      /aria-label=["']Retour à Toolbox["']/i,
      filename + " : libellé accessible manquant"
    );
    assert.ok(
      fs.existsSync(path.resolve(toolsDirectory, "../index.html")),
      "La destination ../index.html doit exister"
    );
  }
});

test("La page principale référence uniquement des outils existants", () => {
  const references = [...portal.matchAll(/href=["'](tools\/[^"']+\.html)["']/g)]
    .map(match => match[1]);
  assert.ok(references.length > 0, "Aucun lien d'outil dans index.html");
  assert.equal(new Set(references).size, references.length, "Liens d'outils dupliqués");
  for (const href of references) {
    assert.ok(fs.existsSync(path.join(root, href)), href + " introuvable");
    assert.ok(toolFiles.includes(path.basename(href)), href + " n'a pas été audité");
  }
});

test("Le retour reste lisible sur mobile dans Password Studio", () => {
  const stylesheet = fs.readFileSync(
    path.join(toolsDirectory, "password-generator.css"), "utf8"
  );
  assert.match(stylesheet, /@media\s*\(max-width:\s*480px\)/);
  assert.match(stylesheet, /\.back-text\s*\{\s*display:\s*inline;\s*\}/);
  assert.doesNotMatch(stylesheet, /\.nav-separator\s*,\s*\.back-text\s*\{\s*display:\s*none;/);
});

test("Le lien ne dépend d'aucun JavaScript", () => {
  for (const filename of toolFiles) {
    const source = fs.readFileSync(path.join(toolsDirectory, filename), "utf8");
    const link = findBackLinks(source)[0];
    assert.ok(link, filename);
    assert.doesNotMatch(link[0], /onclick\s*=/i);
    assert.doesNotMatch(link[0], /target\s*=\s*["']_blank["']/i);
  }
});
