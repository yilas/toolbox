"use strict";
const test=require("node:test"),assert=require("node:assert/strict"),fs=require("node:fs"),path=require("node:path");
const root=path.resolve(__dirname,"..");
test("Standalone tools use shared design tokens and navigation",()=>{
 for(const name of ["clean-text.html","subnets_calculator.html"]){
  const html=fs.readFileSync(path.join(root,"tools",name),"utf8");
  for(const asset of ["tokens.css","components.css"]){
   assert.ok(html.includes('href="../assets/css/'+asset+'"'),name);
   assert.ok(fs.existsSync(path.join(root,"assets/css",asset)));
  }
  assert.match(html,/id="theme"/);
  assert.match(html,/var\(--accent\)/);
 }
});

test("Scratchpad shares palette and preserves full-width note grid",()=>{
 const html=fs.readFileSync(path.join(root,"tools/scratchpad.html"),"utf8");
 assert.match(html,/href="\.\.\/assets\/css\/tokens\.css"/);
 assert.match(html,/\.wrap\s*\{[^}]*max-width:\s*none/);
 assert.match(html,/repeat\(auto-fill, minmax\(min\(100%, 360px\), 1fr\)\)/);
 assert.match(html,/--field: var\(--soft\)/);
});

test("Portal consumes shared palette and keeps system fallback",()=>{
 const css=fs.readFileSync(path.join(root,"portal.css"),"utf8");
 const tokens=fs.readFileSync(path.join(root,"assets/css/tokens.css"),"utf8");
 assert.ok(css.includes('@import url("./assets/css/tokens.css")'));
 assert.ok(css.includes("--background:var(--bg)"));
 assert.ok(!/--accent:\s*#/.test(css));
 assert.ok(tokens.includes("html:not([data-theme])"));
 assert.ok(tokens.includes("--portal-shadow:"));
});
