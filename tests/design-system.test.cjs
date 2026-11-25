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
