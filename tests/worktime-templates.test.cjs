"use strict";
const test=require("node:test"),assert=require("node:assert/strict"),fs=require("node:fs"),path=require("node:path"),vm=require("node:vm");
const source=fs.readFileSync(path.join(__dirname,"../tools/worktime-calculator.js"),"utf8");
test("Workday templates have valid non-overlapping slots and expected totals",()=>{
 const helpers=source.slice(source.indexOf("const validTime="),source.indexOf("const rows="));
 const templates=source.slice(source.indexOf("const TEMPLATE_KEY="),source.indexOf("let custom=[],selected="));
 const {presets,validSlots,minutes}=vm.runInNewContext(helpers+templates+";({presets,validSlots,minutes})");
 assert.equal(presets.length,13);
 assert.equal(presets.every(t=>validSlots(t.slots)),true);
 assert.equal(presets.filter(t=>t.group==='Journées partielles').length,2);
 assert.equal(presets.filter(t=>t.group==='Journées complètes').length,11);
 assert.equal(Array.from(presets,t=>t.slots.reduce((n,p)=>n+minutes(p[1])-minutes(p[0]),0)).join(","),"480,480,480,480,480,480,480,420,540,480,480,240,240");
 assert.equal(validSlots([["08:00","12:00"],["11:00","13:00"]]),false);
 assert.equal(validSlots([["12:00","08:00"]]),false);
 assert.equal(validSlots([["","12:00"]]),false);
});
test("Worktime template controls exist and script parses",()=>{
 new vm.Script(source);
 const html=fs.readFileSync(path.join(__dirname,"../tools/worktime-calculator.html"),"utf8");
 for(const id of ["templates","saveTemplate","updateTemplate","deleteTemplate","templateStatus"])assert.ok(html.includes('id="'+id+'"'));
});

test("Compact template selector and explicit apply",()=>{
 const html=fs.readFileSync(path.join(__dirname,"../tools/worktime-calculator.html"),"utf8");
 assert.match(html, /<select id="templates"/);
 assert.match(html, /id="applyTemplate" disabled/);
 assert.match(html, /id="templateActions" hidden/);
 assert.match(source, /createElement\('optgroup'\)/);
 assert.match(source, /\$\('applyTemplate'\)\.addEventListener\('click'/);
});
