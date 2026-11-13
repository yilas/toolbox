"use strict";
const test=require("node:test"),assert=require("node:assert/strict"),fs=require("node:fs"),path=require("node:path");
const root=path.resolve(__dirname,".."),html=fs.readFileSync(path.join(root,"tools/service-status.html"),"utf8");
const js=fs.readFileSync(path.join(root,"tools/service-status.js"),"utf8");
const css=fs.readFileSync(path.join(root,"tools/service-status.css"),"utf8");
const portal=fs.readFileSync(path.join(root,"index.html"),"utf8");
test("Service Status est référencé dans le portail et permet le retour",()=>{
 assert.match(portal,/href="tools\/service-status.html"/);
 assert.match(html,/href="\.\.\/index.html" aria-label="Retour à Toolbox"/);
 assert.match(html,/href="service-status.css"/);assert.match(html,/src="service-status.js"/);
});
test("Trente pages fournisseurs et un agrégateur communautaire",()=>{
 const links=[...html.matchAll(/<a class="status-link" href="([^"]+)" target="_blank" rel="noopener noreferrer"/g)].map(m=>m[1]);
 assert.equal(links.length,31);assert.equal(new Set(links).size,links.length);
 links.forEach(url=>assert.ok(url.startsWith("https://")));
 assert.ok(links.includes("https://downdetector.fr/en/"));
 assert.match(html,/Détection communautaire/);
 ["status.cloud.microsoft","azure.status.microsoft","status.dev.azure.com","githubstatus.com","slack-status.com"].forEach(host=>assert.ok(links.some(url=>url.includes(host)),host));
});
test("Recherche, filtres, thème et politique réseau restrictive",()=>{
 assert.match(js,/aria-pressed/);assert.match(js,/password-studio-theme/);
 assert.match(html,/connect-src 'none'/);assert.match(html,/pas de supervision en temps réel/);
 assert.match(css,/@media\(max-width:570px\)/);
});
