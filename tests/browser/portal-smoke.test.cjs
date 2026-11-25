"use strict";
const {test}=require("node:test");
const assert=require("node:assert/strict");
const {chromium}=require("playwright");
const http=require("node:http"),fs=require("node:fs"),path=require("node:path");
const root=path.resolve(__dirname,"../..");
test("Published tools load without uncaught JavaScript errors",async()=>{
 const server=http.createServer((req,res)=>{
  const pathname=new URL(req.url,"http://localhost").pathname;
  const file=path.resolve(root,"."+pathname);
  if(!file.startsWith(root+path.sep)){res.writeHead(403).end();return;}
  try{res.setHeader("Content-Type",({".html":"text/html",".js":"text/javascript",".mjs":"text/javascript",".css":"text/css",".json":"application/json",".wasm":"application/wasm"})[path.extname(file)]||"application/octet-stream");res.end(fs.readFileSync(file));}catch{res.writeHead(404).end();}
 });
 await new Promise(r=>server.listen(0,"127.0.0.1",r));
 let browser;
 try{
  browser=await chromium.launch();
  const base="http://127.0.0.1:"+server.address().port;
  const pages=["index.html","watch/index.html",...fs.readdirSync(path.join(root,"tools")).filter(x=>x.endsWith(".html")).map(x=>"tools/"+x)];
  for(const file of pages){
   const page=await browser.newPage({viewport:{width:1440,height:900}});
   const errors=[];page.on("pageerror",e=>errors.push(e.message));
   try{
    const response=await page.goto(base+"/"+file);
    assert.equal(response.status(),200,file);
    await page.waitForLoadState("networkidle");
    assert.deepEqual(errors,[],file);
    if(file.endsWith("worktime-calculator.html"))assert.equal(await page.locator("#templates option").count(),14);
   }finally{await page.close();}
  }
  const context=await browser.newContext({colorScheme:"light"});
  try {
   const pages=await Promise.all(["index.html","tools/service-status.html","tools/security-advisories.html","tools/security-advisories-sources.html","tools/auto-reply-generator.html","tools/base64-encoder-decoder.html","tools/decodeur_analyseur_jwt_standalone.html","tools/pdf-compressor.html","tools/random-numbers-generator.html"].map(async file=>{const p=await context.newPage();await p.goto(base+"/"+file);return p;}));
   await pages[0].locator("#themeToggle").click();
   await pages[0].locator("#themeToggle").click();
   for(const p of pages) await p.waitForFunction(()=>document.documentElement.dataset.theme==="dark");
   await pages[1].locator("#themeToggle").click();
   for(const p of pages) await p.waitForFunction(()=>document.documentElement.dataset.theme==="light"&&localStorage.getItem("password-studio-theme")==="system");
  } finally {await context.close();}
 }finally{await browser?.close();await new Promise(r=>server.close(r));}
});
