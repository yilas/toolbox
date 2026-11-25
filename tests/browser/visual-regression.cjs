"use strict";
const fs=require("node:fs"),path=require("node:path"),http=require("node:http");
const {chromium}=require("playwright");
const pixelmatch=require("pixelmatch");
const {PNG}=require("pngjs");
const [baseRoot,headRoot,outArg]=process.argv.slice(2);
if(!baseRoot||!headRoot||!outArg)throw Error("Usage: node visual-regression.cjs BASE HEAD OUTPUT");
const out=path.resolve(outArg);
const pages=["index.html","watch/index.html",...fs.readdirSync(path.join(headRoot,"tools")).filter(x=>x.endsWith(".html")).sort().map(x=>"tools/"+x)];
const mime={".html":"text/html",".js":"text/javascript",".mjs":"text/javascript",".css":"text/css",".json":"application/json",".svg":"image/svg+xml",".wasm":"application/wasm"};
async function serve(root){
 root=path.resolve(root);
 const server=http.createServer((req,res)=>{
  const file=path.resolve(root,"."+new URL(req.url,"http://localhost").pathname);
  if(!file.startsWith(root+path.sep)){res.writeHead(403).end();return;}
  try{res.setHeader("Content-Type",mime[path.extname(file)]||"application/octet-stream");res.end(fs.readFileSync(file));}catch{res.writeHead(404).end();}
 });
 await new Promise(r=>server.listen(0,"127.0.0.1",r));
 return {server,url:"http://127.0.0.1:"+server.address().port};
}
async function capture(browser,site,file,theme,viewport){
 const context=await browser.newContext({colorScheme:theme,viewport,deviceScaleFactor:1,locale:"fr-FR",timezoneId:"Europe/Luxembourg",reducedMotion:"reduce"});
 try{
  await context.route("**/*",r=>r.request().url().startsWith(site.url+"/")?r.continue():r.abort());
  await context.addInitScript(()=>{localStorage.setItem("password-studio-theme",matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light");});
  const page=await context.newPage();
  await page.clock.setFixedTime(new Date("2026-11-25T12:00:00Z"));
  const response=await page.goto(site.url+"/"+file);
  if(!response||!response.ok())throw Error("Page unavailable: "+file);
  await page.waitForLoadState("networkidle");
  await page.evaluate(()=>document.fonts.ready);
  return await page.screenshot({animations:"disabled",caret:"hide",fullPage:false});
 }finally{await context.close();}
}
(async()=>{
 fs.mkdirSync(out,{recursive:true});
 const base=await serve(baseRoot),head=await serve(headRoot);
 let browser;const report=[];
 try{
  browser=await chromium.launch();
  for(const file of pages)for(const theme of ["light","dark"])for(const [device,viewport] of [["desktop",{width:1440,height:900}],["mobile",{width:390,height:844}]]){
   const name=file.replaceAll("/","-").replace(".html","")+"-"+theme+"-"+device;
   const after=await capture(browser,head,file,theme,viewport);
   if(!fs.existsSync(path.join(baseRoot,file))){fs.writeFileSync(path.join(out,name+"-after.png"),after);report.push({name,status:"new"});continue;}
   const before=await capture(browser,base,file,theme,viewport);
   const a=PNG.sync.read(before),b=PNG.sync.read(after),diff=new PNG({width:a.width,height:a.height});
   const pixels=pixelmatch(a.data,b.data,diff.data,a.width,a.height,{threshold:.1});
   const percent=100*pixels/(a.width*a.height);
   report.push({name,status:pixels?"changed":"unchanged",pixels,percent});
   if(pixels){for(const [suffix,data] of [["before",before],["after",after],["diff",PNG.sync.write(diff)]])fs.writeFileSync(path.join(out,name+"-"+suffix+".png"),data);}
  }
 }finally{await browser?.close();await Promise.all([base,head].map(s=>new Promise(r=>s.server.close(r))));fs.writeFileSync(path.join(out,"report.json"),JSON.stringify(report,null,2));}
 const changed=report.filter(r=>r.status!=="unchanged");
 const summary=["## Visual comparison", "",report.length+" configurations; "+changed.length+" changed or new.","","| Configuration | Changed pixels |","|---|---:|",...changed.map(r=>"| "+r.name+" | "+(r.percent===undefined?"new":r.percent.toFixed(3)+"%")+" |")].join("\n");
 console.log(summary);if(process.env.GITHUB_STEP_SUMMARY)fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,summary+"\n");
})().catch(e=>{console.error(e);process.exitCode=1;});
