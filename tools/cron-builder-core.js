"use strict";
(function(root){
 function field(raw,min,max,dow=false){
  const values=new Set();
  for(const part of raw.split(",")){
   const m=/^(\*|\d+(?:-\d+)?)(?:\/(\d+))?$/.exec(part);
   if(!m)throw Error("Champ invalide : "+part);
   const step=m[2]===undefined?1:Number(m[2]);
   if(!Number.isSafeInteger(step)||step<1||step>max-min+1)throw Error("Pas invalide : "+part);
   let [a,b]=m[1]==="*"?[min,max]:m[1].split("-").map(Number);
   if(b===undefined)b=m[2]?max:a;
   if(a<min||b>max||a>b)throw Error("Valeur hors limites : "+part);
   for(let i=a;i<=b;i+=step)values.add(dow&&i===7?0:i);
  }
  return values;
 }
 function parse(expression){
  const parts=expression.trim().split(/\s+/);
  if(parts.length!==5)throw Error("Cinq champs Unix sont requis.");
  const [minute,hour,day,month,weekday]=parts.map((p,i)=>field(p,...[[0,59],[0,23],[1,31],[1,12],[0,7,true]][i]));
  return {minute,hour,day,month,weekday,dayStar:parts[2].startsWith("*"),weekdayStar:parts[4].startsWith("*")};
 }
 function next(expression,start,count=10){
  const p=parse(expression),out=[];
  if(!Number.isFinite(start.getTime()))throw Error("Date invalide");
  let t=new Date(Math.floor(start.getTime()/60000)*60000+60000);
  const limit=t.getTime()+366*5*86400000;
  while(t.getTime()<limit&&out.length<count){
   const d=p.day.has(t.getUTCDate()),w=p.weekday.has(t.getUTCDay());
   const dayOK=p.dayStar||p.weekdayStar?d&&w:d||w;
   if(!p.month.has(t.getUTCMonth()+1)||!dayOK){t.setUTCHours(24,0,0,0);continue;}
   if(p.hour.has(t.getUTCHours())&&p.minute.has(t.getUTCMinutes()))out.push(new Date(t));
   t=new Date(t.getTime()+60000);
  }
  return out;
 }
 const api={parse,next};if(typeof module!=="undefined")module.exports=api;else root.CronCore=api;
})(globalThis);
