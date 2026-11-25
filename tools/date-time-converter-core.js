"use strict";
(function(root){
 function parse(value,kind){
  value=value.trim();
  if(kind==="seconds"||kind==="milliseconds"){
   if(!/^[+-]?\d+$/.test(value))throw Error("Timestamp entier requis.");
   const n=Number(value);if(!Number.isSafeInteger(n))throw Error("Timestamp hors limites.");
   const d=new Date(n*(kind==="seconds"?1000:1));if(!Number.isFinite(d.getTime()))throw Error("Date hors limites.");return d;
  }
  const m=/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if(!m)throw Error("ISO 8601 avec Z ou décalage explicite requis.");
  const [y,mo,day,h,mi,se]=m.slice(1,7).map(x=>Number(x||0));
  const check=new Date(0);check.setUTCFullYear(y,mo-1,day);check.setUTCHours(h,mi,se,0);
  if(check.getUTCFullYear()!==y||check.getUTCMonth()!==mo-1||check.getUTCDate()!==day||h>23||mi>59||se>59)throw Error("Date ou heure invalide.");
  const d=new Date(value);if(!Number.isFinite(d.getTime()))throw Error("Date invalide.");return d;
 }
 function format(d,zone){return new Intl.DateTimeFormat("fr-FR",{timeZone:zone,year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",second:"2-digit",hourCycle:"h23",timeZoneName:"longOffset"}).format(d);}
 const api={parse,format};if(typeof module!=="undefined")module.exports=api;else root.DateCore=api;
})(globalThis);
