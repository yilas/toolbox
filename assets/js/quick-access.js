/* Local portal shortcuts; no network or tool data collected. */
(() => {
 "use strict";
 const panel=document.getElementById("quickAccess");
 if(!panel)return;
 const cards=[...document.querySelectorAll(".tool-card")];
 const catalog=new Map(cards.map(a=>[a.getAttribute("href"),{a,title:a.querySelector("h3").textContent}]));
 const key="toolbox-quick-access-v1";
 let state={favorites:[],recent:[]};
 function load(){
  try{
   const saved=JSON.parse(localStorage.getItem(key)||"{}");
   for(const name of ["favorites","recent"])state[name]=Array.isArray(saved?.[name])?[...new Set(saved[name].filter(x=>catalog.has(x)))].slice(0,name==="recent"?5:catalog.size):[];
  }catch{}
 }
 function save(){try{localStorage.setItem(key,JSON.stringify(state));}catch{}}
 function visit(href){state.recent=[href,...state.recent.filter(x=>x!==href)].slice(0,5);save();render();}
 const buttons=new Map();
 function render(){
  for(const [href,button] of buttons){
   const selected=state.favorites.includes(href);
   button.textContent=selected?"★":"☆";
   button.setAttribute("aria-pressed",String(selected));
   button.setAttribute("aria-label",(selected?"Retirer des favoris : ":"Ajouter aux favoris : ")+catalog.get(href).title);
   button.title=button.getAttribute("aria-label");
  }
  for(const [name,id] of [["favorites","favoriteLinks"],["recent","recentLinks"]]){
   const list=document.getElementById(id);list.replaceChildren();
   for(const href of state[name]){
    const a=document.createElement("a");a.href=href;a.textContent=catalog.get(href).title;
    a.addEventListener("click",()=>visit(href));list.append(a);
   }
   list.parentElement.hidden=!state[name].length;
  }
  panel.hidden=!(state.favorites.length||state.recent.length);
 }
 load();
 for(const [href,{a}] of catalog){
  const wrapper=document.createElement("div");wrapper.className="favorite-card-wrap";
  a.before(wrapper);wrapper.append(a);
  const button=document.createElement("button");button.type="button";button.className="favorite-toggle";
  button.addEventListener("click",()=>{state.favorites=state.favorites.includes(href)?state.favorites.filter(x=>x!==href):[...state.favorites,href];save();render();});
  wrapper.append(button);buttons.set(href,button);
  a.addEventListener("click",()=>visit(href));
  a.addEventListener("auxclick",e=>{if(e.button===1)visit(href);});
 }
 document.getElementById("clearRecent").addEventListener("click",()=>{state.recent=[];save();render();});
 window.addEventListener("storage",e=>{if(e.key===key||e.key===null){load();render();}});
 render();
})();
