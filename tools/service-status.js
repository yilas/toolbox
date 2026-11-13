/* Service Status — annuaire statique sans appels réseau. */
(() => {
"use strict";
document.documentElement.classList.remove("no-js");
const search=document.getElementById("statusSearch");
const sections=[...document.querySelectorAll(".status-section")];
const buttons=[...document.querySelectorAll(".category-button")];
const count=document.getElementById("resultCount");
const empty=document.getElementById("emptyState");
const clear=document.getElementById("clearSearch");
const toggle=document.getElementById("themeToggle");
const themeIcon=document.getElementById("themeIcon");
const media=window.matchMedia?window.matchMedia("(prefers-color-scheme: dark)"):null;
let category="all",theme="system";
const normalize=s=>s.normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLocaleLowerCase("fr").trim();
function filter(){
 const terms=normalize(search.value).split(/\s+/).filter(Boolean);
 let visible=0;
 for(const section of sections){
  const inCategory=category==="all"||section.dataset.group===category;
  let groupVisible=0;
  for(const card of section.querySelectorAll(".status-link")){
   const value=normalize(card.dataset.search||card.textContent||"");
   const show=inCategory&&terms.every(term=>value.includes(term));
   card.hidden=!show;
   if(show){visible++;groupVisible++;}
  }
  section.hidden=groupVisible===0;
 }
 count.textContent=visible+(visible===1?" service":" services");
 empty.hidden=visible!==0;
}
function selectCategory(value){
 category=value;
 buttons.forEach(b=>{const active=b.dataset.category===category;b.classList.toggle("active",active);b.setAttribute("aria-pressed",String(active));});
 filter();
}
buttons.forEach(button=>button.addEventListener("click",()=>selectCategory(button.dataset.category)));
search.addEventListener("input",filter);
clear.addEventListener("click",()=>{search.value="";selectCategory("all");search.focus();});
document.addEventListener("keydown",event=>{
 if(event.defaultPrevented||event.metaKey||event.ctrlKey||event.altKey)return;
 const t=event.target;
 const editing=t instanceof HTMLElement&&(t.isContentEditable||["INPUT","TEXTAREA","SELECT"].includes(t.tagName));
 if(event.key==="/"&&!editing){event.preventDefault();search.focus();}
 else if(event.key==="Escape"&&document.activeElement===search){search.value="";filter();search.blur();}
});
try{const value=localStorage.getItem("password-studio-theme");if(value==="dark"||value==="light")theme=value;}catch(_){}
function setTheme(){
 const dark=theme==="dark"||(theme==="system"&&media&&media.matches);
 document.documentElement.dataset.theme=dark?"dark":"light";
 themeIcon.textContent=dark?"☀":"☾";
 const label=dark?"Activer le thème clair":"Activer le thème sombre";
 toggle.setAttribute("aria-label",label);toggle.title=label;
 const meta=document.querySelector('meta[name="theme-color"]');
 if(meta)meta.content=dark?"#0b1120":"#f5f7fc";
}
toggle.addEventListener("click",()=>{theme=document.documentElement.dataset.theme==="dark"?"light":"dark";try{localStorage.setItem("password-studio-theme",theme);}catch(_){}setTheme();});
if(media){const change=()=>{if(theme==="system")setTheme();};if(media.addEventListener)media.addEventListener("change",change);else if(media.addListener)media.addListener(change);}
setTheme();filter();
})();