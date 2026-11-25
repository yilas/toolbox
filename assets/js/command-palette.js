(() => {
 "use strict";
 const dialog=document.getElementById("commandPalette");
 if(!dialog||!dialog.showModal)return;
 const open=document.getElementById("commandOpen"),input=document.getElementById("commandSearch"),results=document.getElementById("commandResults"),empty=document.getElementById("commandEmpty");
 const normalize=s=>s.normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase().trim();
 const catalog=[...document.querySelectorAll(".tool-card")].map(a=>({href:a.getAttribute("href"),title:a.querySelector("h3").textContent,search:normalize(a.textContent+" "+(a.dataset.search||""))}));
 let previous;
 function render(){
  const terms=normalize(input.value).split(/\s+/).filter(Boolean);
  const matches=catalog.filter(x=>terms.every(t=>x.search.includes(t)));
  results.replaceChildren();
  for(const item of matches){const a=document.createElement("a");a.href=item.href;a.textContent=item.title;results.append(a);}
  empty.hidden=matches.length!==0;
 }
 function show(){if(dialog.open)return;previous=document.activeElement;input.value="";render();dialog.showModal();input.focus();}
 open.hidden=false;open.addEventListener("click",show);
 document.getElementById("commandClose").addEventListener("click",()=>dialog.close());
 dialog.addEventListener("close",()=>previous?.focus());
 input.addEventListener("input",render);
 document.addEventListener("keydown",e=>{if((e.ctrlKey||e.metaKey)&&!e.altKey&&e.key.toLowerCase()==="k"){e.preventDefault();show();}});
 dialog.addEventListener("keydown",e=>{
  const links=[...results.querySelectorAll("a")],index=links.indexOf(document.activeElement);
  if(e.key==="ArrowDown"||e.key==="ArrowUp"){e.preventDefault();if(!links.length)return;const next=e.key==="ArrowDown"?(index+1)%links.length:index<=0?links.length-1:index-1;links[next].focus();}
  else if(e.key==="Enter"&&document.activeElement===input&&links.length){e.preventDefault();links[0].click();}
 });
 try{localStorage.removeItem("toolbox-quick-access-v1");}catch{}
})();
