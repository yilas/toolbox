/* Toolbox — accueil. Aucun framework, aucun appel réseau. */
(() => {
  "use strict";

  document.documentElement.classList.remove("no-js");

  const search = document.getElementById("toolSearch");
  const count = document.getElementById("resultCount");
  const empty = document.getElementById("emptyState");
  const clear = document.getElementById("clearSearch");
  const sections = [...document.querySelectorAll(".portal-section")];
  const cards = [...document.querySelectorAll(".tool-card")];
  function normalize(value) {
    return value
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLocaleLowerCase("fr")
      .trim();
  }

  const indexedCards = cards.map(card => ({
    card,
    text: normalize((card.textContent || "") + " " + (card.dataset.search || ""))
  }));

  function filterTools() {
    const query = normalize(search.value);
    const terms = query.split(/\s+/).filter(Boolean);
    let visible = 0;

    indexedCards.forEach(({ card, text }) => {
      const matches = terms.every(term => text.includes(term));
      card.hidden = !matches;
      if (matches) visible++;
    });

    count.textContent = visible + (visible === 1 ? " outil" : " outils");
    empty.hidden = visible !== 0;
    sections.forEach(section => {
      section.hidden = !section.querySelector(".tool-card:not([hidden])");
    });
  }

  search.addEventListener("input", filterTools);
  clear.addEventListener("click", () => {
    search.value = "";
    filterTools();
    search.focus();
  });

  document.addEventListener("keydown", event => {
    if (event.defaultPrevented || event.metaKey || event.ctrlKey || event.altKey) return;
    const target = event.target;
    const editing = target instanceof HTMLElement && (
      target.isContentEditable ||
      ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName)
    );

    if (event.key === "/" && !editing) {
      event.preventDefault();
      search.focus();
    } else if (event.key === "Escape" && document.activeElement === search) {
      search.value = "";
      filterTools();
      search.blur();
    }
  });

  filterTools();
})();
