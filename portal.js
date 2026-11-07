/* Toolbox — accueil. Aucun framework, aucun appel réseau. */
(() => {
  "use strict";

  document.documentElement.classList.remove("no-js");

  const search = document.getElementById("toolSearch");
  const count = document.getElementById("resultCount");
  const empty = document.getElementById("emptyState");
  const clear = document.getElementById("clearSearch");
  const upcoming = document.getElementById("upcomingSection");
  const cards = [...document.querySelectorAll(".tool-card")];
  const themeButton = document.getElementById("themeToggle");
  const themeMoon = document.getElementById("themeMoon");
  const themeSun = document.getElementById("themeSun");
  const themeMedia = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;

  // Même préférence que Password Studio, pour une navigation cohérente.
  const THEME_KEY = "password-studio-theme";
  let theme = "system";

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
    upcoming.hidden = query !== "";
  }

  function setTheme() {
    const dark = theme === "dark" || (theme === "system" && themeMedia && themeMedia.matches);
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    themeMoon.hidden = Boolean(dark);
    themeSun.hidden = !dark;
    const label = dark ? "Activer le thème clair" : "Activer le thème sombre";
    themeButton.setAttribute("aria-label", label);
    themeButton.title = label;
    const themeMeta = document.querySelector('meta[name="theme-color"]');
    if (themeMeta) themeMeta.setAttribute("content", dark ? "#0b1120" : "#f5f7fc");
  }

  try {
    const storedTheme = localStorage.getItem(THEME_KEY);
    if (storedTheme === "dark" || storedTheme === "light") theme = storedTheme;
  } catch (_) {
    // Le stockage peut être interdit par le navigateur.
  }
  setTheme();

  themeButton.addEventListener("click", () => {
    theme = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    try { localStorage.setItem(THEME_KEY, theme); } catch (_) {}
    setTheme();
  });
  if (themeMedia) {
    const onThemeChange = () => {
      if (theme === "system") setTheme();
    };
    if (typeof themeMedia.addEventListener === "function") {
      themeMedia.addEventListener("change", onThemeChange);
    } else if (typeof themeMedia.addListener === "function") {
      themeMedia.addListener(onThemeChange);
    }
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
