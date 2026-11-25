/* Shared theme preference, compatible with existing Toolbox tools. */
(() => {
  "use strict";
  const key = "password-studio-theme";
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  const button = document.getElementById("theme");
  let preference = "system";
  try {
    const value = localStorage.getItem(key);
    if (["light", "dark", "system"].includes(value)) preference = value;
    else if (button && document.documentElement.dataset.theme === "dark") preference = "dark";
  } catch {}
  function apply() {
    const dark = preference === "dark" || (preference === "system" && media.matches);
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    if (button) {
      button.textContent = dark ? "☀" : "☾";
      button.setAttribute("aria-label", "Thème : " + ({system:"système",light:"clair",dark:"sombre"})[preference]);
      button.title = button.getAttribute("aria-label");
    }
    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta) meta.content = dark ? "#0b1120" : "#f5f7fc";
  }
  if (button) button.addEventListener("click", () => {
    preference = ({system:"light",light:"dark",dark:"system"})[preference];
    try { localStorage.setItem(key, preference); } catch {}
    apply();
  });
  media.addEventListener("change", apply);
  window.addEventListener("storage", e => {
    if (e.key === key) { preference = ["light","dark","system"].includes(e.newValue) ? e.newValue : "system"; apply(); }
  });
  apply();
})();
