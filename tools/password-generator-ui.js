/* Password Studio V2 — interface : aucune donnée secrète en stockage persistant. */
(() => {
  "use strict";

  const $ = id => document.getElementById(id);
  const core = window.PasswordCore;
  const themeMedia = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;
  const state = {
    mode: "password",
    results: [],
    concealed: false,
    preset: "balanced",
    theme: "system",
    statusTimer: null,
    generateFrame: null
  };

  const PRESETS = Object.freeze({
    balanced: { mode: "password", length: 18, upper: true, lower: true, digits: true, symbols: true, guarantee: true, excludeAmbiguous: false },
    readable: { mode: "password", length: 20, upper: true, lower: true, digits: true, symbols: false, guarantee: true, excludeAmbiguous: true },
    strong: { mode: "password", length: 32, upper: true, lower: true, digits: true, symbols: true, guarantee: true, excludeAmbiguous: false },
    memorable: { mode: "passphrase", words: 7, language: "fr", separator: "-", capitalize: false, appendDigit: false }
  });

  function showStatus(message = "", kind = "") {
    if (state.statusTimer !== null) window.clearTimeout(state.statusTimer);
    $("status").textContent = message;
    $("status").dataset.kind = kind;
    state.statusTimer = message
      ? window.setTimeout(() => {
        $("status").textContent = "";
        $("status").dataset.kind = "";
        state.statusTimer = null;
      }, 3800)
      : null;
  }

  function setTheme() {
    const dark = state.theme === "dark" || (state.theme === "system" && themeMedia && themeMedia.matches);
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    $("themeIcon").textContent = dark ? "☀" : "☾";
    const label = dark ? "Activer le thème clair" : "Activer le thème sombre";
    $("themeToggle").setAttribute("aria-label", label);
    $("themeToggle").title = label;
    document.querySelector('meta[name="theme-color"]').content = dark ? "#0b1120" : "#f5f7fc";
  }

  function initTheme() {
    try {
      const preference = window.localStorage.getItem("password-studio-theme");
      if (preference === "light" || preference === "dark") state.theme = preference;
    } catch (_) {
      // Le stockage peut être désactivé ; les secrets n'y sont jamais enregistrés.
    }
    setTheme();
    $("themeToggle").addEventListener("click", () => {
      state.theme = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
      try { window.localStorage.setItem("password-studio-theme", state.theme); } catch (_) {}
      setTheme();
    });
    if (themeMedia) {
      const onChange = () => { if (state.theme === "system") setTheme(); };
      if (typeof themeMedia.addEventListener === "function") themeMedia.addEventListener("change", onChange);
      else if (typeof themeMedia.addListener === "function") themeMedia.addListener(onChange);
    }
  }

  function updateLabels() {
    $("passwordLengthValue").textContent = $("passwordLength").value;
    $("wordCountValue").textContent = $("wordCount").value;
    const many = Number($("quantity").value) > 1;
    $("generateLabel").textContent = many
      ? "Générer les " + $("quantity").value + " résultats"
      : state.mode === "password" ? "Générer un mot de passe" : "Générer une phrase de passe";
  }

  function setActivePreset(id) {
    state.preset = id;
    document.querySelectorAll("[data-preset]").forEach(button => {
      button.setAttribute("aria-pressed", String(button.dataset.preset === id));
    });
  }

  function setMode(mode) {
    state.mode = mode;
    $("passwordOptions").hidden = mode !== "password";
    $("passphraseOptions").hidden = mode !== "passphrase";
    document.querySelectorAll("[data-mode]").forEach(tab => {
      const selected = tab.dataset.mode === mode;
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = selected ? 0 : -1;
    });
    $("primaryValue").setAttribute("aria-label", mode === "password" ? "Mot de passe généré" : "Phrase de passe générée");
    updateLabels();
  }

  function applyPreset(id) {
    const preset = PRESETS[id];
    if (!preset) return;
    setMode(preset.mode);
    if (preset.mode === "password") {
      $("passwordLength").value = String(preset.length);
      $("useUpper").checked = preset.upper;
      $("useLower").checked = preset.lower;
      $("useDigits").checked = preset.digits;
      $("useSymbols").checked = preset.symbols;
      $("guarantee").checked = preset.guarantee;
      $("excludeAmbiguous").checked = preset.excludeAmbiguous;
    } else {
      $("wordCount").value = String(preset.words);
      $("language").value = preset.language;
      $("separator").value = preset.separator;
      $("capitalize").checked = preset.capitalize;
      $("appendDigit").checked = preset.appendDigit;
    }
    setActivePreset(id);
    updateLabels();
    generate(false);
  }

  function getOptions() {
    if (state.mode === "password") {
      return {
        length: Number($("passwordLength").value),
        upper: $("useUpper").checked,
        lower: $("useLower").checked,
        digits: $("useDigits").checked,
        symbols: $("useSymbols").checked,
        guarantee: $("guarantee").checked,
        excludeAmbiguous: $("excludeAmbiguous").checked
      };
    }
    return {
      words: Number($("wordCount").value),
      language: $("language").value,
      separator: $("separator").value,
      capitalize: $("capitalize").checked,
      appendDigit: $("appendDigit").checked
    };
  }

  function updateStrength(result) {
    const strength = core.strengthForEntropy(result.entropy);
    $("strengthPill").textContent = strength.label;
    $("strengthPill").dataset.tone = strength.tone;
    $("entropyValue").textContent = "≈ " + result.entropy.toFixed(1).replace(".", ",") + " bits";
    $("entropyDescription").textContent = result.mode === "password"
      ? "Tirage Web Crypto uniforme"
      : "Mots indépendants · Web Crypto";
    const meter = $("strengthMeter");
    meter.dataset.tone = strength.tone;
    meter.setAttribute("aria-label", "Robustesse estimée : " + strength.label);
    [...meter.children].forEach((bar, index) => {
      bar.classList.toggle("is-filled", index < strength.level);
    });
  }

  function resetResults() {
    state.results = [];
    $("primaryValue").textContent = "Aucun résultat";
    $("strengthPill").textContent = "—";
    $("strengthPill").dataset.tone = "empty";
    $("entropyValue").textContent = "—";
    $("entropyDescription").textContent = "Source : Web Crypto";
    $("strengthMeter").dataset.tone = "empty";
    $("strengthMeter").setAttribute("aria-label", "Robustesse non évaluée");
    [...$("strengthMeter").children].forEach(bar => bar.classList.remove("is-filled"));
    $("copyPrimary").disabled = true;
    $("batchSection").hidden = true;
    $("batchList").replaceChildren();
  }

  function renderBatch() {
    const remaining = state.results.slice(1);
    const batch = $("batchSection");
    const list = $("batchList");
    list.replaceChildren();
    batch.hidden = remaining.length === 0;
    if (batch.hidden) return;

    const fragment = document.createDocumentFragment();
    remaining.forEach((result, index) => {
      const row = document.createElement("div");
      row.className = "batch-item";
      const number = document.createElement("span");
      number.className = "batch-number";
      number.textContent = String(index + 2).padStart(2, "0");
      const value = document.createElement("span");
      value.className = "batch-value secret-value";
      value.textContent = result.value;
      const button = document.createElement("button");
      button.type = "button";
      button.className = "icon-button";
      button.title = "Copier le résultat " + (index + 2);
      button.setAttribute("aria-label", button.title);
      const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
      svg.setAttribute("aria-hidden", "true");
      const use = document.createElementNS("http://www.w3.org/2000/svg", "use");
      use.setAttribute("href", "#i-copy");
      svg.append(use);
      button.append(svg);
      button.addEventListener("click", () => copyText(result.value, "Résultat " + (index + 2) + " copié."));
      row.append(number, value, button);
      fragment.append(row);
    });
    list.append(fragment);
    batch.classList.toggle("is-concealed", state.concealed);
  }

  function renderResults() {
    const first = state.results[0];
    $("primaryValue").textContent = first.value;
    $("copyPrimary").disabled = false;
    updateStrength(first);
    renderBatch();
    updateLabels();
  }

  function generate(announce = true) {
    if (state.generateFrame !== null) {
      window.cancelAnimationFrame(state.generateFrame);
      state.generateFrame = null;
    }
    try {
      if (!core) throw new Error("Le moteur de génération est indisponible.");
      const quantity = Number($("quantity").value);
      if (![1, 3, 5, 10].includes(quantity)) throw new Error("Quantité invalide.");
      const options = getOptions();
      const next = [];
      for (let i = 0; i < quantity; i++) {
        next.push(state.mode === "password"
          ? core.generatePassword(options)
          : core.generatePassphrase(options));
      }
      state.results = next;
      renderResults();
      showStatus(announce ? (quantity > 1 ? quantity + " résultats générés." : "Nouveau résultat généré.") : "", announce ? "success" : "");
    } catch (error) {
      resetResults();
      showStatus(error instanceof Error ? error.message : "La génération a échoué.", "error");
    }
  }

  function scheduleGenerate() {
    if (state.generateFrame !== null) window.cancelAnimationFrame(state.generateFrame);
    state.generateFrame = window.requestAnimationFrame(() => {
      state.generateFrame = null;
      generate(false);
    });
  }

  async function copyText(value, successMessage) {
    if (!value) {
      showStatus("Aucun résultat à copier.", "error");
      return;
    }
    try {
      if (!navigator.clipboard || typeof navigator.clipboard.writeText !== "function") {
        throw new Error("Presse-papiers indisponible : utilisez HTTPS ou localhost.");
      }
      await navigator.clipboard.writeText(value);
      showStatus(successMessage, "success");
    } catch (error) {
      showStatus(error instanceof Error ? error.message : "Impossible de copier.", "error");
    }
  }

  function bindEvents() {
    document.querySelectorAll("[data-preset]").forEach(button => {
      button.addEventListener("click", () => applyPreset(button.dataset.preset));
    });
    const tabs = [...document.querySelectorAll("[data-mode]")];
    tabs.forEach((tab, index) => {
      tab.addEventListener("click", () => {
        setActivePreset(null);
        setMode(tab.dataset.mode);
        generate(false);
      });
      tab.addEventListener("keydown", event => {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
        event.preventDefault();
        const nextIndex = (index + (event.key === "ArrowRight" ? 1 : tabs.length - 1)) % tabs.length;
        tabs[nextIndex].focus();
        setActivePreset(null);
        setMode(tabs[nextIndex].dataset.mode);
        generate(false);
      });
    });

    for (const id of ["passwordLength", "wordCount"]) {
      $(id).addEventListener("input", () => {
        updateLabels();
        setActivePreset(null);
        scheduleGenerate();
      });
    }
    for (const id of ["useUpper", "useLower", "useDigits", "useSymbols", "guarantee", "excludeAmbiguous", "language", "separator", "capitalize", "appendDigit", "quantity"]) {
      $(id).addEventListener("change", () => {
        if (id !== "quantity") setActivePreset(null);
        generate(false);
      });
    }
    $("generateButton").addEventListener("click", () => generate(true));
    $("copyPrimary").addEventListener("click", () => {
      if (state.results.length) copyText(state.results[0].value, "Résultat copié dans le presse-papiers.");
    });
    $("copyAll").addEventListener("click", () => {
      if (state.results.length > 1) {
        copyText(state.results.map(result => result.value).join("\n"), "Tous les résultats ont été copiés.");
      }
    });
    $("visibilityToggle").addEventListener("click", () => {
      state.concealed = !state.concealed;
      $("outputCard").classList.toggle("is-concealed", state.concealed);
      $("batchSection").classList.toggle("is-concealed", state.concealed);
      $("visibilityToggle").setAttribute("aria-pressed", String(state.concealed));
      const label = state.concealed ? "Afficher les résultats" : "Masquer les résultats";
      $("visibilityToggle").setAttribute("aria-label", label);
      $("visibilityToggle").title = label;
    });
  }

  initTheme();
  bindEvents();
  applyPreset("balanced");
})();
