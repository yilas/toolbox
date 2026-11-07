/*
 * Analyseur JWT — contrôleur DOM / UI.
 * Historique en mémoire seulement ; jamais de JWT, secret ou clé en localStorage.
 * Toutes les données provenant d'un jeton sont rendues par textContent.
 */
(() => {
  "use strict";

  const $ = id => document.getElementById(id);
  const core = window.JWTCore;
  const LS_KEY = "jwt-decode-analyze";
  const THEME_KEY = "password-studio-theme";
  const MAX_HISTORY = 20;
  const media = typeof window.matchMedia === "function"
    ? window.matchMedia("(prefers-color-scheme: dark)") : null;
  const state = {
    history: [],
    current: null,
    serial: 0,
    theme: "system",
    messageTimer: null,
    savedRules: null
  };

  function showMessage(message = "", kind = "") {
    if (state.messageTimer) window.clearTimeout(state.messageTimer);
    $("message").textContent = message;
    $("message").dataset.kind = kind;
    state.messageTimer = message && kind === "success"
      ? window.setTimeout(() => {
        $("message").textContent = "";
        $("message").dataset.kind = "";
        state.messageTimer = null;
      }, 4800)
      : null;
  }

  function readStorage(key) {
    try { return localStorage.getItem(key); } catch (_) { return null; }
  }

  function writeStorage(key, value) {
    try { localStorage.setItem(key, value); return true; } catch (_) { return false; }
  }

  function initPreferences() {
    let migrated = 0;
    const previous = readStorage(LS_KEY);
    if (previous) {
      try {
        const stored = JSON.parse(previous);
        if (stored && typeof stored === "object") {
          const rules = core.expectedRules(stored.rules || {});
          state.savedRules = rules;
          if (Array.isArray(stored.history)) migrated = stored.history.length;
          // Migration : l'ancien format sauvegardait des tokens et SECRETS en clair.
          // On ne conserve que les règles ; aucune copie des anciens JWT/secrets.
          const sanitized = JSON.stringify({ rules });
          if (sanitized !== previous && !writeStorage(LS_KEY, sanitized)) {
            try { localStorage.removeItem(LS_KEY); } catch (_) {}
          }
          if (!readStorage(THEME_KEY) && (stored.theme === "dark" || stored.theme === "light")) {
            writeStorage(THEME_KEY, stored.theme);
          }
        }
      } catch (_) {
        try { localStorage.removeItem(LS_KEY); } catch (_) {}
      }
    }

    const currentTheme = readStorage(THEME_KEY);
    if (currentTheme === "dark" || currentTheme === "light") state.theme = currentTheme;
    applyTheme();

    if (state.savedRules) populateRules(state.savedRules);
    if (migrated) {
      showMessage(
        migrated + " ancienne(s) entrée(s) d'historique supprimée(s) du stockage local (protection des JWT et secrets).",
        "warning"
      );
    }
  }

  function applyTheme() {
    const dark = state.theme === "dark" ||
      (state.theme === "system" && media && media.matches);
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    $("themeMoon").hidden = Boolean(dark);
    $("themeSun").hidden = !dark;
    const label = dark ? "Activer le thème clair" : "Activer le thème sombre";
    $("themeToggle").setAttribute("aria-label", label);
    $("themeToggle").title = label;
    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta) meta.setAttribute("content", dark ? "#0b1120" : "#f5f7fc");
  }

  function currentRules() {
    return core.expectedRules({
      iss: $("expectedIss").value,
      azp: $("expectedAzp").value,
      aud: $("expectedAud").value,
      audMode: $("audMode").value,
      alg: $("expectedAlg").value,
      skew: Number($("skewSeconds").value)
    });
  }

  function populateRules(rules) {
    const r = core.expectedRules(rules);
    $("expectedIss").value = r.iss;
    $("expectedAzp").value = r.azp;
    $("expectedAud").value = r.aud.join(", ");
    $("audMode").value = r.audMode;
    $("expectedAlg").value = r.alg;
    $("skewSeconds").value = String([0, 60, 300].includes(r.skew) ? r.skew : 0);
  }

  function saveRules() {
    const rules = currentRules();
    state.savedRules = rules;
    if (writeStorage(LS_KEY, JSON.stringify({ rules }))) {
      showMessage("Règles enregistrées localement (aucun secret sauvegardé).", "success");
    } else {
      showMessage("Stockage désactivé : les règles restent disponibles dans cet onglet.", "warning");
    }
  }

  function resetRules() {
    populateRules({});
    state.savedRules = core.expectedRules({});
    if (!writeStorage(LS_KEY, JSON.stringify({ rules: state.savedRules }))) {
      try { localStorage.removeItem(LS_KEY); } catch (_) {}
    }
    invalidate();
    showMessage("Règles réinitialisées.", "success");
  }

  function setBadge(id, value, status) {
    const badge = $(id);
    badge.textContent = value;
    badge.dataset.state = status;
  }

  function resetOutputs() {
    state.current = null;
    $("resultsSection").hidden = true;
    $("tokenFacts").hidden = true;
    $("diagnostics").hidden = true;
    $("timelineBlock").hidden = true;
    $("headerJson").textContent = "";
    $("payloadJson").textContent = "";
    $("checkList").replaceChildren();
    setBadge("signatureBadge", "Non analysée", "idle");
    setBadge("claimsBadge", "Non analysés", "idle");
    $("signatureDetail").textContent = "Aucun jeton analysé.";
    $("claimsDetail").textContent = "Les contrôles ne sont pas encore exécutés.";
    $("trustNotice").dataset.type = "";
    $("trustNotice").textContent =
      "Un JWT décodé n'est pas automatiquement un JWT valide. La vérification de signature nécessite une clé adaptée.";
    $("copyHeader").disabled = true;
    $("copyPayload").disabled = true;
  }

  function invalidate() {
    state.serial++;
    resetOutputs();
    showMessage("");
  }

  function clearInputs() {
    state.serial++;
    $("jwtInput").value = "";
    $("hmacSecret").value = "";
    $("publicKey").value = "";
    $("hmacSecret").type = "password";
    $("revealSecret").textContent = "Afficher";
    $("revealSecret").setAttribute("aria-pressed", "false");
    $("revealSecret").setAttribute("aria-label", "Afficher le secret HMAC");
    resetOutputs();
    showMessage("Champs sensibles effacés.", "success");
    $("jwtInput").focus();
  }

  function renderCheckList(checks) {
    const list = $("checkList");
    list.replaceChildren();
    const fragment = document.createDocumentFragment();
    const statuses = { ok: "Conforme", error: "Échec", warning: "Attention", info: "Info" };
    for (const check of checks) {
      const row = document.createElement("div");
      row.className = "check";
      row.dataset.state = check.state;

      const status = document.createElement("span");
      status.className = "check-status";
      status.textContent = statuses[check.state] || "Info";
      const name = document.createElement("span");
      name.className = "check-name";
      name.textContent = check.name;
      const detail = document.createElement("span");
      detail.className = "check-detail";
      detail.textContent = check.detail;

      row.append(status, name, detail);
      fragment.append(row);
    }
    list.append(fragment);
    $("diagnostics").hidden = checks.length === 0;
  }

  function renderTimeline(payload) {
    const tl = core.timeline(payload);
    $("timelineBlock").hidden = !tl;
    if (!tl) return;
    $("timelineProgress").style.width = tl.current.toFixed(2) + "%";
    $("timelineMarker").style.left = tl.current.toFixed(2) + "%";
    $("timelineStart").textContent = core.describeDate(tl.start);
    $("timelineEnd").textContent = core.describeDate(tl.end);
  }

  function displayAnalysis(parsed, analysis) {
    $("resultsSection").hidden = false;
    $("headerJson").textContent = JSON.stringify(parsed.header, null, 2);
    $("payloadJson").textContent = JSON.stringify(parsed.payload, null, 2);
    $("copyHeader").disabled = false;
    $("copyPayload").disabled = false;
    $("tokenFacts").hidden = false;
    $("factAlg").textContent = String(parsed.header.alg);
    $("factTyp").textContent = typeof parsed.header.typ === "string" ? parsed.header.typ : "—";
    $("factIss").textContent = typeof parsed.payload.iss === "string" ? parsed.payload.iss : "—";
    $("factExp").textContent = core.isNumericDate(parsed.payload.exp)
      ? core.describeDate(parsed.payload.exp) : "Non précisée";

    if (analysis.errors) {
      setBadge("claimsBadge", analysis.errors + " erreur(s)", "error");
    } else if (analysis.warnings) {
      setBadge("claimsBadge", analysis.warnings + " alerte(s)", "warning");
    } else {
      setBadge("claimsBadge", "Sans alerte", "ok");
    }
    $("claimsDetail").textContent = analysis.checks.length + " contrôles — " +
      analysis.errors + " erreur(s), " + analysis.warnings + " avertissement(s).";
    renderCheckList(analysis.checks);
    renderTimeline(parsed.payload);
  }

  function renderSignature(signature, analysis) {
    setBadge("signatureBadge", signature.label, signature.state);
    $("signatureDetail").textContent = signature.detail;
    const notice = $("trustNotice");
    if (signature.state === "verified") {
      notice.dataset.type = analysis.errors ? "warning" : "";
      notice.textContent = analysis.errors
        ? "La signature correspond à la clé fournie, mais certains claims ou règles sont invalides."
        : "Signature vérifiée et aucun contrôle en erreur. Il reste nécessaire de valider les exigences de votre application.";
    } else if (signature.state === "invalid" || signature.state === "blocked") {
      notice.dataset.type = "error";
      notice.textContent = "Jeton non authentifié. Ne faites pas confiance à son contenu pour autoriser un accès.";
    } else {
      notice.dataset.type = "warning";
      notice.textContent = "Contenu décodé mais signature non vérifiée : les informations affichées ne sont pas authentifiées.";
    }
  }

  function addHistory(parsed, analysis, signature) {
    const entry = {
      token: parsed.token,
      header: parsed.header,
      payload: parsed.payload,
      ts: Date.now(),
      algorithm: parsed.header.alg,
      summary: signature.label,
      claimErrors: analysis.errors
      // Pas de secret HMAC ni de clé publique dans l'historique.
    };
    const existing = state.history.findIndex(item => item.token === entry.token);
    if (existing !== -1) state.history.splice(existing, 1);
    state.history.unshift(entry);
    state.history.length = Math.min(state.history.length, MAX_HISTORY);
    renderHistory();
  }

  function renderHistory() {
    const list = $("historyList");
    list.replaceChildren();
    $("clearHistory").disabled = state.history.length === 0;
    if (!state.history.length) {
      const empty = document.createElement("div");
      empty.className = "history-empty";
      empty.textContent = "Aucune analyse pour le moment.";
      list.append(empty);
      return;
    }
    const fragment = document.createDocumentFragment();
    state.history.forEach((entry, index) => {
      const wrap = document.createElement("div");
      wrap.className = "history-item";

      const top = document.createElement("div");
      top.className = "history-top";
      const alg = document.createElement("span");
      alg.className = "state-label";
      alg.textContent = entry.algorithm;
      alg.dataset.state = entry.summary === "Signature vérifiée" ? "ok" : "idle";
      const date = document.createElement("time");
      date.dateTime = new Date(entry.ts).toISOString();
      date.textContent = new Date(entry.ts).toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" });
      top.append(alg, date);

      const title = document.createElement("p");
      title.className = "history-title";
      title.textContent = entry.summary;
      const detail = document.createElement("div");
      detail.className = "history-meta";
      const subject = typeof entry.payload.sub === "string" && entry.payload.sub
        ? "sub : " + entry.payload.sub
        : "Sans subject";
      detail.textContent = subject.slice(0, 90) + (entry.claimErrors ? " · " + entry.claimErrors + " erreur(s)" : "");

      const actions = document.createElement("div");
      actions.className = "history-actions";
      const restore = document.createElement("button");
      restore.type = "button";
      restore.className = "btn btn-small";
      restore.textContent = "Réexaminer";
      restore.setAttribute("aria-label", "Réexaminer le jeton de " + date.textContent);
      restore.addEventListener("click", () => {
        $("jwtInput").value = entry.token;
        void analyze();
        $("jwtInput").scrollIntoView({ behavior: "smooth", block: "center" });
      });

      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "btn btn-small btn-danger";
      remove.textContent = "Supprimer";
      remove.setAttribute("aria-label", "Supprimer l'analyse de " + date.textContent);
      remove.addEventListener("click", () => {
        state.history.splice(index, 1);
        renderHistory();
        showMessage("Entrée supprimée de l'historique temporaire.", "success");
      });
      actions.append(restore, remove);
      wrap.append(top, title, detail, actions);
      fragment.append(wrap);
    });
    list.append(fragment);
  }

  function clearHistory() {
    state.history = [];
    renderHistory();
    showMessage("Historique temporaire effacé.", "success");
  }

  async function analyze() {
    const serial = ++state.serial;
    resetOutputs();
    const input = $("jwtInput").value;
    let parsed;
    try {
      parsed = core.parseJwt(input);
    } catch (error) {
      showMessage(error && error.message || "JWT invalide.", "error");
      return;
    }

    const rules = currentRules();
    const analysis = core.checkClaims(parsed.header, parsed.payload, rules);
    state.current = { parsed, analysis };
    displayAnalysis(parsed, analysis);
    setBadge("signatureBadge", "Vérification…", "idle");
    $("signatureDetail").textContent = "Contrôle cryptographique en cours.";
    showMessage("");

    const signature = await core.verifySignature(parsed, {
      secret: $("hmacSecret").value,
      publicKey: $("publicKey").value,
      expectedAlg: rules.alg
    });

    if (serial !== state.serial) return;
    state.current.signature = signature;
    renderSignature(signature, analysis);
    addHistory(parsed, analysis, signature);
    showMessage("JWT décodé. Consultez séparément les statuts de signature et de claims.", "success");
  }

  async function copyText(text, label) {
    if (!text) {
      showMessage("Aucune donnée à copier.", "warning");
      return;
    }
    try {
      if (!navigator.clipboard || typeof navigator.clipboard.writeText !== "function") {
        throw new Error("Presse-papiers indisponible. Utilisez HTTPS ou localhost.");
      }
      await navigator.clipboard.writeText(text);
      showMessage(label, "success");
    } catch (error) {
      showMessage(error && error.message || "Copie impossible.", "error");
    }
  }

  async function loadExample() {
    try {
      if (!crypto.subtle) throw new Error("Web Crypto indisponible.");
      const now = Math.floor(Date.now() / 1000);
      const header = { alg: "HS256", typ: "JWT", kid: "exemple-local" };
      const payload = {
        iss: "https://example.local",
        sub: "utilisateur-demo",
        aud: "toolbox-demo",
        iat: now, nbf: now, exp: now + 3600,
        scope: "read:example"
      };
      const secret = "exemple-uniquement-ne-pas-utiliser-en-production";
      const bytes = new TextEncoder();
      const h = core.encodeBase64Url(bytes.encode(JSON.stringify(header)));
      const p = core.encodeBase64Url(bytes.encode(JSON.stringify(payload)));
      const signingInput = h + "." + p;
      const key = await crypto.subtle.importKey("raw", bytes.encode(secret),
        { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, bytes.encode(signingInput)));

      $("jwtInput").value = signingInput + "." + core.encodeBase64Url(signature);
      $("hmacSecret").value = secret;
      $("publicKey").value = "";
      populateRules({ iss: payload.iss, aud: [payload.aud], azp: "", alg: "HS256", skew: 0 });
      $("signatureOptions").open = true;
      $("validationOptions").open = true;
      await analyze();
      showMessage("Exemple signé localement (clé de démonstration publique dans le code).", "success");
    } catch (error) {
      showMessage("Exemple indisponible : " + (error && error.message || "erreur"), "error");
    }
  }

  function bind() {
    $("jwtForm").addEventListener("submit", event => {
      event.preventDefault();
      void analyze();
    });

    $("jwtInput").addEventListener("keydown", event => {
      if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
        event.preventDefault();
        void analyze();
      }
    });

    for (const id of [
      "jwtInput", "hmacSecret", "publicKey", "expectedIss", "expectedAzp",
      "expectedAud", "audMode", "expectedAlg", "skewSeconds"
    ]) {
      const input = $(id);
      input.addEventListener("input", invalidate);
      input.addEventListener("change", invalidate);
    }

    $("exampleButton").addEventListener("click", () => { void loadExample(); });
    $("clearButton").addEventListener("click", clearInputs);
    $("clearHistory").addEventListener("click", clearHistory);

    $("copyHeader").addEventListener("click", () => {
      void copyText($("headerJson").textContent, "En-tête copié.");
    });
    $("copyPayload").addEventListener("click", () => {
      void copyText($("payloadJson").textContent, "Payload copié.");
    });

    $("saveRules").addEventListener("click", saveRules);
    $("resetRules").addEventListener("click", resetRules);

    $("revealSecret").addEventListener("click", () => {
      const input = $("hmacSecret");
      const revealed = input.type === "password";
      input.type = revealed ? "text" : "password";
      $("revealSecret").textContent = revealed ? "Masquer" : "Afficher";
      $("revealSecret").setAttribute("aria-pressed", String(revealed));
      $("revealSecret").setAttribute("aria-label", revealed ? "Masquer le secret HMAC" : "Afficher le secret HMAC");
    });

    $("themeToggle").addEventListener("click", () => {
      state.theme = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
      writeStorage(THEME_KEY, state.theme);
      applyTheme();
    });
    if (media) {
      const onChange = () => { if (state.theme === "system") applyTheme(); };
      if (media.addEventListener) media.addEventListener("change", onChange);
      else if (media.addListener) media.addListener(onChange);
    }
  }

  if (!core) {
    $("message").textContent = "Le moteur JWT est indisponible.";
    $("analyzeButton").disabled = true;
  } else {
    resetOutputs();
    bind();
    initPreferences();
  }
})();
