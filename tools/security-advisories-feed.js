/* Security Advisories: render the reviewed, same-origin JSON snapshot only. */
(() => {
  "use strict";

  const section = document.getElementById("advisoryFeed");
  if (!section) return;
  const state = document.getElementById("feedState");
  const list = document.getElementById("feedList");
  const filters = document.getElementById("feedFilters");
  const severity = document.getElementById("feedSeverity");
  const product = document.getElementById("feedProduct");
  const count = document.getElementById("feedCount");
  const severityNames = {
    critical: "Critique", high: "Élevée", moderate: "Modérée",
    low: "Faible", unknown: "Non précisée",
  };
  let alerts = [];

  function node(tag, className, value) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (value !== undefined) element.textContent = String(value);
    return element;
  }

  function validUrl(value) {
    try {
      const url = new URL(value);
      return url.protocol === "https:" &&
        (url.hostname === "github.com" || url.hostname === "learn.microsoft.com");
    } catch {
      return false;
    }
  }

  function render() {
    const selected = alerts.filter(item =>
      (severity.value === "all" || item.severity === severity.value) &&
      (product.value === "all" || item.product === product.value)
    );
    list.replaceChildren();
    count.textContent = selected.length + (selected.length === 1 ? " avis affiché" : " avis affichés");
    if (!selected.length) {
      list.appendChild(node("p", "feed-empty",
        "Aucun avis dans ce filtre. Cela ne signifie pas qu'aucune vulnérabilité n'existe."));
      return;
    }
    for (const item of selected.slice(0, 15)) {
      const card = node("article", "feed-item");
      const header = node("div", "feed-item-header");
      header.appendChild(node("strong", "", item.product));
      const badge = node("span", "feed-severity feed-severity-" + item.severity,
        severityNames[item.severity] || severityNames.unknown);
      header.appendChild(badge);
      card.appendChild(header);
      card.appendChild(node("h3", "", item.title));
      const meta = [item.id, item.published_at?.slice(0, 10)].filter(Boolean).join(" · ");
      card.appendChild(node("p", "feed-meta", meta));
      if (item.cves.length) card.appendChild(node("p", "feed-detail", "CVE : " + item.cves.join(", ")));
      if (item.affected_versions.length) {
        card.appendChild(node("p", "feed-detail",
          "Versions affectées : " + item.affected_versions.slice(0, 3).join(" ; ")));
      }
      if (item.patched_versions.length) {
        card.appendChild(node("p", "feed-detail",
          "Premières versions corrigées : " + item.patched_versions.slice(0, 3).join(" ; ")));
      }
      if (item.summary) card.appendChild(node("p", "feed-detail", item.summary));
      const link = node("a", "feed-source-link", "Consulter l'avis officiel ↗");
      link.href = item.url;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      card.appendChild(link);
      list.appendChild(card);
    }
    if (selected.length > 15) {
      list.appendChild(node("p", "feed-empty",
        "Seuls les 15 premiers résultats sont affichés. Affine les filtres pour les autres avis."));
    }
  }

  async function load() {
    try {
      const response = await fetch("./data/security-advisories.json", { cache: "no-store" });
      if (!response.ok) throw new Error("Snapshot unavailable");
      const snapshot = await response.json();
      if (snapshot?.schema_version !== 1 || !Array.isArray(snapshot.alerts) ||
          !snapshot.sources || typeof snapshot.sources !== "object") {
        throw new Error("Invalid snapshot");
      }
      if (!snapshot.generated_at) {
        state.textContent = "Collecte initiale non publiée. L'annuaire officiel ci-dessous reste accessible.";
        return;
      }
      alerts = snapshot.alerts.filter(item =>
        item && typeof item.id === "string" && typeof item.title === "string" &&
        typeof item.product === "string" && validUrl(item.url) &&
        Array.isArray(item.cves) && Array.isArray(item.affected_versions) &&
        Array.isArray(item.patched_versions) &&
        ["critical", "high", "moderate", "low", "unknown"].includes(item.severity)
      ).sort((a, b) => (b.published_at || "").localeCompare(a.published_at || ""));
      const sourceStates = Object.values(snapshot.sources);
      const errors = sourceStates.filter(source => source?.status !== "ok").length;
      const checked = new Date(snapshot.generated_at);
      const old = Number.isNaN(checked.getTime()) || Date.now() - checked.getTime() > 48 * 3600 * 1000;
      const parts = ["Dernière collecte publiée : " + snapshot.generated_at.slice(0, 16).replace("T", " ") + " UTC."];
      if (errors) parts.push(errors + " source(s) indisponible(s) : certains avis peuvent être anciens.");
      if (old) parts.push("Données potentiellement périmées.");
      parts.push("Les avis ne prouvent pas qu'un environnement est affecté.");
      state.textContent = parts.join(" ");
      const names = [...new Set(alerts.map(item => item.product))].sort((a, b) => a.localeCompare(b, "fr"));
      for (const name of names) {
        const option = node("option", "", name);
        option.value = name;
        product.appendChild(option);
      }
      filters.hidden = false;
      render();
    } catch {
      state.textContent = "Flux non disponible. Consulte directement les sources officielles ci-dessous.";
    }
  }

  severity.addEventListener("change", render);
  product.addEventListener("change", render);
  load();
})();
