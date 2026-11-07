/* Toolbox — Calculateur IPv4 V2 / interface (sans framework) */
(() => {
  "use strict";

  const $ = id => document.getElementById(id);
  const core = window.SubnetsCore;
  const storage = window.SubnetsStorage;
  const PAGE_SIZE = 64;
  const MAX_PREVIEW_SUBNETS = 128;
  const MAX_GROUPED_CARDS = 64;
  const MAX_HIDDEN_CARDS = 200;
  const VIEW_PREF_KEY = "toolbox-subnets-view-";
  const STATUS = Object.freeze({
    free: "Libre", reserved: "Réservé", inherited: "Hérité",
    partial: "Partiel", full: "Complet"
  });
  const state = {
    data: storage.emptyState(),
    active: null,
    coverage: null,
    view: "overview",
    visibleMasks: new Set(),
    hiddenCards: new Set(),
    gridPrefix: 32,
    gridPage: 0,
    gridFilter: "all",
    selected: null,
    editing: null,
    blocked: false,
    persistent: true,
    theme: "system"
  };
  const systemTheme = typeof window.matchMedia === "function"
    ? window.matchMedia("(prefers-color-scheme: dark)") : null;

  function say(message = "", kind = "") {
    const el = $("message");
    el.textContent = message;
    el.dataset.kind = kind;
  }

  function formatNumber(value) {
    return value.toLocaleString("fr-FR");
  }

  function formatPercent(value) {
    return value.toLocaleString("fr-FR", {
      maximumFractionDigits: value && value < 0.1 ? 3 : 1
    }) + " %";
  }

  function setStorageNotice(message = "", kind = "") {
    const element = $("storageNotice");
    element.textContent = message;
    element.dataset.kind = kind;
    element.hidden = !message;
  }

  function initTheme() {
    try {
      const stored = localStorage.getItem("password-studio-theme");
      if (stored === "light" || stored === "dark") state.theme = stored;
    } catch (_) {}
    updateTheme();

    $("themeToggle").addEventListener("click", () => {
      state.theme = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
      try { localStorage.setItem("password-studio-theme", state.theme); } catch (_) {}
      updateTheme();
    });
    if (systemTheme) {
      const callback = () => { if (state.theme === "system") updateTheme(); };
      if (typeof systemTheme.addEventListener === "function") systemTheme.addEventListener("change", callback);
      else if (typeof systemTheme.addListener === "function") systemTheme.addListener(callback);
    }
  }

  function updateTheme() {
    const dark = state.theme === "dark" ||
      (state.theme === "system" && systemTheme && systemTheme.matches);
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    $("themeMoon").hidden = Boolean(dark);
    $("themeSun").hidden = !dark;
    $("themeToggle").setAttribute("aria-label", dark ? "Activer le thème clair" : "Activer le thème sombre");
    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta) meta.content = dark ? "#0b1120" : "#f5f7fc";
  }

  function allocations() {
    if (!state.active) return [];
    return state.data.networks[state.active.cidr]?.allocations || [];
  }

  function refreshSavedNetworks() {
    const select = $("savedNetworks");
    select.replaceChildren();
    const empty = document.createElement("option");
    empty.value = "";
    empty.textContent = "Sélectionner un réseau";
    select.append(empty);
    const keys = Object.keys(state.data.networks).sort((a, b) => {
      const x = core.parseCIDR(a);
      const y = core.parseCIDR(b);
      return x.start - y.start || x.prefix - y.prefix;
    });
    keys.forEach(key => {
      const option = document.createElement("option");
      option.value = key;
      option.textContent = key;
      select.append(option);
    });
    select.value = state.active ? state.active.cidr : "";
  }

  function persistData(data) {
    state.data = data;
    if (!state.blocked) {
      const result = storage.save(data);
      state.persistent = result.ok;
      if (!result.ok) setStorageNotice(result.error, "warning");
    } else {
      setStorageNotice(
        "Le stockage V2 existant est illisible. Il n'est pas écrasé : les modifications restent uniquement en mémoire. Exportez vos données pour les conserver.",
        "error"
      );
    }
    refreshSavedNetworks();
    render();
  }

  function openNetwork(value) {
    try {
      const parent = core.parseCIDR(value);
      const next = storage.withNetwork(state.data, parent.cidr);
      state.active = parent;
      state.selected = null;
      loadViewPreferences();
      state.gridPrefix = Math.min(32, parent.prefix + 2);
      state.gridPage = 0;
      state.gridFilter = "all";
      $("statusFilter").value = "all";
      $("networkInput").value = parent.cidr;
      populateMasks();
      persistData(next);
      say("Réseau " + parent.cidr + " chargé.", "success");
    } catch (error) {
      say(error && error.message || "Réseau IPv4 invalide.", "error");
    }
  }

  function populateMasks() {
    const select = $("maskSelect");
    select.replaceChildren();
    if (!state.active) return;
    for (let mask = state.active.prefix; mask <= 32; mask++) {
      const option = document.createElement("option");
      option.value = String(mask);
      option.textContent = "/" + mask;
      select.append(option);
    }
    select.value = String(state.gridPrefix);
  }

  // Préférences purement visuelles, indépendantes des réservations réseau.
  function defaultVisibleMasks(parent) {
    const masks = new Set();
    for (let prefix = parent.prefix; prefix <= Math.min(32, parent.prefix + 5); prefix++) {
      masks.add(prefix);
    }
    return masks;
  }

  function viewPreferenceKey() {
    return VIEW_PREF_KEY + state.active.cidr;
  }

  function loadViewPreferences() {
    state.visibleMasks = defaultVisibleMasks(state.active);
    state.hiddenCards = new Set();
    try {
      const saved = JSON.parse(localStorage.getItem(viewPreferenceKey()) || "null");
      if (!saved || typeof saved !== "object") return;
      if (Array.isArray(saved.masks)) {
        state.visibleMasks = new Set(saved.masks.filter(prefix =>
          Number.isInteger(prefix) && prefix >= state.active.prefix && prefix <= 32));
      }
      if (Array.isArray(saved.hidden)) {
        state.hiddenCards = new Set(saved.hidden
          .filter(key => typeof key === "string" && key.length < 70)
          .slice(0, MAX_HIDDEN_CARDS));
      }
    } catch (_) {
      // Une préférence illisible ne doit jamais gêner le calcul ou l'inventaire.
    }
  }

  function saveViewPreferences() {
    try {
      localStorage.setItem(viewPreferenceKey(), JSON.stringify({
        masks: [...state.visibleMasks].sort((a, b) => a - b),
        hidden: [...state.hiddenCards]
      }));
    } catch (_) {}
  }

  function hiddenCardKey(prefix, cidr) {
    return prefix + "|" + cidr;
  }

  function setVisibleMasks(masks) {
    state.visibleMasks = new Set(masks);
    saveViewPreferences();
    renderMaskFilters();
    renderOverview();
  }

  function renderMaskFilters() {
    const list = $("maskFilterList");
    list.replaceChildren();
    if (!state.active) return;
    const total = 33 - state.active.prefix;
    for (let prefix = state.active.prefix; prefix <= 32; prefix++) {
      const label = document.createElement("label");
      label.className = "mask-filter-item";
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = state.visibleMasks.has(prefix);
      checkbox.setAttribute("aria-label", "Afficher les sous-réseaux /" + prefix);
      checkbox.addEventListener("change", () => {
        if (checkbox.checked) state.visibleMasks.add(prefix);
        else state.visibleMasks.delete(prefix);
        saveViewPreferences();
        renderMaskFilters();
        renderOverview();
      });
      const name = document.createElement("span");
      name.textContent = "/" + prefix;
      label.append(checkbox, name);
      list.append(label);
    }
    $("visibleMaskCount").textContent = state.visibleMasks.size + " / " + total + " niveaux";
    $("restoreHidden").hidden = state.hiddenCards.size === 0;
    $("restoreHidden").textContent = "Réafficher " + state.hiddenCards.size + " bloc(s) masqué(s)";
  }

  function currentCoverage() {
    state.coverage = core.prepareCoverage(allocations(), state.active);
    return state.coverage;
  }

  function renderSummary() {
    const coverage = currentCoverage();
    const root = core.describeNode(state.active, coverage);
    $("activeNetwork").textContent = root.cidr;
    $("metricTotal").textContent = formatNumber(root.size);
    $("metricOccupied").textContent = formatNumber(root.occupied);
    $("metricFree").textContent = formatNumber(root.free);
    $("metricReservations").textContent = formatNumber(coverage.records.length);
    $("networkRange").textContent = root.first + " — " + root.last;
    $("occupationBar").style.width = root.percent.toFixed(4) + "%";
    $("occupationMeter").setAttribute("aria-label", "Occupation " + formatPercent(root.percent));
    $("occupationLabel").textContent = formatPercent(root.percent) + " occupé";
    $("rootStatus").textContent = STATUS[root.status];
    $("rootStatus").dataset.status = root.status;
    $("overlapNotice").hidden = !coverage.hasOverlap;
  }

  function createBadge(status) {
    const label = document.createElement("span");
    label.className = "badge";
    label.dataset.status = status;
    label.textContent = STATUS[status] || status;
    return label;
  }

  function nodeDescription(node) {
    if (node.status === "reserved" && node.direct?.name) return node.direct.name;
    if (node.status === "inherited") return "Réservé via " + node.inheritedFrom;
    if (node.status === "partial" || node.status === "full") {
      return formatNumber(node.occupied) + " / " + formatNumber(node.size) + " adresses";
    }
    return formatNumber(node.size) + " adresses IPv4";
  }

  function checkboxFor(node) {
    const cb = document.createElement("input");
    cb.className = "node-checkbox";
    cb.type = "checkbox";
    cb.checked = node.status === "reserved";
    cb.disabled = node.status !== "reserved" && node.status !== "free";
    cb.setAttribute("aria-label", (node.status === "reserved" ? "Gérer la réservation " : "Réserver ") + node.cidr);
    if (cb.disabled) {
      cb.title = node.status === "inherited"
        ? "Réservé par " + node.inheritedFrom
        : "Des réservations descendantes existent : gérer les enfants d'abord.";
    }
    cb.addEventListener("change", () => {
      cb.checked = node.status === "reserved";
      openAllocationDialog(node.cidr);
    });
    return cb;
  }

  function bindHighlight(element, cidr) {
    element.dataset.netCidr = cidr;
    element.addEventListener("mouseenter", () => highlightRelations(cidr));
    element.addEventListener("mouseleave", clearHighlights);
  }

  function clearHighlights() {
    document.querySelectorAll("[data-net-cidr]").forEach(element => {
      element.classList.remove("is-target", "is-related");
    });
  }

  function highlightRelations(cidr) {
    const source = core.parseCIDR(cidr);
    document.querySelectorAll("[data-net-cidr]").forEach(element => {
      const candidate = core.parseCIDR(element.dataset.netCidr);
      element.classList.toggle("is-target", candidate.cidr === source.cidr);
      element.classList.toggle("is-related",
        candidate.cidr !== source.cidr &&
        (core.contains(source, candidate) || core.contains(candidate, source)));
    });
  }

  function nodeAction(node) {
    if (node.status !== "free" && node.status !== "reserved") return null;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "btn small";
    button.textContent = node.status === "reserved" ? "Modifier" : "Réserver";
    button.setAttribute("aria-label", button.textContent + " " + node.cidr);
    button.addEventListener("click", () => openAllocationDialog(node.cidr));
    return button;
  }

  // Les CIDR sont lisibles dans des cartes, sans réduire les textes en microcases.
  // Les niveaux > 128 sous-réseaux deviennent des cartes de plages agrégées.
  // Masquer un bloc n'affecte jamais son état de réservation.
  function createOverviewCard(bucket, prefix, aggregated, groupSize) {
    const node = core.describeNode(bucket, state.coverage);
    const card = document.createElement("div");
    card.className = "overview-card";
    card.dataset.status = node.status;
    const main = document.createElement("button");
    main.type = "button";
    main.className = "overview-card-main";
    bindHighlight(main, node.cidr);

    const cidr = document.createElement("strong");
    cidr.className = "overview-card-cidr";
    cidr.textContent = node.cidr;
    const detail = document.createElement("span");
    detail.className = "overview-card-detail";
    detail.textContent = aggregated
      ? formatNumber(groupSize) + " sous-réseaux /" + prefix + " · " + formatPercent(node.percent)
      : STATUS[node.status] + " · " + formatPercent(node.percent);

    const meter = document.createElement("span");
    meter.className = "overview-card-meter";
    const fill = document.createElement("span");
    fill.style.width = node.percent.toFixed(4) + "%";
    meter.append(fill);
    main.append(cidr, detail, meter);

    const canManage = !aggregated && (node.status === "free" || node.status === "reserved");
    main.title = node.cidr + " — " + STATUS[node.status] + " — " +
      formatPercent(node.percent) + " occupé" +
      (aggregated ? " (" + groupSize + " sous-réseaux /" + prefix + ")" : "");
    main.setAttribute("aria-label", main.title + " : " +
      (canManage ? "gérer la réservation" : "ouvrir la grille détaillée"));
    main.addEventListener("click", () => {
      if (canManage) openAllocationDialog(node.cidr);
      else navigateTo(core.cidrFromStart(bucket.start, prefix).cidr);
    });

    const hide = document.createElement("button");
    hide.type = "button";
    hide.className = "overview-card-hide";
    hide.textContent = "Masquer";
    hide.title = "Masquer uniquement la carte " + node.cidr + " de la vue";
    hide.setAttribute("aria-label", hide.title);
    hide.addEventListener("click", () => {
      if (state.hiddenCards.size >= MAX_HIDDEN_CARDS) {
        say("Limite de blocs masqués atteinte : réaffichez-en certains.", "warning");
        return;
      }
      state.hiddenCards.add(hiddenCardKey(prefix, bucket.cidr));
      saveViewPreferences();
      renderMaskFilters();
      renderOverview();
    });
    card.append(main, hide);
    return card;
  }

  function renderOverview() {
    const container = $("maskOverview");
    container.replaceChildren();
    if (!state.active) return;
    renderMaskFilters();
    const parent = state.active;
    const fragment = document.createDocumentFragment();
    let visibleBands = 0;

    for (let prefix = parent.prefix; prefix <= 32; prefix++) {
      if (!state.visibleMasks.has(prefix)) continue;
      visibleBands++;
      const total = 2 ** (prefix - parent.prefix);
      const aggregated = total > MAX_PREVIEW_SUBNETS;
      // Comme tous les CIDR sont des blocs explicites tant que total <= 128,
      // un /19 → /24 présente réellement ses 32 sous-réseaux nommés.
      const bucketCount = aggregated ? MAX_GROUPED_CARDS : total;
      const groupingPrefix = aggregated ? parent.prefix + 6 : prefix;
      const grouped = aggregated ? 2 ** (prefix - groupingPrefix) : 1;
      const subnetSize = 2 ** (32 - groupingPrefix);

      const band = document.createElement("section");
      band.className = "overview-band";
      band.dataset.prefix = String(prefix);
      band.setAttribute("aria-label", "Sous-réseaux /" + prefix);

      const heading = document.createElement("div");
      heading.className = "overview-band-heading";
      const title = document.createElement("strong");
      title.className = "overview-band-name";
      title.textContent = "/" + prefix;
      const meta = document.createElement("span");
      meta.className = "overview-band-meta";
      meta.textContent = formatNumber(total) + " sous-réseau" + (total > 1 ? "x" : "") +
        " · " + formatNumber(2 ** (32 - prefix)) + " IP / sous-réseau";
      const counter = document.createElement("span");
      counter.className = "overview-band-stat";
      counter.textContent = aggregated
        ? "Regroupés par " + formatNumber(grouped) + " (ouvrir pour le détail)"
        : formatNumber(total) + " CIDR lisibles";

      heading.append(title, meta, counter);
      const track = document.createElement("div");
      track.className = "overview-track";
      track.setAttribute("role", "group");
      track.setAttribute("aria-label", "Cartes /" + prefix);

      let hidden = 0;
      for (let i = 0; i < bucketCount; i++) {
        const bucket = core.cidrFromStart(parent.start + i * subnetSize, groupingPrefix);
        if (state.hiddenCards.has(hiddenCardKey(prefix, bucket.cidr))) {
          hidden++;
          continue;
        }
        track.append(createOverviewCard(bucket, prefix, aggregated, grouped));
      }
      if (hidden) {
        const hiddenCounter = document.createElement("span");
        hiddenCounter.className = "overview-band-hidden";
        hiddenCounter.textContent = hidden + " bloc(s) masqué(s) dans ce niveau (réaffichage en haut)";
        heading.append(hiddenCounter);
      }
      if (!track.children.length) {
        const empty = document.createElement("p");
        empty.className = "overview-empty";
        empty.textContent = "Tous les blocs de ce niveau sont masqués.";
        track.append(empty);
      }
      band.append(heading, track);
      fragment.append(band);
    }

    if (!visibleBands) {
      const empty = document.createElement("p");
      empty.className = "overview-empty";
      empty.textContent = "Aucun masque affiché. Utilisez « Choisir les masques » pour en réactiver.";
      fragment.append(empty);
    }
    container.append(fragment);
  }

  function createGridCard(net) {
    const node = core.describeNode(net, state.coverage);
    const card = document.createElement("div");
    card.className = "grid-node";
    card.dataset.status = node.status;
    bindHighlight(card, node.cidr);

    const top = document.createElement("div");
    top.className = "grid-top";
    top.append(checkboxFor(node), createBadge(node.status));
    const cidr = document.createElement("div");
    cidr.className = "grid-main";
    cidr.textContent = node.cidr;
    const bottom = document.createElement("div");
    bottom.className = "grid-bottom";
    const description = document.createElement("span");
    description.textContent = node.direct?.name || formatPercent(node.percent) + " occupé";
    const action = nodeAction(node);
    bottom.append(description);
    if (action) bottom.append(action);
    card.append(top, cidr, bottom);
    return card;
  }

  function renderGrid() {
    const container = $("subnetGrid");
    container.replaceChildren();
    if (!state.active) return;
    const data = core.pageOfSubnets(state.active, state.gridPrefix, state.gridPage, PAGE_SIZE);
    $("pageInfo").textContent = "Page " + formatNumber(data.page + 1) + " / " +
      formatNumber(data.totalPages) + " · " + formatNumber(data.count) + " sous-réseaux";
    $("prevPage").disabled = data.page === 0;
    $("nextPage").disabled = data.page >= data.totalPages - 1;
    const fragment = document.createDocumentFragment();
    let count = 0;
    for (const net of data.subnets) {
      const summary = core.describeNode(net, state.coverage);
      if (state.gridFilter !== "all" && summary.status !== state.gridFilter) continue;
      fragment.append(createGridCard(net));
      count++;
    }
    if (!count) {
      const empty = document.createElement("p");
      empty.className = "hint";
      empty.textContent = "Aucun sous-réseau correspondant sur cette page. Essayez un autre filtre ou une autre page.";
      fragment.append(empty);
    }
    container.append(fragment);
    $("gridDescription").textContent = formatNumber(count) +
      " résultat(s) visible(s) sur cette page · 64 sous-réseaux maximum chargés";
  }

  function showMode(view) {
    state.view = view;
    $("overviewView").hidden = view !== "overview";
    $("gridView").hidden = view !== "grid";
    $("overviewMode").setAttribute("aria-pressed", String(view === "overview"));
    $("gridMode").setAttribute("aria-pressed", String(view === "grid"));
    if (view === "overview") renderOverview();
    else renderGrid();
  }

  function renderAllocations() {
    const container = $("allocationsList");
    container.replaceChildren();
    const items = allocations();
    $("clearInventory").disabled = items.length === 0;
    if (!items.length) {
      const empty = document.createElement("div");
      empty.className = "empty-state";
      const heading = document.createElement("strong");
      heading.textContent = "Aucune réservation";
      const text = document.createElement("span");
      text.textContent = "Sélectionnez un bloc libre dans la vue d'ensemble ou la grille détaillée.";
      empty.append(heading, text);
      container.append(empty);
      return;
    }
    const fragment = document.createDocumentFragment();
    for (const item of items) {
      const card = document.createElement("div");
      card.className = "allocation";
      const head = document.createElement("div");
      head.className = "allocation-top";
      const cidr = document.createElement("strong");
      cidr.textContent = item.cidr;
      const buttons = document.createElement("div");
      buttons.className = "actions";
      const locate = document.createElement("button");
      locate.type = "button";
      locate.className = "btn small";
      locate.textContent = "Voir";
      locate.setAttribute("aria-label", "Afficher " + item.cidr + " dans la grille");
      locate.addEventListener("click", () => navigateTo(item.cidr));
      const edit = document.createElement("button");
      edit.type = "button";
      edit.className = "btn small";
      edit.textContent = "Modifier";
      edit.setAttribute("aria-label", "Modifier " + item.cidr);
      edit.addEventListener("click", () => openAllocationDialog(item.cidr));
      buttons.append(locate, edit);
      head.append(cidr, buttons);
      card.append(head);
      if (item.name) {
        const name = document.createElement("div");
        name.className = "allocation-name";
        name.textContent = item.name;
        card.append(name);
      }
      if (item.description) {
        const note = document.createElement("div");
        note.className = "allocation-note";
        note.textContent = item.description;
        card.append(note);
      }
      fragment.append(card);
    }
    container.append(fragment);
  }

  function render() {
    $("summarySection").hidden = !state.active;
    $("workspace").hidden = !state.active;
    if (!state.active) return;
    renderSummary();
    renderAllocations();
    showMode(state.view);
    $("maskSelect").value = String(state.gridPrefix);
  }

  function openAllocationDialog(cidr) {
    if (!state.active) return;
    const node = core.describeNode(cidr, state.coverage);
    if (node.status !== "free" && node.status !== "reserved") {
      say("Ce sous-réseau est déjà couvert : gérez d'abord les réservations existantes.", "warning");
      return;
    }
    state.editing = node.cidr;
    $("dialogCidr").textContent = node.cidr;
    const existing = node.direct;
    $("allocationName").value = existing?.name || "";
    $("allocationDescription").value = existing?.description || "";
    $("dialogTitle").textContent = existing ? "Modifier la réservation" : "Réserver un sous-réseau";
    $("saveAllocation").textContent = existing ? "Enregistrer" : "Réserver";
    $("releaseButton").hidden = !existing;
    $("allocationDialog").showModal();
    $("allocationName").focus();
  }

  function closeDialog() {
    $("allocationDialog").close();
    state.editing = null;
    render();
  }

  function saveAllocation() {
    if (!state.editing || !state.active) return;
    const cidr = state.editing;
    const metadata = {
      name: $("allocationName").value,
      description: $("allocationDescription").value
    };
    try {
      const existing = allocations().some(item => item.cidr === cidr);
      const nextAllocations = existing
        ? core.updateAllocation(allocations(), state.active, cidr, metadata)
        : core.addAllocation(allocations(), state.active, cidr, metadata);
      const next = storage.withAllocations(state.data, state.active.cidr, nextAllocations);
      closeDialog();
      persistData(next);
      say((existing ? "Réservation modifiée : " : "Sous-réseau réservé : ") + cidr, "success");
    } catch (error) {
      say(error && error.message || "Réservation impossible.", "error");
    }
  }

  function releaseAllocation() {
    if (!state.editing || !state.active) return;
    if (!window.confirm("Libérer " + state.editing + " ? Cette action supprime la réservation directe.")) return;
    const cidr = state.editing;
    const nextAllocations = core.removeAllocation(allocations(), state.active, cidr);
    const next = storage.withAllocations(state.data, state.active.cidr, nextAllocations);
    closeDialog();
    persistData(next);
    say("Réservation libérée : " + cidr, "success");
  }

  function clearInventory() {
    if (!state.active || !allocations().length) return;
    if (!window.confirm("Effacer les " + allocations().length + " réservations de " +
        state.active.cidr + " ? Exportez au préalable si nécessaire.")) return;
    persistData(storage.withAllocations(state.data, state.active.cidr, []));
    say("Réservations du réseau effacées.", "success");
  }

  function navigateTo(cidr) {
    if (!state.active) return;
    try {
      const target = core.parseCIDR(cidr);
      const position = core.pageForCidr(state.active, target, PAGE_SIZE);
      state.gridPrefix = position.prefix;
      state.gridPage = position.page;
      state.gridFilter = "all";
      $("statusFilter").value = "all";
      state.selected = target.cidr;
      render();
      showMode("grid");
      const match = [...document.querySelectorAll("[data-net-cidr]")]
        .find(element => !element.closest("[hidden]") && element.dataset.netCidr === target.cidr);
      if (match) {
        match.classList.add("is-target");
        match.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
      say("Sous-réseau " + target.cidr + " localisé.", "success");
    } catch (error) {
      say(error && error.message || "Réseau introuvable.", "error");
    }
  }

  function search() {
    if (!state.active) return;
    const query = $("searchInput").value.trim();
    if (!query) return say("Saisissez une adresse IP, un CIDR ou un nom.", "warning");
    try {
      if (query.includes("/")) {
        navigateTo(core.parseCIDR(query).cidr);
      } else {
        let target = null;
        try { target = core.cidrFromStart(core.parseIPv4(query), 32).cidr; } catch (_) {}
        if (!target) {
          const searchText = query.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
          const found = allocations().find(item =>
            (item.name + " " + item.description).normalize("NFD")
              .replace(/[\u0300-\u036f]/g, "").toLowerCase().includes(searchText));
          if (!found) throw new Error("Aucune réservation ne correspond à cette recherche.");
          target = found.cidr;
        }
        navigateTo(target);
      }
    } catch (error) {
      say(error && error.message || "Recherche invalide.", "error");
    }
  }

  function download(name, content, type) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = name;
    document.body.append(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  async function importJSON() {
    const input = $("importFile");
    const file = input.files && input.files[0];
    if (!file) return;
    try {
      if (file.size > 4 * 1024 * 1024) throw new Error("Fichier trop volumineux (max. 4 Mo).");
      const result = storage.importJSON(await file.text(), state.data);
      const beforeActive = state.active ? state.active.cidr : null;
      const active = beforeActive || result.data.active || Object.keys(result.data.networks)[0] || null;
      if (active) {
        result.data.active = active;
        state.active = core.parseCIDR(active);
        loadViewPreferences();
        state.gridPrefix = Math.min(32, state.active.prefix + 2);
        state.gridPage = 0;
        populateMasks();
        $("networkInput").value = active;
      }
      persistData(result.data);
      say("Import terminé : " + result.importedNetworks + " réseau(x) et " +
        result.importedAllocations + " réservation(s) ajoutés.", "success");
    } catch (error) {
      say("Import impossible : " + (error && error.message || "fichier invalide"), "error");
    } finally {
      input.value = "";
    }
  }

  function bindEvents() {
    $("networkForm").addEventListener("submit", event => {
      event.preventDefault();
      openNetwork($("networkInput").value);
    });
    $("savedNetworks").addEventListener("change", event => {
      if (event.target.value) openNetwork(event.target.value);
    });
    $("networkInput").addEventListener("input", () => {
      if (state.active && $("networkInput").value.trim() !== state.active.cidr) {
        say("Saisie modifiée : cliquez sur Analyser pour charger ce réseau. L'inventaire actif n'a pas changé.", "warning");
      }
    });
    $("overviewMode").addEventListener("click", () => showMode("overview"));
    $("showAllMasks").addEventListener("click", () => {
      setVisibleMasks(Array.from({ length: 33 - state.active.prefix }, (_, i) => state.active.prefix + i));
    });
    $("showDefaultMasks").addEventListener("click", () => {
      setVisibleMasks(defaultVisibleMasks(state.active));
    });
    $("hideAllMasks").addEventListener("click", () => setVisibleMasks([]));
    $("restoreHidden").addEventListener("click", () => {
      state.hiddenCards.clear();
      saveViewPreferences();
      renderMaskFilters();
      renderOverview();
    });
    $("gridMode").addEventListener("click", () => showMode("grid"));
    $("maskSelect").addEventListener("change", () => {
      state.gridPrefix = Number($("maskSelect").value);
      state.gridPage = 0;
      renderGrid();
    });
    $("statusFilter").addEventListener("change", () => {
      state.gridFilter = $("statusFilter").value;
      renderGrid();
    });
    $("prevPage").addEventListener("click", () => {
      if (state.gridPage > 0) { state.gridPage--; renderGrid(); }
    });
    $("nextPage").addEventListener("click", () => {
      const pageCount = core.pageOfSubnets(state.active, state.gridPrefix, state.gridPage, PAGE_SIZE).totalPages;
      if (state.gridPage + 1 < pageCount) { state.gridPage++; renderGrid(); }
    });
    $("searchForm").addEventListener("submit", event => { event.preventDefault(); search(); });
    $("allocationForm").addEventListener("submit", event => { event.preventDefault(); saveAllocation(); });
    $("cancelDialog").addEventListener("click", closeDialog);
    $("releaseButton").addEventListener("click", releaseAllocation);
    $("allocationDialog").addEventListener("close", () => { state.editing = null; render(); });
    $("clearInventory").addEventListener("click", clearInventory);
    $("exportJson").addEventListener("click", () => {
      download("toolbox-inventaires-ipv4.json", storage.exportJSON(state.data), "application/json;charset=utf-8");
    });
    $("exportCsv").addEventListener("click", () => {
      if (!state.active) return;
      download("inventaire-" + state.active.cidr.replace("/", "-") + ".csv",
        core.inventoryCSV(state.active, allocations()), "text/csv;charset=utf-8");
    });
    $("importButton").addEventListener("click", () => $("importFile").click());
    $("importFile").addEventListener("change", () => { void importJSON(); });
  }

  function initialize() {
    initTheme();
    bindEvents();
    const loaded = storage.load();
    state.data = loaded.data;
    state.blocked = Boolean(loaded.blocked);
    state.persistent = loaded.persistent;
    if (loaded.warnings.length) {
      setStorageNotice(loaded.warnings.join(" "), loaded.blocked ? "error" : "warning");
    } else if (!state.persistent) {
      setStorageNotice("Le stockage local est indisponible : les modifications restent temporaires.", "warning");
    }
    if (loaded.data.active) {
      state.active = core.parseCIDR(loaded.data.active);
      $("networkInput").value = state.active.cidr;
      loadViewPreferences();
      state.gridPrefix = Math.min(32, state.active.prefix + 2);
      populateMasks();
    }
    refreshSavedNetworks();
    render();
  }

  if (!core || !storage) {
    say("Le moteur IPv4 ou le module de stockage est indisponible.", "error");
  } else {
    initialize();
  }
})();
