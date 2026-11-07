/*
 * Persistance des inventaires IPv4.
 * L'ancien format snet_calc_inventory_* est importé une seule fois.
 * Les clés historiques restent intactes : sauvegarde de récupération.
 */
(function (root) {
  "use strict";

  const core = root.SubnetsCore;
  const STORAGE_KEY = "toolbox-subnets-v2";
  const LEGACY_PREFIX = "snet_calc_inventory_";
  const LEGACY_ACTIVE = "snet_calc_last_active_network";
  const MAX_NETWORKS = 500;
  const MAX_JSON = 4 * 1024 * 1024;

  function emptyState() {
    return { version: 2, active: null, networks: {} };
  }

  function normalizeState(raw) {
    if (!raw || typeof raw !== "object" || raw.version !== 2 ||
        !raw.networks || typeof raw.networks !== "object" || Array.isArray(raw.networks)) {
      throw new Error("Fichier invalide : format d'inventaire V2 attendu.");
    }
    const data = emptyState();
    const networks = Object.entries(raw.networks);
    if (networks.length > MAX_NETWORKS) throw new Error("Trop de réseaux dans le fichier.");
    for (const [rawCidr, record] of networks) {
      let parent;
      try { parent = core.parseCIDR(rawCidr); } catch (_) { continue; }
      const entries = record && Array.isArray(record.allocations) ? record.allocations : [];
      const normalized = core.normalizeAllocations(entries, parent);
      if (!data.networks[parent.cidr]) {
        data.networks[parent.cidr] = { allocations: [], updatedAt: null };
      }
      const target = data.networks[parent.cidr];
      target.allocations = core.normalizeAllocations([...target.allocations, ...normalized], parent);
      target.updatedAt = Number.isFinite(record && record.updatedAt) ? record.updatedAt : target.updatedAt;
    }
    try {
      const active = core.parseCIDR(raw.active);
      if (data.networks[active.cidr]) data.active = active.cidr;
    } catch (_) {}
    return data;
  }

  function getStorage(storage) {
    if (storage !== undefined) return storage;
    try { return root.localStorage || null; } catch (_) { return null; }
  }

  function readValue(storage, key) {
    try { return storage ? storage.getItem(key) : null; } catch (_) { return null; }
  }

  function scanLegacy(storage) {
    const data = emptyState();
    const warnings = [];
    const keys = [];
    if (!storage) return { data, legacyCount: 0, warnings };
    try {
      for (let i = 0; i < storage.length; i++) {
        const key = storage.key(i);
        if (typeof key === "string" && key.startsWith(LEGACY_PREFIX)) keys.push(key);
      }
    } catch (_) {
      warnings.push("Impossible de lire les anciens inventaires.");
      return { data, legacyCount: 0, warnings };
    }
    for (const key of keys) {
      const rawCidr = key.slice(LEGACY_PREFIX.length);
      let parent;
      try { parent = core.parseCIDR(rawCidr); }
      catch (_) {
        warnings.push("Réseau ancien ignoré (CIDR invalide) : " + rawCidr.slice(0, 100));
        continue;
      }
      let object;
      try {
        const raw = readValue(storage, key);
        if (!raw || raw.length > MAX_JSON) throw new Error("Objet absent ou trop grand");
        object = JSON.parse(raw);
        if (!object || typeof object !== "object" || Array.isArray(object)) throw new Error("Format invalide");
      } catch (_) {
        warnings.push("Inventaire ancien illisible : " + rawCidr.slice(0, 100));
        continue;
      }
      const selected = [];
      for (const [cidr, value] of Object.entries(object)) {
        if (value === "taken") selected.push({ cidr });
      }
      if (!data.networks[parent.cidr]) {
        data.networks[parent.cidr] = { allocations: [], updatedAt: null };
      }
      const current = data.networks[parent.cidr];
      try {
        current.allocations = core.normalizeAllocations([...current.allocations, ...selected], parent);
      } catch (_) {
        warnings.push("Inventaire ancien trop volumineux : " + parent.cidr);
      }
    }
    const priorActive = readValue(storage, LEGACY_ACTIVE);
    if (priorActive) {
      try {
        const canonical = core.parseCIDR(priorActive).cidr;
        if (!data.networks[canonical]) {
          data.networks[canonical] = { allocations: [], updatedAt: null };
        }
        data.active = canonical;
      } catch (_) {}
    }
    if (!data.active) data.active = Object.keys(data.networks)[0] || null;
    return { data, legacyCount: keys.length, warnings };
  }

  function save(data, storage) {
    const source = getStorage(storage);
    if (!source) return { ok: false, error: "Stockage local indisponible : session temporaire." };
    try {
      const normalized = normalizeState(data);
      source.setItem(STORAGE_KEY, JSON.stringify(normalized));
      return { ok: true };
    } catch (error) {
      return { ok: false, error: "Sauvegarde impossible : " + (error && error.message || "stockage indisponible") };
    }
  }

  function load(storage) {
    const source = getStorage(storage);
    const warnings = [];
    const existing = readValue(source, STORAGE_KEY);
    if (existing !== null) {
      try {
        if (existing.length > MAX_JSON) throw new Error("Données trop volumineuses");
        return { data: normalizeState(JSON.parse(existing)), migrated: false, warnings, persistent: true };
      } catch (_) {
        warnings.push("Données V2 illisibles : sauvegardez vos anciennes données avant de les remplacer.");
        // Ne pas écraser la clé V2 corrompue automatiquement.
        return { data: emptyState(), migrated: false, warnings, persistent: false, blocked: true };
      }
    }

    const legacy = scanLegacy(source);
    warnings.push(...legacy.warnings);
    if (legacy.legacyCount === 0) {
      return { data: legacy.data, migrated: false, warnings, persistent: Boolean(source) };
    }
    const saved = save(legacy.data, source);
    if (!saved.ok) warnings.push(saved.error);
    else warnings.push("Inventaires anciens copiés dans la V2. Les clés historiques sont conservées.");
    return {
      data: legacy.data, migrated: true,
      legacyCount: legacy.legacyCount, warnings,
      persistent: saved.ok
    };
  }

  function withNetwork(data, inputCidr) {
    const network = core.parseCIDR(inputCidr);
    const next = normalizeState(data);
    if (!next.networks[network.cidr]) {
      if (Object.keys(next.networks).length >= MAX_NETWORKS) throw new Error("Trop de réseaux enregistrés.");
      next.networks[network.cidr] = { allocations: [], updatedAt: Date.now() };
    }
    next.active = network.cidr;
    return next;
  }

  function withAllocations(data, network, allocations, time = Date.now()) {
    const parent = core.parseCIDR(network);
    const next = withNetwork(data, parent.cidr);
    next.networks[parent.cidr] = {
      allocations: core.normalizeAllocations(allocations, parent), updatedAt: time
    };
    return next;
  }

  function exportJSON(data) {
    const normalized = normalizeState(data);
    return JSON.stringify({
      ...normalized,
      exportedAt: new Date().toISOString(),
      format: "Toolbox IPv4 inventory"
    }, null, 2) + "\n";
  }

  function importJSON(text, existingData) {
    if (typeof text !== "string" || text.length > MAX_JSON) {
      throw new Error("Le fichier JSON dépasse 4 Mo.");
    }
    let raw;
    try { raw = JSON.parse(text); }
    catch (_) { throw new Error("JSON invalide."); }
    const imported = normalizeState(raw);
    const base = normalizeState(existingData);
    let importedAllocations = 0;
    let importedNetworks = 0;
    for (const [cidr, record] of Object.entries(imported.networks)) {
      if (!base.networks[cidr]) {
        if (Object.keys(base.networks).length >= MAX_NETWORKS) {
          throw new Error("Trop de réseaux après l'import.");
        }
        base.networks[cidr] = { allocations: [], updatedAt: null };
        importedNetworks++;
      }
      const current = base.networks[cidr];
      const seen = new Set(current.allocations.map(item => item.cidr));
      const additions = record.allocations.filter(item => !seen.has(item.cidr));
      current.allocations = core.normalizeAllocations([...current.allocations, ...additions], cidr);
      importedAllocations += additions.length;
      current.updatedAt = Date.now();
    }
    if (!base.active && imported.active) base.active = imported.active;
    return { data: base, importedNetworks, importedAllocations };
  }

  root.SubnetsStorage = Object.freeze({
    STORAGE_KEY, LEGACY_PREFIX, emptyState, normalizeState,
    scanLegacy, load, save, withNetwork, withAllocations,
    exportJSON, importJSON
  });
})(window);
