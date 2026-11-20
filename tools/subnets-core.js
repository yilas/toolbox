/*
 * Toolbox — Calculateur IPv4 V2 (moteur pur, sans DOM ni stockage).
 * La couverture est calculée par union d'intervalles : pas de double comptage.
 * Un enfant hérite de la réservation de son ancêtre ; un parent peut être
 * partiellement ou entièrement couvert par des descendants.
 */
(function (root) {
  "use strict";

  const MAX_IPV4 = 0xffffffff;
  const IPV4_SPACE = 2 ** 32;
  const MAX_ALLOCATIONS = 5000;

  function parseIPv4(value) {
    if (typeof value !== "string" || !/^(0|[1-9]\d{0,2})(\.(0|[1-9]\d{0,2})){3}$/.test(value)) {
      throw new Error("Adresse IPv4 invalide : quatre octets décimaux de 0 à 255 sont attendus.");
    }
    const octets = value.split(".").map(Number);
    if (octets.some(octet => octet > 255)) {
      throw new Error("Adresse IPv4 invalide : un octet dépasse 255.");
    }
    return ((octets[0] * 2 ** 24) +
      (octets[1] * 2 ** 16) +
      (octets[2] * 2 ** 8) +
      octets[3]) >>> 0;
  }

  function formatIPv4(value) {
    if (!Number.isInteger(value) || value < 0 || value > MAX_IPV4) {
      throw new RangeError("Entier IPv4 hors limites.");
    }
    return [
      (value >>> 24) & 255,
      (value >>> 16) & 255,
      (value >>> 8) & 255,
      value & 255
    ].join(".");
  }

  function cidrFromStart(address, prefix) {
    if (!Number.isInteger(prefix) || prefix < 0 || prefix > 32) {
      throw new RangeError("Préfixe IPv4 invalide (0 à 32).");
    }
    if (!Number.isInteger(address) || address < 0 || address > MAX_IPV4) {
      throw new RangeError("Adresse réseau invalide.");
    }
    const mask = prefix === 0 ? 0 : (MAX_IPV4 << (32 - prefix)) >>> 0;
    const start = (address & mask) >>> 0;
    const size = 2 ** (32 - prefix);
    const end = start + size - 1;
    return Object.freeze({
      cidr: formatIPv4(start) + "/" + prefix,
      prefix, mask, start, end, size,
      first: formatIPv4(start), last: formatIPv4(end)
    });
  }

  function parseCIDR(input) {
    const source = typeof input === "string" ? input.trim() : "";
    const match = /^((?:\d+\.){3}\d+)\/(0|[1-9]|[12]\d|3[012])$/.exec(source);
    if (!match) {
      throw new Error("Format CIDR invalide : utilisez une IPv4 suivie d'un préfixe /0 à /32.");
    }
    return cidrFromStart(parseIPv4(match[1]), Number(match[2]));
  }

  function contains(parent, child) {
    return parent.start <= child.start && parent.end >= child.end;
  }

  function intersects(first, second) {
    return first.start <= second.end && first.end >= second.start;
  }

  function children(cidr) {
    const net = typeof cidr === "string" ? parseCIDR(cidr) : cidr;
    if (net.prefix === 32) return [];
    const childPrefix = net.prefix + 1;
    const childSize = net.size / 2;
    return [
      cidrFromStart(net.start, childPrefix),
      cidrFromStart(net.start + childSize, childPrefix)
    ];
  }

  function ancestors(cidr, rootCidr) {
    const target = typeof cidr === "string" ? parseCIDR(cidr) : cidr;
    const parent = typeof rootCidr === "string" ? parseCIDR(rootCidr) : rootCidr;
    if (!contains(parent, target) || target.prefix < parent.prefix) {
      throw new Error("Le sous-réseau n'appartient pas au réseau parent.");
    }
    const result = [];
    for (let prefix = parent.prefix; prefix <= target.prefix; prefix++) {
      result.push(cidrFromStart(target.start, prefix));
    }
    return result;
  }

  function sanitizeText(value, maxLength) {
    return typeof value === "string"
      ? value.replace(/\u0000/g, "").trim().slice(0, maxLength) : "";
  }

  function normalizeAllocations(entries, rootCidr) {
    const parent = typeof rootCidr === "string" ? parseCIDR(rootCidr) : rootCidr;
    if (!Array.isArray(entries)) return [];
    if (entries.length > MAX_ALLOCATIONS * 2) {
      throw new Error("Inventaire trop volumineux.");
    }
    const seen = new Set();
    const normalized = [];
    for (const entry of entries) {
      const source = typeof entry === "string" ? { cidr: entry } : entry;
      if (!source || typeof source !== "object" || typeof source.cidr !== "string") continue;
      let item;
      try { item = parseCIDR(source.cidr); } catch (_) { continue; }
      if (!contains(parent, item) || seen.has(item.cidr)) continue;
      seen.add(item.cidr);
      normalized.push({
        cidr: item.cidr,
        name: sanitizeText(source.name, 100),
        description: sanitizeText(source.description, 500),
        createdAt: Number.isFinite(source.createdAt) ? source.createdAt : null,
        updatedAt: Number.isFinite(source.updatedAt) ? source.updatedAt : null
      });
      if (normalized.length > MAX_ALLOCATIONS) {
        throw new Error("Limite d'inventaire atteinte (" + MAX_ALLOCATIONS + " réservations).");
      }
    }
    normalized.sort((a, b) =>
      parseCIDR(a.cidr).start - parseCIDR(b.cidr).start ||
      parseCIDR(a.cidr).prefix - parseCIDR(b.cidr).prefix
    );
    return normalized;
  }

  function prepareCoverage(allocations, rootCidr) {
    const parent = typeof rootCidr === "string" ? parseCIDR(rootCidr) : rootCidr;
    const records = normalizeAllocations(allocations, parent).map(entry => ({
      ...entry, ...parseCIDR(entry.cidr)
    }));
    const merged = [];
    let hasOverlap = false;
    for (const record of records) {
      const previous = merged[merged.length - 1];
      if (previous && record.start <= previous.end + 1) {
        if (record.start <= previous.end) hasOverlap = true;
        previous.end = Math.max(previous.end, record.end);
      } else {
        merged.push({ start: record.start, end: record.end });
      }
    }
    const occupied = merged.reduce((sum, item) => sum + item.end - item.start + 1, 0);
    return { root: parent, records, intervals: merged, occupied, hasOverlap };
  }

  function describeNode(cidr, coverage) {
    const node = typeof cidr === "string" ? parseCIDR(cidr) : cidr;
    if (!contains(coverage.root, node) || node.prefix < coverage.root.prefix) {
      throw new Error("Réseau hors du périmètre de l'inventaire.");
    }
    let occupied = 0;
    for (const interval of coverage.intervals) {
      if (interval.end < node.start) continue;
      if (interval.start > node.end) break;
      occupied += Math.max(0, Math.min(interval.end, node.end) - Math.max(interval.start, node.start) + 1);
    }
    const direct = coverage.records.find(item => item.cidr === node.cidr) || null;
    const ancestor = coverage.records
      .filter(item => item.prefix < node.prefix && contains(item, node))
      .sort((a, b) => b.prefix - a.prefix)[0] || null;
    const contained = coverage.records.filter(item => item.cidr !== node.cidr && intersects(item, node));
    const status = direct ? "reserved"
      : ancestor ? "inherited"
      : occupied === node.size ? "full"
      : occupied > 0 ? "partial"
      : "free";
    return {
      ...node, status, occupied, free: node.size - occupied,
      percent: occupied / node.size * 100,
      direct, inheritedFrom: ancestor ? ancestor.cidr : null,
      overlap: Boolean(direct && contained.length),
      relatedCount: contained.length
    };
  }

  function addAllocation(allocations, network, cidr, metadata = {}, time = Date.now()) {
    const parent = typeof network === "string" ? parseCIDR(network) : network;
    const candidate = typeof cidr === "string" ? parseCIDR(cidr) : cidr;
    if (!contains(parent, candidate) || candidate.prefix < parent.prefix) {
      throw new Error("Cette réservation est en dehors du réseau parent.");
    }
    const previous = normalizeAllocations(allocations, parent);
    if (previous.some(item => intersects(parseCIDR(item.cidr), candidate))) {
      throw new Error("Cette réservation chevauche un réseau déjà réservé. Libérez d'abord le réseau concerné.");
    }
    if (previous.length >= MAX_ALLOCATIONS) throw new Error("Limite de réservations atteinte.");
    return normalizeAllocations([...previous, {
      cidr: candidate.cidr,
      name: sanitizeText(metadata.name, 100),
      description: sanitizeText(metadata.description, 500),
      createdAt: time, updatedAt: time
    }], parent);
  }

  function updateAllocation(allocations, network, cidr, metadata = {}, time = Date.now()) {
    const parent = typeof network === "string" ? parseCIDR(network) : network;
    const candidate = typeof cidr === "string" ? parseCIDR(cidr) : cidr;
    const existing = normalizeAllocations(allocations, parent);
    if (!existing.some(item => item.cidr === candidate.cidr)) throw new Error("Réservation introuvable.");
    return existing.map(item => item.cidr === candidate.cidr ? {
      ...item, name: sanitizeText(metadata.name, 100),
      description: sanitizeText(metadata.description, 500), updatedAt: time
    } : item);
  }

  function removeAllocation(allocations, network, cidr) {
    const parent = typeof network === "string" ? parseCIDR(network) : network;
    const candidate = typeof cidr === "string" ? parseCIDR(cidr) : cidr;
    return normalizeAllocations(allocations, parent).filter(item => item.cidr !== candidate.cidr);
  }

  function pageOfSubnets(rootCidr, prefix, page = 0, limit = 64) {
    const rootNet = typeof rootCidr === "string" ? parseCIDR(rootCidr) : rootCidr;
    if (!Number.isInteger(prefix) || prefix < rootNet.prefix || prefix > 32) {
      throw new Error("Préfixe demandé en dehors du réseau.");
    }
    if (!Number.isInteger(limit) || limit < 1 || limit > 256) throw new Error("Taille de page invalide.");
    const count = 2 ** (prefix - rootNet.prefix);
    const totalPages = Math.ceil(count / limit);
    if (!Number.isInteger(page) || page < 0 || page >= totalPages) throw new Error("Page invalide.");
    const subnetSize = 2 ** (32 - prefix);
    const offset = page * limit;
    const subnets = [];
    for (let index = offset; index < Math.min(count, offset + limit); index++) {
      subnets.push(cidrFromStart(rootNet.start + index * subnetSize, prefix));
    }
    return { prefix, count, page, pageSize: limit, totalPages, subnets };
  }

  function pageForCidr(rootCidr, cidr, pageSize = 64) {
    const parent = typeof rootCidr === "string" ? parseCIDR(rootCidr) : rootCidr;
    const node = typeof cidr === "string" ? parseCIDR(cidr) : cidr;
    if (!contains(parent, node) || node.prefix < parent.prefix) {
      throw new Error("Ce réseau n'appartient pas au réseau parent.");
    }
    const index = (node.start - parent.start) / node.size;
    return { prefix: node.prefix, page: Math.floor(index / pageSize), index };
  }

  function csvEscape(value) {
    let text = String(value == null ? "" : value);
    if (/^[=+\-@\t\r\n]/.test(text)) text = "'" + text;
    return '"' + text.replace(/"/g, '""') + '"';
  }

  function inventoryCSV(network, allocations) {
    const parent = typeof network === "string" ? parseCIDR(network) : network;
    const rows = [["Réseau parent", "CIDR", "Nom", "Description", "Première IP", "Dernière IP", "Nombre d'IP", "Date modification"]];
    for (const item of normalizeAllocations(allocations, parent)) {
      const range = parseCIDR(item.cidr);
      rows.push([parent.cidr, range.cidr, item.name, item.description, range.first, range.last, String(range.size),
        item.updatedAt == null ? "" : new Date(item.updatedAt).toISOString()]);
    }
    return "\ufeff" + rows.map(row => row.map(csvEscape).join(";")).join("\r\n") + "\r\n";
  }

  root.SubnetsCore = Object.freeze({
    IPV4_SPACE, parseIPv4, formatIPv4, cidrFromStart, parseCIDR,
    contains, intersects, children, ancestors,
    normalizeAllocations, prepareCoverage, describeNode,
    addAllocation, updateAllocation, removeAllocation,
    pageOfSubnets, pageForCidr, inventoryCSV
  });
})(window);
