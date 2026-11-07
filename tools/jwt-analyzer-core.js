/*
 * Analyseur JWT — moteur de décodage, d'évaluation et de vérification JWS.
 * Dépend uniquement de Web Crypto et des API JavaScript natives.
 * Un JWT décodé ne doit jamais être considéré comme authentifié.
 */
(function (root) {
  "use strict";

  const MAX_TOKEN_LENGTH = 200000;
  const MAX_KEY_LENGTH = 100000;
  const encoder = new TextEncoder();
  const utf8 = new TextDecoder("utf-8", { fatal: true });

  const ALGORITHMS = Object.freeze({
    HS256: { family: "HMAC", hash: "SHA-256" },
    HS384: { family: "HMAC", hash: "SHA-384" },
    HS512: { family: "HMAC", hash: "SHA-512" },
    RS256: { family: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    RS384: { family: "RSASSA-PKCS1-v1_5", hash: "SHA-384" },
    RS512: { family: "RSASSA-PKCS1-v1_5", hash: "SHA-512" },
    PS256: { family: "RSA-PSS", hash: "SHA-256", saltLength: 32 },
    PS384: { family: "RSA-PSS", hash: "SHA-384", saltLength: 48 },
    PS512: { family: "RSA-PSS", hash: "SHA-512", saltLength: 64 },
    ES256: { family: "ECDSA", hash: "SHA-256", namedCurve: "P-256", size: 64 },
    ES384: { family: "ECDSA", hash: "SHA-384", namedCurve: "P-384", size: 96 },
    ES512: { family: "ECDSA", hash: "SHA-512", namedCurve: "P-521", size: 132 }
  });

  function object(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }

  function encodeBase64Url(bytes) {
    let text = "";
    for (let offset = 0; offset < bytes.length; offset += 8192) {
      text += String.fromCharCode(...bytes.subarray(offset, offset + 8192));
    }
    return btoa(text).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  }

  function decodeBase64Url(value, allowEmpty = false) {
    if (typeof value !== "string" ||
        (!allowEmpty && value.length === 0) ||
        value.length > MAX_TOKEN_LENGTH ||
        (value && (!/^[A-Za-z0-9_-]+$/.test(value) || value.length % 4 === 1))) {
      throw new Error("Segment Base64URL invalide.");
    }
    if (!value) return new Uint8Array(0);

    let decoded;
    try {
      const padded = value.replace(/-/g, "+").replace(/_/g, "/")
        .padEnd(Math.ceil(value.length / 4) * 4, "=");
      const binary = atob(padded);
      decoded = Uint8Array.from(binary, char => char.charCodeAt(0));
    } catch (_) {
      throw new Error("Segment Base64URL invalide.");
    }

    if (encodeBase64Url(decoded) !== value) {
      throw new Error("Segment Base64URL non canonique.");
    }
    return decoded;
  }

  function parseJsonSegment(segment, name) {
    try {
      const parsed = JSON.parse(utf8.decode(decodeBase64Url(segment)));
      if (!object(parsed)) throw new Error("JSON objet attendu.");
      return parsed;
    } catch (error) {
      throw new Error(name + " : " + error.message);
    }
  }

  function parseJwt(input) {
    if (typeof input !== "string") throw new Error("JWT absent.");
    const token = input.trim().replace(/^Bearer\s+/i, "").trim();
    if (!token) throw new Error("Collez un JWT à analyser.");
    if (token.length > MAX_TOKEN_LENGTH) {
      throw new Error("JWT trop volumineux (limite : 200 000 caractères).");
    }

    const segments = token.split(".");
    if (segments.length === 5) {
      throw new Error("Ce jeton comporte 5 segments (JWE chiffré) : le déchiffrement n'est pas pris en charge.");
    }
    if (segments.length !== 3) {
      throw new Error("Un JWT signé compact doit contenir exactement 3 segments.");
    }

    const [encodedHeader, encodedPayload, encodedSignature] = segments;
    const header = parseJsonSegment(encodedHeader, "En-tête");
    if (header.b64 === false) {
      throw new Error("Le mode JWS b64=false (payload non encodé) n'est pas pris en charge.");
    }
    const payload = parseJsonSegment(encodedPayload, "Payload");
    if (typeof header.alg !== "string" || !header.alg) {
      throw new Error("Le champ alg de l'en-tête doit être une chaîne non vide.");
    }

    const signature = decodeBase64Url(encodedSignature, true);
    if (header.alg !== "none" && !signature.length) {
      // Le décodage diagnostique reste possible ; la signature sera signalée invalide.
    }

    return {
      token, header, payload, signature,
      signingInput: encoder.encode(encodedHeader + "." + encodedPayload)
    };
  }

  function expectedRules(input = {}) {
    const audience = Array.isArray(input.aud)
      ? input.aud
      : typeof input.aud === "string" ? input.aud.split(",") : [];
    const aud = [...new Set(audience.map(value => String(value).trim()).filter(Boolean))];
    const skew = Number(input.skew);
    return {
      iss: typeof input.iss === "string" ? input.iss.trim() : "",
      aud,
      audMode: input.audMode === "all" ? "all" : "any",
      azp: typeof input.azp === "string" ? input.azp.trim() : "",
      alg: typeof input.alg === "string" && Object.hasOwn(ALGORITHMS, input.alg) ? input.alg : "",
      skew: Number.isInteger(skew) && skew >= 0 && skew <= 300 ? skew : 0
    };
  }

  function isNumericDate(value) {
    return typeof value === "number" && Number.isFinite(value);
  }

  function describeDate(value) {
    if (!isNumericDate(value) || !Number.isFinite(value * 1000) ||
        Math.abs(value) > 8640000000000) return "date hors plage";
    try {
      return new Date(value * 1000).toLocaleString("fr-FR", {
        year: "numeric", month: "short", day: "2-digit",
        hour: "2-digit", minute: "2-digit", second: "2-digit"
      });
    } catch (_) {
      return "date hors plage";
    }
  }

  function checkClaims(header, payload, inputRules = {}, now = Math.floor(Date.now() / 1000)) {
    const rules = expectedRules(inputRules);
    const checks = [];
    const add = (name, state, detail, group = "claims") =>
      checks.push({ name, state, detail, group });

    if (header.alg === "none") {
      add("Algorithme", "error", "alg=none : le jeton n'est pas signé.", "header");
    } else if (!Object.hasOwn(ALGORITHMS, header.alg)) {
      add("Algorithme", "warning", header.alg + " : vérification non prise en charge.", "header");
    } else {
      add("Algorithme", "info", header.alg + " — indiqué par l'en-tête non fiable.", "header");
    }

    if (rules.alg) {
      add("Algorithme attendu", header.alg === rules.alg ? "ok" : "error",
        header.alg === rules.alg
          ? "Correspond à " + rules.alg
          : "Attendu : " + rules.alg + " · Reçu : " + header.alg,
        "rules");
    }

    if (header.crit !== undefined) {
      add("En-tête critique", "error",
        "Paramètre crit non interprété : refus de la vérification cryptographique.",
        "header");
    }
    if (header.typ !== undefined) {
      add("Type déclaré", typeof header.typ === "string" ? "info" : "error",
        "typ=" + String(header.typ) + " (à interpréter selon le profil d'application).", "header");
    }
    if (header.kid !== undefined) {
      add("Identifiant de clé", typeof header.kid === "string" ? "info" : "error",
        "kid=" + String(header.kid) + " — aucune clé téléchargée automatiquement.", "header");
    }
    if (header.jku !== undefined || header.x5u !== undefined) {
      add("Références de clé", "warning",
        "jku/x5u présents : leurs URL ne sont jamais consultées.", "header");
    }

    for (const field of ["iss", "sub", "jti"]) {
      if (payload[field] !== undefined && typeof payload[field] !== "string") {
        add(field, "error", "Type invalide : une chaîne est attendue.");
      } else if (payload[field] !== undefined) {
        add(field, "info", String(payload[field]));
      } else if (field === "iss" || field === "sub") {
        add(field, "info", "Non renseigné (claim optionnel selon le profil).");
      }
    }

    if (payload.aud !== undefined) {
      const validAudience = typeof payload.aud === "string" ||
        (Array.isArray(payload.aud) && payload.aud.length > 0 &&
          payload.aud.every(value => typeof value === "string" && value.length));
      add("aud (format)", validAudience ? "info" : "error",
        validAudience
          ? (Array.isArray(payload.aud) ? payload.aud.join(", ") : payload.aud)
          : "Format invalide : chaîne ou tableau non vide de chaînes attendu.");
    }

    for (const claim of ["exp", "nbf", "iat"]) {
      const value = payload[claim];
      if (value === undefined) {
        if (claim === "exp") add("Expiration", "warning", "Claim exp absent : durée de validité non précisée.");
        continue;
      }
      if (!isNumericDate(value)) {
        add(claim, "error", "NumericDate invalide : un nombre est attendu.");
        continue;
      }
      const pretty = describeDate(value) + " (UTC converti en heure locale)";
      if (claim === "exp") {
        add("Expiration", value <= now - rules.skew ? "error" : "ok",
          pretty + (value <= now - rules.skew ? " — expiré." : " — non expiré."));
      } else if (claim === "nbf") {
        add("Not before", value > now + rules.skew ? "error" : "ok",
          pretty + (value > now + rules.skew ? " — pas encore utilisable." : ""));
      } else {
        add("Issued at", value > now + rules.skew ? "warning" : "info",
          pretty + (value > now + rules.skew ? " — dans le futur." : ""));
      }
    }

    if (isNumericDate(payload.nbf) && isNumericDate(payload.exp) &&
        payload.nbf > payload.exp) {
      add("Cohérence nbf / exp", "error", "nbf est supérieur à exp.");
    }
    if (isNumericDate(payload.iat) && isNumericDate(payload.exp) &&
        payload.iat > payload.exp) {
      add("Cohérence iat / exp", "error", "iat est supérieur à exp.");
    }

    if (rules.iss) {
      add("Issuer attendu", payload.iss === rules.iss ? "ok" : "error",
        payload.iss === rules.iss
          ? "Correspond à " + rules.iss
          : "Attendu : " + rules.iss + " · Reçu : " + String(payload.iss ?? "absent"),
        "rules");
    }
    if (rules.azp) {
      add("Authorized party", payload.azp === rules.azp ? "ok" : "error",
        payload.azp === rules.azp
          ? "Correspond à " + rules.azp
          : "Attendu : " + rules.azp + " · Reçu : " + String(payload.azp ?? "absent"),
        "rules");
    }
    if (rules.aud.length) {
      const claimAud = typeof payload.aud === "string" ? [payload.aud]
        : Array.isArray(payload.aud) && payload.aud.every(value => typeof value === "string")
          ? payload.aud : [];
      const matches = rules.aud.filter(item => claimAud.includes(item));
      const valid = rules.audMode === "all"
        ? matches.length === rules.aud.length
        : matches.length > 0;
      add("Audience attendue", valid ? "ok" : "error",
        valid
          ? "Correspondance (" + (rules.audMode === "all" ? "toutes" : "au moins une") + ") : " + matches.join(", ")
          : "Attendue : " + rules.aud.join(", ") + " · Reçue : " + (claimAud.join(", ") || "absente"),
        "rules");
    }

    return {
      rules, checks,
      errors: checks.filter(item => item.state === "error").length,
      warnings: checks.filter(item => item.state === "warning").length
    };
  }

  function pemToSpki(input) {
    if (typeof input !== "string" || input.length > MAX_KEY_LENGTH) {
      throw new Error("Clé publique PEM absente ou trop volumineuse.");
    }
    const trimmed = input.trim();
    const match = trimmed.match(/^-----BEGIN PUBLIC KEY-----\s*([A-Za-z0-9+\/=\s]+?)\s*-----END PUBLIC KEY-----$/);
    if (!match) {
      throw new Error("Format PEM attendu : BEGIN PUBLIC KEY (SPKI), pas un certificat ni une clé privée.");
    }
    const b64 = match[1].replace(/\s+/g, "");
    const decoded = atob(b64);
    const result = new Uint8Array(decoded.length);
    for (let i = 0; i < decoded.length; i++) result[i] = decoded.charCodeAt(i);
    return result;
  }

  function keyImportParams(algorithm) {
    return algorithm.family === "ECDSA"
      ? { name: "ECDSA", namedCurve: algorithm.namedCurve }
      : { name: algorithm.family, hash: algorithm.hash };
  }

  async function importPublicKey(source, alg, algorithm, subtle) {
    if (typeof source !== "string" || !source.trim() || source.length > MAX_KEY_LENGTH) {
      throw new Error("Fournissez une clé publique PEM ou JWK.");
    }
    const input = source.trim();
    const params = keyImportParams(algorithm);

    if (input.startsWith("{")) {
      const jwk = JSON.parse(input);
      if (!object(jwk) || Object.hasOwn(jwk, "d") || Object.hasOwn(jwk, "p") ||
          Object.hasOwn(jwk, "q") || Object.hasOwn(jwk, "k")) {
        throw new Error("Une clé JWK publique est nécessaire ; les clés privées ne sont pas acceptées.");
      }
      const expectedType = algorithm.family === "ECDSA" ? "EC" : "RSA";
      if (jwk.kty !== expectedType) throw new Error("Type JWK incompatible avec " + alg + ".");
      if (jwk.alg && jwk.alg !== alg) throw new Error("L'algorithme déclaré par la JWK ne correspond pas au JWT.");
      if (jwk.use && jwk.use !== "sig") throw new Error("JWK non destinée aux signatures.");
      if (jwk.key_ops && (!Array.isArray(jwk.key_ops) || !jwk.key_ops.includes("verify"))) {
        throw new Error("JWK non autorisée pour la vérification.");
      }
      if (expectedType === "EC" && jwk.crv !== algorithm.namedCurve) {
        throw new Error("Courbe JWK incompatible avec " + alg + ".");
      }
      return subtle.importKey("jwk", jwk, params, false, ["verify"]);
    }
    return subtle.importKey("spki", pemToSpki(input), params, false, ["verify"]);
  }

  async function verifySignature(parsed, options = {}) {
    const alg = parsed.header.alg;
    if (alg === "none") {
      return { state: "blocked", label: "Non signée", detail: "alg=none : aucune authentification possible." };
    }
    if (parsed.header.crit !== undefined) {
      return { state: "blocked", label: "Non vérifiée", detail: "Paramètre critique crit non pris en charge." };
    }
    if (!Object.hasOwn(ALGORITHMS, alg)) {
      return { state: "unsupported", label: "Non prise en charge", detail: "Algorithme " + alg + " non pris en charge." };
    }
    if (options.expectedAlg && options.expectedAlg !== alg) {
      return { state: "blocked", label: "Algorithme refusé", detail: "Attendu : " + options.expectedAlg + " ; reçu : " + alg + "." };
    }
    if (!parsed.signature || parsed.signature.length === 0) {
      return { state: "invalid", label: "Signature invalide", detail: "Signature absente." };
    }

    const algorithm = ALGORITHMS[alg];
    const subtle = root.crypto && root.crypto.subtle;
    if (!subtle) {
      return { state: "unavailable", label: "Non vérifiée", detail: "Web Crypto indisponible (utilisez HTTPS ou localhost)." };
    }

    const isHmac = algorithm.family === "HMAC";
    const secret = typeof options.secret === "string" ? options.secret : "";
    const publicKey = typeof options.publicKey === "string" ? options.publicKey : "";
    if (isHmac && !secret) {
      return { state: "missing", label: "Non vérifiée", detail: "Saisissez un secret HMAC pour vérifier " + alg + "." };
    }
    if (!isHmac && !publicKey.trim()) {
      return { state: "missing", label: "Non vérifiée", detail: "Fournissez une clé publique PEM ou JWK pour vérifier " + alg + "." };
    }

    try {
      let key;
      let params;
      if (isHmac) {
        key = await subtle.importKey("raw", encoder.encode(secret),
          { name: "HMAC", hash: algorithm.hash }, false, ["verify"]);
        params = { name: "HMAC" };
      } else {
        key = await importPublicKey(publicKey, alg, algorithm, subtle);
        params = algorithm.family === "RSA-PSS"
          ? { name: "RSA-PSS", saltLength: algorithm.saltLength }
          : algorithm.family === "ECDSA"
            ? { name: "ECDSA", hash: algorithm.hash }
            : { name: "RSASSA-PKCS1-v1_5" };
      }
      if (algorithm.family === "ECDSA" && parsed.signature.length !== algorithm.size) {
        return { state: "invalid", label: "Signature invalide", detail: "Longueur de signature ECDSA incorrecte." };
      }
      const verified = await subtle.verify(params, key, parsed.signature, parsed.signingInput);
      return verified
        ? { state: "verified", label: "Signature vérifiée", detail: "Vérification cryptographique réussie (" + alg + ")." }
        : { state: "invalid", label: "Signature invalide", detail: "La signature ne correspond pas à la clé fournie." };
    } catch (error) {
      return {
        state: "error", label: "Vérification impossible",
        detail: "Clé ou opération cryptographique invalide (" + (error && error.message || "erreur inconnue") + ")."
      };
    }
  }

  function timeline(payload, now = Math.floor(Date.now() / 1000)) {
    const points = [payload.iat, payload.nbf, payload.exp].filter(isNumericDate);
    if (!points.length) return null;
    const start = Math.min(...points, now);
    const end = Math.max(...points, now);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return null;
    const percent = value => Math.max(0, Math.min(100, ((value - start) / (end - start)) * 100));
    return {
      start, end, now,
      nbf: isNumericDate(payload.nbf) ? percent(payload.nbf) : null,
      iat: isNumericDate(payload.iat) ? percent(payload.iat) : null,
      exp: isNumericDate(payload.exp) ? percent(payload.exp) : null,
      current: percent(now)
    };
  }

  root.JWTCore = Object.freeze({
    ALGORITHMS,
    encodeBase64Url, decodeBase64Url, parseJwt,
    expectedRules, isNumericDate, describeDate,
    checkClaims, verifySignature, timeline
  });
})(window);
