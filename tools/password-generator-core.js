/*
 * Password Studio — moteur indépendant de l'interface.
 * API Web Crypto native, sans Math.random(), sans ressource réseau.
 * Les tirages conditionnels sont uniformes grâce au rejet.
 */
(function (root) {
  "use strict";

  const UINT32_RANGE = 2 ** 32;
  const CHARACTERS = Object.freeze({
    upper: "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    lower: "abcdefghijklmnopqrstuvwxyz",
    digits: "0123456789",
    symbols: "!@#$%^&*()-_=+[]{}|;:,.<>?/"
  });
  const AMBIGUOUS = new Set(["0", "O", "1", "l", "I", "|"]);
  const CATEGORIES = Object.keys(CHARACTERS);

  function randomInt(max) {
    if (!Number.isSafeInteger(max) || max < 1 || max > UINT32_RANGE) {
      throw new RangeError("Borne aléatoire invalide.");
    }
    if (!root.crypto || typeof root.crypto.getRandomValues !== "function") {
      throw new Error("La source cryptographique du navigateur est indisponible.");
    }

    const limit = Math.floor(UINT32_RANGE / max) * max;
    const values = new Uint32Array(1);
    let value;
    do {
      root.crypto.getRandomValues(values);
      value = values[0];
    } while (value >= limit);
    return value % max;
  }

  function selectedGroups(options) {
    return CATEGORIES
      .filter(id => Boolean(options[id]))
      .map(id => {
        const chars = options.excludeAmbiguous
          ? [...CHARACTERS[id]].filter(char => !AMBIGUOUS.has(char)).join("")
          : CHARACTERS[id];
        return { id, chars };
      });
  }

  function log2BigInt(value) {
    if (value <= 0n) throw new RangeError("Aucune combinaison valide.");
    const bits = value.toString(2).length;
    const shift = Math.max(0, bits - 52);
    const significant = Number(value >> BigInt(shift));
    return Math.log2(significant) + shift;
  }

  // Inclusion-exclusion : nombre exact de chaînes contenant chaque
  // catégorie requise, pour une distribution conditionnelle uniforme.
  function countValidPasswords(length, groupSizes) {
    const totalSize = groupSizes.reduce((sum, size) => sum + size, 0);
    let valid = 0n;
    for (let mask = 0; mask < (1 << groupSizes.length); mask++) {
      let remaining = totalSize;
      let parity = 0;
      for (let i = 0; i < groupSizes.length; i++) {
        if (mask & (1 << i)) {
          remaining -= groupSizes[i];
          parity++;
        }
      }
      const combinations = BigInt(remaining) ** BigInt(length);
      valid += parity % 2 ? -combinations : combinations;
    }
    return valid;
  }

  function generatePassword(options) {
    const length = Number(options.length);
    if (!Number.isInteger(length) || length < 4 || length > 128) {
      throw new RangeError("La longueur doit être comprise entre 4 et 128.");
    }
    const groups = selectedGroups(options);
    if (groups.length === 0) {
      throw new Error("Sélectionnez au moins une catégorie de caractères.");
    }
    const guarantee = Boolean(options.guarantee);
    if (guarantee && length < groups.length) {
      throw new Error("La longueur est insuffisante pour garantir chaque catégorie.");
    }

    const alphabet = groups.map(group => group.chars).join("");
    if (!alphabet) throw new Error("L'alphabet est vide.");

    let value;
    // Le rejet conserve l'uniformité parmi les mots de passe respectant
    // les contraintes, contrairement à un tirage imposé suivi d'un mélange.
    do {
      const chars = [];
      for (let i = 0; i < length; i++) {
        chars.push(alphabet[randomInt(alphabet.length)]);
      }
      value = chars.join("");
    } while (guarantee && groups.some(group => ![...value].some(char => group.chars.includes(char))));

    const entropy = guarantee
      ? log2BigInt(countValidPasswords(length, groups.map(group => group.chars.length)))
      : length * Math.log2(alphabet.length);

    return { mode: "password", value, entropy, alphabetSize: alphabet.length };
  }

  function generatePassphrase(options) {
    const count = Number(options.words);
    if (!Number.isInteger(count) || count < 4 || count > 12) {
      throw new RangeError("Le nombre de mots doit être compris entre 4 et 12.");
    }

    const language = options.language === "en" ? "en" : "fr";
    const list = root.PASSWORD_WORDLISTS && root.PASSWORD_WORDLISTS[language];
    if (!Array.isArray(list) || list.length !== 2048 || new Set(list).size !== 2048) {
      throw new Error("Le dictionnaire hors ligne est incomplet.");
    }

    const separator = ["-", " ", "."].includes(options.separator) ? options.separator : "-";
    const chosen = [];
    for (let i = 0; i < count; i++) {
      let word = list[randomInt(list.length)];
      if (options.capitalize) {
        word = word.charAt(0).toLocaleUpperCase(language) + word.slice(1);
      }
      chosen.push(word);
    }

    let value = chosen.join(separator);
    let entropy = count * Math.log2(list.length);
    if (options.appendDigit) {
      value += String(randomInt(10));
      entropy += Math.log2(10);
    }
    return { mode: "passphrase", value, entropy, words: count, language };
  }

  function strengthForEntropy(entropy) {
    if (entropy < 40) return { label: "Faible", level: 1, tone: "weak" };
    if (entropy < 60) return { label: "Modérée", level: 2, tone: "medium" };
    if (entropy < 80) return { label: "Solide", level: 3, tone: "good" };
    return { label: "Excellente", level: 4, tone: "excellent" };
  }

  root.PasswordCore = Object.freeze({
    randomInt,
    generatePassword,
    generatePassphrase,
    strengthForEntropy,
    countValidPasswords
  });
})(window);
