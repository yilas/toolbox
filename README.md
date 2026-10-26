# 🛠️ Tools Portal

Bienvenue sur mon portail personnel d'outils en ligne, hébergé sur GitHub Pages.  
Ce projet regroupe plusieurs micro-outils autonomes pour l'automatisation, la génération de contenu, l'analyse technique et l'assistance au quotidien.

## 🌐 Accès en ligne

👉 [**Consulter le portail**](https://yilas.github.io/toolbox/)

## 🧩 Outils disponibles

| Outil | Fonctionnalités principales | Lien |
| --- | --- | --- |
| **Générateur de message d'absence** | Génère une réponse automatique multilingue (FR, LU, EN, DE) avec date et moment du retour, raccourcis de dates, contact d'urgence optionnel, copie rapide et mémorisation locale des réglages. | [`auto-reply-generator`](tools/auto-reply-generator.html) |
| **Encodeur / Décodeur Base64** | Encode et décode du texte UTF-8 en Base64, avec validation, copie du résultat, historique des opérations, consultation détaillée d'une entrée et affichage de l'état interne de l'outil. | [`base64-encoder-decoder`](tools/base64-encoder-decoder.html) |
| **Break Reminder** | Minuteur de rappel de pause active avec intervalle configurable, mode Hardcore bloquant l'écran, notifications, signal sonore, clignotement de l'onglet, suivi des pauses validées et streak. | [`break-reminder`](tools/break-reminder.html) |
| **Clean Text** | Nettoie et normalise du texte en préservant les zones Markdown sensibles. Propose conversion ASCII, simplification stylistique, linter de formulations répétitives, statistiques, chargement/téléchargement de fichiers et diff unifié façon Linux pour visualiser les lignes ajoutées ou supprimées. | [`clean-text`](tools/clean-text.html) |
| **Décodeur & Analyseur JWT** | Décode header et payload JWT, analyse les claims temporels et attendus (`iss`, `aud`, `azp`, etc.), vérifie les signatures HMAC et RSA/PS lorsque les clés sont fournies, affiche une timeline, conserve un historique et permet d'enregistrer des règles de validation. | [`decodeur-analyseur-jwt`](tools/decodeur_analyseur_jwt_standalone.html) |
| **Générateur de mot de passe** | Génère des mots de passe avec `crypto.getRandomValues()`, longueur et jeux de caractères configurables, estimation d'entropie, indicateur de robustesse et mode Chaos. | [`random-numbers-generator`](tools/random-numbers-generator.html) |
| **Calculateur de sous-réseaux** | Décompose un réseau CIDR en sous-réseaux, affiche les bornes d'adresses et le nombre d'IP, filtre les tailles de masque et maintient un inventaire persistant des sous-réseaux utilisés par réseau parent. | [`subnets-calculator`](tools/subnets_calculator.html) |
| **Scratchpad** | Bloc-notes autonomes pour snippets, commandes et liens : copie individuelle ou globale, collage depuis le presse-papiers, duplication, recherche, suppression confirmée, import/export JSON et persistance optionnelle dans IndexedDB. | [`scratchpad`](tools/scratchpad.html) |
| **Calculateur de temps de travail** | Permet de saisir plusieurs plages entrée/sortie, de les réordonner par glisser-déposer, de calculer automatiquement le temps total travaillé et de conserver les plages localement. | [`worktime-calculator`](tools/worktime-calculator.html) |

> Le tableau ci-dessus correspond aux outils réellement présents dans le répertoire `tools/`. Les idées affichées comme « à venir » sur le portail ne sont pas listées ici tant qu'aucun outil correspondant n'est présent dans ce répertoire.

## ✳ Scratchpad : stockage local

Ouvrir [`tools/scratchpad.html`](tools/scratchpad.html) dans le navigateur ou depuis le portail GitHub Pages.

- **Par défaut**, un bloc reste temporaire dans `sessionStorage` (session de l’onglet). Il peut aussi expirer au bout de 1 h ou 24 h.
- **📌 Persistant** enregistre le bloc dans **IndexedDB** sur l’origine du site. Décocher l’option retire le bloc de cette base et le remet en mode temporaire.
- La corbeille supprime définitivement une entrée, après confirmation ; **Export JSON** permet une sauvegarde manuelle et **Import JSON** restaure des blocs sans écraser les existants.
- **Ctrl/⌘ + Entrée** crée un bloc ; **Ctrl/⌘ + K** ouvre la recherche. La copie utilise une solution de secours si l’API Clipboard n’est pas accessible.

Les données sont **propres au navigateur et à l’origine** (`https://yilas.github.io` sur GitHub Pages). Elles ne sont ni envoyées au dépôt ni synchronisées entre appareils ; vider les données du site les supprime. Ne pas y déposer de mots de passe, tokens ni secrets : le stockage local n’est pas chiffré.

## 🏗️ Structure

- `index.html` : portail GitHub Pages et point d'entrée vers les outils.
- `tools/` : outils finalisés accessibles depuis le portail.
- `tools_dev/` : travaux ou outils encore en développement.

La plupart des outils sont des pages HTML autonomes avec CSS et JavaScript embarqués, afin de rester simples à déployer et à utiliser directement depuis GitHub Pages.

---

## 📄 Licence

Ce projet est sous licence **Apache License 2.0**.  
Voir le fichier [`LICENSE`](LICENSE) pour plus de détails.

---

## 🙋‍♂️ À propos

Ce projet est maintenu par moi-même.  
Il est utilisé pour centraliser des outils pratiques de manière simple, ergonomique et portable.
