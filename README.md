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
| **Scratchpad** | Notes persistantes par défaut (IndexedDB), catégories visuelles personnalisables, tags, réorganisation, corbeille restaurable, raccourcis, mode jour/nuit, import/export JSON. | [`scratchpad`](tools/scratchpad.html) |
| **Calculateur de temps de travail** | Permet de saisir plusieurs plages entrée/sortie, de les réordonner par glisser-déposer, de calculer automatiquement le temps total travaillé et de conserver les plages localement. | [`worktime-calculator`](tools/worktime-calculator.html) |

> Le tableau ci-dessus correspond aux outils réellement présents dans le répertoire `tools/`. Les idées affichées comme « à venir » sur le portail ne sont pas listées ici tant qu'aucun outil correspondant n'est présent dans ce répertoire.

## ✳ Scratchpad : utilisation et stockage local

Ouvrir [`tools/scratchpad.html`](tools/scratchpad.html) dans le navigateur ou depuis le portail GitHub Pages. Aucune installation, aucune dépendance externe.

- **Tags** : saisir des tags séparés par des virgules dans une carte (ex. `aks, terraform`). La recherche couvre titres, contenu et tags ; le menu **Tous les tags** et les badges **#tag** filtrent les blocs.
- **Réorganisation** : glisser un bloc avec la poignée `⠿`, ou utiliser les boutons **↑/↓** (également accessibles au clavier). Désactiver les filtres pour modifier l'ordre ; il est enregistré avec les notes.
- **Corbeille** : supprimer un bloc le place dans la corbeille pour **7 jours**. Il peut être restauré ou supprimé définitivement. Vider la corbeille nécessite une confirmation. Les blocs initialement persistants vont dans un magasin `trash` d'**IndexedDB** (migration v1 → v2 sans supprimer les notes existantes) ; les temporaires vont dans `sessionStorage` et disparaissent à la fermeture de la session.
- **Persistance par défaut** : toute nouvelle note est enregistrée dans **IndexedDB** (📌 Persistant activé), y compris les notes créées par collage ou duplication d'une note persistante. Décocher **📌 Persistant** la rend temporaire dans `sessionStorage` avec expiration optionnelle de 1 minute, 5 minutes, 30 minutes, 1 heure, 6 heures, 8 heures ou 24 heures. Les notes épinglées n'expirent pas : décocher « Persistant » pour utiliser un TTL. Les notes expirées sont retirées automatiquement (vérification toutes les 5 secondes lorsque la page est ouverte). En cas d'indisponibilité d'IndexedDB, une nouvelle note reste temporaire et un avertissement apparaît. Les notes temporaires existantes ne sont pas converties automatiquement.
- **Catégories visuelles personnalisables** : les six catégories d’origine sont disponibles dès le premier chargement. Le bouton **⚙ Gérer les catégories** permet d’en créer d’autres, de modifier le nom et la couleur de chaque catégorie, ou de les supprimer. Le catalogue est conservé localement dans `localStorage`. La catégorie **À classer** est renommable et recolorable, mais non supprimable : elle sert de destination de secours.
- **Suppression sans perte** : lors de la suppression d’une catégorie, ses notes actives et celles de la corbeille sont reclassées dans « À classer ». Les blocs persistants sont mis à jour dans IndexedDB, les temporaires dans `sessionStorage` ; les contenus et les tags ne sont pas supprimés.
- **Catégories et import/export** : chaque bloc contient un identifiant de catégorie, mais le catalogue global n’est pas exporté dans le JSON actuel. Sur un autre navigateur sans cette catégorie, la note est classée dans « À classer ».
- **Import/export JSON** : importe des blocs sans écraser ceux qui existent ; exporte toutes les **notes actives** avec leurs tags et leur ordre, **pas les éléments de la corbeille**.
- **Mode jour/nuit** : bouton dans l'en-tête ; thème initial déterminé par le système, puis préférence enregistrée dans `localStorage`.

### Raccourcis

| Raccourci | Action |
| --- | --- |
| `Ctrl/⌘ + Entrée` | Créer un bloc |
| `Ctrl/⌘ + K` | Rechercher |
| `Ctrl/⌘ + Maj + V` | Nouveau bloc depuis le presse-papiers (hors éditeur de texte) |
| `Ctrl/⌘ + Maj + C` | Copier le contenu du bloc sélectionné |
| `Alt + ↑/↓` | Déplacer le bloc ayant le focus (sans filtre actif) |
| `Échap` | Fermer la corbeille |

Les données sont **propres au navigateur et à l'origine** (`https://yilas.github.io` sur GitHub Pages). Elles ne sont ni envoyées au dépôt ni synchronisées entre appareils ; vider les données du site les supprime. Ne pas y déposer de mots de passe, tokens ni secrets : le stockage local n'est pas chiffré.

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
