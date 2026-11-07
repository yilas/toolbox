# 🛠️ Tools Portal

Bienvenue sur mon portail personnel d'outils en ligne, hébergé sur GitHub Pages.  
Ce projet regroupe plusieurs micro-outils autonomes pour l'automatisation, la génération de contenu, l'analyse technique et l'assistance au quotidien.

## 🌐 Accès en ligne

👉 [**Consulter le portail**](https://yilas.github.io/toolbox/)

## 🧭 Accueil du portail

La page `index.html` présente les neuf outils disponibles dans une grille responsive, avec une recherche locale insensible aux accents et un raccourci `/` pour ouvrir la recherche. La préférence de thème clair/sombre est partagée avec le générateur de mots de passe (stockée sous la clé `password-studio-theme`). Les outils en préparation sont affichés sans lien fictif.

L'accueil utilise uniquement `index.html`, `portal.css` et `portal.js`, sans bibliothèque ni appel réseau. Les cartes restent consultables même si JavaScript est désactivé.

Pour vérifier les liens et les interactions de base :

```bash
node --test tests/portal.test.cjs
```

## ↩️ Navigation entre les outils

Chaque page HTML du répertoire `tools/` doit proposer, au début de son interface, un **lien natif** `← Retour à Toolbox` vers `../index.html`. Le lien reste accessible sans JavaScript et ne doit pas être dupliqué sur une même page.

Le test ci-dessous vérifie automatiquement cette convention pour **tous les outils HTML présents**, y compris ceux qui seront ajoutés ultérieurement :

```bash
node --test tests/toolbox-return-navigation.test.cjs
```

La vérification est également lancée par GitHub Actions sur les pull requests modifiant les pages d'outils.

## 🧩 Outils disponibles

| Outil | Fonctionnalités principales | Lien |
| --- | --- | --- |
| **Générateur de message d'absence** | Génère une réponse automatique multilingue (FR, LU, EN, DE) avec date et moment du retour, raccourcis de dates, contact d'urgence optionnel, copie rapide et mémorisation locale des réglages. | [`auto-reply-generator`](tools/auto-reply-generator.html) |
| **Encodeur / Décodeur Base64** | Encode et décode du texte UTF-8 en Base64, avec validation, copie du résultat, historique des opérations, consultation détaillée d'une entrée et affichage de l'état interne de l'outil. | [`base64-encoder-decoder`](tools/base64-encoder-decoder.html) |
| **Break Reminder** | Minuteur de rappel de pause active avec intervalle configurable, mode Hardcore bloquant l'écran, notifications, signal sonore, clignotement de l'onglet, suivi des pauses validées et streak. | [`break-reminder`](tools/break-reminder.html) |
| **Clean Text** | Nettoie et normalise du texte en préservant les zones Markdown sensibles. Propose conversion ASCII, simplification stylistique, linter de formulations répétitives, statistiques, chargement/téléchargement de fichiers et diff unifié façon Linux pour visualiser les lignes ajoutées ou supprimées. | [`clean-text`](tools/clean-text.html) |
| **Décodeur & Analyseur JWT** | Décode les JWT compacts signés, distingue décodage et vérification, valide les claims temporels et règles (`iss`, `aud`, `azp`, algorithme), vérifie HS/RS/PS/ES avec Web Crypto (clé publique PEM ou JWK), propose une chronologie et un historique temporaire **sans stockage de JWT ni de secrets**. | [`decodeur-analyseur-jwt`](tools/decodeur_analyseur_jwt_standalone.html) |
| **Générateur de mot de passe** | Génère localement des mots de passe et des phrases de passe (FR/EN) avec l’API Web Crypto, tirages uniformes, préréglages, contraintes de caractères, estimation d’entropie et génération multiple. | [`random-numbers-generator`](tools/random-numbers-generator.html) |
| **Calculateur de sous-réseaux** | Vérifie et normalise les CIDR IPv4 de `/0` à `/32`, visualise l'occupation parent/enfant dans une arborescence ou une grille paginée, gère les réservations annotées, calcule l'union exacte des plages et exporte les inventaires JSON/CSV. | [`subnets-calculator`](tools/subnets_calculator.html) |
| **Scratchpad** | Notes persistantes par défaut (IndexedDB), blocs compacts/standards/larges, catégories visuelles personnalisables, tags, réorganisation, corbeille restaurable, raccourcis, mode jour/nuit, import/export JSON. | [`scratchpad`](tools/scratchpad.html) |
| **Calculateur de temps de travail** | Permet de saisir plusieurs plages entrée/sortie, de les réordonner par glisser-déposer, de calculer automatiquement le temps total travaillé et de conserver les plages localement. | [`worktime-calculator`](tools/worktime-calculator.html) |

> Le tableau ci-dessus correspond aux outils réellement présents dans le répertoire `tools/`. Les idées affichées comme « à venir » sur le portail ne sont pas listées ici tant qu'aucun outil correspondant n'est présent dans ce répertoire.

## ✳ Scratchpad : utilisation et stockage local

**Aide discrète :** la page principale n'affiche plus les longs messages d'aide. Appuyer sur **?** en dehors d'un champ de saisie (ou cliquer sur le bouton **?** de l'en-tête) ouvre une fenêtre contenant les explications, les conseils de stockage et les raccourcis. **Échap**, le bouton « Fermer » ou un clic sur l'arrière-plan referme la fenêtre. La saisie de « ? » dans une note n'ouvre jamais l'aide.

Ouvrir [`tools/scratchpad.html`](tools/scratchpad.html) dans le navigateur ou depuis le portail GitHub Pages. Aucune installation, aucune dépendance externe.

- **Reprise de position** : le dernier bloc sélectionné ou visible lors du défilement est mémorisé dans `localStorage`. À l'ouverture d'un nouvel onglet ou d'une nouvelle fenêtre sur la même origine, Scratchpad retrouve ce bloc et le ramène à l'écran après chargement des notes (s'il existe encore). Le retour se fait après calcul de la grille responsive et évite que le défilement automatique remplace prématurément le bloc mémorisé. Les blocs temporaires d'un onglet ne sont pas nécessairement accessibles dans un autre onglet ; les blocs persistants le sont. La suppression d'une note retire sa référence mémorisée.
- **Pleine largeur** : l'interface occupe désormais toute la largeur du navigateur au lieu d'être limitée à 1 260 px ; les cartes compactes, standards et larges restent responsive.
- **Interface de bloc épurée** : sur chaque carte, seul le titre, le contenu, une indication discrète de catégorie et le bouton **Copier** sont affichés en permanence. Le menu **⋯** déplie les réglages secondaires (catégorie, format, persistance, TTL, tags) ainsi que Dupliquer, Déplacer et Supprimer. Aucun réglage ni aucune donnée ne sont supprimés ; ce menu est indépendant de l'aide globale disponible avec `?`.
- **Formats de cartes** : choisir **Compact**, **Standard** ou **Large** sur chaque note. Compact optimise l'espace pour une courte valeur ou un lien ; Large étend la carte et l'éditeur pour du texte long. Le format est enregistré avec la note (IndexedDB pour les blocs persistants, sessionStorage pour les temporaires), conservé lors de la duplication, dans la corbeille et dans les exports/imports JSON. Les anciennes notes restent en format Standard. La grille est fluide avec une largeur minimale cible de **360 px par colonne** (contre 255 px auparavant) : le nombre de cartes par ligne augmente ou diminue automatiquement selon la largeur du navigateur (sans limite fixe à trois). Les cartes Compact et Standard occupent une colonne, Large deux colonnes lorsque l'espace le permet, puis une seule sur mobile. L'ordre des éléments reste celui choisi par l'utilisateur.
- **Tags** : saisir des tags séparés par des virgules dans une carte (ex. `aks, terraform`). La recherche couvre titres, contenu et tags ; le menu **Tous les tags** et les badges **#tag** filtrent les blocs.
- **Réorganisation** : glisser un bloc avec la poignée `⠿`, ou utiliser les boutons **↑/↓** (également accessibles au clavier). Désactiver les filtres pour modifier l'ordre ; il est enregistré avec les notes.
- **Corbeille** : supprimer un bloc le place dans la corbeille pour **7 jours**. Il peut être restauré ou supprimé définitivement. Vider la corbeille nécessite une confirmation. Les blocs initialement persistants vont dans un magasin `trash` d'**IndexedDB** (migration v1 → v2 sans supprimer les notes existantes) ; les temporaires vont dans `sessionStorage` et disparaissent à la fermeture de la session.
- **Persistance par défaut** : toute nouvelle note est enregistrée dans **IndexedDB** (📌 Persistant activé), y compris les notes créées par collage ou duplication d'une note persistante. Décocher **📌 Persistant** la rend temporaire dans `sessionStorage` avec expiration optionnelle de 1 minute, 5 minutes, 30 minutes, 1 heure, 6 heures, 8 heures ou 24 heures. Les notes épinglées n'expirent pas : décocher « Persistant » pour utiliser un TTL. Les notes expirées sont retirées automatiquement (vérification toutes les 5 secondes lorsque la page est ouverte). En cas d'indisponibilité d'IndexedDB, une nouvelle note reste temporaire et un avertissement apparaît. Les notes temporaires existantes ne sont pas converties automatiquement.
- **Catégorie active mémorisée** : sélectionner une catégorie (par exemple « Personnel ») conserve le filtre dans `localStorage`. La même catégorie reste affichée après un rechargement ou dans un nouvel onglet du même navigateur. Le filtre n'est pas effacé lors de la création ou de l'import de notes. Si une catégorie sélectionnée est supprimée, le filtre revient à « Toutes ».
- **Catégories visuelles personnalisables** : les six catégories d’origine sont disponibles dès le premier chargement. Le bouton **⚙ Gérer les catégories** permet d’en créer d’autres, de modifier le nom et la couleur de chaque catégorie, de les supprimer, mais aussi de **changer leur ordre** (poignée ⠿ à glisser-déposer ou boutons ↑/↓ accessibles au clavier). Cet ordre est conservé dans `localStorage` et repris dans la barre de catégories ainsi que dans les sélecteurs des notes. Le catalogue est conservé localement dans `localStorage`. La catégorie **À classer** est renommable et recolorable, mais non supprimable : elle sert de destination de secours.
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

Les données sont **propres au navigateur et à l'origine** (`https://yilas.github.io` sur GitHub Pages). Elles ne sont ni envoyées au dépôt ni synchronisées entre appareils ; vider les données du site les supprime. Ne pas y déposer de mots de passe, tokens ni secrets sensibles : le stockage local n'est pas chiffré, même pour les petites cartes.

## 🔏 Analyseur JWT : sécurité, données et usage

L'outil `tools/decodeur_analyseur_jwt_standalone.html` utilise les fichiers locaux `jwt-analyzer.css`, `jwt-analyzer-core.js` et `jwt-analyzer-ui.js`. Il ne charge ni dépendance externe, ni JWKS depuis une URL présente dans le JWT. Il fonctionne sur GitHub Pages (HTTPS) ou sur un serveur `localhost`.

- **Décodage ≠ authentification** : les claims sont visibles même sans clé, mais la signature est alors indiquée **Non vérifiée**. Un échec cryptographique est distingué d'une clé manquante ou mal formée.
- **Signatures** : HMAC `HS256/384/512`, RSA `RS256/384/512`, RSA-PSS `PS256/384/512`, ECDSA `ES256/384/512`. Les clés publiques RSA/EC sont acceptées en PEM SPKI (`BEGIN PUBLIC KEY`) ou JWK JSON, avec contrôle de compatibilité avec l'algorithme choisi.
- **Règles** : issuer, audiences (au moins une / toutes), authorized party, algorithme attendu et tolérance d'horloge.
- **Confidentialité** : les JWT, secrets HMAC et clés fournies sont conservés uniquement dans les champs et en mémoire pendant la session de l'onglet. L'historique est limité à vingt entrées et n'est **jamais persistant**. Les règles et la préférence de thème peuvent rester dans le navigateur.
- **Migration depuis l'ancien analyseur** : au premier chargement, les anciennes entrées d'historique conservées en clair (incluant éventuellement les secrets HMAC) sont **supprimées de `localStorage`** ; seules les règles sont conservées. Cette suppression ne permet pas de récupérer ensuite l'ancien historique. Un message en avertit l'utilisateur.
- **Périmètre** : les JWT chiffrés à 5 segments (JWE), le mode `b64=false` et les en-têtes `crit` non interprétés ne sont pas validés. Ce diagnostic local ne se substitue pas aux vérifications de sécurité et de politique de l'application utilisatrice.
- **Test rapide** : cliquer sur **Exemple** crée un JWT HS256 local, signé avec une clé connue de démonstration ; cette clé ne doit jamais servir en production.

Pour exécuter les tests automatisés (sans dépendances tierces) :

```bash
node --test tests/jwt-analyzer.test.cjs
```

## 🌐 Calculateur IPv4 : inventaire hiérarchique V2

Le calculateur conserve **deux représentations complémentaires** :

- **Arborescence IPv4 dépliable** : chaque réseau peut révéler ses deux enfants (`/n+1`), jusqu'à `/32`. Les liens parent-enfant restent visibles et les plages associées sont mises en évidence au survol.
- **Grille par masque** : sélection de `/0` à `/32` selon le réseau parent, affichage de 64 sous-réseaux par page, navigation et recherche directe par IP, CIDR ou description de réservation.

La hiérarchie utilise cinq états : **Libre**, **Réservé** (sélection directe), **Hérité** (réservation d'un ancêtre), **Partiel** (couverture incomplète par les descendants) et **Complet** (100 % couvert par les descendants). Le nombre d'adresses occupées est calculé par **union des intervalles**, sans double comptage : `/26 + /25` imbriqués ne sont pas additionnés deux fois.

**Contraintes et stockage :**

- Les nouvelles réservations ne peuvent pas chevaucher une réservation existante. Pour réserver un parent déjà partiellement couvert, il faut d'abord modifier/libérer les réservations enfants.
- Les chevauchements déjà présents dans des données historiques sont préservés et signalés, sans fausser les pourcentages.
- Les anciens inventaires `snet_calc_inventory_*` et le dernier réseau actif sont **copiés une seule fois** dans la structure versionnée `toolbox-subnets-v2`, avec normalisation des CIDR ; les anciennes clés restent intactes pour récupération.
- Le réseau actif est distinct du texte en cours de saisie : une modification du champ ne redirige jamais une réservation vers un autre inventaire.
- Export **JSON** de tous les réseaux (réimportable avec fusion sans écrasement des entrées existantes) et export **CSV** de l'inventaire actif (séparateur `;`, compatible Excel).
- Pas de synchronisation serveur. Le stockage local reste propre au navigateur et au domaine GitHub Pages : pensez à exporter une sauvegarde.
- L'inventaire s'applique aux **adresses IPv4 totales** (y compris adresses réseau/diffusion selon le préfixe), pas à un calcul cloud de ressources IP utilisables.

Les fichiers `tools/subnets-core.js`, `tools/subnets-storage.js`, `tools/subnets-ui.js` et `tools/subnets-calculator.css` sont natifs, sans dépendance. Tests :

```bash
node --test tests/subnets-core.test.cjs tests/subnets-ui.test.cjs
```

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
