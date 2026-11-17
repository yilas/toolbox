# CI Toolbox — organisation

La CI est définie dans `.github/workflows/ci.yml` et remplace les sept anciens
workflows de tests. Le workflow `security-advisories-refresh.yml` reste
indépendant : il collecte des avis et propose une PR de données.

## Déclenchements

- Toute Pull Request, **sans filtre global `paths`** : les contrôles Node.js
  et Python sont toujours présents, même sur une PR de documentation.
- Push sur `main` : mêmes contrôles.
- `workflow_dispatch` : tous les contrôles, y compris Chromium et la source AKS.
- Chaque jour à 03:23 UTC : uniquement le contrôle de disponibilité/format
  de la source MicrosoftDocs AKS. Les indisponibilités externes ne bloquent
  donc pas les PR.

## Jobs

| Job | Fonction |
| --- | --- |
| `changes` | Détecte les modifications pertinentes pour Chromium. |
| `node-tests` | Syntaxe JS + tous les tests `tests/*.test.cjs` une seule fois. |
| `python-tests` | Tests hors ligne du collecteur + validation JSON. |
| `browser-tests` | Playwright/Chromium et captures PDF, seulement si PDF, assets vendor ou CI modifiés, ou exécution manuelle. |
| `integration-tests` | Vérification de la source AKS réelle, uniquement planifiée/manuelle. |

Le job `changes` compare la base et la tête de la PR, ou les deux commits
du push. Si la base n'est pas disponible, il lance Chromium par précaution.

## Migration

Si des règles de protection de `main` exigent les anciens jobs
(`unit-tests`, `pdf-compressor`, `portal`, `collector-tests`,
`validate-directory`, `check-back-navigation`), il faut les remplacer
par les nouveaux checks **avant de fusionner la PR** :

- `Node.js unit and repository tests`
- `Security collector and snapshot tests`
- `PDF Chromium regression tests` (job conditionnel ; si obligatoire, vérifier
  le comportement des checks sautés dans les règles de protection).

Le statut des anciens workflows disparaît après la migration. Aucune
modification des fichiers de tests n'est nécessaire.

La vérification AKS n'est pas un contrôle de PR obligatoire : elle dépend
d'un service externe.
