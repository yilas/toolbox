# Security Advisories — collecte et publication

## Périmètre initial

La page `tools/security-advisories.html` conserve son annuaire officiel et affiche un
instantané JSON **publié après revue**, pas une supervision temps réel.

Sources automatisées :
- MicrosoftDocs : bulletins AKS (`AKS-YYYY-NNNN`), lus depuis le Markdown officiel du dépôt public `MicrosoftDocs/azure-aks-docs` et reliés à Microsoft Learn.
- GitHub Security Advisories publics : `argoproj/argo-cd`, `cilium/cilium`,
  `kyverno/kyverno`, `goharbor/harbor`, `coder/coder`,
  `akuity/kargo` et `kubernetes/kubernetes`.

La collecte couvre les publications des **365 derniers jours** et limite le nombre
d'avis par source. Les avis Cilium OSS ne remplacent **pas** les communications
de sécurité Cilium Enterprise/Isovalent.

## Architecture

1. `scripts/collect_security_advisories.py` interroge les sources officielles côté
   GitHub Actions, sans dépendances Python externes.
2. Le résultat est enregistré dans `tools/data/security-advisories.json`.
3. `.github/workflows/security-advisories-refresh.yml` s'exécute toutes les
   6 heures (UTC) ou via `workflow_dispatch`.
4. Le workflow ouvre/met à jour une **PR de données**, branche
   `automation/security-advisories-data`, sans fusion automatique.
5. Après revue et fusion, GitHub Pages sert le JSON. Le navigateur ne charge
   que ce fichier **même origine**, avec `connect-src 'self'`.

**Important :** une PR créée par le `GITHUB_TOKEN` standard ne déclenche
pas nécessairement les workflows `pull_request` habituels. Le workflow de
collecte exécute donc les tests du collecteur avant de proposer les données.

La création de PR par GitHub Actions peut nécessiter d'activer
`Settings → Actions → General → Allow GitHub Actions to create and approve pull requests`.
Vérifier également les permissions du workflow. Aucun PAT personnel n'est requis
dans la configuration prévue.

## Validation et reprise sur erreur

- Tests hors ligne : `python -m unittest discover -s tests -p 'test_security_collector.py'`
- Test d'intégration CI : téléchargement du Markdown AKS actuel depuis `raw.githubusercontent.com/MicrosoftDocs/azure-aks-docs/main/articles/aks/security-bulletins/overview.md`, puis vérification de la présence d'avis récents. Une modification incompatible du document fait échouer la CI plutôt que de publier silencieusement une collecte vide.
- Tests de l'interface : `node --test tests/security-advisories-feed.test.cjs`
- Si une source échoue, son dernier jeu d'avis est conservé et son état passe
  à `error`. Le JSON indique `checked_at` et `last_success_at`.
- Si **toutes** les sources échouent, le collecteur s'arrête en erreur et
  **n'écrase pas** l'instantané précédent.
- Une collecte vide et réussie n'est pas une preuve d'absence de vulnérabilités.
- L'instantané initial contient `generated_at: null` : la page indique
  explicitement qu'aucune collecte n'a encore été publiée.

## Limites de la première version

- GitHub : sévérité, CVE et plages de versions provenant des champs structurés
  des avis publics. Les composants Enterprise peuvent avoir des avis séparés.
- AKS : identifiant, titre, date, CVE cité dans le titre ou la première description, et lien vers la section Microsoft Learn. Les CVE mentionnées seulement comme exemples historiques ne sont pas associées automatiquement à l'avis.
  La sévérité reste `unknown` et les versions sont laissées vides tant qu'un
  extracteur structuré fiable n'existe pas. Ne pas inférer une version affectée
  depuis du texte libre ou un simple scanner CVE.
- Les bulletins peuvent être mis à jour après publication : vérifier la page
  de l'éditeur avant toute décision d'exploitation.
- Les données ne sont pas mises à jour automatiquement sur le site tant que
  la PR de données n'est pas fusionnée.

## Première activation

1. Fusionner la PR de fonctionnalité après CI et revue.
2. Déclencher **Actions → Refresh security advisory snapshot → Run workflow**,
   ou attendre le premier passage planifié.
3. Vérifier la PR `data: refresh AKS and GitHub security advisories` :
   sources en erreur, dates, versions, URLs et données attendues.
4. Fusionner cette PR **manuellement** pour publier les alertes sur GitHub Pages.
