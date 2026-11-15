# PDF Studio — dépendances embarquées

Les fichiers distribués sont copiés depuis les archives npm officielles, sans CDN à l’exécution. Aucun build n’est nécessaire pour GitHub Pages.

| Bibliothèque | Version | Licence | Source |
| --- | --- | --- | --- |
| PDF.js | 6.5.438 | Apache-2.0 (voir aussi licences des polices/décodeurs) | https://github.com/mozilla/pdf.js |
| pdf-lib | 1.17.1 | MIT | https://github.com/Hopding/pdf-lib |

## Reproduire la copie

```sh
npm install --prefix /tmp/pdf-vendor --ignore-scripts --no-audit --no-fund pdfjs-dist@6.5.438 pdf-lib@1.17.1
```

Copier `pdfjs-dist/build/pdf.min.mjs`, `pdf.worker.min.mjs` et les répertoires `cmaps`, `standard_fonts`, `wasm`, `iccs`, avec leurs licences, dans `assets/vendor/pdfjs/`. Copier `pdf-lib/dist/pdf-lib.esm.min.js` sous `assets/vendor/pdf-lib/pdf-lib.min.mjs`, avec `LICENSE.md`. Les sourcemaps et le viewer complet ne sont pas nécessaires.

## Intégrité npm

- `pdfjs-dist@6.5.438` : `sha512-vg8Pw1x+AFKy4AMDvhuWl5cOYV8d698UoEANoFFhc79hqXDnZQqnOXoxw3HG907WHOg1JYOvLhP756boddFxpA==`
- `pdf-lib@1.17.1` : `sha512-V/mpyJAoTsN4cnP31vc0wfNA1+p20evqqnap0KLoRUN0Yk/p3wN52DOEsL4oBFcLdb76hlpKPtzJIgo67j/XLw==`

## Maintenance

Les mises à jour sont explicites : remplacer les fichiers, vérifier les licences et l’intégrité, puis exécuter les tests unitaires et navigateur. Les bibliothèques ne sont pas mises à jour automatiquement.

## Empreintes des modules principaux

- `assets/vendor/pdf-lib/pdf-lib.min.mjs` : SHA-256 `72c052d97b4d5d9fa6cdbdcb7ad709f03d4ddb1122390cb3afeba4d88651d969`
- `assets/vendor/pdfjs/pdf.min.mjs` : SHA-256 `3a88402882cfa3e7e72c2c9b5149f52f0f88baf1c5b62aa11a41abdbc2072440`
- `assets/vendor/pdfjs/pdf.worker.min.mjs` : SHA-256 `1e64c8240e54735ec5913b37acd5134747799cb8c6bb3777c70adbc01232d2aa`
