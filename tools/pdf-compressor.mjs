import { LIMITS, PRESETS, validateFile, validateOptions, pixelSize, formatSize, compareSizes, outputName, PDFDocument } from './pdf-compressor-core.mjs';
const $ = id => document.getElementById(id);
let pdfjs, source = null, pdf = null, pageNumber = 1, busy = false, cancelled = false;
let loadingTask = null, renderTask = null, worker = null, rejectWorker = null;
let downloadUrl = null, previewUrl = null, ready = false;
const pause = () => new Promise(resolve => setTimeout(resolve, 0));
function checkpoint() { if (cancelled) throw new DOMException('Traitement annulé.', 'AbortError'); }
function message(text) { $('status').textContent = text; }
function clearError() { $('error').hidden = true; $('error').textContent = ''; }
function fail(error) {
  if (cancelled || error.name === 'AbortError' || error.name === 'RenderingCancelledException') { message('Traitement annulé.'); return; }
  $('error').textContent = error.name === 'PasswordException' ? 'PDF chiffré : exportez une copie non protégée depuis votre lecteur PDF.' : (error.message || 'Impossible de traiter ce document. Essayez un PDF plus petit ou un navigateur récent.');
  $('error').hidden = false;
  message('Traitement interrompu.');
}
function controls() {
  $('file').disabled = busy || !ready;
  $('clear').disabled = busy;
  $('settings').disabled = busy || !pdf;
  $('compress').disabled = busy || !pdf || ($('mode').value === 'raster' && !$('acceptLoss').checked);
  $('preview').disabled = busy || !pdf || $('mode').value !== 'raster';
  $('prev').disabled = busy || !pdf || pageNumber <= 1;
  $('next').disabled = busy || !pdf || pageNumber >= pdf.numPages;
  $('cancel').hidden = !busy;
  $('progress').hidden = !busy;
  $('settings').setAttribute('aria-busy', String(busy));
}
function start(text) { busy = true; cancelled = false; clearError(); message(text); $('progress').value = 0; controls(); }
function finish() { busy = false; cancelled = false; controls(); }
function clearPreview() {
  if (previewUrl) URL.revokeObjectURL(previewUrl);
  previewUrl = null; $('afterImage').removeAttribute('src'); $('afterFigure').hidden = true;
}
function clearResult() {
  if (downloadUrl) URL.revokeObjectURL(downloadUrl);
  downloadUrl = null; $('download').removeAttribute('href'); $('result').hidden = true;
}
async function dispose() {
  clearResult(); clearPreview();
  const old = loadingTask; loadingTask = null; pdf = null; source = null;
  if (old) await old.destroy();
  $('originalCanvas').width = 0; $('originalCanvas').height = 0;
  $('originalCanvas').hidden = true; $('previewPlaceholder').hidden = false;
  $('fileInfo').hidden = true; $('fileName').textContent = ''; $('fileMeta').textContent = '';
  $('pageLabel').textContent = 'Aucun document'; $('file').value = ''; $('acceptLoss').checked = false;
}
function inWorker(action, bytes) {
  return new Promise((resolve, reject) => {
    worker = new Worker(new URL('./pdf-compressor-core.mjs', import.meta.url), { type: 'module' });
    rejectWorker = reject;
    const cleanup = () => { worker?.terminate(); worker = null; rejectWorker = null; };
    worker.onmessage = ({ data }) => { cleanup(); data.error ? reject(Error(data.error)) : resolve(data); };
    worker.onerror = () => { cleanup(); reject(Error('Impossible de charger le moteur PDF. Rechargez la page depuis HTTPS ou localhost.')); };
    const copy = bytes.slice(); worker.postMessage({ action, bytes: copy }, [copy.buffer]);
  });
}
$('cancel').addEventListener('click', () => {
  cancelled = true; renderTask?.cancel();
  if (worker) { worker.terminate(); worker = null; rejectWorker?.(new DOMException('Annulé', 'AbortError')); rejectWorker = null; }
  if (!pdf) loadingTask?.destroy().catch(() => {});
  message('Annulation en cours…');
});
async function render(page, canvas, scale) {
  const viewport = page.getViewport({ scale });
  const size = pixelSize(viewport.width, viewport.height, 72);
  canvas.width = size.width; canvas.height = size.height;
  try {
    renderTask = page.render({ canvasContext: canvas.getContext('2d', { alpha: false }), viewport, background: 'rgb(255,255,255)', annotationMode: pdfjs.AnnotationMode.ENABLE });
    await renderTask.promise;
  } finally { renderTask = null; }
  checkpoint();
}
async function originalPreview() {
  const page = await pdf.getPage(pageNumber); checkpoint();
  const scratch = document.createElement('canvas');
  try {
    const base = page.getViewport({ scale: 1 });
    await render(page, scratch, Math.min(1.5, 900 / Math.max(base.width, base.height)));
    // Publish only a completed render, so cancellation never leaves a partial preview.
    $('originalCanvas').width = scratch.width; $('originalCanvas').height = scratch.height;
    $('originalCanvas').getContext('2d').drawImage(scratch, 0, 0);
    $('originalCanvas').hidden = false; $('previewPlaceholder').hidden = true;
    $('pageLabel').textContent = `Page ${pageNumber} / ${pdf.numPages}`;
  } finally { scratch.width = 0; scratch.height = 0; page.cleanup(); }
}
async function loadFile(file) {
  if (!file || busy || !ready) return;
  start('Vérification du document…');
  try {
    await dispose(); checkpoint();
    if (!file.size || file.size > LIMITS.bytes) throw Error('Choisissez un PDF non vide de 50 Mio maximum.');
    const bytes = new Uint8Array(await file.arrayBuffer()); checkpoint(); validateFile(file.size, bytes);
    await inWorker('inspect', bytes); checkpoint();
    loadingTask = pdfjs.getDocument({ data: bytes.slice(), isEvalSupported: false, stopAtErrors: true,
      cMapUrl: new URL('../assets/vendor/pdfjs/cmaps/', import.meta.url).href, cMapPacked: true,
      standardFontDataUrl: new URL('../assets/vendor/pdfjs/standard_fonts/', import.meta.url).href,
      wasmUrl: new URL('../assets/vendor/pdfjs/wasm/', import.meta.url).href,
      iccUrl: new URL('../assets/vendor/pdfjs/iccs/', import.meta.url).href,
      canvasMaxAreaInBytes: LIMITS.pixels * 4 });
    pdf = await loadingTask.promise; checkpoint();
    if (!pdf.numPages || pdf.numPages > LIMITS.pages) throw Error('Le document doit contenir entre 1 et 200 pages.');
    source = { bytes, name: file.name, size: file.size }; pageNumber = 1;
    await originalPreview(); checkpoint();
    $('fileName').textContent = file.name;
    $('fileMeta').textContent = `${formatSize(file.size)} · ${pdf.numPages} page${pdf.numPages > 1 ? 's' : ''}`;
    $('fileInfo').hidden = false;
    message('Document prêt. Choisissez une méthode puis lancez la compression.');
  } catch (error) { await dispose(); fail(error); }
  finally { finish(); }
}
$('file').addEventListener('change', event => loadFile(event.target.files[0]));
for (const type of ['dragover', 'drop']) document.addEventListener(type, event => event.preventDefault());
$('dropzone').addEventListener('dragover', () => { if (!busy) $('dropzone').classList.add('dragover'); });
$('dropzone').addEventListener('dragleave', () => $('dropzone').classList.remove('dragover'));
$('dropzone').addEventListener('drop', event => {
  $('dropzone').classList.remove('dragover'); if (busy || !ready) return;
  if (event.dataTransfer.files.length !== 1) { fail(Error('Déposez un seul PDF à la fois.')); return; }
  loadFile(event.dataTransfer.files[0]);
});
$('clear').addEventListener('click', async () => { start('Retrait du document…'); try { await dispose(); clearError(); message('Document retiré.'); } catch (error) { fail(error); } finally { finish(); } });
function options() { const value = { dpi: Number($('dpi').value), quality: Number($('quality').value), grayscale: $('grayscale').checked }; validateOptions(value); return value; }
function settingsChanged() {
  clearPreview(); clearResult(); clearError();
  const raster = $('mode').value === 'raster'; $('rasterSettings').hidden = !raster;
  $('modeHelp').textContent = raster ? 'Réécrit chaque page sous forme d’image JPEG. Utile surtout pour les scans et les photos.' : 'Réorganise le PDF sans réduire la qualité des images. Texte sélectionnable et éléments du document conservés ; gain parfois faible ou nul.';
  $('dpiValue').textContent = `${$('dpi').value} ppp`; $('qualityValue').textContent = `${$('quality').value} %`;
  $('previewHint').textContent = raster ? 'L’aperçu JPEG utilise les réglages actuels. Le gain total sera connu après compression de toutes les pages.' : 'L’optimisation légère conserve l’apparence du document sans recomprimer les images.';
  if (pdf) message('Réglages modifiés. Relancez la compression pour obtenir un résultat à jour.');
  controls();
}
$('mode').addEventListener('change', () => { $('acceptLoss').checked = false; settingsChanged(); });
$('preset').addEventListener('change', () => { const preset = PRESETS[$('preset').value]; if (preset) { $('dpi').value = preset.dpi; $('quality').value = preset.quality; } settingsChanged(); });
for (const id of ['dpi', 'quality']) $(id).addEventListener('input', () => { $('preset').value = 'custom'; settingsChanged(); });
$('grayscale').addEventListener('change', settingsChanged);
$('target').addEventListener('input', () => { clearResult(); });
$('acceptLoss').addEventListener('change', controls);
for (const [id, delta] of [['prev', -1], ['next', 1]]) $(id).addEventListener('click', async () => {
  const previousPage = pageNumber;
  start('Chargement de la page…'); clearPreview(); pageNumber += delta;
  try { await originalPreview(); message('Aperçu original affiché.'); } catch (error) { pageNumber = previousPage; fail(error); } finally { finish(); }
});
async function jpegPage(page, opts) {
  const base = page.getViewport({ scale: 1 }); pixelSize(base.width, base.height, opts.dpi);
  const canvas = document.createElement('canvas');
  try {
    await render(page, canvas, opts.dpi / 72);
    if (opts.grayscale) {
      const ctx = canvas.getContext('2d'); const image = ctx.getImageData(0, 0, canvas.width, canvas.height);
      for (let i = 0; i < image.data.length; i += 4) { const gray = Math.round(image.data[i] * .299 + image.data[i + 1] * .587 + image.data[i + 2] * .114); image.data[i] = image.data[i + 1] = image.data[i + 2] = gray; }
      ctx.putImageData(image, 0, 0);
    }
    const blob = await new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', opts.quality / 100)); checkpoint();
    if (!blob || blob.type !== 'image/jpeg') throw Error('Ce navigateur ne peut pas encoder les pages en JPEG.');
    return { blob, width: base.width, height: base.height };
  } finally { canvas.width = 0; canvas.height = 0; page.cleanup(); }
}
function showJpeg(blob, label) { clearPreview(); previewUrl = URL.createObjectURL(blob); $('afterImage').src = previewUrl; $('afterCaption').textContent = label; $('afterFigure').hidden = false; }
$('preview').addEventListener('click', async () => {
  start('Calcul de l’aperçu JPEG…');
  try { const opts = options(); const page = await pdf.getPage(pageNumber); checkpoint(); const { blob } = await jpegPage(page, opts); checkpoint(); showJpeg(blob, `Réglages actuels · page ${pageNumber}`); message('Aperçu calculé. Vérifiez notamment les petits caractères.'); }
  catch (error) { fail(error); } finally { finish(); }
});
$('compress').addEventListener('click', async () => {
  if (!source || busy) return;
  const raster = $('mode').value === 'raster';
  if (raster && !$('acceptLoss').checked) return;
  if (!$('target').checkValidity()) { $('target').reportValidity(); return; }
  const target = $('target').value ? Number($('target').value) * 1024 ** 2 : null;
  start('Préparation de la compression…'); clearResult(); clearPreview();
  try {
    let candidate, selectedPreview = null;
    const opts = options();
    if (!raster) {
      message('Optimisation de la structure du PDF…');
      candidate = (await inWorker('optimize', source.bytes)).bytes; checkpoint();
    } else {
      // Preflight every page before allocating any large canvas.
      let totalPixels = 0;
      for (let i = 1; i <= pdf.numPages; i++) {
        checkpoint(); const page = await pdf.getPage(i); const view = page.getViewport({ scale: 1 });
        totalPixels += pixelSize(view.width, view.height, opts.dpi).pixels; page.cleanup();
        if (totalPixels > LIMITS.totalPixels) throw Error('Document trop volumineux à cette résolution. Réduisez les ppp ou utilisez l’optimisation légère.');
      }
      const output = await PDFDocument.create(); let totalBytes = 0;
      for (let i = 1; i <= pdf.numPages; i++) {
        checkpoint(); message(`Compression de la page ${i} / ${pdf.numPages}…`);
        const page = await pdf.getPage(i); const encoded = await jpegPage(page, opts); checkpoint();
        totalBytes += encoded.blob.size;
        if (totalBytes > LIMITS.outputBytes) throw Error('Résultat trop volumineux. Réduisez la résolution ou la qualité.');
        const image = await output.embedJpg(await encoded.blob.arrayBuffer());
        output.addPage([encoded.width, encoded.height]).drawImage(image, { x: 0, y: 0, width: encoded.width, height: encoded.height });
        if (i === pageNumber) selectedPreview = encoded.blob;
        $('progress').value = i / pdf.numPages * 90; await pause();
      }
      checkpoint(); message('Assemblage du PDF…');
      candidate = await output.save({ useObjectStreams: true, addDefaultPage: false, objectsPerTick: 25 }); checkpoint();
    }
    if (candidate.length > LIMITS.outputBytes) throw Error('Résultat trop volumineux. Réduisez la résolution ou la qualité.');
    const comparison = compareSizes(source.size, candidate.length);
    const bytes = comparison.improved ? candidate : source.bytes;
    downloadUrl = URL.createObjectURL(new Blob([bytes], { type: 'application/pdf' }));
    $('download').href = downloadUrl; $('download').download = outputName(source.name, comparison.improved);
    $('download').textContent = comparison.improved ? 'Télécharger le PDF compressé' : 'Télécharger l’original (inchangé)';
    $('beforeSize').textContent = formatSize(source.size); $('afterSize').textContent = formatSize(comparison.size);
    $('gain').textContent = `${comparison.percent.toLocaleString('fr-FR', { maximumFractionDigits: 1 })} %`;
    $('resultMode').textContent = comparison.improved ? (raster ? `${pdf.numPages} pages en images · ${opts.dpi} ppp · JPEG ${opts.quality} %${opts.grayscale ? ' · niveaux de gris' : ''}` : 'Optimisation légère · texte et structure conservés') : 'Original conservé · aucune compression utile';
    $('resultNote').textContent = comparison.improved ? `${formatSize(comparison.saved)} économisés. ${raster ? 'La copie ne contient plus de texte sélectionnable ni de champs interactifs.' : 'Contrôlez le document dans votre lecteur habituel.'}` : `Le PDF produit pesait ${formatSize(candidate.length)}, contre ${formatSize(source.size)} pour l’original. Aucun gain : le téléchargement contient l’original, inchangé.`;
    $('targetNote').textContent = target ? (comparison.size <= target ? 'Objectif de taille atteint.' : 'Objectif de taille non atteint. Essayez le mode scans ou des réglages plus bas si les pertes sont acceptables.') : '';
    if (comparison.improved && selectedPreview) showJpeg(selectedPreview, `Page ${pageNumber} du PDF compressé`);
    $('result').hidden = false; $('progress').value = 100; message('Traitement terminé. Votre téléchargement est prêt.'); $('result').focus();
  } catch (error) { clearResult(); fail(error); } finally { finish(); }
});
const media = matchMedia('(prefers-color-scheme: dark)'); let theme = 'system';
try { const saved = localStorage.getItem('password-studio-theme'); if (['dark', 'light'].includes(saved)) theme = saved; } catch {}
function applyTheme() {
  const dark = theme === 'dark' || (theme === 'system' && media.matches);
  document.documentElement.dataset.theme = dark ? 'dark' : 'light'; $('theme').textContent = dark ? '☀' : '☾';
  $('theme').setAttribute('aria-label', dark ? 'Activer le thème clair' : 'Activer le thème sombre');
  document.querySelector('meta[name="theme-color"]').content = dark ? '#0b1120' : '#f5f7fc';
}
$('theme').addEventListener('click', () => { theme = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'; try { localStorage.setItem('password-studio-theme', theme); } catch {} applyTheme(); });
media.addEventListener('change', applyTheme); applyTheme();
try {
  pdfjs = await import('../assets/vendor/pdfjs/pdf.min.mjs');
  pdfjs.GlobalWorkerOptions.workerSrc = new URL('../assets/vendor/pdfjs/pdf.worker.min.mjs', import.meta.url).href;
  ready = true; message('Choisissez un PDF pour commencer.'); controls();
} catch { fail(Error('Moteur PDF indisponible. Utilisez un navigateur récent et ouvrez cette page via HTTPS ou localhost, puis rechargez-la.')); }
