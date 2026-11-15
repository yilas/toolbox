import { PDFDocument, PDFDict, PDFName } from '../assets/vendor/pdf-lib/pdf-lib.min.mjs';
export { PDFDocument };
export const LIMITS = Object.freeze({ bytes: 50 * 1024 ** 2, pages: 200, pixels: 12_000_000, totalPixels: 300_000_000, outputBytes: 100 * 1024 ** 2 });
export const PRESETS = Object.freeze({ balanced: { dpi: 120, quality: 75 }, small: { dpi: 72, quality: 50 }, quality: { dpi: 180, quality: 88 } });
export function validateFile(size, bytes) {
  if (!size || size > LIMITS.bytes) throw Error('Choisissez un PDF non vide de 50 Mio maximum.');
  if (!new TextDecoder('latin1').decode(bytes.subarray(0, 1024)).includes('%PDF-')) throw Error('Ce fichier ne contient pas un en-tête PDF valide.');
}
export function validateOptions({ dpi, quality }) {
  if (!Number.isInteger(dpi) || dpi < 72 || dpi > 200 || !Number.isInteger(quality) || quality < 30 || quality > 95) throw Error('Réglages de compression invalides.');
}
export function pixelSize(width, height, dpi) {
  const w = Math.ceil(width * dpi / 72), h = Math.ceil(height * dpi / 72);
  if (!Number.isFinite(w * h) || w <= 0 || h <= 0 || w * h > LIMITS.pixels || w > 16384 || h > 16384) throw Error('Page trop grande pour cette résolution. Réduisez les ppp ou utilisez l’optimisation légère.');
  return { width: w, height: h, pixels: w * h };
}
export function formatSize(bytes) {
  return bytes >= 1024 ** 2 ? `${(bytes / 1024 ** 2).toLocaleString('fr-FR', { maximumFractionDigits: 2 })} Mio` : `${(bytes / 1024).toLocaleString('fr-FR', { maximumFractionDigits: 1 })} Kio`;
}
export function compareSizes(original, candidate) {
  const improved = candidate < original;
  return { improved, size: improved ? candidate : original, percent: improved ? (1 - candidate / original) * 100 : 0, saved: Math.max(0, original - candidate) };
}
export function outputName(name, improved) {
  return name.replace(/\.pdf$/i, '') + (improved ? '-compresse.pdf' : '-original.pdf');
}
export async function inspectPdf(bytes) {
  const pdf = await PDFDocument.load(bytes, { updateMetadata: false, throwOnInvalidObject: true });
  const count = pdf.getPageCount();
  if (!count || count > LIMITS.pages) throw Error('Le document doit contenir entre 1 et 200 pages.');
  // Inspect dictionaries, including detached signature dictionaries, not raw byte patterns.
  for (const [, object] of pdf.context.enumerateIndirectObjects()) {
    if (!(object instanceof PDFDict)) continue;
    if (String(object.get(PDFName.of('FT'))) === '/Sig' || String(object.get(PDFName.of('Type'))) === '/Sig' || object.has(PDFName.of('ByteRange'))) throw Error('Signature numérique détectée : conservez ce PDF intact.');
    if (object.has(PDFName.of('XFA'))) throw Error('Les formulaires XFA ne sont pas pris en charge.');
  }
  // Inline form field dictionaries can also carry a signature.
  const form = pdf.catalog.lookupMaybe(PDFName.of('AcroForm'), PDFDict);
  if (form?.has(PDFName.of('XFA'))) throw Error('Les formulaires XFA ne sont pas pris en charge.');
  if (pdf.catalog.has(PDFName.of('Perms')) || (form && pdf.getForm().getFields().some(field => String(field.acroField.dict.get(PDFName.of('FT'))) === '/Sig'))) throw Error('Signature numérique détectée : conservez ce PDF intact.');
  return pdf;
}
export async function optimizePdf(bytes) {
  const pdf = await inspectPdf(bytes);
  return pdf.save({ useObjectStreams: true, addDefaultPage: false, updateFieldAppearances: false, objectsPerTick: 25 });
}
// The same module is the dedicated parsing/optimization worker. Termination cancels it.
if (typeof WorkerGlobalScope !== 'undefined' && globalThis instanceof WorkerGlobalScope) {
  globalThis.onmessage = async ({ data }) => {
    try {
      if (data.action === 'inspect') {
        const pdf = await inspectPdf(data.bytes);
        postMessage({ pages: pdf.getPageCount() });
      } else if (data.action === 'optimize') {
        const bytes = await optimizePdf(data.bytes);
        postMessage({ bytes }, [bytes.buffer]);
      } else throw Error('Opération inconnue.');
    } catch (error) {
      postMessage({ error: /encrypt/i.test(error.message) ? 'PDF chiffré : exportez une copie non protégée depuis votre lecteur PDF.' : error.message });
    }
  };
}
