'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const core = import('../tools/pdf-compressor-core.mjs');
const lib = import('../assets/vendor/pdf-lib/pdf-lib.min.mjs');

test('Validation du contenu et des limites de taille', async () => {
  const { validateFile, LIMITS } = await core;
  assert.throws(() => validateFile(0, new Uint8Array()), /non vide/);
  assert.throws(() => validateFile(LIMITS.bytes + 1, new Uint8Array()), /50 Mio/);
  assert.throws(() => validateFile(12, new TextEncoder().encode('not a PDF')), /en-tête/);
  validateFile(12, new TextEncoder().encode('%PDF-1.7\n'));
});
test('Bornes des réglages, dimensions et mémoire', async () => {
  const { pixelSize, validateOptions } = await core;
  validateOptions({ dpi: 120, quality: 75 });
  for (const dpi of [NaN, 71, 201, 100.5]) assert.throws(() => validateOptions({ dpi, quality: 75 }));
  assert.throws(() => validateOptions({ dpi: 120, quality: 100 }));
  assert.deepEqual(pixelSize(612, 792, 72), { width: 612, height: 792, pixels: 484704 });
  for (const dimensions of [[0, 20], [1e6, 1e6], [Infinity, 20], [17000, 1]]) assert.throws(() => pixelSize(...dimensions, 72));
});
test('Aucun faux gain : résultats égaux ou plus gros gardent l’original', async () => {
  const { compareSizes, outputName } = await core;
  assert.deepEqual(compareSizes(1000, 500), { improved: true, size: 500, percent: 50, saved: 500 });
  for (const size of [1000, 2000]) assert.deepEqual(compareSizes(1000, size), { improved: false, size: 1000, percent: 0, saved: 0 });
  assert.equal(outputName('rapport.PDF', true), 'rapport-compresse.pdf');
  assert.equal(outputName('rapport.pdf', false), 'rapport-original.pdf');
});
test('Optimisation réelle : pages, texte, rotation, formulaires et métadonnées', async () => {
  const { optimizePdf } = await core;
  const { PDFDocument, degrees, PDFName } = await lib;
  const pdf = await PDFDocument.create(); pdf.setTitle('Document de test');
  for (let i = 0; i < 4; i++) { const page = pdf.addPage([300, 500]); page.drawText(`Page ${i + 1}`); if (i === 1) page.setRotation(degrees(90)); }
  const field = pdf.getForm().createTextField('nom'); field.setText('Yann'); field.addToPage(pdf.getPage(0));
  const bytes = await pdf.save({ useObjectStreams: false });
  const compressed = await optimizePdf(bytes); assert.ok(compressed.length < bytes.length);
  const result = await PDFDocument.load(compressed);
  assert.equal(result.getPageCount(), 4); assert.equal(result.getTitle(), 'Document de test');
  assert.equal(result.getPage(1).getRotation().angle, 90); assert.equal(result.getPage(0).getWidth(), 300);
  assert.equal(result.getForm().getTextField('nom').getText(), 'Yann');
  assert.ok(result.getPage(0).node.has(PDFName.of('Contents')));
});
test('Un document simple ne gagne pas de formulaire vide lors de la réécriture', async () => {
  const { optimizePdf } = await core; const { PDFDocument, PDFName } = await lib;
  const pdf = await PDFDocument.create(); pdf.addPage();
  const result = await PDFDocument.load(await optimizePdf(await pdf.save()));
  assert.equal(result.catalog.has(PDFName.of('AcroForm')), false);
});
test('Refus des signatures, XFA, documents chiffrés et pages excessives', async () => {
  const { inspectPdf } = await core; const { PDFDocument, PDFName } = await lib;
  for (const dict of [{ Type: 'Sig', ByteRange: [0, 10, 20, 30] }, { FT: 'Sig' }, { XFA: 'unsupported' }]) {
    const pdf = await PDFDocument.create(); pdf.addPage(); pdf.context.register(pdf.context.obj(dict));
    await assert.rejects(inspectPdf(await pdf.save()), /Signature|XFA/);
  }
  const encrypted = await PDFDocument.create(); encrypted.addPage();
  encrypted.context.trailerInfo.Encrypt = encrypted.context.register(encrypted.context.obj({ Filter: 'Standard', V: 1 }));
  await assert.rejects(inspectPdf(await encrypted.save()), /encrypt/i);
  const many = await PDFDocument.create(); for (let i = 0; i < 201; i++) many.addPage();
  await assert.rejects(inspectPdf(await many.save()), /200 pages/);
  await assert.rejects(inspectPdf(new TextEncoder().encode('%PDF-1.7\ninvalid')), /./);
});
