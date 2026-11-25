'use strict';
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { chromium } = require('playwright');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
let browser, server, url;
const root = path.resolve(__dirname, '../..');
const output = process.env.PDF_SCREENSHOTS || '/tmp/pdf-compressor-screenshots';
before(async () => {
  server = http.createServer((req, res) => {
    const pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
    const file = path.resolve(root, '.' + pathname);
    if (!file.startsWith(root + path.sep)) { res.writeHead(403).end(); return; }
    try {
      const data = fs.readFileSync(file);
      res.setHeader('Content-Type', ({ '.html': 'text/html', '.mjs': 'text/javascript', '.js': 'text/javascript', '.css': 'text/css', '.wasm': 'application/wasm' })[path.extname(file)] || 'application/octet-stream'); res.end(data);
    } catch { res.writeHead(404).end(); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  url = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}) });
  fs.mkdirSync(output, { recursive: true });
});
after(async () => { await browser?.close(); if (server) await new Promise(resolve => server.close(resolve)); });
async function open(viewport = { width: 1440, height: 1000 }) {
  const page = await browser.newPage({ viewport }); const errors = [], requests = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('console', msg => { if (msg.type() === 'error') errors.push(msg.text()); });
  page.on('request', req => requests.push({ url: req.url(), method: req.method() }));
  await page.goto(url + '/tools/pdf-compressor.html');
  try {
    await page.waitForFunction(() => !document.getElementById('file').disabled || !document.getElementById('error').hidden, null, { timeout: 15000 });
    assert.equal(await page.locator('#error').isVisible(), false, await page.locator('#error').textContent());
  } catch (error) {
    await page.screenshot({ path: path.join(output, 'startup-failure.png'), fullPage: true });
    const status = await page.locator('#status').textContent(); await page.close();
    throw Error(`${error.message} | ${status} | ${errors.join('; ')}`);
  }
  return { page, errors, requests };
}
async function fixture(page, kind = 'text') {
  return Buffer.from(await page.evaluate(async kind => {
    const { PDFDocument, degrees, PDFName } = await import('../assets/vendor/pdf-lib/pdf-lib.min.mjs');
    const pdf = await PDFDocument.create(); pdf.setTitle('Test PDF Studio');
    const p = pdf.addPage([400, 600]); p.drawText('Document de verification', { x: 20, y: 560, size: 16 });
    if (kind === 'scan') {
      const canvas = document.createElement('canvas'); canvas.width = 1600; canvas.height = 2400;
      const ctx = canvas.getContext('2d'); ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, 1600, 2400);
      // Seeded noise makes a large scan without committing binary fixtures.
      let seed = 7; const data = ctx.getImageData(0, 0, 1600, 2400);
      for (let i = 0; i < data.data.length; i += 4) { seed = (Math.imul(seed, 1664525) + 1013904223) | 0; const gray = 210 + ((seed >>> 24) % 45); data.data[i] = gray; data.data[i + 1] = gray; data.data[i + 2] = gray; }
      ctx.putImageData(data, 0, 0); ctx.fillStyle = '#222'; ctx.font = '70px sans-serif'; ctx.fillText('SCAN : texte lisible', 100, 250);
      ctx.fillStyle = '#5864ce'; ctx.fillRect(100, 450, 1300, 300);
      const image = await pdf.embedJpg(await (await new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', .98))).arrayBuffer());
      p.drawImage(image, { x: 0, y: 0, width: 400, height: 600 });
      const second = pdf.addPage([300, 500]); second.drawImage(image, { x: 0, y: 0, width: 300, height: 500 }); second.setRotation(degrees(90));
    } else if (kind === 'signed') pdf.context.register(pdf.context.obj({ Type: 'Sig', ByteRange: [0, 1, 2, 3] }));
    else if (kind === 'encrypted') pdf.context.trailerInfo.Encrypt = pdf.context.register(pdf.context.obj({ Filter: 'Standard', V: 1 }));
    else if (kind === 'many') for (let i = 0; i < 30; i++) pdf.addPage().drawText('Page de test');
    else if (kind === 'huge') p.setSize(20000, 20000);
    if (kind === 'form') { const f = pdf.getForm().createTextField('name'); f.setText('Test'); f.addToPage(p, { x: 20, y: 200 }); }
    return Array.from(await pdf.save({ useObjectStreams: kind === 'tiny' }));
  }, kind));
}
async function load(page, bytes, name = 'document.pdf') {
  await page.locator('#file').setInputFiles({ name, mimeType: 'application/pdf', buffer: bytes });
  await page.waitForFunction(() => !document.getElementById('file').disabled);
}
async function compress(page) {
  await page.locator('#compress').click();
  await page.waitForFunction(() => !document.getElementById('result').hidden || !document.getElementById('error').hidden, null, { timeout: 60000 });
  assert.equal(await page.locator('#error').isVisible(), false, await page.locator('#error').textContent());
}
async function resultBytes(page) {
  const pending = page.waitForEvent('download'); await page.locator('#download').click();
  return fs.readFileSync(await (await pending).path());
}
test('Optimisation réelle et structure du document ; pas de requête externe', async () => {
  const { page, errors, requests } = await open();
  try {
    await load(page, await fixture(page, 'form'), '<rapport>.pdf');
    assert.equal(await page.locator('#fileName').textContent(), '<rapport>.pdf');
    await compress(page); const bytes = await resultBytes(page);
    const { PDFDocument } = await import('../../assets/vendor/pdf-lib/pdf-lib.min.mjs');
    const pdf = await PDFDocument.load(bytes); assert.equal(pdf.getForm().getTextField('name').getText(), 'Test'); assert.equal(pdf.getTitle(), 'Test PDF Studio');
    assert.ok(requests.every(req => req.method === 'GET' && (req.url.startsWith(url) || req.url.startsWith('blob:')))); assert.deepEqual(errors, []);
    await page.screenshot({ path: path.join(output, 'desktop-light.png'), fullPage: true });
  } finally { await page.close(); }
});
test('Scan : consentement, aperçu, compression, orientation, texte absent et navigation', async () => {
  const { page, errors, requests } = await open();
  try {
    const input = await fixture(page, 'scan'); await load(page, input);
    await page.locator('#mode').selectOption('raster'); assert.ok(await page.locator('#compress').isDisabled());
    await page.locator('#preview').click(); await page.locator('#afterImage').waitFor({ state: 'visible' });
    await page.waitForFunction(() => !document.getElementById('preview').disabled);
    await page.locator('#acceptLoss').check(); await page.locator('#target').fill('0.1');
    await compress(page); const result = await resultBytes(page); assert.ok(result.length < input.length * .7);
    const { PDFDocument } = await import('../../assets/vendor/pdf-lib/pdf-lib.min.mjs'); const pdf = await PDFDocument.load(result);
    assert.equal(pdf.getPageCount(), 2); assert.equal(pdf.getPage(0).getWidth(), 400); assert.equal(pdf.getPage(1).getWidth(), 500); assert.equal(pdf.getPage(1).getHeight(), 300);
    const texts = await page.evaluate(async bytes => { const lib = await import('../assets/vendor/pdfjs/pdf.min.mjs'); const task = lib.getDocument({ data: new Uint8Array(bytes) }); const doc = await task.promise; const text = await (await doc.getPage(1)).getTextContent(); await task.destroy(); return text.items.length; }, [...result]); assert.equal(texts, 0);
    await page.locator('#theme').click(); await page.locator('#theme').click(); assert.equal(await page.locator('html').getAttribute('data-theme'), 'dark'); await page.screenshot({ path: path.join(output, 'desktop-dark.png'), fullPage: true });
    await page.locator('#next').click(); await page.waitForFunction(() => document.getElementById('pageLabel').textContent.includes('2 / 2'));
    assert.equal(await page.locator('#afterFigure').isVisible(), false);
    await page.locator('#preset').selectOption('small'); assert.equal(await page.locator('#result').isVisible(), false);
    await page.locator('#grayscale').check(); await compress(page);
    assert.deepEqual(errors, []); assert.ok(requests.every(req => req.url.startsWith(url) || req.url.startsWith('blob:')));
  } finally { await page.close(); }
});
test('Absence de gain : téléchargement original octet pour octet', async () => {
  const { page } = await open();
  try { const bytes = await fixture(page, 'tiny'); await load(page, bytes); await page.locator('#mode').selectOption('raster'); await page.locator('#acceptLoss').check(); await compress(page); assert.match(await page.locator('#resultNote').textContent(), /Aucun gain/); assert.deepEqual(await resultBytes(page), bytes); } finally { await page.close(); }
});
test('Erreurs, fichier invalide, signature et chiffrement ; récupération ensuite', async () => {
  const { page } = await open();
  try {
    await load(page, Buffer.from('Not a PDF')); assert.match(await page.locator('#error').textContent(), /en-tête/); assert.ok(await page.locator('#compress').isDisabled());
    await load(page, await fixture(page, 'signed')); assert.match(await page.locator('#error').textContent(), /Signature/);
    await load(page, await fixture(page, 'encrypted')); assert.match(await page.locator('#error').textContent(), /chiffré/);
    await load(page, await fixture(page)); await compress(page);
  } finally { await page.close(); }
});
test('Annulation puis retrait et nouveau traitement', async () => {
  const { page } = await open();
  try {
    await load(page, await fixture(page, 'many')); await page.locator('#mode').selectOption('raster'); await page.locator('#acceptLoss').check();
    await page.locator('#compress').click(); await page.locator('#cancel').click(); await page.waitForFunction(() => !document.getElementById('file').disabled);
    assert.equal(await page.locator('#result').isVisible(), false); assert.match(await page.locator('#status').textContent(), /annulé/i);
    await page.locator('#clear').click(); await page.waitForFunction(() => !document.getElementById('file').disabled); assert.equal(await page.locator('#fileInfo').isVisible(), false);
    await load(page, await fixture(page)); await page.locator('#mode').selectOption('optimize'); await compress(page);
  } finally { await page.close(); }
});
test('Mobile 390 et 320 px : pas de débordement, thème, aperçu et téléchargement', async () => {
  const { page, errors } = await open({ width: 390, height: 844 });
  try {
    await load(page, await fixture(page, 'scan')); await page.locator('#mode').selectOption('raster'); await page.locator('#acceptLoss').check(); await compress(page);
    for (const width of [390, 320]) { await page.setViewportSize({ width, height: 844 }); await page.screenshot({ path: path.join(output, `mobile-${width}.png`), fullPage: true }); const overflow = await page.evaluate(() => [...document.querySelectorAll('body *')].filter(el => el.getBoundingClientRect().right > innerWidth + 1).map(el => `${el.tagName}#${el.id}.${el.className}:${el.getBoundingClientRect().right}`).slice(0, 20)); assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), JSON.stringify(overflow)); }
    const download = page.waitForEvent('download'); await page.locator('#download').click(); assert.match((await download).suggestedFilename(), /compresse.pdf$/); assert.deepEqual(errors, []);
  } finally { await page.close(); }
});
