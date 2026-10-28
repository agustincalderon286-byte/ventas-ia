import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const context = {};
vm.runInNewContext(readFileSync(new URL('../public/coach/product-videos-catalog.js', import.meta.url), 'utf8'), context);
const catalog = context.TecProductVideoCatalog;
const ids = (code, name) => Array.from(catalog.forProduct({ codigo_producto: code, nombre_producto: name }), v => v.id);

test('each approved video has a unique official YouTube watch link and a valid family', () => {
  assert.equal(catalog.videos.length, 43);
  assert.equal(new Set(catalog.videos.map(v => v.id)).size, 43);
  for (const video of catalog.videos) {
    assert.match(video.id, /^[a-zA-Z0-9_-]{11}$/);
    assert.equal(video.url, `https://www.youtube.com/watch?v=${video.id}`);
    assert.ok(catalog.families.some(f => f.id === video.family && f.videoIds.includes(video.id)));
    assert.equal(catalog.byId(video.id), video);
  }
  assert.equal(catalog.byId('unknown'), null);
});

test('blender models never inherit one another\'s videos, even with a misleading name', () => {
  assert.deepEqual(ids('ES0082', 'POWER BLENDER MAX'), ['CWVjdd84sZA']);
  assert.deepEqual(ids('ES4200', 'POWER BLENDER GO'), ['mN2JvoB9aeo']);
  assert.deepEqual(ids('ES4200', 'POWER BLENDER MAX'), ['mN2JvoB9aeo']);
  assert.deepEqual(ids('', 'Power Blender Go Plus'), ['Naoe05OwjW8']);
  assert.deepEqual(ids('UNKNOWN', 'Power Blender Go Plus'), []);
});

test('cookware lines remain separate and ambiguous repeated price-sheet codes fail closed', () => {
  assert.deepEqual(ids('CO9893'), ['4zKG8m3jhkA']);
  assert.deepEqual(ids('CO9846'), ['5G86YsDe6-4']);
  assert.deepEqual(ids('CO7014'), ['aa3c1fX83N8']);
  assert.deepEqual(ids('CO9738'), ['gIRieCFeMQc', 'm7tI3WypyVQ']);
  assert.deepEqual(ids('CO9630', 'SARTEN DE 8 INNOVE'), ['5G86YsDe6-4']);
  assert.deepEqual(ids('CO9630', 'SARTEN GOURMET 8'), ['vwvQFg9pKyo']);
  assert.deepEqual(ids('CO9630', 'SARTEN 8'), []);
});

test('parts, older bowls and unrelated knives cannot inherit whole-product or newer-model videos', () => {
  for (const code of ['AP2704', 'SP0066', 'SP0068', 'PR4902', 'CU0148', 'PR1841', 'PE0029', 'PE0028', 'WF0482', 'WF0485', 'WF0419', 'WF0654']) {
    assert.deepEqual(ids(code, 'Power Blender Max'), [], code);
  }
  for (const name of ['licuadora', 'olla', 'aro de silicona', 'válvula', 'tazón', 'filtro de agua']) {
    assert.deepEqual(ids('', name), [], name);
  }
  assert.ok(ids('WF1200').length === 3);
  assert.deepEqual(ids('CO4900'), ['LggddajOKlA']);
});

test('blender attachments receive only their own model tutorial, never the whole blender or Go Plus', () => {
  assert.deepEqual(ids('PE0050'), ['zXhP18jvRK8']);
  assert.deepEqual(ids('PE0051'), ['GwBDGFtjJno']);
  assert.deepEqual(ids('PE0040'), ['vp1LX7r7pyw', 'gD0q9oD6knE']);
  assert.deepEqual(ids('ES0071'), ['OroIxlvUVTA', '1J5rF6naZ9o']);
  assert.deepEqual(ids('PE0029'), []);
  assert.deepEqual(ids('PE0028'), []);
});

test('FrescaFlow parts share only the cartridge-replacement tutorial, never FrescaPure or air-filter videos', () => {
  for (const code of ['PR2002', 'PR2003', 'PR2004']) assert.deepEqual(ids(code), ['yCRIsyi62GU']);
  for (const code of ['WF0451', 'WF0500', 'WF0530', 'WF0075', 'AP2704']) assert.deepEqual(ids(code), []);
  assert.deepEqual(ids('WF1200'), ['N5UzL3Lgj38', 'psqkTxfZ3Pw', 'yCRIsyi62GU']);
});

test('shared knife care preserves exact model presentations and does not imply a different size or sharpener', () => {
  assert.deepEqual(ids('SP0296'), ['OoMRj50udSY']);
  assert.deepEqual(ids('SP0297'), ['OoMRj50udSY']);
  assert.deepEqual(ids('CU0820'), ['p668z2hXy2c', 'OoMRj50udSY']);
  assert.deepEqual(ids('CU0815'), ['UNd9ca53PW0', 'OoMRj50udSY']);
  assert.deepEqual(ids('CU0056'), ['CuTE0_XE66U']);
  assert.deepEqual(ids('PR1841'), []);
});

test('all 84 reviewed codes retain their explicit audit decision and every mapped video exists', () => {
  const audit = JSON.parse(readFileSync(new URL('../docs/tec-product-video-audit-2026-10-28.json', import.meta.url), 'utf8'));
  assert.equal(audit.reviews.length, 84);
  assert.equal(new Set(audit.reviews.map(r => r.code)).size, 84);
  assert.equal(audit.reviews.filter(r => r.decision === 'verified-match').length, 22);
  for (const row of audit.reviews) {
    assert.deepEqual(ids(row.code, row.catalogName), row.videoIds, row.code);
    assert.ok(row.channelSearches.length > 0);
    assert.ok(row.reason);
  }
  for (const family of catalog.families) {
    for (const id of family.videoIds) assert.ok(catalog.byId(id), `${family.id}: ${id}`);
  }
});

test('close stops playback synchronously and an old close event cannot break a reopened viewer', () => {
  const source = readFileSync(new URL('../public/coach/product-videos.js', import.meta.url), 'utf8');
  let frames = 1, focused = 0;
  const ui = vm.createContext({
    player: { replaceChildren() { frames = 0; }, hidden: false },
    dialog: { open: true, close() { this.open = false; } },
    viewerActive: true, activeVideo: { id: 'first' }, previousOverflow: '',
    opener: { isConnected: true, focus() { focused++; } },
    document: { body: { style: { overflow: 'hidden' } } }
  });
  vm.runInContext(source.slice(source.indexOf('  function stopPlayback()'), source.indexOf('  function play(video)')), ui);
  ui.closeDialog();
  assert.equal(frames, 0, 'sound is stopped without waiting for a browser event');
  assert.equal(ui.document.body.style.overflow, '');
  assert.equal(focused, 1);
  ui.dialog.open = true; ui.viewerActive = true; ui.activeVideo = { id: 'next' };
  ui.document.body.style.overflow = 'hidden'; frames = 1;
  ui.finishClose(); // Delayed event belonging to the previous opening.
  assert.equal(frames, 1);
  assert.equal(ui.activeVideo.id, 'next');
  assert.equal(ui.document.body.style.overflow, 'hidden');
  ui.closeDialog();
  assert.equal(frames, 0);
  assert.equal(ui.document.body.style.overflow, '');
});
