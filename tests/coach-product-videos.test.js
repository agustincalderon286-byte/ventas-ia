import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const context = {};
vm.runInNewContext(readFileSync(new URL('../public/coach/product-videos-catalog.js', import.meta.url), 'utf8'), context);
const catalog = context.TecProductVideoCatalog;
const ids = (code, name) => Array.from(catalog.forProduct({ codigo_producto: code, nombre_producto: name }), v => v.id);

test('each approved video has a unique official YouTube watch link and a valid family', () => {
  assert.equal(catalog.videos.length, 23);
  assert.equal(new Set(catalog.videos.map(v => v.id)).size, 23);
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
  assert.deepEqual(ids('CO9630', 'SARTEN GOURMET 8'), []);
});

test('parts, older bowls and unrelated knives cannot inherit whole-product or newer-model videos', () => {
  for (const code of ['PR2002', 'PR2003', 'AP2704', 'SP0066', 'SP0068', 'PR4902', 'CU0148', 'CU0056', 'PE0051', 'PE0029']) {
    assert.deepEqual(ids(code, 'Power Blender Max'), [], code);
  }
  for (const name of ['licuadora', 'olla', 'aro de silicona', 'válvula', 'tazón', 'filtro de agua']) {
    assert.deepEqual(ids('', name), [], name);
  }
  assert.ok(ids('WF1200').length === 3);
  assert.deepEqual(ids('CO4900'), ['LggddajOKlA']);
});
