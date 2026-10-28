import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const context = { URL };
vm.runInNewContext(readFileSync(new URL('../public/coach/product-videos-catalog.js', import.meta.url), 'utf8'), context);
const catalog = context.TecProductVideoCatalog;
const tutorialIds = (code, name) => Array.from(catalog.forProduct({ codigo_producto: code, nombre_producto: name }).filter(v => v.type !== 'Recetas'), v => v.id);

test('each approved video has a unique official watch link and a valid family', () => {
  assert.equal(catalog.videos.length, 73);
  assert.equal(new Set(catalog.videos.map(v => v.id)).size, 73);
  for (const video of catalog.videos) {
    if (video.provider === 'hycite') {
      assert.match(video.id, /^hycite-/);
      assert.equal(new URL(video.url).hostname, 'hycite.widen.net');
      assert.equal(catalog.embedUrl(video.id), video.embedUrl);
    } else {
      assert.match(video.id, /^[a-zA-Z0-9_-]{11}$/);
      assert.equal(video.url, `https://www.youtube.com/watch?v=${video.id}`);
    }
    assert.ok(catalog.embedUrl(video.id));
    if (video.id !== 'hycite-5500-uc') assert.ok(catalog.thumbnailUrl(video.id));
    else assert.equal(catalog.thumbnailUrl(video.id), '', 'do not use a countertop photo for the under-counter model');
    assert.ok(catalog.families.some(f => f.id === video.family && f.videoIds.includes(video.id)));
    assert.equal(catalog.byId(video.id), video);
  }
  assert.equal(catalog.byId('unknown'), null);
});

test('blender models never inherit one another\'s videos, even with a misleading name', () => {
  assert.deepEqual(tutorialIds('ES0082', 'POWER BLENDER MAX'), ['CWVjdd84sZA']);
  assert.deepEqual(tutorialIds('ES4200', 'POWER BLENDER GO'), ['mN2JvoB9aeo']);
  assert.deepEqual(tutorialIds('ES4200', 'POWER BLENDER MAX'), ['mN2JvoB9aeo']);
  assert.deepEqual(tutorialIds('', 'Power Blender Go Plus'), ['Naoe05OwjW8']);
  assert.deepEqual(tutorialIds('UNKNOWN', 'Power Blender Go Plus'), []);
});

test('cookware lines remain separate and ambiguous repeated price-sheet codes fail closed', () => {
  assert.deepEqual(tutorialIds('CO9893'), ['4zKG8m3jhkA']);
  assert.deepEqual(tutorialIds('CO9846'), ['5G86YsDe6-4']);
  assert.deepEqual(tutorialIds('CO7014'), ['aa3c1fX83N8']);
  assert.deepEqual(tutorialIds('CO9738'), ['gIRieCFeMQc', 'm7tI3WypyVQ']);
  assert.deepEqual(tutorialIds('CO9630', 'SARTEN DE 8 INNOVE'), ['5G86YsDe6-4']);
  assert.deepEqual(tutorialIds('CO9630', 'SARTEN GOURMET 8'), ['vwvQFg9pKyo']);
  assert.deepEqual(tutorialIds('CO9630', 'SARTEN 8'), []);
});

test('parts, older bowls and unrelated knives cannot inherit whole-product or newer-model videos', () => {
  for (const code of ['AP2704', 'SP0066', 'SP0068', 'PR4902', 'CU0148', 'PE0029', 'PE0028', 'WF0654', 'PR0333']) {
    assert.deepEqual(tutorialIds(code, 'Power Blender Max'), [], code);
  }
  for (const name of ['licuadora', 'olla', 'aro de silicona', 'válvula', 'tazón', 'filtro de agua']) {
    assert.deepEqual(tutorialIds('', name), [], name);
  }
  assert.ok(tutorialIds('WF1200').length === 3);
  assert.deepEqual(tutorialIds('CO4900'), ['LggddajOKlA']);
});

test('blender attachments receive only their own model tutorial, never the whole blender or Go Plus', () => {
  assert.deepEqual(tutorialIds('PE0050'), ['zXhP18jvRK8']);
  assert.deepEqual(tutorialIds('PE0051'), ['GwBDGFtjJno']);
  assert.deepEqual(tutorialIds('PE0040'), ['vp1LX7r7pyw', 'gD0q9oD6knE']);
  assert.deepEqual(tutorialIds('ES0071'), ['OroIxlvUVTA', '1J5rF6naZ9o']);
  assert.deepEqual(tutorialIds('PE0029'), []);
  assert.deepEqual(tutorialIds('PE0028'), []);
});

test('FrescaFlow parts share only the cartridge-replacement tutorial, never FrescaPure or air-filter videos', () => {
  for (const code of ['PR2002', 'PR2003', 'PR2004']) assert.deepEqual(tutorialIds(code), ['yCRIsyi62GU']);
  for (const code of ['WF0451', 'WF0500', 'WF0530', 'WF0075', 'AP2704']) assert.deepEqual(tutorialIds(code), []);
  assert.deepEqual(tutorialIds('WF1200'), ['N5UzL3Lgj38', 'psqkTxfZ3Pw', 'yCRIsyi62GU']);
});

test('shared knife care preserves exact model presentations and does not imply a different size or sharpener', () => {
  assert.deepEqual(tutorialIds('SP0296'), ['OoMRj50udSY']);
  assert.deepEqual(tutorialIds('SP0297'), ['OoMRj50udSY']);
  assert.deepEqual(tutorialIds('CU0820'), ['p668z2hXy2c', 'OoMRj50udSY']);
  assert.deepEqual(tutorialIds('CU0815'), ['UNd9ca53PW0', 'OoMRj50udSY']);
  assert.deepEqual(tutorialIds('CU0056'), ['CuTE0_XE66U']);
  assert.deepEqual(tutorialIds('PR1841'), ['gfIyzRlbXaI']);
});

test('historical audit and explicit priority supplement account for every reviewed code', () => {
  const audit = JSON.parse(readFileSync(new URL('../docs/tec-product-video-audit-2026-10-28.json', import.meta.url), 'utf8'));
  assert.equal(audit.reviews.length, 84);
  assert.equal(new Set(audit.reviews.map(r => r.code)).size, 84);
  assert.equal(audit.reviews.filter(r => r.decision === 'verified-match').length, 22);
  const supplement = JSON.parse(readFileSync(new URL('../docs/tec-priority-video-audit-2026-10-28.json', import.meta.url), 'utf8'));
  assert.equal(supplement.reviews.length, 7);
  assert.equal(supplement.reviews.filter(r => r.decision === 'verified-match').length, 6);
  for (const row of supplement.reviews) {
    assert.deepEqual(tutorialIds(row.code), row.videoIds, row.code);
    assert.ok(row.officialSources.length > 0);
    assert.ok(row.reason);
  }
  for (const row of audit.reviews) {
    const current = supplement.reviews.find(r => r.code === row.code) || row;
    assert.deepEqual(tutorialIds(row.code, row.catalogName), current.videoIds, row.code);
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
    requirements: null, viewerActive: true, activeVideo: { id: 'first' }, previousOverflow: '',
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


test('priority tutorials stay with their exact retail models, never replacement cartridges', () => {
  assert.deepEqual(tutorialIds('CO9222'), ['hycite-wok-innove']);
  assert.deepEqual(tutorialIds('PR2614'), ['y1sz5QIV5oc']);
  assert.deepEqual(tutorialIds('WF0482', '5500 bajo el mostrador'), ['hycite-5500-ct']);
  assert.deepEqual(tutorialIds('WF0485', '5500 sobre el mostrador'), ['hycite-5500-uc']);
  assert.deepEqual(tutorialIds('WF0419'), ['hycite-3500']);
  for (const code of ['WF0451', 'WF0500', 'WF0530', 'WF0075', 'PR0333']) assert.deepEqual(tutorialIds(code), []);
});

test('player URLs resolve only trusted canonical IDs; external callers cannot supply an iframe URL', () => {
  assert.equal(catalog.embedUrl('unknown'), '');
  assert.equal(catalog.thumbnailUrl('unknown'), '');
  assert.equal(catalog.embedUrl({id:'hycite-3500', embedUrl:'https://example.com'}), '');
  assert.match(catalog.embedUrl('hycite-3500'), /^https:\/\/hycite\.widen\.net\/view\/video\/kn6ykc3sxg\//);
  assert.equal(catalog.thumbnailUrl('hycite-3500'), '/coach/assets/catalog/innove/products/frescapure-3500.webp');
  assert.ok(Object.isFrozen(catalog.byId('hycite-3500')));
  const source = readFileSync(new URL('../public/coach/product-videos.js', import.meta.url), 'utf8');
  assert.ok(source.includes('const source = catalog.embedUrl(video.id)'));
  assert.ok(source.includes('frame.src = source'));
});

test('untrusted provider hosts, protocols, paths and posters are rejected even in a malformed catalog', () => {
  const source = readFileSync(new URL('../public/coach/product-videos-catalog.js', import.meta.url), 'utf8');
  const valid = catalog.byId('hycite-3500').embedUrl;
  for (const bad of ['https://hycite.widen.net.evil.test/view/video/kn6ykc3sxg/a.mp4', 'http://hycite.widen.net/view/video/kn6ykc3sxg/a.mp4', 'javascript:alert(1)', 'https://hycite.widen.net/other/a.mp4', 'https://u:p@hycite.widen.net/view/video/kn6ykc3sxg/a.mp4']) {
    const altered = {URL}; vm.runInNewContext(source.replace(valid, bad), altered);
    assert.equal(altered.TecProductVideoCatalog.embedUrl('hycite-3500'), '', bad);
  }
  const badPoster = {URL}; vm.runInNewContext(source.replace('"thumbnail": "/coach/assets/catalog/innove/products/frescapure-3500.webp"', '"thumbnail": "https://example.com/tracker"'), badPoster);
  assert.equal(badPoster.TecProductVideoCatalog.thumbnailUrl('hycite-3500'), '');
});


const recipeIds = code => Array.from(catalog.forProduct({ codigo_producto: code }).filter(v => v.type === 'Recetas'), v => v.id);

test('all 24 user-approved recipes retain official links, requirements and explicit product associations', () => {
  const audit = JSON.parse(readFileSync(new URL('../docs/tec-recipe-video-audit-2026-10-28.json', import.meta.url), 'utf8'));
  const recipes = catalog.videos.filter(v => v.type === 'Recetas');
  assert.equal(recipes.length, 24);
  assert.equal(catalog.videos.filter(v => v.type !== 'Recetas').length, 49);
  assert.equal(audit.recipes.length, 24);
  assert.equal(new Set(audit.recipes.map(r => r.id)).size, 24);
  for (const row of audit.recipes) {
    const video = catalog.byId(row.id);
    assert.equal(video.type, 'Recetas'); assert.equal(video.title, row.title);
    assert.ok(video.requirements.length > 5);
    assert.equal(row.authorUrl, 'https://www.youtube.com/@RoyalPrestigeMXOficial');
    assert.ok(row.sources.includes(video.url));
    assert.ok(row.codes.length > 0, `${row.id} must be reachable from a real product`);
    for (const code of row.codes) assert.ok(recipeIds(code).includes(row.id), `${code}: ${row.id}`);
  }
});

test('recipe capacities and skillet sizes never leak into another model or pot', () => {
  assert.deepEqual(recipeIds('CO1253'), ['R57vXzfvnOI']);
  assert.deepEqual(recipeIds('CO1258'), ['iH3BuWHXIGQ']);
  assert.deepEqual(recipeIds('CO9736'), ['S9e6fkvYX4s']);
  assert.deepEqual(recipeIds('CO9739'), ['S9e6fkvYX4s']);
  assert.deepEqual(recipeIds('CO9738'), ['wp7e0izJSOs', 'S9e6fkvYX4s', '_p1vb_ffosM']);
  assert.deepEqual(recipeIds('CO9740'), recipeIds('CO9738'));
  for (const code of ['CO9780', 'CO9777', 'CO9570', 'CO9580', 'CO9573', 'CO9232', 'CO9222', 'ES0071']) assert.deepEqual(recipeIds(code), [], code);
  for (const code of ['CO9590', 'CO9893', 'CO9563', 'CO9883', 'CO9554']) assert.deepEqual(recipeIds(code), ['h0I_lgJ1758', 'xdAOOykr3Nk']);
});

test('recipe assignments preserve blender models, accessory requirements and old/new bowl distinctions', () => {
  assert.deepEqual(recipeIds('ES0082'), ['5tCU3oBmKss', 'DhcwmfZ-jVU']);
  assert.deepEqual(recipeIds('ES4200'), ['HDhDwSA8628', 'wp7e0izJSOs']);
  assert.match(catalog.byId('HDhDwSA8628').requirements, /recipiente picador Go Plus/);
  assert.match(catalog.byId('wp7e0izJSOs').requirements, /jarra Go Plus.*10 pulgadas/);
  assert.deepEqual(recipeIds('CO4900'), ['ULpGq-7tooA']);
  for (const code of ['PR4902', 'SP0066', 'SP0068', 'PE0028', 'PE0029', 'PE0050', 'PE0051', 'PR2002', 'UNKNOWN']) assert.deepEqual(recipeIds(code), [], code);
  assert.deepEqual(recipeIds('CO4911'), ['REj6w1WIzcw', '467WPWSALpg', 'a79DV18g4Eo']);
  assert.deepEqual(recipeIds('PR2614'), ['y0E9xvDDhjo']);
});

test('recipe filters combine type, product category and accent-insensitive search', () => {
  const filter = options => Array.from(catalog.filterVideos(catalog.videos, options), v => v.id);
  assert.equal(filter({type:'Recetas'}).length, 24);
  assert.deepEqual(filter({type:'Recetas', query:'CAFÉ'}), ['Fg1T6lbrlbg', 'D5c5W1UBCKQ', 'rxSm2HLkSuE']);
  assert.equal(filter({type:'Recetas', category:'Licuadoras'}).length, 4);
  assert.deepEqual(filter({type:'Recetas', category:'Licuadoras', query:'picador'}), ['HDhDwSA8628']);
  assert.deepEqual(filter({type:'Recetas', query:'not-a-real-dish'}), []);
  assert.equal(filter({type:'Uso y cuidados'}).some(id => catalog.byId(id).type === 'Recetas'), false);
  assert.equal(filter({}).length, 73);
});
