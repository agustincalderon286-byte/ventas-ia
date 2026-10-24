import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('../public/prototipo-agustin20-chef-ar/index.html', import.meta.url), 'utf8');
const script = html.match(/<script>\s*([\s\S]*?)<\/script>/)[1];
function harness(product = 'maleta') {
  const elements = new Map();
  const node = id => ({ id, attrs: {}, style: {}, handlers: {}, dataset: {}, loaded: false,
    classList: { toggle() {} }, textContent: '', hidden: false, disabled: false,
    setAttribute(k, v) { this.attrs[k] = v; }, removeAttribute(k) { delete this.attrs[k]; },
    toggleAttribute(k, value) { if (value) this.attrs[k] = ''; else delete this.attrs[k]; },
    addEventListener(k, f) { this.handlers[k] = f; },
    fire(k, e = {}) { return this.handlers[k]?.(e); },
    click() { this.clicked = true; }, scrollIntoView() {},
  });
  const get = id => { if (!elements.has(id)) elements.set(id, node(id)); return elements.get(id); };
  const cards = ['maleta', 'novel', 'multipan', 'royalprestige', 'revoexample'].map(id => {
    const card = node(id); card.dataset.productId = id; return card;
  });
  const location = new URL(`https://example.test/prototipo-agustin20-chef-ar/?producto=${product}`);
  const context = {
    document: { getElementById: get, querySelectorAll: () => cards },
    location, URL, URLSearchParams, history: { replaceState() {} },
    customElements: { whenDefined: async () => {} },
    window: { navigator: { userAgent: 'Desktop', platform: 'MacIntel', maxTouchPoints: 0 } },
  };
  vm.runInNewContext(script, context);
  return { get, cards, open: () => vm.runInNewContext('openActiveAr()', context) };
}

test('direct link loads the case privately and preserves existing product selection', () => {
  const h = harness(), viewer = h.get('arViewer');
  assert.match(viewer.attrs.src, /maleta-royal-prestige-ar-v3\.glb$/);
  assert.equal(viewer.attrs['ar-modes'], 'webxr quick-look');
  assert.equal(viewer.attrs['ios-src'], undefined, 'Quick Look must generate USDZ from the authenticated loaded asset');
  assert.equal(h.get('interactiveCaseLink').hidden, false);
  assert.equal(h.get('openArButton').disabled, true);
  viewer.loaded = true;
  viewer.fire('load');
  assert.equal(h.get('openArButton').disabled, false);
  h.cards.find(c => c.dataset.productId === 'novel').fire('click');
  assert.match(viewer.attrs['ios-src'], /novel\.usdz$/);
  assert.equal(viewer.attrs['ar-modes'], 'webxr scene-viewer quick-look');
  assert.equal(h.get('interactiveCaseLink').hidden, true);
  h.cards.find(c => c.dataset.productId === 'multipan').fire('click');
  assert.equal(viewer.style.display, 'none');
  assert.match(h.get('quickLookLink').attrs.href, /multipan\.usdz$/);
});

test('AR gives useful feedback for loading, unsupported devices and launch errors', async () => {
  const h = harness(), viewer = h.get('arViewer'), status = h.get('viewerStatus');
  await h.open(); assert.match(status.textContent, /Espera/);
  viewer.loaded = true; viewer.canActivateAR = false;
  await h.open(); assert.match(status.textContent, /Safari/);
  viewer.canActivateAR = true;
  viewer.activateAR = async () => { throw new Error('permission denied'); };
  await h.open(); assert.match(status.textContent, /No se pudo/);
  viewer.activateAR = async () => {};
  await h.open(); assert.equal(status.textContent, '');
  viewer.fire('error'); assert.equal(h.get('openArButton').disabled, true);
});

test('invalid product query falls back to Novel', () => {
  for (const query of ['missing', '__proto__', 'constructor']) {
    assert.match(harness(query).get('arViewer').attrs.src, /novel\.glb$/);
  }
});

test('AR model contains all seven parts with no animation, studio ground or required decoder', () => {
  const buffer = fs.readFileSync(new URL('../public/prototipo-agustin20-chef-ar/assets/maleta-royal-prestige-ar-v3.glb', import.meta.url));
  assert.equal(buffer.toString('ascii', 0, 4), 'glTF');
  assert.equal(buffer.readUInt32LE(8), buffer.length);
  const data = JSON.parse(buffer.toString('utf8', 20, 20 + buffer.readUInt32LE(12)));
  assert.deepEqual(data.nodes.filter(n => n.extras?.pieceId).map(n => n.extras.pieceId).sort(),
    ['multipan', 'multipan_tapa', 'olla_3', 'olla_6', 'tapa_3', 'tapa_6', 'vaporera']);
  assert.equal(data.animations?.length || 0, 0);
  assert.equal(data.cameras?.length || 0, 0);
  assert.ok(!data.nodes.some(n => /Suelo de estudio/.test(n.name)));
  assert.ok(!(data.extensionsRequired || []).some(x => /draco|meshopt/.test(x)));
  assert.ok(data.images.every(image => image.bufferView !== undefined), 'Textures must be embedded for private AR');
});

test('camera permission remains behind the PIN gate', () => {
  const source = fs.readFileSync(new URL('../server.js', import.meta.url), 'utf8');
  const start = source.indexOf('app.use(AR_PROTOTYPE_PATH_PREFIX,');
  const middleware = source.slice(start, source.indexOf('\n});', start) + 4);
  const context = { AR_PROTOTYPE_PATH_PREFIX: '/private-ar',
    arPrototypeTieneAcceso: req => req.authenticated,
    renderArPrototypeLockPage: () => 'PIN required', app: { use: (_path, fn) => { context.handler = fn; } } };
  vm.runInNewContext(middleware, context);
  for (const authenticated of [false, true]) {
    let next = false;
    const response = { headers: {}, setHeader(k, v) { this.headers[k] = v; },
      status(code) { this.code = code; return this; }, type() { return this; }, send(body) { this.body = body; } };
    context.handler({ authenticated }, response, () => { next = true; });
    assert.equal(next, authenticated);
    if (authenticated) assert.match(response.headers['Permissions-Policy'], /camera=\(self\), xr-spatial-tracking=\(self\)/);
    else { assert.equal(response.code, 401); assert.equal(response.headers['Permissions-Policy'], undefined); }
  }
});
