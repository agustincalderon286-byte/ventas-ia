import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { existsSync } from 'node:fs';
import { buildAudiobookSeo, registerAudiobookSeo, bookPath, collectionPath, sampleDuration, isoDuration } from '../src/tec-audiobook-seo.js';
import { audioCollections, bookEditorial } from '../src/knowledge/audiobook-editorial.js';
import { audiobookCatalog } from '../src/tec-audiobooks.js';

const origin = 'https://totalelitecoaching.com';
const schemas = html => [...html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)].map(match => JSON.parse(match[1]));
const generated = buildAudiobookSeo({ publicOrigin: origin });

test('HTML includes the entire public catalogue without JS and links to all canonical book pages', () => {
  assert.equal(schemas(generated.landing).length, 1);
  const graph = schemas(generated.landing)[0]['@graph'];
  const list = graph.find(item => item['@type'] === 'ItemList');
  assert.equal(list.numberOfItems, audiobookCatalog.length);
  assert.equal(list.itemListElement.length, audiobookCatalog.length);
  assert.equal((generated.landing.match(/<article class="book">/g) || []).length, audiobookCatalog.length);
  for (const book of audiobookCatalog) {
    assert.ok(generated.landing.includes(book.title));
    assert.ok(generated.landing.includes(`href="${bookPath(book)}"`));
  }
  assert.match(generated.landing, /<link rel="canonical" href="https:\/\/totalelitecoaching.com\/audiolibros\/">/);
  assert.match(generated.landing, /<h1>Grandes ideas/);
  assert.match(generated.landing, /Audiolibros en español/);
  assert.ok(!generated.landing.includes('Cargando audiolibros'));
});

test('each indexable book describes the actual edition, keeps prices truthful and exposes only the public sample', () => {
  for (const book of audiobookCatalog) {
    const html = generated.pages.get(book.id);
    assert.match(html, /<html lang="es">/);
    assert.equal((html.match(/<h1>/g) || []).length, 1);
    assert.ok(html.includes(book.description));
    assert.ok(html.includes(origin + bookPath(book)));
    assert.ok(html.includes('og:image'));
    const graph = schemas(html)[0]['@graph'];
    const audio = graph.find(item => item['@type'] === 'Audiobook');
    assert.equal(audio.name, book.title);
    assert.equal(audio.duration, isoDuration(book.duration));
    assert.equal(audio.requiresSubscription, true);
    assert.equal(audio.isAccessibleForFree, false);
    assert.equal(audio.offers.price, '20.00');
    assert.equal(audio.offers.priceSpecification.billingDuration, 'P1M');
    assert.equal(audio.hasPart.isAccessibleForFree, true);
    assert.equal(audio.hasPart.duration, isoDuration(sampleDuration(book)));
    assert.ok(existsSync(new URL('../public' + new URL(audio.hasPart.contentUrl).pathname, import.meta.url)));
    assert.ok(existsSync(new URL('../public' + new URL(audio.image).pathname, import.meta.url)));
    for (const name of ['r2Key', 'sha256', 'fileId', 'stripeCustomerId', 'cloudflarestorage.com', `/api/audiolibros/libros/${book.id}/audio`]) assert.ok(!html.includes(name), name);
    const decoded = html.replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>');
    for (const chapter of book.chapters) assert.ok(decoded.includes(chapter.title));
  }
});

test('sitemap and crawler guide list only canonical public pages with no invented update dates or private URLs', () => {
  const urls = [...generated.sitemap.matchAll(/<loc>(.*?)<\/loc>/g)].map(x => x[1]);
  assert.equal(urls.length, audiobookCatalog.length + audioCollections.length + 1);
  assert.equal(new Set(urls).size, urls.length);
  assert.ok(!generated.sitemap.includes('lastmod'));
  for (const url of urls) assert.ok(url.startsWith(origin + '/audiolibros/') && !url.includes('?'));
  assert.match(generated.robots, /User-agent: \*\nAllow: \/audiolibros\//);
  assert.match(generated.robots, /Allow: \/coach\/assets\/audiobooks\//);
  assert.match(generated.robots, /Disallow: \/api\//);
  assert.match(generated.robots, /Sitemap: https:\/\/totalelitecoaching.com\/sitemap.xml/);
  for (const book of audiobookCatalog) assert.ok(generated.llms.includes(origin + bookPath(book)));
  assert.ok(!generated.llms.includes('r2Key'));
});

test('all generated markup escapes catalogue content and verifies ownership metadata without injection', () => {
  const hostile = { ...audiobookCatalog[0], title: '<img src=x onerror=alert(1)>', description: '</script><script>alert(1)</script>' };
  const result = buildAudiobookSeo({ catalog: [hostile], googleVerification: '\"><script>bad</script>', hasSample: () => false });
  for (const html of [result.landing, result.pages.get(hostile.id)]) {
    assert.ok(!html.includes('<img src=x onerror=alert(1)>'));
    assert.ok(!html.includes('<script>alert(1)</script>'));
    assert.ok(!html.includes('<script>bad</script>'));
    assert.equal(schemas(html).length, 1);
  }
});

test('HTTP pages, redirects and crawlers work without database or user sessions and return real 404s', async t => {
  const app = express(); registerAudiobookSeo(app);
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const base = 'http://127.0.0.1:' + server.address().port;
  for (const url of ['/audiolibros/', ...audiobookCatalog.map(bookPath), ...audioCollections.map(collectionPath), '/robots.txt', '/sitemap.xml', '/llms.txt']) {
    const r = await fetch(base + url); assert.equal(r.status, 200, url);
    assert.match(r.headers.get('cache-control'), /^public/);
    assert.equal(r.headers.get('set-cookie'), null);
    assert.ok((await r.text()).length > 200);
  }
  const redirect = await fetch(base + '/audiolibros/index.html?libro=el-kybalion&t=30', { redirect: 'manual' });
  assert.equal(redirect.status, 301); assert.equal(redirect.headers.get('location'), '/audiolibros/?libro=el-kybalion&t=30');
  const bookRedirect = await fetch(base + '/audiolibros/libros/el-kybalion', { redirect: 'manual' });
  assert.equal(bookRedirect.status, 301); assert.equal(bookRedirect.headers.get('location'), '/audiolibros/libros/el-kybalion/');
  const missing = await fetch(base + '/audiolibros/libros/does-not-exist/');
  assert.equal(missing.status, 404); assert.equal(missing.headers.get('x-robots-tag'), 'noindex');
  assert.equal((await fetch(base + '/audiolibros/colecciones/no-existe/')).status, 404);
  const c = await fetch(base + '/audiolibros/colecciones/biblia-en-audio', { redirect: 'manual' });
  assert.equal(c.status, 301); assert.equal(c.headers.get('location'), '/audiolibros/colecciones/biblia-en-audio/');
});

test('all eight books have original complete editorial notes, unique questions and collection links', () => {
  assert.equal(Object.keys(bookEditorial).length, 8);
  const questions = new Set();
  for (const book of audiobookCatalog) {
    const notes = bookEditorial[book.id], html = generated.pages.get(book.id);
    assert.equal(notes.summary.length, 2); assert.ok(notes.summary.join(' ').length > 550);
    assert.equal(notes.ideas.length, 3); assert.equal(notes.questions.length, 3);
    assert.ok(notes.audience.length > 100 && notes.tip.length > 100);
    for (const p of [...notes.summary, ...notes.ideas, notes.audience, notes.tip]) assert.ok(html.includes(p));
    for (const [q, a] of notes.questions) { assert.ok(!questions.has(q), q); questions.add(q); assert.ok(a.length > 70); }
    assert.ok(audioCollections.some(c => c.bookIds.includes(book.id)));
    assert.ok(html.includes('Encuéntralo en estas colecciones'));
  }
});

test('collections have real books, useful context, reciprocal links and truthful Bible coverage', () => {
  assert.equal(generated.collectionPages.size, 4);
  for (const c of audioCollections) {
    const html = generated.collectionPages.get(c.id);
    assert.ok(c.bookIds.length >= 2);
    assert.ok(html.includes(c.intro)); assert.ok(html.includes(c.route));
    assert.ok(generated.landing.includes(collectionPath(c))); assert.ok(generated.llms.includes(collectionPath(c)));
    assert.equal((html.match(/<article class="book">/g) || []).length, c.bookIds.length);
    const list = schemas(html)[0]['@graph'].find(x => x['@type'] === 'ItemList');
    assert.equal(list.numberOfItems, c.bookIds.length);
    for (const id of c.bookIds) { assert.ok(audiobookCatalog.some(b => b.id === id)); assert.ok(html.includes(bookPath({ id }))); assert.ok(generated.pages.get(id).includes(collectionPath(c))); }
  }
  const bible = generated.collectionPages.get('biblia-en-audio');
  assert.ok(bible.includes('no presenta todavía la Biblia completa'));
  assert.ok(bible.includes('Reina-Valera 1909'));
});
