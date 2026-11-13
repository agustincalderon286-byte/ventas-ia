import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { SOCIAL_PIECES, SOCIAL_NETWORKS, socialCampaign, socialLink, socialReferral } from '../public/audiolibros/social-campaigns.js';
import { safeCampaign, safeCampaignFields, campaignEventParams } from '../public/audiolibros/analytics-events.js';
import { audiobookCatalog } from '../src/tec-audiobooks.js';
import { campaignQuery, socialTarget, registerAudioSocial } from '../src/tec-audio-social.js';

test('every registered publication has a unique, nonprivate link and bounded attribution that survives serialization', () => {
  assert.equal(new Set(SOCIAL_PIECES.map(p => p.id)).size, SOCIAL_PIECES.length);
  const books = new Set(audiobookCatalog.map(b => b.id));
  for (const piece of SOCIAL_PIECES) {
    assert.match(piece.id, /^[a-z0-9_]{1,64}$/); assert.ok(piece.title.length <= 100);
    if (piece.bookId) assert.ok(books.has(piece.bookId));
    if (piece.availableFrom) assert.ok(Number.isFinite(Date.parse(piece.availableFrom)), piece.id + ' has a valid publication time');
    for (const source of piece.source === 'meta' ? ['fb','ig','msg','an'] : [piece.source]) {
      const campaign = socialCampaign(source, piece.id);
      assert.deepEqual(safeCampaign(campaignQuery(campaign)), campaign);
      assert.deepEqual(safeCampaignFields(campaign), campaign);
      const dimensions = campaignEventParams(campaign);
      assert.equal(dimensions.tec_audio_content, piece.id); assert.equal(dimensions.tec_audio_piece, piece.title);
      assert.ok(socialLink(source, piece.id)); assert.ok(socialTarget(source, piece.id));
    }
  }
});
test('unknown or cross-network publication labels, duplicate parameters and personal input cannot become attribution', () => {
  for (const [source, piece] of [['instagram','tt_7707440558070975775'],['tiktok','PRIVATE@example.test'],['toString','perfil']]) {
    assert.deepEqual(socialCampaign(source, piece), {}); assert.equal(socialTarget(source, piece), null);
  }
  for (const query of [
    'utm_source=tiktok&utm_medium=social&utm_campaign=tec_redes_es&utm_content=PRIVATE@example.test',
    'utm_source=tiktok&utm_medium=social&utm_campaign=tec_redes_es&utm_content=tt_7707440558070975775&utm_content=SECRET',
    'utm_source=tiktok&utm_medium=paid_social&utm_campaign=tec_redes_es&utm_content=tt_7707440558070975775'
  ]) assert.deepEqual(safeCampaign(query), {});
});
test('recognized social referrers identify only the network and never guess a reel or forward a private path', () => {
  for (const [host, network] of [['www.facebook.com','facebook'], ['l.instagram.com','instagram'], ['youtu.be','youtube'], ['vm.tiktok.com','tiktok']]) {
    const campaign = socialReferral(`https://${host}/PRIVATE?email=SECRET`);
    assert.equal(campaign.campaign_source, network); assert.equal(campaign.campaign_content, 'origen_sin_reel');
    assert.ok(!JSON.stringify(campaign).match(/SECRET|PRIVATE/));
  }
  for (const referrer of ['http://www.youtube.com/', 'https://www.youtube.com.evil.test/', 'https://private.test/']) assert.deepEqual(socialReferral(referrer), {});
});
test('short links redirect to approved TEC pages only, and bio pages expose each published reel without granting membership', async t => {
  const app = express(); registerAudioSocial(app, { now: () => Date.parse('2026-11-13T12:00:00Z') });
  const server = await new Promise(resolve => { const server = app.listen(0,'127.0.0.1',() => resolve(server)); });
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const get = (path, options = {}) => fetch(`http://127.0.0.1:${server.address().port}` + path, { redirect:'manual', ...options });
  for (const source of SOCIAL_NETWORKS) {
    const redirect = await get('/audiolibros/r/' + source + '/perfil?next=https://evil.test&email=SECRET');
    assert.equal(redirect.status, 307); assert.equal(redirect.headers.get('cache-control'), 'private, no-store');
    const target = redirect.headers.get('location'); assert.ok(target.startsWith('/audiolibros/redes/' + source + '/?'));
    assert.ok(!target.match(/SECRET|evil/));
    const hub = await get(target); assert.equal(hub.status, 200); const html = await hub.text();
    assert.match(html, /¿Qué historia/); assert.match(html,/sin registrarte/); assert.match(html,/analytics.js\?v=20261113-social1/);
    assert.ok(!html.match(/secret|stripeCustomer|password|r2Key|X-Amz-Signature|utm_.*PRIVATE/));
  }
  const response = await get('/audiolibros/r/instagram/ig_dfqtbe5ngiy');
  const location = new URL(response.headers.get('location'),'https://totalelitecoaching.com');
  assert.equal(location.searchParams.get('libro'), 'el-hombre-mas-rico-de-babilonia');
  assert.equal(location.searchParams.get('utm_content'), 'ig_dfqtbe5ngiy');
  const tiktokHub = await get('/audiolibros/redes/tiktok/').then(r=>r.text());
  assert.ok(!tiktokHub.includes('tt_7707363372332846366'), 'future December publications are not displayed before their scheduled date');
  for (const path of ['/audiolibros/r/instagram/PRIVATE','/audiolibros/r/unknown/perfil','/audiolibros/redes/unknown/']) assert.equal((await get(path)).status, 404);
});
