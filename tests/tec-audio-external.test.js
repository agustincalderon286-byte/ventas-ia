import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { audiobookCatalog, findReadyAudiobook, registerTecAudiobooks } from '../src/tec-audiobooks.js';
import { externalAudioReady, externalAudioDelivery } from '../src/tec-audio-external.js';
import { buildAudiobookSeo, registerAudiobookSeo } from '../src/tec-audiobook-seo.js';

const historical = { id:'el-criterio', audio:{provider:'internet-archive',url:'https://archive.org/download/criterio_2408_librivox/Criterio_librivox.m4b',bytes:246432109} };
test('the retired LibriVox recording cannot return through the catalogue, discovery pages or old source allowlist', async () => {
  assert.ok(!audiobookCatalog.some(book => book.id==='el-criterio'));
  for (const id of ['el-libro-de-los-cinco-anillos','la-psicologia-de-las-ventas']) assert.ok(audiobookCatalog.some(book => book.id===id));
  assert.ok(audiobookCatalog.every(book => book.audio.r2Key && !book.audio.provider));
  const unavailable = { findOne() { throw new Error('Retired audio must not probe media stores'); } };
  assert.equal(externalAudioReady(historical),false);
  assert.equal(await findReadyAudiobook(historical,unavailable),null);
  assert.throws(()=>externalAudioDelivery(historical.audio),/Unapproved/);
  const seo=buildAudiobookSeo();
  assert.equal(seo.pages.has('el-criterio'),false);
  for(const text of [seo.landing,seo.sitemap,seo.llms,...seo.collectionPages.values()]) assert.ok(!text.includes('/libros/el-criterio/'));
});

test('old pages and Coach API bookmarks return Gone without a media redirect, while active R2 books still work', async t => {
  const app=express(), lease={url:'https://example.test/approved-r2-audio',expiresAt:Date.now()+300000};let deliveries=0;
  const requireActive=async(req,res)=>{if(!req.get('X-Test-Member')){res.status(401).end();return null;}return{user:{_id:'fixture'}};};
  registerTecAudiobooks(app,{requireActive,requireAdmin:requireActive,
    media:{find:()=>({lean:async()=>[]})},r2:{ready:async()=>true,delivery:async()=>{deliveries++;return lease;}},
    getBucket(){throw new Error('No proxy');},reviews:{aggregate:async()=>[]},progress:{findOne:()=>({lean:async()=>null})}});
  registerAudiobookSeo(app);
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});
  t.after(()=>new Promise(resolve=>{server.closeAllConnections();server.close(resolve);}));
  const request=(path,options)=>fetch(`http://127.0.0.1:${server.address().port}`+path,{redirect:'manual',...options});
  for(const headers of [{},{'X-Test-Member':'active'}]){
    for(const suffix of ['', '/audio','/audio?delivery=1','/text','/progress']){
      const r=await request('/api/coach/audiobooks/el-criterio'+suffix,{headers});assert.equal(r.status,410);assert.equal(r.headers.get('location'),null);
    }
  }
  assert.equal(deliveries,0);
  const head=await request('/api/coach/audiobooks/el-criterio/audio',{method:'HEAD',headers:{'X-Test-Member':'active'}});assert.equal(head.status,410);
  const active=await request('/api/coach/audiobooks/'+audiobookCatalog[0].id+'/audio?delivery=1',{headers:{'X-Test-Member':'active'}});assert.deepEqual(await active.json(),lease);assert.equal(deliveries,1);
  for(const path of ['/audiolibros/libros/el-criterio/','/audiolibros/pruebas/el-criterio/','/audiolibros/libros/el-criterio/index.html']){
    const r=await request(path);assert.equal(r.status,410);assert.match(r.headers.get('x-robots-tag'),/noindex/);assert.ok((await r.text()).includes('Volver a Audiolibros TEC'));
  }
  for(const file of ['libro.json','reproductor.js'])assert.equal((await request('/audiolibros/pruebas/el-criterio/'+file)).status,410);
});
