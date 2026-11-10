import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import express from 'express';
import { readingCatalog, getReadingText, readingBookHtml, readingPath } from '../src/tec-reading-books.js';
import { audiobookCatalog, getAudiobookTranscript } from '../src/tec-audiobooks.js';
import { registerAudioMembership } from '../src/tec-audio-membership.js';
import { registerAudiobookSeo } from '../src/tec-audiobook-seo.js';

test('the 16 covered Spanish editions contain complete text, existing covers and no draft placeholders',()=>{
  assert.equal(readingCatalog.length,16);
  assert.equal(new Set(readingCatalog.map(b=>b.id)).size,16);
  for(const b of readingCatalog){
    assert.equal(b.language,'Español');assert.ok(fs.existsSync(new URL('../public'+b.cover,import.meta.url)));
    const t=getReadingText(b.id);assert.equal(t.chapters.length,b.chapterCount);
    assert.ok(b.wordCount>100);
    for(const c of t.chapters){assert.ok(c.title&&c.paragraphs.length);assert.ok(c.paragraphs.every(p=>typeof p==='string'&&p.trim()));}
    if(b.audioId){
      const original=getAudiobookTranscript(b.audioId).cues.map(c=>(c.separator||'')+c.text).join('').replace(/\s/g,'');
      const actual=t.chapters.flatMap(c=>c.paragraphs).join('').replace(/\s/g,'');
      assert.equal(actual,original,b.id+' lost text');
      assert.ok(audiobookCatalog.some(a=>a.id===b.audioId));
    }
  }
  assert.equal(getReadingText('genesis-rv1909').chapters.length,50);
  assert.equal(getReadingText('exodo-rv1909').chapters.length,40);
  assert.equal(getReadingText('hamlet').chapters.length,6);
  assert.equal(getReadingText('hamlet').chapters.at(-1).paragraphs.at(-1),'FIN DEL DRAMA');
  assert.ok(getReadingText('el-monte-de-las-animas').chapters[0].paragraphs[0].startsWith('La noche de difuntos'));
  const aphorisms=getReadingText('oraculo-manual').chapters.slice(1).flatMap(c=>c.paragraphs).map(p=>p.match(/^(\d+)\./)?.[1]).filter(Boolean).map(Number);
  assert.deepEqual(aphorisms,Array.from({length:300},(_,i)=>i+1));
  assert.equal(getReadingText('../server'),null);
});

test('public cards and reader shells advertise the paid plan without exposing complete chapter text',async t=>{
  const app=express();registerAudiobookSeo(app);
  const s=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>{s.closeAllConnections();s.close();});
  const base='http://127.0.0.1:'+s.address().port;
  const landing=await(await fetch(base+'/audiolibros/')).text();
  assert.equal((landing.match(/data-reading-card /g)||[]).length,16);assert.ok(landing.includes('id="libros"'));
  assert.ok(landing.includes('Lectura completa incluida en la misma membresía de $20'));
  for(const b of readingCatalog){
    const res=await fetch(base+readingPath(b)),html=await res.text();assert.equal(res.status,200);
    assert.ok(html.includes(b.title));assert.ok(html.includes('isAccessibleForFree":false'));
    const excerpt=getReadingText(b.id).chapters.find(c=>c.paragraphs.some(p=>p.length>120))?.paragraphs.find(p=>p.length>120);
    if(excerpt)assert.ok(!html.includes(excerpt),b.id+' leaked complete text');
    assert.ok(!html.includes('/Volumes/')&&!html.includes('r2Key'));
  }
  const sitemap=await(await fetch(base+'/sitemap.xml')).text();for(const b of readingCatalog)assert.ok(sitemap.includes(readingPath(b)));
  assert.equal((await fetch(base+'/audiolibros/leer/does-not-exist/')).status,404);
  const redirect=await fetch(base+'/audiolibros/leer/hamlet',{redirect:'manual'});assert.equal(redirect.status,301);assert.equal(redirect.headers.get('location'),readingPath({id:'hamlet'}));
  const hostile={...readingCatalog[0],title:'<img src=x onerror=alert(1)>'};assert.ok(!readingBookHtml(hostile).includes('<img src=x onerror=alert(1)>'));
});

test('members-only reading uses existing subscription/session checks, isolates progress and rejects unsafe writes',async t=>{
  const records=new Map(),userRows=new Map(),sessionRows=new Map(),cookies={};
  for(const name of ['alice','bob','revoked','unpaid']){
    const user={_id:'test:'+name,isTest:true,analyticsExcluded:true,testAccessEnabled:name!=='revoked',accessType:'test',credentialVersion:'v1'};
    if(name==='unpaid'){user.isTest=false;user.stripeCustomerId='cus_unpaid';}
    userRows.set(user._id,user);const secret=crypto.randomBytes(32).toString('base64url');const hash=crypto.createHash('sha256').update(secret).digest('hex');
    sessionRows.set(hash,{email:user._id,expiresAt:new Date(Date.now()+60000),credentialVersion:'v1'});cookies[name]='tec_audio_session='+secret;
  }
  const lean=v=>({lean:async()=>v});const app=express();app.use(express.json());
  registerAudioMembership(app,{publicOrigin:'https://totalelitecoaching.com',secure:false,catalog:[],getTranscript:()=>null,
    accounts:{findById:id=>lean(userRows.get(id))},sessions:{findOne:({_id})=>lean(sessionRows.get(_id))},
    readingProgress:{findById:id=>lean(records.get(id)),updateOne:async({_id},{$set})=>records.set(_id,{...records.get(_id),...$set})},
    stripe:{subscriptions:{async *list(){yield {status:'canceled',metadata:{audience:'tec-audiolibros'},items:{data:[{price:{id:'price_audio'}}]}};}}},priceId:'price_audio'});
  const s=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>{s.closeAllConnections();s.close();});
  const base='http://127.0.0.1:'+s.address().port+'/api/audiolibros/lecturas/hamlet';
  const call=(suffix='',name='',method='GET',data,origin='https://totalelitecoaching.com')=>fetch(base+suffix,{method,headers:{Cookie:cookies[name]||'',Origin:origin,'Content-Type':'application/json'},...(data?{body:JSON.stringify(data)}:{})});
  assert.equal((await call()).status,401);assert.equal((await call('/capitulos/1')).status,401);
  assert.equal((await call('','revoked')).status,403);assert.equal((await call('/capitulos/1','unpaid')).status,403);
  let res=await call('','alice');assert.equal(res.status,200);assert.equal(res.headers.get('cache-control'),'private, no-store');assert.equal(res.headers.get('vary'),'Cookie');
  const meta=await res.json();assert.equal(meta.book.id,'hamlet');assert.equal(meta.progress,null);
  res=await call('/capitulos/2','alice');assert.equal(res.status,200);const chapter=await res.json();assert.ok(chapter.paragraphs.length>20);
  for(const c of ['0','-1','999','01','oops'])assert.equal((await call('/capitulos/'+c,'alice')).status,404,c);
  const point={chapter:3,paragraph:2};assert.equal((await call('/progreso','alice','PUT',point)).status,200);
  assert.deepEqual(Object.fromEntries(Object.entries((await(await call('','alice')).json()).progress).filter(([k])=>k!=='updatedAt')),point);
  assert.equal((await(await call('','bob')).json()).progress,null);
  assert.equal((await call('/progreso','alice','PUT',point,'https://evil.test')).status,403);
  for(const p of [{chapter:0,paragraph:0},{chapter:100,paragraph:0},{chapter:1,paragraph:-1},{chapter:1,paragraph:99999},{chapter:1.5,paragraph:0},{chapter:'1',paragraph:0}])assert.equal((await call('/progreso','alice','PUT',p)).status,400);
  userRows.get('test:alice').testAccessEnabled=false;
  assert.equal((await call('/capitulos/2','alice')).status,403);
});
