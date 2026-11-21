import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { connectionConfig,registerCoachConnections } from '../src/coach-connections.js';
import { createWaProfile } from '../src/coach-wa-profile.js';

test('el panel ofrece el envío desde la app de WhatsApp de cada vendedor del piloto',async()=>{
  const routes=new Map(), loaded=[], rows=new Map();
  const app={ get:(path,handler)=>routes.set(path,handler),post:(path,handler)=>routes.set(path,handler) };
  const config=connectionConfig({COACH_GMAIL_ENCRYPTION_KEY:randomBytes(32).toString('base64'),COACH_CONNECTIONS_PILOT_EMAILS:'a@example.test,b@example.test'});
  const waProfile=createWaProfile({key:config.key,store:{
    findById:id=>({lean:async()=>rows.get(id)}),
    updateOne:async({_id},{$set})=>rows.set(_id,$set),
    deleteOne:async({_id})=>rows.delete(_id)
  }});
  registerCoachConnections(app,{config,waProfile,switched:req=>req.switched,requireActive:async req=>({
    user:{_id:req.id,email:req.id+'@example.test'},session:{tokenHash:'session-'+req.id}}),
    store:{findById(id){loaded.push(id);return {lean:async()=>null};}}
  });
  async function open(id,switched=false,action='',body={},site='same-origin') {
    const path=action?'/api/coach/conexiones/whatsapp/'+action:'/mis-conexiones/';
    const result={code:200,set(){return this;},vary(){return this;},status(code){this.code=code;return this;},send(body){this.body=body;},
      redirect(code,to){this.code=code;this.to=to;}};
    await routes.get(path)({id,switched,path,method:action?'POST':'GET',query:{},body,get:header=>header==='Sec-Fetch-Site'?site:null},result);
    return result;
  }
  const session={};
  for (const id of ['a','b']) {
    const page=await open(id);
    assert.equal(page.code,200);
    assert.match(page.body,/<h2>WhatsApp<\/h2>/);
    assert.match(page.body,/href="\/coach\/app\/\?abrirCoach=1">Preparar un WhatsApp/);
    assert.match(page.body,/WhatsApp Messenger o WhatsApp Business en tu teléfono/);
    assert.match(page.body,/cuenta activa es la tuya y pulsa Enviar en la app/);
    assert.ok(page.body.includes('Coach de <strong>'+id+'@example.test</strong>'));
    assert.match(page.body,/Mi número de WhatsApp \(con prefijo de país\)/);
    session[id]=page.body.match(/name="csrf" value="([^"]+)"/)[1];
    assert.doesNotMatch(page.body,/wa\.me|connect\.facebook|graph\.facebook|Conectar (?:mi )?WhatsApp/);
  }
  assert.doesNotMatch((await open('other')).body,/<h2>WhatsApp<\/h2>/);
  assert.equal((await open('a',true)).code,403);
  assert.deepEqual(loaded,['a','b']);
  const myPhone={csrf:session.a,phone:'+1 (312) 555-0101',owner:'b'};
  assert.equal((await open('b',false,'save',myPhone)).code,403);
  assert.equal((await open('a',true,'save',myPhone)).code,403);
  assert.equal((await open('other',false,'save',myPhone)).code,403);
  assert.equal((await open('a',false,'save',myPhone,'same-site')).code,403);
  assert.match((await open('a',false,'save',myPhone)).to,/waProfileSaved$/);
  assert.match((await open('a',false,'save',{csrf:session.a,phone:'31255'})).to,/waPhoneInvalid$/);
  const normalized='+13125550101';
  assert.equal(await waProfile.load('a'),normalized);
  assert.ok(!(await open('b')).body.includes(normalized));
  assert.ok((await open('a')).body.includes('name="phone" value="'+normalized+'"'));
  assert.ok(!JSON.stringify(rows.get('a')).includes(normalized));
  assert.match((await open('b',false,'save',{csrf:session.b,phone:'+13125550102'})).to,/waProfileSaved$/);
  assert.equal(await waProfile.load('b'),'+13125550102');
  assert.equal(await waProfile.load('a'),normalized);
  rows.set('b',rows.get('a')); // A copied ciphertext must not reveal another account's number.
  await assert.rejects(()=>waProfile.load('b'));
  const degraded=(await open('b')).body;
  assert.match(degraded,/Tu número de referencia no está disponible/);
  assert.match(degraded,/Gmail y ventas de Hy Cite/);
  assert.ok(!degraded.includes(normalized));
  assert.equal((await open('b',false,'remove',{csrf:session.a})).code,403);
  assert.match((await open('a',false,'remove',{csrf:session.a})).to,/waProfileRemoved$/);
  assert.equal(await waProfile.load('a'),'');
  config.key=Buffer.alloc(0);
  assert.doesNotMatch((await open('a')).body,/<h2>WhatsApp<\/h2>/);
});
