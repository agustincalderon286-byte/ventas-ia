import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {randomBytes} from 'node:crypto';
import {connectionConfig, registerCoachConnections} from '../src/coach-connections.js';
import {createAgentConsent, CONSENT_VERSION} from '../src/coach-agent-consent.js';

function memoryStore() {
  const rows=new Map();
  return {rows,
    findById:id=>({lean:async()=>rows.get(id) || null}),
    updateOne:async({_id},{$set})=>{rows.set(_id,{...$set});},
    deleteOne:async({_id})=>{rows.delete(_id);}};
}

test('el permiso se guarda por vendedor, con fecha, y una versión anterior deja de valer',async()=>{
  const store=memoryStore();
  const consent=createAgentConsent({store,now:()=>Date.parse('2026-11-22T18:00:00Z')});
  assert.equal(await consent.accepted('a'),null);
  await consent.accept('a');
  assert.equal((await consent.accepted('a')).toISOString(),'2026-11-22T18:00:00.000Z');
  assert.equal(await consent.accepted('b'),null);
  assert.deepEqual(await consent.state('b'),{acceptedAt:null,outdated:false});
  assert.equal((await consent.state('a')).outdated,false);
  store.rows.get('a').version=CONSENT_VERSION-1;
  assert.equal(await consent.accepted('a'),null);
  assert.deepEqual(await consent.state('a'),{acceptedAt:null,outdated:true});
  await consent.accept('a');
  await consent.revoke('a');
  assert.equal(await consent.accepted('a'),null);
});

test('Mis conexiones muestra el permiso solo al piloto del asistente y lo activa o retira desde su propia sesión',async()=>{
  const routes=new Map();
  const app={get:(path,handler)=>routes.set('GET '+path,handler),post:(path,handler)=>routes.set('POST '+path,handler)};
  const config=connectionConfig({COACH_GMAIL_ENCRYPTION_KEY:randomBytes(32).toString('base64'),
    COACH_CONNECTIONS_PILOT_EMAILS:'a@example.test,b@example.test',COACH_AGENT_LOOP_EMAILS:'a@example.test,asiento@example.test'});
  const store=memoryStore();
  const agentConsent=createAgentConsent({store});
  const accounts={asiento:{accountType:'seat'}};
  registerCoachConnections(app,{config,agentConsent,switched:req=>req.switched,requireActive:async req=>({
    user:{_id:req.id,email:req.id+'@example.test',...accounts[req.id]},session:{tokenHash:'session-'+req.id}}),
    store:{findById:()=>({lean:async()=>null})}});
  async function open(id,{action='',body={},site='same-origin',switched=false}={}) {
    const path=action ? '/api/coach/conexiones/asistente/'+action : '/mis-conexiones/';
    const result={code:200,set(){return this;},vary(){return this;},status(code){this.code=code;return this;},send(html){this.body=html;},
      redirect(code,to){this.code=code;this.to=to;}};
    await routes.get((action?'POST ':'GET ')+path)({id,switched,path,method:action?'POST':'GET',query:result.query || {},body,
      get:header=>header==='Sec-Fetch-Site' ? site : null},result);
    return result;
  }
  const csrfOf=page=>/name="csrf" value="([^"]+)"/.exec(page.body)[1];

  const before=await open('a');
  assert.match(before.body,/Asistente que consulta tus cuentas/);
  assert.match(before.body,/Permiso sin activar\./);
  assert.match(before.body,/action="\/api\/coach\/conexiones\/asistente\/accept"/);
  assert.match(before.body,/Nunca el texto completo/);
  assert.match(before.body,/No se envían los teléfonos/);
  assert.match(before.body,/href="\/coach\/privacidad\/"/);
  const csrf=csrfOf(before);

  assert.equal((await open('a',{action:'accept',body:{csrf:'otro'}})).code,403);
  assert.equal((await open('a',{action:'accept',body:{csrf},site:'cross-site'})).code,403);
  assert.equal((await open('a',{action:'accept',body:{csrf},switched:true})).code,403);
  assert.equal(store.rows.size,0);

  const accepted=await open('a',{action:'accept',body:{csrf}});
  assert.equal(accepted.code,303);
  assert.equal(accepted.to,'/mis-conexiones/?estado=assistantOn');
  assert.equal(store.rows.get('a').version,CONSENT_VERSION);
  const after=await open('a');
  assert.match(after.body,/Permiso activo desde el /);
  assert.match(after.body,/action="\/api\/coach\/conexiones\/asistente\/revoke"/);
  assert.doesNotMatch(after.body,/asistente\/accept/);

  // Un permiso de una versión anterior no vale y la página pide revisarlo.
  store.rows.get('a').version=CONSENT_VERSION-1;
  const outdated=await open('a');
  assert.match(outdated.body,/Cambiaron las condiciones del asistente/);
  assert.match(outdated.body,/conserva cifradas durante 30 días/);
  assert.match(outdated.body,/asistente\/accept/);
  assert.match(outdated.body,/su ciudad o código postal/);
  store.rows.get('a').version=CONSENT_VERSION;

  const revoked=await open('a',{action:'revoke',body:{csrf}});
  assert.equal(revoked.to,'/mis-conexiones/?estado=assistantOff');
  assert.equal(store.rows.size,0);

  // Fuera del piloto del asistente, o desde un asiento de equipo, no hay sección ni rutas.
  const other=await open('b');
  assert.doesNotMatch(other.body,/Asistente que consulta tus cuentas/);
  assert.equal((await open('b',{action:'accept',body:{csrf:'x'}})).code,403);
  assert.equal((await open('asiento',{action:'accept',body:{csrf:'x'}})).code,403);
  assert.equal(store.rows.size,0);

  // Un fallo al guardar no se presenta como permiso activo.
  store.updateOne=async()=>{throw Error('sin base');};
  assert.equal((await open('a',{action:'accept',body:{csrf}})).to,'/mis-conexiones/?estado=assistantFailed');
});

test('la política de privacidad describe el asistente y lleva la fecha del cambio',()=>{
  const html=readFileSync(new URL('../public/coach/privacidad/index.html',import.meta.url),'utf8');
  assert.match(html,/Actualizada el 22 de noviembre de 2026/);
  assert.match(html,/asistente que consulta tus cuentas/);
  assert.match(html,/un extracto corto de cada mensaje/);
  assert.match(html,/la ciudad o el código postal de tus fichas/);
  assert.match(html,/retirar el permiso/);
});
