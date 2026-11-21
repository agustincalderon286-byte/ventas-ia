import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import { connectionConfig, registerCoachConnections } from '../src/coach-connections.js';
import { sealToken } from '../src/coach-hycite-gmail.js';
import { scopeCoachChat } from '../src/tec-security.js';

const server = readFileSync(new URL('../server.js', import.meta.url), 'utf8');
function section(start, end) {
  const first = server.indexOf(start), last = server.indexOf(end, first + 1);
  assert.ok(first >= 0 && last > first, 'Se espera encontrar la ruta o función de producción ' + start);
  return server.slice(first, last);
}

const chat = section('app.post("/chat",', '// Central Express 4 async error boundary');
const mode = section('function normalizarModoChat(', 'function limpiarUrlExterna(');
const streaming = section('function prepararRespuestaStreamingChat(', 'async function consumirStreamingOpenAiChat(');
const user = { _id:'test-seller', email:'vendedor@example.invalid' };
const base = { accountId:user._id, sessionId:'session-mail-123', mode:'coach', pregunta:'Revisa mis correos' };

function harness() {
  const key = randomBytes(32);
  const config = { ...connectionConfig({COACH_CONNECTIONS_PILOT_EMAILS:user.email}), key, ready:true };
  const doc = { _id:user._id, mailbox:'piloto@example.invalid', grant:'test-grant',
    encryptedToken:sealToken('personal-refresh-token',key,user._id) };
  const recorded = { db:[], gmail:[], usage:[], model:[], scopes:[] };
  const requireCoachActivo = async (req,res) => {
    if (req.anonymous) {
      res.status(401).json({ error:'Sesión requerida' });
      return null;
    }
    return { user:{...user,email:req.loginEmail || user.email},session:{tokenHash:'dummy-session-hash'} };
  };
  let route;
  const app = {
    get(){},
    post(path, handler) { if (path === '/chat') route=handler; }
  };
  const agent = registerCoachConnections(app,{
    config, requireActive:requireCoachActivo, switched:req => req.switched,
    store:{findById(id) { recorded.db.push(id); return {lean:async()=>String(id)===doc._id?doc:null}; }},
    gmail:{
      refresh:async token=>{ assert.equal(token,'personal-refresh-token');return {access_token:'seller-access'}; },
      inbox:async(token,opts)=>{
        recorded.gmail.push({token,opts});
        return [{id:'abcd1234',subject:'Asunto confidencial del piloto',from:'cliente@example.invalid',
          snippet:'Texto privado de prueba',date:'2026-11-20T12:34:56Z'}];
      }
    }
  });
  // This marker stops the normal model path at its first call. Anything else unexpected is a test failure.
  const normalChat = new Error('Entered normal chat');
  const inert = () => null;
  const context = {
    app, requireCoachActivo, scopeCoachChat,
    limpiarCoachHealthSurveyActiveContext:inert, limpiarCoachProgram414ActiveContext:inert,
    limpiarCoachDecisionContext:inert, limpiarCoachOrderCalcContext:inert, limpiarCoachDemoOutcomeContext:inert,
    asegurarSesionRedisCargada:inert, obtenerCoachContextoSesion:inert, validarLimiteUsoCoach:async()=>({allowed:true}),
    validarLimiteUsoChef:async()=>({allowed:true}), marcarSesionActiva:inert, limpiarMemoriaSesiones:inert,
    runCoachMailAgent:args=>{recorded.scopes.push(args.sessionId);return agent(args);},
    guardarMensajeRaw:async row=>recorded.usage.push(row),
    construirContextoModoPrompt:mode=>{recorded.model.push(mode); throw normalChat;},
    console:{error:err=>{ if (err!==normalChat) throw err; }}
  };
  vm.runInNewContext(mode + streaming + chat, context);
  async function request(body, extras = {}) {
    const res = {
      headers:{}, chunks:[], statusCode:200,
      setHeader(k,v){this.headers[k.toLowerCase()]=v;},
      status(n){this.statusCode=n;return this;}, json(value){this.data=JSON.parse(JSON.stringify(value));},
      write(chunk){this.chunks.push(chunk);}, end(){this.writableEnded=true;}
    };
    await route({...extras,body},res);
    return res;
  }
  return {recorded,request};
}

for (const stream of [false,true]) {
  test('el chat consulta Gmail del vendedor en ' + (stream?'NDJSON':'JSON') + ' sin enviar el correo al modelo', async () => {
    const {request,recorded}=harness();
    const result=await request({...base,stream,pregunta:'Revisa mis correos sin leer'});
    assert.equal(result.statusCode,200);
    let reply=result.data;
    if (stream) {
      assert.match(result.headers['content-type'],/application\/x-ndjson/);
      assert.equal(result.writableEnded,true);
      const events=result.chunks.map(chunk=>JSON.parse(chunk));
      assert.deepEqual(events.map(e=>e.type),['delta','done']);
      reply=events[1].payload;
      assert.equal(events[0].content,reply.respuesta);
    } else {
      assert.deepEqual(result.chunks,[]);
    }
    assert.equal(reply.coachMailAgent,true);
    assert.match(reply.respuesta,/Asunto confidencial del piloto/);
    assert.deepEqual(reply.coachHelpActions,[{type:'mailSource',mailbox:'piloto@example.invalid',id:'abcd1234',label:'Abrir correo 1'}]);
    assert.deepEqual(recorded.gmail,[{token:'seller-access',opts:{unread:true}}]);
    assert.deepEqual(recorded.scopes,[scopeCoachChat(user._id,base.sessionId)]);
    assert.deepEqual(recorded.model,[]);
    assert.equal(recorded.usage.length,1);
    assert.doesNotMatch(JSON.stringify(recorded.usage),/sin leer|Asunto confidencial|Texto privado|cliente@example|seller-access|personal-refresh/);
    assert.deepEqual([...new Set(recorded.db)],[user._id]);
  });
}

test('el agente exige sesión propia, participación en el piloto y cuenta activa en la pestaña', async () => {
  const {request,recorded}=harness();
  assert.equal((await request(base,{anonymous:true})).statusCode,401);
  assert.equal((await request({...base,accountId:'stale-account'})).statusCode,403);
  assert.deepEqual(recorded.scopes,[]);
  for (const [body,extras,reason] of [
    [{...base,accountId:undefined},{},/propia cuenta/],
    [base,{switched:true},/propia cuenta/],
    [base,{loginEmail:'outside@example.invalid'},/piloto/]
  ]) {
    const {data}=await request(body,extras);
    assert.equal(data.coachMailAgent,true);
    assert.match(data.respuesta,reason);
  }
  assert.deepEqual(recorded.db,[]); assert.deepEqual(recorded.gmail,[]); assert.deepEqual(recorded.model,[]);
});

test('el Chef público omite el agente privado y una pregunta normal del Coach sigue a su ruta habitual', async () => {
  const {request,recorded}=harness();
  await request({...base,mode:'chef'}, {anonymous:true});
  assert.deepEqual(recorded.scopes,[]);
  await request({...base,pregunta:'¿Cómo explico la garantía?'});
  assert.deepEqual(recorded.model,['chef','coach']);
  assert.deepEqual(recorded.db,[]); assert.deepEqual(recorded.gmail,[]);
  assert.deepEqual(recorded.usage,[]);
});
