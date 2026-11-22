import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createLiveCoach, createLiveLedger, liveLimits, registerLiveJorge } from '../src/coach-live-jorge.js';
import { jorgeSpeechDocument } from '../src/coach-voice.js';

const agustin = 'agustincalderon286@gmail.com';
const rigo = 'calderonrigoberto51@gmail.com';
const auth = (email = agustin) => ({ user:{_id: email === rigo ? 'rigo-id' : 'agustin-id',email},session:{tokenHash:'ok',accessMode:'direct'} });
const turn = () => ({utterance:'¿Cómo presento la olla?',conversationId:randomUUID(),turnId:randomUUID()});
const env = { OPENAI_API_KEY:'test',AZURE_SPEECH_KEY:'test',AZURE_SPEECH_REGION:'eastus' };

// Synchronous match + update emulates Mongo's single-document atomic update, including the CAS.
function collection() {
  const rows = new Map();
  return {
    rows,
    async updateOne({_id},update,options) {
      assert.equal(options.writeConcern.w,'majority');
      if (!rows.has(_id)) rows.set(_id,structuredClone(update.$setOnInsert));
    },
    async findOne({_id}) { return rows.get(_id); },
    async findOneAndUpdate(filter,update,options) {
      assert.equal(options.writeConcern.w,'majority');
      assert.equal(options.returnDocument,'after');
      const row=rows.get(filter._id);
      if (!row || ('operation' in filter && row.operation!==filter.operation) ||
          (filter.busyUntil && row.busyUntil > filter.busyUntil.$lte) ||
          (filter.usedMicros && row.usedMicros > filter.usedMicros.$lte) ||
          (filter.turnIds && row.turnIds.includes(filter.turnIds.$ne))) return null;
      row.usedMicros+=update.$inc.usedMicros;
      Object.assign(row,update.$set);
      if (update.$push) row.turnIds.push(update.$push.turnIds);
      if (update.$pull) row.turnIds=row.turnIds.filter(id => id!==update.$pull.turnIds);
      if (update.$unset) delete row.operation;
      return {usedMicros:row.usedMicros};
    }
  };
}
function setup(overrides={}) {
  const mongo=collection(),ledger=createLiveLedger(mongo);
  const calls=[],audio=[],histories=new Map();
  const history={findOne:async ({_id})=>histories.get(_id),updateOne:async({_id},cmd)=>{
    const old=histories.get(_id)?.messages || [];
    histories.set(_id,{messages:[...old,...cmd.$push.messages.$each].slice(-6)});
  }};
  const service=createLiveCoach({ ledger,history,env,knowledge:()=> 'Biblioteca TEC',
    personalContext:async user => user.email,fetcher:async(_url,init)=>{
      calls.push(JSON.parse(init.body));
      return Response.json({choices:[{message:{content:'Prueba con una pregunta abierta.'}}],usage:{prompt_tokens:1500,completion_tokens:20}});
    },voice:{speak:async(text,id)=>{audio.push([text,id]);return Buffer.from('RIFF');}},...overrides });
  return {mongo,ledger,calls,audio,service,histories};
}
test('dos instancias, USD 5 independientes, bloqueo concurrente, reintentos y lease perdido', async ()=>{
  const db=collection();let clock=0;
  const first=createLiveLedger(db,()=>clock),second=createLiveLedger(db,()=>clock);
  const one=turn().turnId,two=turn().turnId;
  const [a,b]=await Promise.allSettled([first.reserve(agustin,one),second.reserve(agustin,two)]);
  assert.equal(a.status,'fulfilled');assert.equal(b.reason.status,409);
  await first.settle(agustin,a.value,one,7000);
  await assert.rejects(second.reserve(agustin,one),{status:409});
  const abandoned=await first.reserve(agustin,two);
  clock+=90_001;
  const next=turn().turnId, replacement=await second.reserve(agustin,next);
  await assert.rejects(first.settle(agustin,abandoned,two,5));
  await second.settle(agustin,replacement,next,7000);
  assert.equal((await second.read(agustin)).usedMicros,liveLimits.reserveMicros+14000);
  db.rows.get(agustin).usedMicros=liveLimits.limitMicros-liveLimits.reserveMicros;
  const lastId=turn().turnId,last=await second.reserve(agustin,lastId);
  await second.settle(agustin,last,lastId,liveLimits.reserveMicros);
  await assert.rejects(first.reserve(agustin,randomUUID()),{status:402});
  const r=await second.reserve(rigo,one);
  assert.ok(r);
  assert.equal((await first.read(agustin)).usedMicros,5_000_000);
});
test('una respuesta de Jorge se cobra una vez y su memoria está aislada por vendedor', async()=>{
  const {calls,service,ledger,audio}=setup(), first=turn(), output=await service.turn(auth(),first);
  assert.equal(output.mimeType,'audio/wav');assert.equal(Buffer.from(output.audioBase64,'base64').toString(),'RIFF');
  assert.equal(output.budget.usedMicros,395+Buffer.byteLength(jorgeSpeechDocument(output.text))*25);
  assert.deepEqual(audio,[[output.text,auth().user._id]]);
  await assert.rejects(service.turn(auth(),first),{status:409}); assert.equal(calls.length,1);
  const followup={...first,utterance:'¿Y después?',turnId:randomUUID()};
  await service.turn(auth(),followup);
  assert.ok(calls[1].messages.some(m => m.role==='assistant' && m.content===output.text));
  await service.turn(auth(rigo),followup);
  assert.ok(!calls[2].messages.some(m=>m.role==='assistant' || m.content.includes(agustin)));
  assert.equal(calls[0].service_tier,'default');
  assert.equal(calls[0].tools,undefined);
  assert.equal((await ledger.read(rigo)).usedMicros,output.budget.usedMicros);
});
test('sin reserva, configuración o petición válida no se invocan servicios de pago', async()=>{
  let calls=0;
  for (const body of [{...turn(),utterance:'a'.repeat(1401)},{...turn(),turnId:[randomUUID()]},{...turn(),conversationId:'bad'}]) {
    const setupResult=setup({fetcher:async()=>{calls++;}});
    await assert.rejects(setupResult.service.turn(auth(),body),{status:422});
  }
  await assert.rejects(setup({env:{}}).service.turn(auth(),turn()),{status:503});
  await assert.rejects(setup({ledger:{reserve:async()=>{throw new Error('Mongo offline');}},fetcher:async()=>{calls++;}}).service.turn(auth(),turn()));
  assert.equal(calls,0);
});
test('una interrupción previa al proveedor de voz no cobra síntesis; la pérdida de respuesta de Azure sí conserva la reserva de esa solicitud',async()=>{
  const controller=new AbortController();
  const {service,ledger}=setup({fetcher:async()=>{
    controller.abort();
    return Response.json({choices:[{message:{content:'De acuerdo.'}}],usage:{prompt_tokens:400,completion_tokens:20}});
  }});
  await assert.rejects(service.turn(auth(),turn(),controller.signal));
  assert.equal((await ledger.read(agustin)).usedMicros,120);
  const offline=setup({voice:{speak:async()=>{throw new Error('timeout');}}});
  const request=turn();
  await assert.rejects(offline.service.turn(auth(),request));
  assert.ok((await offline.ledger.read(agustin)).usedMicros>395);
  await assert.rejects(offline.service.turn(auth(),request),{status:409});
});
test('reserva se libera antes del primer uso y un fallo posterior ambiguo conserva el máximo del modelo',async()=>{
  const before=setup({knowledge:()=>{throw new Error('offline');}});
  const request=turn();
  await assert.rejects(before.service.turn(auth(),request));
  assert.equal((await before.ledger.read(agustin)).usedMicros,0);
  const after=setup({fetcher:async()=>{throw new Error('timeout after submission');}});
  await assert.rejects(after.service.turn(auth(),request));
  const cost=(await after.ledger.read(agustin)).usedMicros;
  assert.ok(cost>1000 && cost<liveLimits.reserveMicros);
  await assert.rejects(after.service.turn(auth(),request),{status:409});
});
test('el navegador no puede gastar en otra cuenta ni usar una sesión delegada',async()=>{
  const endpoints=new Map();
  const app={get:(key,fn)=>endpoints.set('GET'+key,fn),post:(key,fn)=>endpoints.set('POST'+key,fn)};
  const calls=[];
  registerLiveJorge(app,{requireActive:async req=>req.auth,switched:req=>!!req.switched,
    service:{ready:()=>true,getBudget:async()=>({remainingMicros:5_000_000}),turn:async()=>{calls.push(true);return {ok:true};}}});
  const run=async(method,req)=>{
    const res={code:200,status(code){this.code=code;return this;},set(){return this;},vary(){return this;},json(out){this.out=out;},once(){},off(){}};
    await endpoints.get(method+'/api/coach/live-jorge/'+(method==='GET'?'status':'turn'))({get:()=>req.header,...req},res);
    return res;
  };
  assert.equal((await run('GET',{auth:auth()})).out.accountId,'agustin-id');
  assert.equal((await run('GET',{auth:auth('third@example.com')})).out.eligible,false);
  assert.equal((await run('GET',{auth:auth(rigo),switched:true})).out.eligible,false);
  assert.equal((await run('POST',{auth:auth(rigo),header:'agustin-id'})).code,403);
  assert.equal((await run('POST',{auth:auth(),header:'agustin-id',switched:true})).code,403);
  assert.equal((await run('POST',{auth:auth(),header:'agustin-id'})).code,200);
  assert.equal(calls.length,1);
});
