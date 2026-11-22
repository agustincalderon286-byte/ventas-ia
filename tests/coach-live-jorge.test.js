import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { createLiveCoach, createLiveLedger, liveLimits, liveTurnBody, liveTurnPath, registerLiveJorge } from '../src/coach-live-jorge.js';
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
          ('reservedMicros' in filter && row.reservedMicros!==filter.reservedMicros) ||
          (filter.$or && row.reservedMicros!==undefined && !filter.$or.some(f=>f.reservedMicros===row.reservedMicros)) ||
          (filter.busyUntil && row.busyUntil > filter.busyUntil.$lte) ||
          (filter.usedMicros && row.usedMicros > filter.usedMicros.$lte) ||
          (filter.turnIds && row.turnIds.includes(filter.turnIds.$ne))) return null;
      row.usedMicros+=update.$inc.usedMicros;
      Object.assign(row,update.$set);
      if (update.$push) row.turnIds.push(update.$push.turnIds);
      if (update.$pull) row.turnIds=row.turnIds.filter(id => id!==update.$pull.turnIds);
      for (const field of Object.keys(update.$unset||{})) delete row[field];
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

function liveWav(seconds = 1) {
  const frames = Math.round(16_000 * seconds);
  const wav = Buffer.alloc(44 + frames * 2);
  wav.write('RIFF'); wav.writeUInt32LE(wav.length - 8,4); wav.write('WAVEfmt ',8); wav.writeUInt32LE(16,16);
  wav.writeUInt16LE(1,20); wav.writeUInt16LE(1,22); wav.writeUInt32LE(16_000,24); wav.writeUInt32LE(32_000,28);
  wav.writeUInt16LE(2,32); wav.writeUInt16LE(16,34); wav.write('data',36); wav.writeUInt32LE(frames*2,40);
  wav.writeInt16LE(90,46);
  return wav;
}
function audioRequest(wav) { return { conversationId:randomUUID(),turnId:randomUUID(),audioBase64:wav.toString('base64') }; }

test('Jorge lee conexiones del dueño por voz, conserva su fuente y cobra solo la transcripción y síntesis utilizadas', async()=>{
  const lookups = [], spoken = [];
  const fixture = setup({
    voice: {
      transcribe: async () => 'Revisa mi cuatro en catorce',
      speak: async (text, owner) => { spoken.push({text, owner}); return Buffer.from('audio de prueba'); }
    },
    readRecords: async (viewer, question, recent) => {
      lookups.push({owner:viewer.user._id, question, recent:structuredClone(recent)});
      if (question.includes('catorce')) return {text:'Tienes una referencia de prueba en tu hoja.',sources:['sheets414']};
      if (question.includes('correo')) return {text:'Encontré un correo de prueba de Hy Cite.',sources:['gmail']};
      return null;
    }
  });
  const body = audioRequest(liveWav());
  const sheet = await fixture.service.turn(auth(), body);
  assert.deepEqual(sheet.sources,['sheets414']);
  assert.equal(sheet.transcript,lookups[0].question);
  assert.equal(sheet.budget.usedMicros,4500+Buffer.byteLength(jorgeSpeechDocument(sheet.text))*25);
  const mail = await fixture.service.turn(auth(), {...turn(), conversationId:body.conversationId, utterance:'¿Y mi correo?'});
  assert.deepEqual(lookups[1].recent.at(-1).sources,['sheets414']);
  assert.deepEqual(mail.sources,['gmail']);
  assert.equal(fixture.calls.length,0);
  assert.deepEqual(spoken.map(x=>x.owner),['agustin-id','agustin-id']);
  const general = {...turn(), conversationId:body.conversationId};
  await fixture.service.turn(auth(),general);
  assert.ok(!JSON.stringify(fixture.calls[0]).includes(sheet.text));
  assert.ok(!JSON.stringify(fixture.calls[0]).includes(mail.text));
  await fixture.service.turn(auth(rigo),{...general, turnId:randomUUID(),utterance:'Revisa mi correo'});
  assert.equal(lookups[3].owner,'rigo-id');
  assert.deepEqual(lookups[3].recent,[]);
});

test('el respaldo usa gpt-transcribe multipart con el WAV real una vez, dentro de la misma reserva y sin guardar audio', async()=>{
  for (const seconds of [0.24,15]) {
    const wav=liveWav(seconds), body=audioRequest(wav), urls=[];
    const fixture=setup({voice:undefined,env:{...env,COACH_VOICE_TRANSCRIPTION_MODEL:'a-different-model'},fetcher:async (url,init)=>{
      urls.push(url);
      assert.equal((await fixture.ledger.read(agustin)).usedMicros,liveLimits.audioReserveMicros);
      if (url.endsWith('/audio/transcriptions')) {
        assert.equal(init.method,'POST'); assert.equal(init.body.get('model'),'gpt-transcribe');
        const file=init.body.get('file'); assert.equal(file.name,'coach-audio.wav'); assert.equal(file.type,'audio/wav');
        assert.deepEqual(Buffer.from(await file.arrayBuffer()),wav);
        return Response.json({text:'¿Qué enseño primero?',languages:[{code:'es'}]});
      }
      if (url.endsWith('/chat/completions')) {
        const prompt=JSON.parse(init.body);
        assert.equal(prompt.messages.at(-1).content,'¿Qué enseño primero?');
        return Response.json({choices:[{message:{content:'Empieza por la necesidad.'}}],usage:{prompt_tokens:400,completion_tokens:20}});
      }
      assert.equal(new URL(url).host,'eastus.tts.speech.microsoft.com');
      return new Response(Buffer.alloc(8));
    }});
    const completed=await fixture.service.turn(auth(),body);
    assert.equal(urls.length,3);
    assert.equal(completed.transcript,'¿Qué enseño primero?'); assert.equal(completed.text,'Empieza por la necesidad.');
    assert.equal(completed.canStartTurn,true); assert.equal(completed.canTranscribeAudio,true);
    assert.equal(completed.budget.usedMicros,4500+120+Buffer.byteLength(jorgeSpeechDocument(completed.text))*25);
    assert.ok(!JSON.stringify([...fixture.histories.values(),...fixture.mongo.rows.values()]).includes(body.audioBase64));
    await assert.rejects(fixture.service.turn(auth(),body),{status:409});
    await assert.rejects(fixture.service.turn(auth(),{conversationId:body.conversationId,turnId:body.turnId,utterance:'Lo mismo'}),{status:409});
    assert.equal(urls.length,3);
  }
});
test('rechaza silencio, WAV modificado, tamaños falsos, base64 no canónico y payload ambiguo antes de reservar',async()=>{
  const invalid=[liveWav(0.239),liveWav(15.001),Buffer.alloc(44),Buffer.alloc(10000),Buffer.concat([liveWav(1),Buffer.alloc(4)])];
  for (const index of [0,4,8,12,16,20,22,24,28,32,34,36,40]) {
    const wav=liveWav(); wav[index]^=0x80; invalid.push(wav);
  }
  const silent=liveWav(); silent.fill(0,44); invalid.push(silent);
  const cases=invalid.map(audioRequest);
  const body=audioRequest(liveWav());
  cases.push({...body,audioBase64:' '.repeat(4)+body.audioBase64},{...body,utterance:'a'},{...body,utterance:null},{...body,audioBase64:null});
  const fixture=setup({voice:{transcribe:async()=>assert.fail('STT was invoked')}});
  for (const invalidBody of cases) await assert.rejects(fixture.service.turn(auth(),invalidBody),{status:422});
  assert.equal(fixture.mongo.rows.size,0);
});
test('si la transcripción falla después del envío, se conserva solo su costo una vez; sin saldo ni autorización no hay transcripción',async()=>{
  const body=audioRequest(liveWav()),calls=[];
  const fixture=setup({voice:{transcribe:async()=>{calls.push('stt');throw new Error('timeout');}}});
  await assert.rejects(fixture.service.turn(auth(),body),{status:503,publicCode:'transcription_unavailable'});
  assert.equal((await fixture.ledger.read(agustin)).usedMicros,4500);
  await assert.rejects(fixture.service.turn(auth(),body),{status:409});
  fixture.mongo.rows.get(agustin).usedMicros=liveLimits.limitMicros-liveLimits.audioReserveMicros+1;
  await assert.rejects(fixture.service.turn(auth(),audioRequest(liveWav())),{status:402});
  assert.equal(calls.length,1);
});
test('una reserva de texto previa al despliegue solo se puede liquidar al importe anterior',async()=>{
  const mongo=collection(),ledger=createLiveLedger(mongo);
  const id=randomUUID(),operation=await ledger.reserve(agustin,id);
  delete mongo.rows.get(agustin).reservedMicros;
  await assert.rejects(ledger.settle(agustin,operation,id,20,true));
  assert.equal((await ledger.settle(agustin,operation,id,20)).usedMicros,20);
});
test('el parser admite 15s y rechaza cargas excesivas, JSON inválido y formularios antes de la ruta',async t=>{
  const app=express();let requests=0;
  app.post(liveTurnPath,liveTurnBody,(_req,res)=>{requests++;res.sendStatus(204);});
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});
  t.after(()=>new Promise(resolve=>server.close(resolve)));
  const url='http://127.0.0.1:'+server.address().port+liveTurnPath;
  const send=(input,type='application/json')=>fetch(url,{method:'POST',headers:{'Content-Type':type},body:input});
  assert.equal((await send(JSON.stringify(audioRequest(liveWav(15))))).status,204);
  assert.equal((await send(JSON.stringify({audioBase64:'a'.repeat(680000)}))).status,413);
  const invalid=await send('{"audioBase64":"not-for-logs"');
  assert.equal(invalid.status,400); assert.ok(!(await invalid.text()).includes('not-for-logs'));
  assert.equal((await send('audioBase64=x','application/x-www-form-urlencoded')).status,415);
  assert.equal(requests,1);
});
