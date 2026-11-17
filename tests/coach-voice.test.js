import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import express from 'express';
import { createCoachVoiceProvider, registerCoachVoice, pcmToWav, speechText } from '../src/coach-voice.js';
import { createCoachRequestGuard, securityErrorHandler } from '../src/tec-security.js';
const appSource=fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');

test('voice uses complete PCM output with a valid WAV header and readable text',()=>{
 const wav=pcmToWav([Buffer.from([1,2]),Buffer.from([3,4])]);assert.equal(wav.toString('ascii',0,4),'RIFF');assert.equal(wav.readUInt32LE(24),24000);assert.equal(wav.readUInt32LE(40),4);assert.deepEqual([...wav.subarray(44)],[1,2,3,4]);
 assert.equal(speechText('**Hola** [catálogo](https://example.com/a)\n# Precio: $20.'),'Hola catálogo\n Precio: $20.');
});
test('provider sends the recorded format for transcription and returns actual generated audio only after successful completion',async()=>{
 let request,connection,event;
 class Socket extends EventTarget {
  constructor(url,options){super();connection={url,options};queueMicrotask(()=>this.dispatchEvent(new MessageEvent('message',{data:JSON.stringify({type:'session.created'})})));}
  send(value){event=JSON.parse(value);queueMicrotask(()=>{this.dispatchEvent(new MessageEvent('message',{data:JSON.stringify({type:'response.output_audio.delta',delta:Buffer.from([1,2,3,4]).toString('base64')})}));this.dispatchEvent(new MessageEvent('message',{data:JSON.stringify({type:'response.done',response:{status:'completed'}})}));});}
  close(){}
 }
 const provider=createCoachVoiceProvider({env:{OPENAI_API_KEY:'test-only-key'},Socket,fetcher:async(url,options)=>{request={url,options};return new Response(JSON.stringify({text:' ¿Cómo cierro la venta? '}),{status:200});}});
 assert.equal(await provider.transcribe(Buffer.alloc(100),'audio/mp4'),'¿Cómo cierro la venta?');assert.equal(request.options.body.get('file').name,'coach-audio.m4a');assert.equal(request.options.body.get('model'),'gpt-transcribe');
 const audio=await provider.speak('Hola, vendedor.','private-account');assert.equal(audio.length,48);assert.equal(new URL(connection.url).hostname,'api.openai.com');assert.equal(connection.options.headers['OpenAI-Safety-Identifier'].includes('private-account'),false);
 assert.equal(event.response.conversation,'none');assert.deepEqual(event.response.input,[]);assert.ok(event.response.instructions.includes('Hola, vendedor.'));assert.equal(event.response.audio.output.format.rate,24000);
});
test('incomplete speech or a failed provider never returns a playable partial answer',async()=>{
 class Socket extends EventTarget {
  constructor(){super();queueMicrotask(()=>{this.dispatchEvent(new MessageEvent('message',{data:JSON.stringify({type:'response.output_audio.delta',delta:'AQI='})}));this.dispatchEvent(new MessageEvent('message',{data:JSON.stringify({type:'response.done',response:{status:'failed'}})}));});}
  close(){}
 }
 const provider=createCoachVoiceProvider({env:{OPENAI_API_KEY:'test-only-key'},Socket,fetcher:async()=>new Response('{}',{status:503})});
 await assert.rejects(provider.speak('Hola','alice'),/Incomplete/);await assert.rejects(provider.transcribe(Buffer.alloc(100),'audio/webm'),/Transcription/);
});
async function fixture(t,{provider,maxRequests=40}={}){
 const calls=[];const implementation=provider||{async transcribe(data,mime){calls.push({kind:'transcribe',mime,bytes:data.length});return 'Pregunta transcrita';},async speak(text,user){calls.push({kind:'speech',text,user});return pcmToWav([Buffer.from([1,2,3,4])]);}};
 const authorize=async(req,res)=>{const id=req.get('X-Fixture-User');if(!['alice','bob'].includes(id)){res.status(401).json({error:'Sin sesión'});return null;}return{user:{_id:id}};};
 const app=express();app.use(express.json());app.use('/api/coach',createCoachRequestGuard({requireActive:authorize}));registerCoachVoice(app,{requireActive:authorize,provider:implementation,maxRequests});app.use(securityErrorHandler);
 const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>{server.closeAllConnections();server.close();});
 const send=(kind,body,{user='alice',account='alice',mime='audio/webm;codecs=opus',origin='https://totalelitecoaching.com'}={})=>fetch(`http://127.0.0.1:${server.address().port}/api/coach/voice/${kind}`,{method:'POST',headers:{Origin:origin,'X-Fixture-User':user,'X-TEC-Account-ID':account,'Content-Type':kind==='speech'?'application/json':mime},body:kind==='speech'?JSON.stringify({accountId:account,text:body}):body});
 return{send,calls};
}
test('voice endpoints require active account and origin, accept audio, and return private WAV without touching the chat model',async t=>{
 const f=await fixture(t);let r=await f.send('transcribe',Buffer.alloc(100));assert.equal(r.status,200);assert.equal((await r.json()).text,'Pregunta transcrita');
 r=await f.send('speech','**Respuesta**');assert.equal(r.status,200);assert.equal(r.headers.get('content-type'),'audio/wav');assert.equal(r.headers.get('cache-control'),'private, no-store');assert.equal((await r.arrayBuffer()).byteLength,48);
 assert.deepEqual(f.calls,[{kind:'transcribe',mime:'audio/webm',bytes:100},{kind:'speech',text:'Respuesta',user:'alice'}]);
 for(const options of [{user:'unsigned'},{user:'bob'},{origin:'https://foreign.example'}])assert.ok([401,403].includes((await f.send('transcribe',Buffer.alloc(100),options)).status));assert.equal(f.calls.length,2);
});
test('voice rejects invalid, empty and oversized audio or text before provider calls, and limits repeated use',async t=>{
 const f=await fixture(t,{maxRequests:2});
 assert.equal((await f.send('transcribe',Buffer.alloc(10))).status,400);assert.equal((await f.send('transcribe',Buffer.alloc(100),{mime:'text/plain'})).status,415);
 assert.equal((await f.send('transcribe',Buffer.alloc(8*1024*1024+1))).status,413);
 for(const text of ['',{},'x'.repeat(12001)])assert.equal((await f.send('speech',text)).status,400);
 assert.equal(f.calls.length,0);assert.equal((await f.send('speech','Primero')).status,200);assert.equal((await f.send('speech','Segundo')).status,200);assert.equal((await f.send('speech','Tercero')).status,429);
 assert.equal((await f.send('speech','Otra cuenta',{user:'bob',account:'bob'})).status,200);
});
test('silence and failed audio generation keep explicit errors instead of claiming a transcript or voice exists',async t=>{
 const f=await fixture(t,{provider:{transcribe:async()=>'',speak:async()=>{throw new Error('provider-secret-detail');}}});
 assert.equal((await f.send('transcribe',Buffer.alloc(100))).status,422);const r=await f.send('speech','Respuesta');assert.equal(r.status,503);assert.equal((await r.text()).includes('provider-secret-detail'),false);
});
function chatFixture(){
 const messages=[],spoken=[],sent=[],inputAudio=[];let busy=false;
 const source=appSource.slice(appSource.indexOf('  const sendCoachMessage = async'),appSource.indexOf('  coachVoice = window.TecCoachVoice?.init'));
 const context={String,Number,Array,chatMessages:null,me:{user:{id:'alice'}},window:{sessionStorage:{getItem:()=> 'cierre'}},COACH_WORKSPACE_TAB_KEY:'workspace',
  coachChatBusy:false,coachVoice:{pausePlayback(){},attachInput:(handles,url)=>inputAudio.push(url),speak:async(text)=>{spoken.push(text);return false;}},
  addCoachMessage:(container,role,text)=>{messages.push({role,text});return [{}];},updateCoachMessage:(handles,text)=>messages.push({role:'assistant-update',text}),
  clearCoachChatInputs(){},toggleCoachChatBusy(value){busy=value;context.coachChatBusy=value;},setCoachChatStatusText(){},getCoachDemoStageMeta:()=>({id:'conexion'}),getCoachDemoStageId:()=> 'conexion',getCoachChatSessionId:()=> 'session-test',getCoachVisitorId:()=> 'visitor-test',getCoachDemoEvents:()=>[],
  getActiveCoachHealthSurveyContext:()=>null,getActiveCoachProgram414Context:()=>null,getActiveCoachOrderCalcContext:()=>null,getActiveCoachDecisionContext:()=>null,getActiveCoachDemoOutcomeContext:()=>null,
  streamCoachChatReply:async(body,handlers)=>{sent.push(body);handlers.onDelta('Respuesta');return{respuesta:'Respuesta completa'};},renderCoachRepLeadSummary(){},renderActiveLeadContext(){},floatingCoachMode:'idle',floatingChatInput:null,chatInput:{focus(){}}};
 const send=vm.runInNewContext(source+';sendCoachMessage',context);return{send,messages,spoken,sent,inputAudio,get busy(){return busy;}};
}
test('text stays text; each audio question requests voice for that reply only, keeping the written answer even if speech fails',async()=>{
 const f=chatFixture();await f.send('Texto');assert.equal(f.spoken.length,0);
 assert.equal((await f.send('Audio transcrito',{replyMode:'audio',audioUrl:'blob:my-question'})).ok,true);assert.deepEqual(f.spoken,['Respuesta completa']);assert.deepEqual(f.inputAudio,['blob:my-question']);
 await f.send('Otra pregunta escrita');assert.equal(f.spoken.length,1);assert.equal(f.messages.at(-1).text,'Respuesta completa');assert.ok(f.sent.every(body=>body.accountId==='alice'));assert.equal(f.busy,false);
});
