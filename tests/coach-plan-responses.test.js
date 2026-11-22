import test from 'node:test';
import assert from 'node:assert/strict';
import {interpretPilotPlan} from '../src/coach-plan-language.js';

const key='fixture-only';
const mixed='Checa los cuatro en catorce de Rosa, búscame dos huecos para mañana y déjame el correo listo';
const plan={kind:'plan',operations:['list','appointment','email'],select:[],contact:'',host:'Rosa',ordinal:0,
  bucket:'',leadKind:'referrals',phone:'',salesPeriod:'',note:'',noteSchedule:'',schedule:'mañana',slots:true,
  to:'',mailInstruction:'el correo listo',sendEmail:false,question:''};
const call=(name,args={})=>({type:'function_call',status:'completed',name,arguments:JSON.stringify(args)});
function fixture({output,status='completed'}) {
  let request;
  return {get body(){return request;},apiKey:key,
    async fetchImpl(url,options){
      assert.equal(url,'https://api.openai.com/v1/responses');
      assert.equal(options.headers.Authorization,'Bearer '+key);
      assert.equal(options.redirect,'error');
      request=JSON.parse(options.body);
      return Response.json({status,output,usage:{input_tokens:141,input_tokens_details:{cached_tokens:32},output_tokens:15}});
    }};
}
function run(text,name,args,options={}) {
  const api=fixture({output:[call(name,args)]});
  return interpretPilotPlan(text,{...api,...options});
}

test('agenda por Responses usa selección obligatoria y envía solo contexto permitido',async()=>{
  const phrase='¿Tengo algo para mañana con eva@example.test?';
  const api=fixture({output:[{type:'reasoning'},call('consultar_agenda',{schedule:'mañana'})]});
  let metric;
  const res=await interpretPilotPlan(phrase,{...api,onUsage:m=>{metric=m;},context:{
    hasSelection:true,contacts:['Privado'],operations:['inventado'],waiting:'privado',
    lastRead:'agenda',connections:{calendar:'connected',gmail:'dirección confidencial',sheets:'missing',owner:'Del vendedor'}}});
  assert.equal(res.kind,'agenda');
  assert.equal(res.schedule,'mañana');
  const body=api.body;
  assert.equal(body.store,false);
  assert.equal(body.model,'gpt-6.2-sol');
  assert.equal(body.parallel_tool_calls,false);
  assert.equal(body.tool_choice,'required');
  for (const tool of body.tools) {
    assert.equal(tool.strict,true);
    assert.equal(tool.parameters.additionalProperties,false);
    assert.deepEqual(tool.parameters.required,Object.keys(tool.parameters.properties));
  }
  assert.doesNotMatch(JSON.stringify(body),/eva@example\.test|Privado|inventado|dirección confidencial|Del vendedor/);
  const input=JSON.parse(body.input);
  assert.match(input.message,/\[correo 1\]/);
  assert.equal(input.context.lastRead,'agenda');
  assert.deepEqual(input.context.connections,{calendar:'connected',gmail:'unknown',availability:'unknown',sheets:'missing'});
  assert.equal(metric.latencyMs>=0,true);
  assert.deepEqual({...metric,latencyMs:0},{model:'gpt-6.2-sol',inputTokens:141,outputTokens:15,cachedInputTokens:32,cacheWriteTokens:0,longCacheWriteTokens:0,latencyMs:0,tool:'consultar_agenda'});
});

test('un encargo combinado llega al plan editable con los límites de campo y envío habituales',async()=>{
  const result=await run(mixed,'preparar_plan',{...plan,sendEmail:true});
  assert.deepEqual(result.operations,['list','appointment','email']);
  assert.equal(result.leadKind,'referrals');
  assert.equal(result.sendEmail,false);
  assert.equal(result.schedule,'mañana');
  const invented=await run(mixed,'preparar_plan',{...plan,host:'Adriana'});
  assert.equal(invented.kind,'clarify');
  const changed=await run('Mejor el segundo','preparar_plan',{...plan,kind:'revise',operations:[],ordinal:2,leadKind:'',host:'',mailInstruction:'',schedule:''},
    {context:{operations:['appointment'],waiting:'slot'}});
  assert.equal(changed.kind,'revise');
  assert.equal(changed.ordinal,2);
});

test('Luna usa las mismas herramientas, una conexión faltante llega al servidor y una fecha inventada pide aclaración',async()=>{
  const api=fixture({output:[call('consultar_agenda',{schedule:''})]});
  assert.equal((await interpretPilotPlan('A ver mi agenda',{...api,model:'gpt-6-luna',context:{connections:{calendar:'missing'}}})).kind,'agenda');
  assert.equal(api.body.model,'gpt-6-luna');
  assert.equal((await run('¿Qué tengo mañana?','consultar_agenda',{schedule:'el viernes'})).kind,'clarify');
  assert.equal((await run('Checa lo que cayó al correo','leer_bandeja')).kind,'inbox');
  assert.equal((await run('¿Hubo ventas de Hay Cite?','revisar_ventas')).kind,'sales');
  assert.equal((await run('Respóndele en ese hilo','derivar',{destino:'especialista'})).kind,'none');
});

test('ningún texto de modelo, función desconocida, argumento de identidad o respuesta incompleta autoriza una acción',async()=>{
  for (const output of [
    [],[{type:'message',content:'Tu calendario está libre'}],
    [call('leer_bandeja',{owner:'otro'})],[call('guardar_evento')],
    [call('leer_bandeja'),call('revisar_ventas')],[call('consultar_agenda',{schedule:{}})],
    [call('derivar',{destino:'confirmar'})],[call('preparar_plan',{...plan,kind:'confirm'})],
    [call('preparar_plan',{...plan,phone:33})]
  ]) {
    const value=await interpretPilotPlan(mixed,fixture({output}));
    assert.equal(value.kind,'unavailable');
  }
  assert.equal((await interpretPilotPlan(mixed,fixture({output:[call('leer_bandeja')],status:'incomplete'}))).kind,'unavailable');
  assert.equal((await interpretPilotPlan('Revisa mi agenda',{apiKey:key,fetchImpl:async()=>{throw Error('offline');}})).kind,'unavailable');
});

test('respeta un pedido de no consultar incluso si el proveedor equivoca la herramienta o falla',async()=>{
  for (const phrase of ['No leas mis correos','Sin revisar la bandeja, explícame cómo la trabajaría','No quiero que consultes Gmail']) {
    assert.equal((await run(phrase,'leer_bandeja')).kind,'clarify');
    assert.equal((await run(phrase,'derivar',{destino:'especialista'})).kind,'clarify');
    assert.equal((await interpretPilotPlan(phrase,{apiKey:''})).kind,'clarify');
  }
  assert.equal((await run('No revises el correo, pero dime qué hay en mi agenda mañana','consultar_agenda',{schedule:'mañana'})).kind,'agenda');
  assert.equal((await run('No leas mis correos y revisa mi agenda mañana','consultar_agenda',{schedule:'mañana'})).kind,'agenda');
  assert.equal((await run('No revisé mi correo ayer, ¿qué me ha entrado?','leer_bandeja')).kind,'inbox');
  assert.equal((await run('No me agendes nada, solo dime si estoy ocupado mañana','consultar_agenda',{schedule:'mañana'})).kind,'agenda');
  assert.equal((await run('No busques nada','leer_bandeja')).kind,'clarify');
});
