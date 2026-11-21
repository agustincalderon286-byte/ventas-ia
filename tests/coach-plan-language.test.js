import test from 'node:test';
import assert from 'node:assert/strict';
import { interpretCoachPlan } from '../src/coach-plan-language.js';
import { readFollowup } from '../src/coach-followup-input.js';

const decision = (changes={})=>({kind:'plan',operations:[],select:[],contact:'',host:'',ordinal:0,bucket:'',leadKind:'',
  phone:'',note:'',noteSchedule:'',schedule:'',slots:false,to:'',mailInstruction:'',sendEmail:false,question:'',...changes});
const proposed = (text,changes,context={})=>interpretCoachPlan(text,{context,infer:async()=>decision(changes)});

test('el modelo estructurado recibe una petición coloquial sin filtro de palabras clave ni datos de Google',async t=>{
  const old=process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY='test-key';
  t.after(()=>{if(old===undefined) delete process.env.OPENAI_API_KEY;else process.env.OPENAI_API_KEY=old;});
  let sent;
  t.mock.method(globalThis,'fetch',async(url,options)=>{
    assert.equal(url,'https://api.openai.com/v1/chat/completions');
    sent=JSON.parse(options.body);
    return Response.json({choices:[{message:{content:JSON.stringify(decision({operations:['list'],bucket:'overdue'}))}}]});
  });
  const phrase='¿A quién le prometí marcarle y se me pasó?';
  const result=await interpretCoachPlan(phrase,{context:{operations:['list','token privado'],hasSelection:false,
    rows:[{nombre:'Nombre importado'}],calendarTitle:'Calendario privado',lastEmail:'Respuesta de un cliente',waiting:'otra cuenta'}});
  assert.deepEqual(result.operations,['list']);
  assert.equal(result.bucket,'overdue');
  assert.deepEqual(JSON.parse(sent.messages[1].content),{message:phrase,context:{hasSelection:false,hasReview:false,operations:['list'],waiting:''}});
  assert.equal(sent.store,false);
  assert.equal(sent.response_format.type,'json_schema');
  assert.equal(sent.response_format.json_schema.strict,true);
  assert.equal(sent.response_format.json_schema.schema.additionalProperties,false);
  assert.doesNotMatch(JSON.stringify(sent),/Nombre importado|Calendario privado|Respuesta de un cliente|token privado|otra cuenta/);
});

test('una petición de voz puede combinar hoja, cita y correo y distinguir anfitrión y cliente',async()=>{
  const text='La señora que me refirió Rosa quiere que le marque el viernes. Apúntalo, ponme una cita y déjame un correo para confirmar la llamada.';
  const result=await proposed(text,{operations:['list','note','appointment','email'],host:'Rosa',note:'quiere que le marque el viernes',
    schedule:'el viernes',mailInstruction:'para confirmar la llamada'});
  assert.equal(result.kind,'plan');
  assert.deepEqual(result.operations,['list','note','appointment','email']);
  assert.equal(result.host,'Rosa');
  assert.equal(result.contact,'');
  assert.equal(result.schedule,'el viernes');
  assert.equal(result.to,'');
  assert.deepEqual(result.select,[]);
});

test('revisar un dato conserva la ambigüedad real y las referencias ordinales',async()=>{
  const context={operations:['note','appointment','email'],hasSelection:true};
  const change=await proposed('Mejor a las cinco',{kind:'revise',schedule:'a las cinco'},context);
  assert.equal(change.kind,'revise');
  assert.deepEqual(readFollowup(change.schedule,{}, {timeZone:'America/Chicago'}).state.clock,{ambiguous:5,minute:0});
  const person=await proposed('Con la segunda, porfa',{kind:'revise',ordinal:2},{operations:['list'],waiting:'choice'});
  assert.equal(person.ordinal,2);
  assert.equal((await proposed('Mejor a las cinco',{kind:'revise',schedule:'a las cinco'})).kind,'clarify');
});

test('dirección y teléfono del vendedor solo se restauran localmente; el plan puede ser directo',async()=>{
  const text='Toma la boleta de Eva, (312) 555-0182, y hazle un mail a eva@example.test para confirmar su interés';
  const result=await interpretCoachPlan(text,{infer:async({message})=>{
    assert.doesNotMatch(message,/eva@example|555-0182/);
    const phone=message.match(/\[teléfono \d+\]/)[0];
    return decision({operations:['add','email'],contact:'Eva',phone,leadKind:'tickets',to:'[correo 1]',mailInstruction:'para confirmar su interés'});
  }});
  assert.equal(result.kind,'plan');
  assert.equal(result.phone,'(312) 555-0182');
  assert.equal(result.to,'eva@example.test');
  const direct=await proposed('Agenda una cita con Eva mañana y prepárale el correo',{operations:['appointment','email'],contact:'Eva',schedule:'mañana'});
  assert.deepEqual(direct.operations,['appointment','email']);
  assert.equal(direct.to,'');
});

test('elegir parte de una propuesta es una selección editable y nunca una autorización',async()=>{
  const context={operations:['note','appointment','email'],hasReview:true};
  const text='Guarda nada más la nota y la cita, el correo déjamelo tantito.';
  const subset={kind:'select',select:['note','appointment']};
  const result=await proposed(text,subset,context);
  assert.deepEqual(result.select,['note','appointment']);
  assert.deepEqual(result.operations,[]);
  assert.equal(result.kind,'select');
  assert.equal((await proposed(text,subset)).kind,'clarify');
  assert.equal((await proposed(text,{kind:'select',select:['add']},context)).kind,'clarify');
  assert.equal((await proposed('Guarda la cita a las cinco',{kind:'select',select:['appointment'],schedule:'a las cinco'},context)).kind,'clarify');
});

test('rechaza campos inventados, operaciones fuera de catálogo y varios destinatarios',async()=>{
  assert.equal((await proposed('Es para Rosa',{operations:['note'],contact:'Juan'})).kind,'clarify');
  assert.equal((await proposed('Para eva@example.test',{operations:['email'],to:'otra@example.test'})).kind,'clarify');
  const cc=await proposed('Prepáralo para a@example.test y b@example.test',{operations:['email'],to:'[correo 1]'});
  assert.equal(cc.kind,'clarify');
  for (const invalid of [{operations:['send']},{operations:['email'],owner:'otro'},
    {operations:['note','note']},{operations:['note'],ordinal:-42},{kind:'confirm'}]) {
    assert.equal((await proposed('Sí, hazlo',invalid)).kind,'unavailable');
  }
  assert.equal((await interpretCoachPlan('mira quién quedó de volver',{infer:async()=>{throw Error('timeout');}})).kind,'unavailable');
});

test('las hipótesis, capacidades y operaciones no disponibles nunca llevan pasos ejecutables',async()=>{
  for (const kind of ['none','unsupported','capabilities']) {
    const value=await proposed('Si todos compraran, ¿qué podrías hacer?',{kind,operations:['email'],to:'inventado'});
    assert.equal(value.kind,kind);
    assert.deepEqual(value.operations,[]);
    assert.equal(value.to,'');
  }
});

test('un borrador no ofrece envío hasta que el vendedor se lo pide expresamente',async()=>{
  const context={operations:['note','appointment','email'],hasReview:true};
  for(const text of ['Prepárale un correo para mañana','Prepárale el correo pero no lo mandes','Sin enviar el correo']) {
    const draft=await proposed(text,{operations:['email'],sendEmail:true});
    assert.equal(draft.sendEmail,false,text);
  }
  const send=await proposed('Mándale el correo a eva@example.test',{operations:['email'],to:'[correo 1]',sendEmail:true});
  assert.equal(send.sendEmail,true);
  const pick=await proposed('Ahora sí envíalo',{kind:'select',select:['email'],sendEmail:true},context);
  assert.deepEqual(pick.select,['email']);
  assert.equal(pick.sendEmail,true);
  const keep=await proposed('Guarda la cita, no envíes el correo',{kind:'select',select:['appointment','email'],sendEmail:true},context);
  assert.deepEqual(keep.select,['appointment']);
  assert.equal(keep.sendEmail,false);
});

test('varios clientes o un apodo ambiguo requieren aclaración sin hacer cambios',async()=>{
  const q='¿Con cuál de los dos prospectos quieres empezar?';
  const many=await proposed('A uno pásale el martes, al otro anótale que no le marque',{kind:'clarify',question:q});
  assert.equal(many.question,q);
  assert.deepEqual(many.operations,[]);
});

test('la cita y la próxima gestión en la hoja pueden llevar fechas distintas',async()=>{
  const text='En la hoja deja la próxima llamada para el martes y agéndame la demo el viernes por la tarde.';
  const separate=await proposed(text,{operations:['note','appointment'],noteSchedule:'el martes',schedule:'el viernes por la tarde'});
  assert.equal(separate.noteSchedule,'el martes');
  assert.equal(separate.schedule,'el viernes por la tarde');
  const only=await proposed('Anota que contestó y ponme cita el viernes',{operations:['note','appointment'],note:'contestó',schedule:'el viernes'});
  assert.equal(only.noteSchedule,'');
  const revise=await proposed('Mejor a las cinco',{kind:'revise',schedule:'a las cinco'},{operations:['appointment','note'],hasReview:true});
  assert.equal(revise.noteSchedule,'');
});
