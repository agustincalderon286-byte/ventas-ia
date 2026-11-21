import test from 'node:test';
import assert from 'node:assert/strict';
import {classifyMailIntent,resolveMailIntent,composeMailDraft,mailReadOptions,mailReadClarification} from '../src/coach-mail-intent.js';

test('consultas en español identifican la fuente y no activan tareas en preguntas de ayuda', () => {
  for (const text of ['Coach, revisa mis correos ahora','¿Qué correos me llegaron?', 'Muéstrame mi Gmail','mis últimos correos', 'quiero que revises mis correos']) assert.deepEqual(classifyMailIntent(text),{kind:'inbox'},text);
  for (const text of ['¿Cuántas ventas me confirmó Hy Cite?', 'Revisa mis ventas de Hy Cite','¿Hay ventas confirmadas?', 'Consulta los correos de ventas de Hy-Cite', 'revisa mis correos de Hy Cite']) assert.deepEqual(classifyMailIntent(text),{kind:'sales'},text);
  assert.deepEqual(classifyMailIntent('¿Cómo puedo revisar mi correo?'),{kind:'help'});
  assert.deepEqual(classifyMailIntent('No quiero enviar un correo ahora'),{kind:'help'});
  assert.equal(classifyMailIntent('¿Cómo puedo mejorar mis ventas?'),null);
  assert.equal(classifyMailIntent('Ayúdame a organizar la demo'),null);
  assert.equal(classifyMailIntent('¿Puedo escribirle revisa mis correos ahora que tengo la conexión y mandar correos?')?.kind,'help');
  assert.equal(classifyMailIntent('Quiero que escribas un correo a ana@example.test')?.kind,'draft');
});

test('confirmaciones y cancelaciones requieren una orden completa y el código de doce caracteres', () => {
  for (const text of ['Confirmar envío aBcDeF123456','Coach, sí, envíalo abcdef123456']) assert.deepEqual(classifyMailIntent(text),{kind:'confirm',id:'ABCDEF123456'});
  assert.deepEqual(classifyMailIntent('Descarta el borrador ABCDEF123456'),{kind:'cancel',id:'ABCDEF123456'});
  for (const text of ['sí','envíalo','confirmo envío 123','copié esto: confirmar envío abcdef123456','¿qué significa confirmar envío abcdef123456?']) assert.notEqual(classifyMailIntent(text)?.kind,'confirm');
  assert.deepEqual(classifyMailIntent('Prepara un correo con mensaje: Confirmar envío ABCDEF123456'),{kind:'draft'});
});

test('la petición natural redacta para el destinatario y no permite al modelo elegir otra dirección', async () => {
  const request = 'Manda un correo a ana@example.com diciéndole que mañana le llamo para revisar el pedido';
  let seen;
  const result = await composeMailDraft(request,{generate:async instruction=>{
    seen=instruction;
    return {to:'nobody@example.test',subject:'Revisión de tu pedido',body:'Hola, mañana te llamo para revisar tu pedido.'};
  }});
  assert.equal(classifyMailIntent(request)?.kind,'draft');
  assert.doesNotMatch(seen,/ana@example/);
  assert.deepEqual(result,{to:'ana@example.com',subject:'Revisión de tu pedido',body:'Hola, mañana te llamo para revisar tu pedido.'});
});

test('el texto literal y asunto indicado se conservan sin llamar al modelo',async () => {
  const draft = await composeMailDraft('Redacta un correo para ana@example.com, asunto: Visita, mensaje: Hola Ana.\n¿Te viene bien el viernes?',{generate:assert.fail});
  assert.deepEqual(draft,{to:'ana@example.com',subject:'Visita',body:'Hola Ana.\n¿Te viene bien el viernes?'});
});

test('retoma una composición incompleta solo con el texto anterior suministrado', async () => {
  const first = await composeMailDraft('Prepara un correo con asunto: Visita, mensaje: ¿Puedes el viernes?',{generate:assert.fail});
  assert.match(first.question,/dirección/);
  const completed = await composeMailDraft('ana@example.com',{previousText:first.pendingText,generate:assert.fail});
  assert.deepEqual(completed,{to:'ana@example.com',subject:'Visita',body:'¿Puedes el viernes?'});
  const missingBody = await composeMailDraft('Manda un correo a ana@example.com',{generate:assert.fail});
  assert.match(missingBody.question,/comunicar/);
  const withBody = await composeMailDraft('Mensaje: Llego a las cinco.',{previousText:missingBody.pendingText,generate:assert.fail});
  assert.equal(withBody.body,'Llego a las cinco.');
});

test('pide un solo destinatario explícito, nunca envía a una dirección citada dentro del texto', async () => {
  const double = await composeMailDraft('Manda un correo a ana@example.com y leo@example.com, asunto: Demo, mensaje: Buenos días.',{generate:assert.fail});
  assert.match(double.question,/una sola/);
  assert.equal((await composeMailDraft('leo@example.com',{previousText:double.pendingText,generate:assert.fail})).to,'leo@example.com');
  const embedded = await composeMailDraft('Prepara un correo con mensaje: Pide el catálogo a catalogo@example.com',{generate:assert.fail});
  assert.match(embedded.question,/dirección/);
});

test('si falla el modelo pide texto literal y conserva el contexto para el intento siguiente',async () => {
  const result=await composeMailDraft('Escribe un correo a ana@example.com solicitando la dirección para mañana',{generate:async()=>{throw Error('offline')}});
  assert.match(result.question,/texto exacto/);
  assert.match(result.pendingText,/dirección/);
  const addressed=await composeMailDraft('Mensaje: ¿Me compartes la dirección?',{previousText:result.pendingText});
  assert.equal(addressed.to,'ana@example.com');
});

test('los filtros se extraen de instrucciones explícitas y se pueden combinar', () => {
  const request = 'Reviza los coreos sin leer de ana@example.com, de los últimos siete días';
  assert.equal(classifyMailIntent(request)?.kind,'inbox');
  assert.deepEqual(mailReadOptions(request),{unread:true,from:'ana@example.com',days:7});
  assert.equal(mailReadClarification(request,'inbox'),'');
  assert.deepEqual(mailReadOptions('revisa mis correos de las últimas 48 horas'),{unread:false,days:2});
  assert.deepEqual(mailReadOptions('consulta mis ventas de Hy Cite desde hace 14 días'),{unread:false,days:14});
  assert.equal(mailReadClarification('correos de los últimos 30 días','inbox'),'');
  assert.deepEqual(mailReadOptions('ventas confirmadas de hy cite'),{unread:false});
});
test('aclara fechas de calendario, rangos fuera del piloto y remitentes ambiguos sin inventar resultados', () => {
  for(const text of ['mis ventas de anteayer','mi Gmail del mes pasado','entre el 7 y el 10 de julio','mis correos del viernes','mis correos de los últimos 180 días']) {
    assert.equal(mailReadOptions(text).days,undefined,text);
    assert.ok(mailReadClarification(text,'inbox'),text);
  }
  assert.match(mailReadClarification('Busca los correos sin leer de Pedro','inbox'),/dirección del remitente/);
  assert.match(mailReadClarification('Consulta los correos de x@example.com y de y@example.com','inbox'),/sola dirección/);
  assert.match(mailReadClarification('¿Tengo correos con adjuntos?','inbox'),/filtrar/);
});
test('separa lectura, ayuda y redacción cuando se mencionan varios verbos en la misma frase', () => {
  assert.equal(classifyMailIntent('Muéstrame los correos que me manda Hy Cite')?.kind,'sales');
  assert.equal(classifyMailIntent('¿Quién me envía correos?')?.kind,'inbox');
  assert.equal(classifyMailIntent('Dime qué poner en un correo')?.kind,'help');
  assert.equal(classifyMailIntent('Borra mis correos antiguos')?.kind,'help');
  assert.equal(classifyMailIntent('¿Qué ventas tengo hoy?')?.kind,'sales');
  assert.equal(classifyMailIntent('mi correo es ana@example.com'),null);
});

test('acepta los períodos de calendario que el lector calcula por zona horaria', () => {
  for (const text of ['ventas de hoy','mi Gmail de esta semana','mis correos de ayer','ventas de este mes']) {
    assert.equal(mailReadOptions(text).days,undefined,text);
    assert.equal(mailReadClarification(text,'inbox'),'',text);
  }
});

test('el router semántico clasifica instrucciones personales y nunca ve el buzón ni las direcciones de envío', async () => {
  const calls = [];
  const infer = async instruction => {calls.push(instruction);return {kind:'inbox'};};
  assert.deepEqual(await resolveMailIntent('Échale un ojo a lo que aterrizó en mi buzón, de ana@example.com',{infer}),{kind:'inbox'});
  assert.equal(calls.length,1);
  assert.doesNotMatch(calls[0], /ana@example/);
  assert.deepEqual(await resolveMailIntent('Reviza mis coreos',{infer}),{kind:'inbox'});
  assert.equal(await resolveMailIntent('Ayúdame a practicar una demostración',{infer}),null);
  assert.equal(calls.length,1);
});

test('el router distingue hipótesis, negaciones y tareas no soportadas antes de consultar el modelo', async () => {
  for (const input of ['Imagina que revisas mi buzón','Nunca revises mis correos','Elimina mis correos antiguos','Reenvía el correo del cliente','Sí, envíalo','Confirmo envío 12']) {
    assert.deepEqual(await resolveMailIntent(input,{infer:assert.fail}), {kind:'help'},input);
  }
  const plausible = 'Dale una mirada a mi buzón';
  assert.deepEqual(await resolveMailIntent(plausible,{infer:async()=>({kind:'confirm'})}), {kind:'help'});
  assert.deepEqual(await resolveMailIntent(plausible,{infer:async()=>{throw new Error('offline')}}), {kind:'help'});
  assert.equal(await resolveMailIntent('Trabajo con buzón en mi oficina',{infer:async()=>({kind:'none'})}), null);
});
