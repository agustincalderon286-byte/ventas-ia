import test from 'node:test';
import assert from 'node:assert/strict';
import { classifySalesMailReport, draftSalesReport } from '../src/coach-mail-sales-report.js';

const now = Date.parse('2026-11-20T18:00:00Z');
const day = 86400000;
const mailbox='seller+piloto@example.test';
const row = age => ({date:new Date(now - age * day).toISOString(), subject:'Privado', order:'SENSITIVE-137', customer:'María'});
const snapshot = { confirmed:[row(0),row(7),row(7.001),row(-3)], review:[row(2),row(89)], limited:true };

test('detecta instrucciones compuestas y resumen para el propio vendedor', () => {
  for (const input of [
    'Coach, revisa mis ventas de Hy Cite y prepara un correo a lider@example.test con el resumen.',
    'Mándame por correo mi resumen de ventas',
    'Envíale el reporte de mis pedidos a jefe@example.test',
    'Redáctame un email con el informe de ventas de Hy Cite.',
    'Revisa mis ventas y mándamelas por correo con el resumen',
    'Prepara un correo para alguien con el resumen de mis ventas.'
  ]) assert.equal(classifySalesMailReport(input), true,input);
  for (const input of [
    'Revisa mis correos de Hy Cite', 'Muéstrame mis ventas', 'Prepara un correo para Juan preguntándole por sus ventas',
    'No me mandes por correo el resumen de mis ventas', 'Si te pidiera enviar mis ventas por correo, ¿qué harías?',
    'Dame un ejemplo de cómo redactar el reporte de mis ventas por correo', 'Consulta mis ventas sin enviar el informe por correo'
  ]) assert.equal(classifySalesMailReport(input),false,input);
});

test('el informe incluye solo cifras del período solicitado y señala una consulta parcial', () => {
  const text='Revisa mis ventas y prepara un correo a boss@example.test con mi resumen';
  const draft=draftSalesReport(text,{snapshot,mailbox,days:7,now});
  assert.equal(draft.to,'boss@example.test');assert.match(draft.subject,/7 días/);
  assert.match(draft.body,/Ventas confirmadas: 2/);assert.match(draft.body,/requieren revisión: 1/);
  assert.match(draft.body,/parcial/);assert.match(draft.body,/2026/);
  for(const pii of ['María','Privado','SENSITIVE-137',mailbox]) assert.ok(!draft.body.includes(pii));
});

test('acepta exactamente un destinatario o una indicación explícita del propio buzón',()=>{
  for(const text of ['Mándame por correo mi resumen de ventas','Envíame el reporte de mis ventas','a mi correo','a mí']) {
    assert.equal(draftSalesReport(text,{snapshot,mailbox,now}).to,mailbox,text);
  }
  assert.equal(draftSalesReport('jefe@example.test',{snapshot,mailbox,now}).to,'jefe@example.test');
  for(const text of ['Prepara el correo al jefe','a@e.test y b@e.test','a@@example.test','a mí y a b@e.test']){
    assert.ok(draftSalesReport(text,{snapshot,mailbox,now}).question,text);
  }
});

test('no inventa un conteo si faltó la consulta o el período es inválido',()=>{
  for(const options of [{},{snapshot:null},{snapshot,days:0},{snapshot,days:91},{snapshot,now:'bad'}]){
    assert.ok(draftSalesReport('a mi correo',{mailbox,now,...options}).question);
  }
  const zero=draftSalesReport('a mi correo',{snapshot:{confirmed:[],review:[]},mailbox,now,days:1});
  assert.match(zero.subject,/24 horas/);assert.match(zero.body,/Ventas confirmadas: 0/);
  assert.doesNotMatch(zero.body,/parcial/);
});

test('el período del calendario usa el comienzo inclusive, el final exclusive y solo correos recibidos hasta ahora',()=>{
  const since=Date.parse('2026-11-19T06:00:00Z')/1000;
  const before=Date.parse('2026-11-20T06:00:00Z')/1000;
  const mail=ms=>({date:new Date(ms).toISOString()});
  const data={confirmed:[mail(since*1000-1),mail(since*1000),mail(before*1000-1),mail(before*1000)],review:[mail(now+5000)]};
  const result=draftSalesReport('a mi correo',{snapshot:data,mailbox,now,period:{since,before,label:'ayer'}});
  assert.match(result.subject,/ayer$/);assert.match(result.body,/Ventas confirmadas: 2/);
  const partialDay=draftSalesReport('a mi correo',{snapshot:data,mailbox,now,period:{since:before,before:before+86400,label:'hoy'}});
  assert.match(partialDay.subject,/hoy$/);assert.match(partialDay.body,/Ventas confirmadas: 1/);
  assert.match(partialDay.body,/requieren revisión: 0/);
  assert.ok(draftSalesReport('a mi correo',{snapshot:data,mailbox,now,period:{since:before,before:since,label:'ayer'}}).question);
});
