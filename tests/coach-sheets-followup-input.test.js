import test from 'node:test';
import assert from 'node:assert/strict';
import {classifySheetsFollowup,sheetsToday} from '../src/coach-sheets-followup-input.js';

const today = '2026-11-21';
const classify = (row,opts) => classifySheetsFollowup(row,{today,locale:'en_US',...opts});

test('obtiene el día civil de cada vendedor a ambos lados de medianoche y con horario de verano', () => {
  const instant = Date.parse('2026-11-22T05:30:00Z');
  assert.equal(sheetsToday(instant,'America/Chicago'),today);
  assert.equal(sheetsToday(instant,'America/New_York'),'2026-11-22');
  assert.equal(sheetsToday(Date.parse('2026-07-02T04:30:00Z'),'America/Chicago'),'2026-07-01');
});

test('distingue hoy, futuras, vencidas y fechas inválidas en la columna de próxima gestión', () => {
  assert.equal(classify({followupDate:today}).bucket,'today');
  assert.equal(classify({followupDate:'11/22/26'}).bucket,'future');
  assert.equal(classify({followupDate:'20 de noviembre del 2026'}).bucket,'overdue');
  assert.equal(classify({followupDate:'03/04/2026'}).date,'2026-03-04');
  assert.equal(classify({followupDate:'03/04/2026'},{locale:'es_MX'}).date,'2026-04-03');
  for (const date of ['2026-02-30','31/11/26','21-11/2026','mañana']) {
    const followup = classify({followupDate:date});
    assert.equal(followup.bucket,'undated');
    assert.match(followup.reason,/revisión/);
  }
});

test('asocia una fecha solo con una acción concreta y devuelve un motivo legible', () => {
  for (const content of ['Llamar el 21/11/2026','Quedó en que le llamemos el 2026-11-21',
    'Pide que le marque. Volver a llamar el sábado 21 de noviembre de 2026','Marcarle para el 21 de noviembre de 2026']) {
    const action = classify({history:content});
    assert.equal(action.bucket,'today',content);
    assert.match(action.reason,/llamar/);
  }
  assert.equal(classify({history:'Visita realizada el 21/11/2026'}),null);
  assert.equal(classify({history:'Llamar después de la compra del 21/11/2026'}).bucket,'undated');
  assert.equal(classify({history:'21/11/2026: No contestó.'}).bucket,'undated');
  assert.equal(classify({history:'Intenté llamar el 21/11/26, no contestó.'}).bucket,'undated');
  assert.equal(classify({history:'Se hizo el seguimiento el 21/11/26.'}),null);
  assert.equal(classify({history:'Seguimiento realizado el 21/11/26.'}),null);
  assert.equal(classify({history:'Llamar el viernes 21 de noviembre de 2026.'}).bucket,'undated');
  assert.equal(classify({history:'Llamar el 03/04/2026.'}).bucket,'undated');
});

test('fecha los compromisos relativos únicamente cuando esa gestión está fechada', () => {
  assert.equal(classify({history:'2026-11-20: volver a llamar mañana'}).bucket,'today');
  assert.equal(classify({history:'20 de noviembre de 2026, llamar pasado mañana.'}).date,'2026-11-22');
  assert.equal(classify({history:'Jueves. 19/11/26. 9:00am. Llamar el sábado.'}).bucket,'today');
  assert.equal(classify({history:'2021-11-20: llamar mañana.'}).bucket,'overdue');
  for (const history of ['Volver a llamar hoy','Llamar mañana','Llamar el lunes',
    '20/11/2026: llamar por la mañana','21/11/26: llamar en 2 días hábiles']) {
    assert.equal(classify({history}).bucket,'undated',history);
  }
});

test('usa fecha, hora y resultado de la última gestión en las bitácoras Chicago', () => {
  const early = 'Jueves. 15/07/21. 3:24pm. Llamar el 21/11/2026.';
  for (const finalNote of ['Teléfono desconectado.','No recibe llamadas entrantes.','No le interesa.']) {
    assert.equal(classify({history:early+' / Martes. 19/10/21. 1:00pm. '+finalNote}),null);
  }
  const last = 'Martes. 19/10/21. 1:00pm. Buzón.';
  assert.match(classify({history:last+' / '+early}).reason,/sin respuesta/);
  assert.equal(classify({history:early+' / '+last}).bucket,'undated');
  assert.equal(classify({history:'21/11/26. 4:00pm. Buzón. / 21/11/26. 8:30am. Llamar hoy.'}).bucket,'undated');
  assert.equal(classify({history:'Miércoles. 04/11/26. 3:00pm. Llamar mañana.'}).date,'2026-11-05');
  assert.equal(classify({history:'19/10/21: Buzón. / 04/11/26: Llamar mañana.'}).date,'2026-11-05');
  assert.equal(classify({history:'31/07/21: Buzón. / 04/10/26: Llamar el 05/10/26.'}).date,'2026-10-05');
  assert.equal(classify({history:'Viernes. 04/11/26. 3:00pm. Llamar mañana.'}).bucket,'undated');
});

test('respeta la exclusión de contacto, los resultados de cita y las fechas históricas', () => {
  for (const result of ['Venta','Rechazado','Cita confirmada','Venta realizada','NO LLAMAR']) {
    assert.equal(classify({result,history:'Llamar hoy.',followupDate:today}),null,result);
  }
  assert.equal(classify({history:'19/11/26. Pidió no volver a llamar. / 20/11/26. Pendiente.',followupDate:today}),null);
  assert.equal(classify({history:'2026-11-19: volver a llamar en 2 días.\n2026-11-20: sin respuesta.'}).bucket,'undated');
  assert.equal(classify({result:'19/11/26: volver a llamar el 21/11/26.',history:'20/11/26: buzón.'}).bucket,'undated');
  assert.equal(classify({history:'20/11/2026: llamar mañana',status:'Descartada'}),null);
  assert.equal(classify({history:'No contestó',result:'Reagendar el 21 de noviembre de 2026'}).bucket,'today');
});

test('conserva los pendientes explícitos sin inventar compromisos para toda la base', () => {
  assert.match(classify({history:'Viernes. 20/11/26. 9:00am. NC.'}).reason,/NC.+2026-11-20/);
  for (const row of [{history:'Buzón.'},{result:'no contestó'},{status:'Por contactar'},{history:'Volver a llamar.'}]) {
    assert.equal(classify(row).bucket,'undated');
    assert.equal(classify(row).date,'');
  }
  for (const row of [{},{fullName:'Contacto de prueba'},{history:'Participó el 21/11/2026 en la rifa.'},
    {history:'Fecha de captura 2026-11-21',result:'Compra anterior'}]) assert.equal(classify(row),null);
});

test('las continuaciones de una gestión conservan su fecha al ordenar el historial', () => {
  const old='Viernes. 20/11/26. 8:00. Llamar la próxima semana.';
  for (const separator of ['; ','\n']) {
    const note='sábado. 21/11/26. 10:15. No contestó'+separator+'llamar hoy a las 5';
    for (const history of [note,old+' / '+note,note+' / '+old,note+'\n'+old]) {
      const row=classify({history});
      assert.equal(row.bucket,'today',history);
      assert.match(row.reason,/llamar.+2026-11-21/);
    }
  }
  assert.equal(classify({history:'Sáb. 21/11/26. 10:15. Buzón. / llamar hoy'}).bucket,'undated');
});

test('un resultado genérico sin fecha conserva el compromiso explícito de una gestión fechada', () => {
  const history='sábado. 21/11/26. 10:15. No contestó, llamar hoy';
  for (const result of ['No contestó','Pendiente','Buzón.']) {
    const action=classify({history,result});
    assert.equal(action.date,today);
    assert.match(action.reason,/llamar.+2026-11-21/);
  }
  assert.equal(classify({history,result:'Cita confirmada'}),null);
  assert.equal(classify({history,result:'22/11/26: Buzón.'}).bucket,'undated');
});

test('valida el día abreviado en las cabeceras igual que el nombre completo', () => {
  assert.equal(classify({history:'Sáb. 21/11/26. 10:15. Llamar hoy'}).date,today);
  assert.equal(classify({history:'Vie. 20/11/26. 9:00am. Llamar mañana'}).date,today);
  assert.equal(classify({history:'Mié. 04/11/26. 9:00am. Llamar mañana'}).date,'2026-11-05');
  assert.equal(classify({history:'Mié. 21/11/26. 9:00am. Llamar mañana'}).bucket,'undated');
});
