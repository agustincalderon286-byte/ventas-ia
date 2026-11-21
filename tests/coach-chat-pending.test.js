import test from 'node:test';
import assert from 'node:assert/strict';
import { directDraftInput, scopeCoachPending } from '../src/coach-chat-pending.js';

const all = { accountId:'a', coachMailPending:'m', coachCalendarPending:'c', coachWhatsAppPending:'w' };

test('una corrección llega solo al borrador que el vendedor vio por último', () => {
  assert.deepEqual(scopeCoachPending({ ...all, coachDraftFocus:'mail' }, 'cambia jueves por viernes'),
    { accountId:'a', coachMailPending:'m' });
  assert.deepEqual(scopeCoachPending({ ...all, coachDraftFocus:'whatsapp' }, 'Mejor más amable'),
    { accountId:'a', coachWhatsAppPending:'w' });
});

test('las peticiones explícitas de otro canal conservan todos los contextos', () => {
  for (const text of ['Responde al primero', 'Busca dos horarios libres para la demo', 'Agenda una llamada con María mañana a las 6 pm',
    'Prepara un WhatsApp de seguimiento para María', '1', 'Confirmar cita AABBCC112233']) {
    assert.deepEqual(scopeCoachPending({ ...all, coachDraftFocus:'mail' }, text), all, text);
  }
  assert.deepEqual(scopeCoachPending({ ...all, coachDraftFocus:'mail' }, 'cambia el WhatsApp: hola'), all);
  assert.deepEqual(scopeCoachPending({ ...all, coachDraftFocus:'whatsapp' }, 'corrige el correo: hola'), all);
});

test('sin foco válido o sin su contexto no se altera nada y la pista nunca llega a los agentes', () => {
  assert.deepEqual(scopeCoachPending({ ...all }, 'cambia jueves por viernes'), all);
  assert.deepEqual(scopeCoachPending({ ...all, coachDraftFocus:'calendar' }, 'cambia jueves por viernes'), all);
  assert.deepEqual(scopeCoachPending({ ...all, coachDraftFocus:'__proto__' }, 'cambia jueves por viernes'), all);
  const { coachMailPending, ...noMail } = all;
  assert.deepEqual(scopeCoachPending({ ...noMail, coachDraftFocus:'mail' }, 'cambia jueves por viernes'), noMail);
  assert.equal(scopeCoachPending(undefined, 'x'), undefined);
});

test('los botones con código conservan su canal aunque Calendar tenga una pregunta abierta', () => {
  const pending = { ...all, coachDraftFocus:'whatsapp', coachQuestionFocus:'calendar' };
  for (const text of ['Confirmar envío A0B1C2D3E4F5', 'Cancelar borrador A0B1C2D3E4F5']) {
    assert.deepEqual(directDraftInput(pending, text), { channel:'mail', literal:false });
  }
  for (const text of ['Confirmar cita A0B1C2D3E4F5', 'Descartar WhatsApp A0B1C2D3E4F5', 'Confirmar envío', 'Revisa mis correos']) {
    assert.equal(directDraftInput(pending, text), null);
  }
});

test('los comandos escritos en el texto de un borrador se conservan como texto', () => {
  const text='Mensaje: Busca dos horarios libres y confirma la cita.';
  for (const channel of ['mail','whatsapp']) {
    assert.deepEqual(directDraftInput({...all,coachDraftFocus:channel,coachQuestionFocus:'calendar'},text), {channel,literal:true});
  }
  assert.equal(directDraftInput({...all,coachDraftFocus:'calendar'},text), null);
  assert.equal(directDraftInput({coachDraftFocus:'mail',coachCalendarPending:'c'},text), null);
});
