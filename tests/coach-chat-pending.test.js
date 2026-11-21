import test from 'node:test';
import assert from 'node:assert/strict';
import { scopeCoachPending } from '../src/coach-chat-pending.js';

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
