import test from 'node:test';
import assert from 'node:assert/strict';
import { nameFromTitle, resolveDemoHost } from '../src/coach-demo-host.js';
import { verifyAutoDemoClose } from '../src/coach-demo-auto-close.js';

// Fictitious people and 555-01xx phones only.
const startedAt = new Date('2026-11-30T22:05:00Z');
const records = [
  { id: 'a1', name: 'María López', phone: '7735550101', appointmentAt: new Date('2026-11-30T22:00:00Z') },
  { id: 'b2', name: 'Rosa Gil', phone: '7735550102', appointmentAt: new Date('2026-11-28T16:00:00Z') },
  { id: 'c3', name: 'María Fernanda Ruiz', phone: '7735550103', appointmentAt: null }
];
const visit = 'Buenas tardes María, gracias por recibirnos. Mire, esta olla de acero quirúrgico cocina sin agua.';

test('the calendar title gives the host name', () => {
  assert.equal(nameFromTitle('Demo · María López (4 en 14 de Rosa Gil)'), 'María López');
  assert.equal(nameFromTitle('Seguimiento · Juana Prueba'), 'Juana Prueba');
  assert.equal(nameFromTitle('Demo con Ana Soto'), 'Ana Soto');
  assert.equal(nameFromTitle('Visita TEC'), '');
  assert.equal(nameFromTitle('Llamar 773 555 0101'), '');
});

test('a dictated phone already in the CRM decides the host', () => {
  const host = resolveDemoHost({ heard: { name: 'Rosa', phone: '7735550102' }, transcript: visit, startedAt, records });
  assert.deepEqual(host, { name: 'Rosa Gil', phone: '7735550102', crmId: 'b2', source: 'telefono' });
});

test('a full name heard that is one CRM client is that client', () => {
  const host = resolveDemoHost({ heard: { name: 'Maria Fernanda Ruiz', phone: '' }, transcript: visit, startedAt, records });
  assert.equal(host.crmId, 'c3');
  assert.equal(host.source, 'nombre');
});

test('only a first name, crossed with the CRM appointment at this hour', () => {
  // «María» alone matches two clients, but only María López has an appointment now.
  const host = resolveDemoHost({ heard: { name: 'María', phone: '' }, transcript: visit, startedAt, records });
  assert.equal(host.crmId, 'a1');
  assert.equal(host.source, 'cita');
  // Nobody named as host by the model: the appointment now and her name said in the visit are enough.
  assert.equal(resolveDemoHost({ heard: null, transcript: visit, startedAt, records }).crmId, 'a1');
  // An appointment now with someone never named in the visit is not enough.
  assert.equal(resolveDemoHost({ heard: null, transcript: 'Buenas tardes, gracias por recibirnos.', startedAt, records }), null);
});

test('the TEC calendar appointment at this hour names the host', () => {
  const events = [{ title: 'Demo · Ana Soto (4 en 14 de Rosa Gil)', start: '2026-11-30T16:00:00-06:00' },
    { title: 'Seguimiento · Carla Tres', start: '2026-12-01T18:00:00-06:00' }];
  const host = resolveDemoHost({ heard: { name: 'Ana', phone: '' }, transcript: 'Hola Ana, ¿dónde conecto la olla?', startedAt, records, events });
  assert.deepEqual(host, { name: 'Ana Soto', phone: '', source: 'calendario' });
});

test('a new host heard with name and surname is a new client; the seller and referrals never are the host', () => {
  assert.deepEqual(resolveDemoHost({ heard: { name: 'Lucía Prueba', phone: '3125550147' }, transcript: 'Gracias Lucía', startedAt, records: [] }),
    { name: 'Lucía Prueba', phone: '3125550147', source: 'grabacion' });
  assert.equal(resolveDemoHost({ heard: { name: 'Lucía', phone: '' }, transcript: 'Gracias Lucía', startedAt, records: [] }), null);
  assert.equal(resolveDemoHost({ heard: { name: 'Agustín Vendedor', phone: '' }, transcript: 'Soy Agustín', startedAt, records: [],
    sellerName: 'Agustín Vendedor' }), null);
  assert.equal(resolveDemoHost({ heard: { name: 'Juana Pérez', phone: '' }, transcript: 'anota a Juana Pérez', startedAt, records: [],
    referralNames: ['Juana Pérez'] }), null);
});

test('the host is kept only when the recording names them', () => {
  const chunks = [{ index: 0, durationSeconds: 240, text: 'Muchas gracias Lucía Prueba por abrirnos su casa, su teléfono es 312 555 0147 para el regalo.' }];
  const base = { referrals: [], incompleteReferrals: [], survey: [], products: [], followUp: null, referralAppointments: [] };
  const kept = verifyAutoDemoClose({ ...base, host: { fullName: 'Lucía Prueba', phone: '3125550147', segment: 1,
    quote: 'Muchas gracias Lucía Prueba por abrirnos su casa, su teléfono es 312 555 0147' } }, chunks);
  assert.deepEqual(kept.host, { name: 'Lucía Prueba', phone: '3125550147' });
  const invented = verifyAutoDemoClose({ ...base, host: { fullName: 'Marta Inventada', phone: '', segment: 1,
    quote: 'Muchas gracias Lucía Prueba por abrirnos su casa' } }, chunks);
  assert.equal(invented.host, undefined);
  const wrongPhone = verifyAutoDemoClose({ ...base, host: { fullName: 'Lucía Prueba', phone: '3125550199', segment: 1,
    quote: 'Muchas gracias Lucía Prueba por abrirnos su casa' } }, chunks);
  assert.deepEqual(wrongPhone.host, { name: 'Lucía Prueba', phone: '' });
});
