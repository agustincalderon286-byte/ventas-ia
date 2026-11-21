import test from 'node:test';
import assert from 'node:assert/strict';
import { mailPeriod } from '../src/coach-mail-period.js';
import { gmailClient } from '../src/coach-hycite-gmail.js';

const seconds = date => Date.parse(date) / 1000;
function check(phrase, now, start, end) {
  const period = mailPeriod('Revisa mis correos de ' + phrase, Date.parse(now));
  assert.deepEqual(period, {since:seconds(start), before:seconds(end), label:phrase + ' (hora de Chicago)'});
  return period;
}

test('hoy y ayer usan la fecha de Chicago incluso cuando en UTC ya comenzó otro día', () => {
  check('hoy','2026-11-21T03:00:00Z','2026-11-20T06:00:00Z','2026-11-21T06:00:00Z');
  check('ayer','2026-01-01T06:01:00Z','2025-12-31T06:00:00Z','2026-01-01T06:00:00Z');
  assert.equal(mailPeriod('Revisa las ventas de los últimos 1 días'),null);
  assert.equal(mailPeriod('Revisa mis correos de anteayer'),null);
  assert.equal(mailPeriod('Revisa mis correos de ayer y hoy'),null);
});

test('los días del cambio horario tienen límites reales de 23 y 25 horas', () => {
  const spring=check('hoy','2026-03-08T14:00:00Z','2026-03-08T06:00:00Z','2026-03-09T05:00:00Z');
  assert.equal(spring.before-spring.since,23*3600);
  const fall=check('ayer','2026-11-02T14:00:00Z','2026-11-01T05:00:00Z','2026-11-02T06:00:00Z');
  assert.equal(fall.before-fall.since,25*3600);
});

test('semana comienza en lunes, mes en su primer día y cada límite tiene su propio offset', () => {
  check('esta semana','2026-03-08T14:00:00Z','2026-03-02T06:00:00Z','2026-03-09T05:00:00Z');
  check('esta semana','2026-03-09T05:00:00Z','2026-03-09T05:00:00Z','2026-03-16T05:00:00Z');
  check('este mes','2026-11-20T14:00:00Z','2026-11-01T05:00:00Z','2026-12-01T06:00:00Z');
  check('este mes','2026-12-31T12:00:00Z','2026-12-01T06:00:00Z','2027-01-01T06:00:00Z');
});

test('Gmail distingue un día calendario de la ventana rodante y rechaza intervalos ambiguos', async t => {
  const queries=[];
  t.mock.method(globalThis,'fetch',async address=>{
    queries.push(new URL(address).searchParams.get('q'));
    return Response.json({});
  });
  const gmail=gmailClient({origin:'https://example.invalid'});
  const period=mailPeriod('Revisa mis correos de hoy', Date.parse('2026-11-20T18:13:11Z'));
  await gmail.inbox('token',period);
  await gmail.inbox('token',{days:1});
  assert.deepEqual(queries, ['after:'+period.since+' before:'+period.before,'newer_than:1d']);
  for (const options of [{days:1,...period},{since:'0',before:1},{since:5,before:5},{since:5},
    {before:20},{since:-1,before:2},{since:0,before:Number.MAX_SAFE_INTEGER}]) {
    await assert.rejects(gmail.inbox('token',options),TypeError);
  }
  assert.equal(queries.length,2);
});
