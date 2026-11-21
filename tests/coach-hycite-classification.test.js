import test from 'node:test';
import assert from 'node:assert/strict';
import { summarizeMessages } from '../src/coach-hycite-gmail.js';

function email(id, timestamp, subject, snippet = '') {
  return { id, internalDate: String(timestamp), snippet, payload: { headers: [
    { name: 'From', value: 'Hy Cite <avisos@hycite.com>' },
    { name: 'Subject', value: subject },
    { name: 'Authentication-Results', value: 'mx.google.com; dmarc=pass header.from=hycite.com' }
  ] } };
}
const summarize = emails => summarizeMessages(emails, ['hycite.com']);

test('receipt, pending credit and malformed identifiers never confirm a sale', () => {
  const result = summarize([
    email('receipt', 100, 'Order confirmation #XY-321'),
    email('receipt-es', 101, 'Confirmación de pedido #XY-322'),
    email('pending', 102, 'Venta confirmada #XY-323', 'La venta no ha sido aprobada. Pendiente de autorización.'),
    email('bad', 103, 'Venta confirmada', 'Número de orden: AB123/XYZ'),
    email('ambiguous', 104, 'Confirmación de venta', 'Orden #100. Orden #101.'),
    email('valid', 105, 'Sale confirmed – Order No. AB-123')
  ]);
  assert.deepEqual(result.confirmed.map(item => item.order), ['AB-123']);
  assert.equal(result.review.length, 5);
});

test('an authenticated cancellation for an order supersedes its confirmation even with the same timestamp', () => {
  const first = email('confirm', 300, 'Pedido confirmado #00123');
  const cancel = email('cancel', 300, 'Pedido cancelado #00123');
  for (const messages of [[first,cancel],[cancel,first]]) {
    const result = summarize(messages);
    assert.deepEqual(result.confirmed, []);
    assert.deepEqual(result.review.map(item => [item.id,item.order]), [['cancel','00123']]);
  }
});

test('conflicting receipts mentioning two orders supersede both prior confirmations', () => {
  const result = summarize([
    email('first', 100, 'Sale confirmed #20'),
    email('second', 200, 'Sale confirmed #30'),
    email('multiple', 300, 'Corrección de ventas', 'Order #20 was reversed. Order #30 is pending.'),
    email('uncorrelated', 301, 'Venta confirmada #12', 'Order #1200')
  ]);
  assert.deepEqual(result.confirmed, []);
  assert.deepEqual(result.review.map(item => item.id), ['uncorrelated', 'multiple']);
});

test('long subject is classified before display truncation', () => {
  const result = summarize([email('long', 100, 'Venta confirmada #1000' + ' '.repeat(240) + ' CANCELADA')]);
  assert.deepEqual(result.confirmed, []);
  assert.equal(result.review[0].subject.length, 220);
});
