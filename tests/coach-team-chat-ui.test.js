import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../public/coach/team-chat.js', import.meta.url), 'utf8');
const message = (n, body = `Mensaje ${n}`, senderId = 'bob') => ({ id: n.toString(16).padStart(24, '0'), senderId, senderName: 'Compañero <script>', body, createdAt: '2026-11-17T17:00:00Z' });
const turn = async () => { for (let i = 0; i < 40; i++) await Promise.resolve(); };
class Element {
  constructor(tag = 'div') { this.tagName = tag; this.children = []; this.parentElement = null; this.hidden = false; this.dataset = {}; this.attributes = {}; this.events = new Map(); this.value = ''; this.textContent = ''; this.clientHeight = 200; this.top = 0; this.className = ''; this.classList = { add: name => { this.className += ` ${name}`; } }; }
  append(...nodes) { nodes.forEach(node => { node.parentElement = this; this.children.push(node); }); }
  replaceChildren(...nodes) { this.children.forEach(node => { node.parentElement = null; }); this.children = []; this.append(...nodes); }
  setAttribute(name, value) { this.attributes[name] = value; }
  addEventListener(type, fn) { if (!this.events.has(type)) this.events.set(type, new Set()); this.events.get(type).add(fn); }
  removeEventListener(type, fn) { this.events.get(type)?.delete(fn); }
  fire(type, event = {}) { for (const fn of this.events.get(type) || []) fn({ preventDefault() {}, ...event }); }
  closest() { for (let node = this; node; node = node.parentElement) if (node.hidden) return node; return null; }
  getClientRects() { return this.closest() ? [] : [{}]; }
  getBoundingClientRect() { return this.rect || { top: 100, bottom: 200, left: 10, right: 310 }; }
  get scrollHeight() { return Math.max(this.clientHeight, this.children.length * 100); }
  get scrollTop() { return this.top; }
  set scrollTop(value) { this.top = Math.min(Math.max(0, value), this.scrollHeight - this.clientHeight); }
}
function fixture({ hidden = false, store = new Map(), request } = {}) {
  const root = new Element(); root.hidden = hidden;
  const document = new Element('document'); document.hidden = false; document.visibilityState = 'visible'; document.createElement = tag => new Element(tag); document.querySelector = () => root; document.querySelectorAll = () => [];
  const window = new Element('window'); let sequence = 0; const timers = new Map();
  Object.assign(window, { innerHeight: 800, innerWidth: 400, localStorage: { getItem: key => store.get(key), setItem: (key, value) => store.set(key, value) }, crypto: { randomUUID: () => `00000000-0000-4000-8000-${String(++sequence).padStart(12, '0')}` }, setTimeout: (fn, delay) => { const id = ++sequence; timers.set(id, { fn, delay }); return id; }, clearTimeout: id => timers.delete(id) });
  let now = Date.now();
  class Clock extends Date { static now() { return now; } }
  const observers = [];
  class Observer { constructor(fn) { this.fn = fn; observers.push(this); } observe() {} disconnect() {} }
  vm.runInNewContext(source, { window, document, MutationObserver: Observer, Uint8Array, Date: Clock, console });
  const calls = [], counts = [];
  let handler = request || (async (url, options = {}) => url.endsWith('/read') ? { roomId: 'room-a', unreadCount: 0, lastMessageId: options.body.lastMessageId } : { roomId: 'room-a', messages: [message(1), message(2)], unreadCount: 2, hasMore: false });
  const sendRequest = async (url, options = {}) => { calls.push({ url, options }); return handler(url, options); };
  let controller;
  const start = (userId = 'alice') => { controller = window.TecTeamChat.init({ userId, request: sendRequest, root, onUnreadCount: count => counts.push(count) }); return controller; };
  const find = className => { const all = node => [node, ...node.children.flatMap(all)]; return all(root).find(node => node.className.split(' ').includes(className)); };
  return { root, document, window, store, calls, counts, timers, find, start, advance(ms) { now += ms; }, setRequest(fn) { handler = fn; }, observe() { observers.forEach(item => item.fn()); }, get controller() { return controller; } };
}

test('renders real text safely with sender/time; reads only the visible bottom and scopes requests to account', async () => {
  const f = fixture({ request: async (url, options) => url.endsWith('/read') ? { roomId: 'room-a', unreadCount: 0 } : { roomId: 'room-a', messages: [message(1, '<img src=x onerror=alert(1)>'), message(2, 'Mi mensaje', 'alice')], unreadCount: 2, hasMore: false } });
  f.start();
  assert.equal(f.find('tec-team-chat__status').dataset.error, undefined, 'native dataset starts without an error value');
  await turn();
  assert.equal(f.find('tec-team-chat__status').dataset.error, 'false', 'first active refresh completes safely');
  const list = f.find('tec-team-chat__list');
  assert.equal(list.children[0].children[1].textContent, '<img src=x onerror=alert(1)>');
  assert.equal(list.children[0].children[0].children[0].textContent, 'Compañero <script>');
  assert.match(list.children[1].className, /message--own/);
  assert.equal(list.children[1].children[0].children[0].textContent, 'Tú');
  assert.equal(list.children[1].children[0].children[1].dateTime, '2026-11-17T17:00:00.000Z');
  assert.equal(f.calls[0].options.headers['X-TEC-Account-ID'], 'alice');
  const read = f.calls.find(call => call.url.endsWith('/read'));
  assert.equal(read.options.body.accountId, 'alice'); assert.equal(read.options.body.lastMessageId, message(2).id);
  assert.equal(f.counts.at(-1), 0);
  f.controller.destroy();
});

test('hidden and background panels do not acknowledge reading; opening fetches the actual history', async () => {
  const f = fixture({ hidden: true }); f.start(); await turn();
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, 0);
  assert.equal(f.find('tec-team-chat__list').children[0].textContent, 'Cargando el chat del equipo…');
  assert.equal(f.counts.at(-1), 2);
  f.root.hidden = false; f.document.hidden = true; f.observe(); await turn();
  assert.equal(f.calls.length, 1);
  f.document.hidden = false; f.document.fire('visibilitychange'); await turn();
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, 1);
  f.controller.destroy();
});

test('new arrivals preserve old scroll and unread count until explicitly jumping to the latest message', async () => {
  let latest = 5;
  const f = fixture({ request: async (url, options) => url.endsWith('/read') ? { roomId: 'room-a', unreadCount: 0 } : { roomId: 'room-a', messages: Array.from({ length: latest }, (_, n) => message(n + 1)), unreadCount: latest - 5, hasMore: false } });
  f.start(); await turn(); const list = f.find('tec-team-chat__list'); list.scrollTop = 50;
  const reads = f.calls.filter(call => call.url.endsWith('/read')).length;
  latest = 7; await f.controller.refresh(); await turn();
  assert.equal(list.scrollTop, 50); assert.equal(f.counts.at(-1), 2);
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, reads);
  const jump = f.find('tec-team-chat__jump'); assert.equal(jump.hidden, false); assert.match(jump.textContent, /2 mensajes nuevos/);
  jump.fire('click'); await turn(); assert.equal(jump.hidden, true);
  assert.equal(f.calls.at(-1).options.body.lastMessageId, message(7).id); assert.equal(f.counts.at(-1), 0);
  f.controller.destroy();
});

test('loading earlier history preserves the visual position and does not mark old messages read', async () => {
  const f = fixture({ request: async (url, options) => url.endsWith('/read') ? { roomId: 'room-a', unreadCount: 0 } : url.includes('?before=') ? { roomId: 'room-a', messages: [message(1), message(2)], hasMore: false, unreadCount: 0 } : { roomId: 'room-a', messages: [message(3), message(4), message(5)], hasMore: true, unreadCount: 0 } });
  f.start(); await turn(); const list = f.find('tec-team-chat__list'); list.scrollTop = 0;
  const reads = f.calls.filter(call => call.url.endsWith('/read')).length;
  f.find('tec-team-chat__older').fire('click'); await turn();
  assert.equal(list.children.length, 5); assert.equal(list.scrollTop, 200);
  assert.equal(f.find('tec-team-chat__older').hidden, true);
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, reads);
  f.controller.destroy();
});

test('uncertain sends require an explicit retry with the same UUID, including after reload; other accounts get no draft', async () => {
  let attempts = 0; const sent = [];
  const f = fixture({ request: async (url, options) => {
    if (url.endsWith('/messages')) { sent.push(options.body); if (++attempts < 2) throw new Error('Conexión interrumpida'); return { roomId: 'room-a', message: message(3, options.body.body, 'alice') }; }
    return url.endsWith('/read') ? { roomId: 'room-a', unreadCount: 0 } : { roomId: 'room-a', messages: attempts >= 2 ? [message(1), message(3, 'Texto privado', 'alice')] : [message(1)], hasMore: false, unreadCount: 0 };
  } });
  f.start(); await turn(); let input = f.find('tec-team-chat__input'); input.value = 'Texto privado'; input.fire('input');
  f.find('tec-team-chat__form').fire('submit'); await turn();
  assert.equal(sent.length, 1); assert.equal(input.value, 'Texto privado'); assert.equal(input.disabled, true);
  await f.controller.refresh(); assert.equal(sent.length, 1, 'polling never retries a send');
  f.start('bob'); await turn(); assert.equal(f.find('tec-team-chat__input').value, '');
  f.start('alice'); await turn(); input = f.find('tec-team-chat__input'); assert.equal(input.value, 'Texto privado');
  assert.equal(f.find('tec-team-chat__retry').hidden, false); assert.equal(sent.length, 1);
  f.find('tec-team-chat__retry').fire('click'); await turn();
  assert.equal(sent.length, 2); assert.equal(sent[0].clientMessageId, sent[1].clientMessageId);
  assert.equal(sent[1].accountId, 'alice'); assert.equal(input.value, ''); assert.equal(input.disabled, false);
  assert.equal(f.find('tec-team-chat__retry').hidden, true);
  f.controller.destroy();
});

test('room changes and stale-account failures freeze retries while preserving the original scoped text', async () => {
  const f = fixture(); f.start(); await turn();
  const input = f.find('tec-team-chat__input'); input.value = 'Borrador de mi equipo anterior'; input.fire('input');
  f.setRequest(async () => ({ roomId: 'room-b', messages: [message(9)], hasMore: false, unreadCount: 1 }));
  await f.controller.refresh(); assert.equal(input.disabled, true); assert.equal(input.value, 'Borrador de mi equipo anterior');
  f.find('tec-team-chat__form').fire('submit'); await turn(); assert.equal(f.calls.some(call => call.url.endsWith('/messages')), false);
  assert.ok([...f.store.keys()].every(key => !key.includes('room-b')));
  f.controller.destroy();
  const g = fixture({ request: async url => { if (url.endsWith('/messages')) throw Object.assign(new Error('Otra cuenta'), { status: 403 }); return url.endsWith('/read') ? { roomId: 'room-a', unreadCount: 0 } : { roomId: 'room-a', messages: [], unreadCount: 0 }; } });
  g.start(); await turn(); const other = g.find('tec-team-chat__input'); other.value = 'Conservar'; other.fire('input');
  g.find('tec-team-chat__form').fire('submit'); await turn(); assert.equal(other.disabled, true);
  assert.equal(g.find('tec-team-chat__retry').hidden, true); assert.match(g.find('tec-team-chat__status').textContent, /sesión o el equipo cambió/);
  g.controller.destroy();
});

test('a scroll during an in-flight refresh is preserved and a response after destroy is ignored', async () => {
  const f = fixture({ request: async url => url.endsWith('/read') ? { roomId: 'room-a', unreadCount: 0 } : { roomId: 'room-a', messages: [1, 2, 3, 4].map(n => message(n)), unreadCount: 0 } });
  f.start(); await turn(); let resolve;
  f.setRequest(() => new Promise(done => { resolve = done; }));
  const request = f.controller.refresh(); const list = f.find('tec-team-chat__list'); list.scrollTop = 0;
  resolve({ roomId: 'room-a', messages: [1, 2, 3, 4, 5].map(n => message(n)), unreadCount: 1 }); await request;
  assert.equal(list.scrollTop, 0); assert.equal(f.find('tec-team-chat__jump').hidden, false);
  const late = f.controller.refresh(); f.controller.destroy(); resolve({ roomId: 'other-room', messages: [message(99)], unreadCount: 99 }); await late;
  assert.equal(list.children.length, 5); assert.notEqual(f.counts.at(-1), 99);
});

test('optional storage failure does not block sending; Enter sends and Shift+Enter keeps the native newline', async () => {
  const f = fixture({ request: async (url, options) => url.endsWith('/messages') ? { roomId: 'room-a', message: message(3, options.body.body, 'alice') } : url.endsWith('/read') ? { roomId: 'room-a', unreadCount: 0 } : { roomId: 'room-a', messages: [], unreadCount: 0 } });
  f.window.localStorage.getItem = () => { throw new Error('Storage blocked'); }; f.window.localStorage.setItem = () => { throw new Error('Storage full'); };
  f.start(); await turn(); const input = f.find('tec-team-chat__input'); input.value = 'Prueba'; input.fire('input');
  input.fire('keydown', { key: 'Enter', shiftKey: true }); await turn(); assert.equal(f.calls.some(call => call.url.endsWith('/messages')), false);
  input.fire('keydown', { key: 'Enter', shiftKey: false }); await turn(); assert.equal(f.calls.filter(call => call.url.endsWith('/messages')).length, 1);
  f.controller.destroy();
});

test('a CSS-visible group below the viewport stays unread until its latest row is actually visible', async () => {
  const f = fixture();
  f.start();
  const list = f.find('tec-team-chat__list'); list.rect = { top: 1100, bottom: 1300, left: 10, right: 310 };
  await turn();
  list.children.at(-1).rect = { top: 1200, bottom: 1300, left: 10, right: 310 };
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, 0);
  assert.equal(f.counts.at(-1), 2);
  list.rect = { top: 100, bottom: 300, left: 10, right: 310 };
  list.children.at(-1).rect = { top: 200, bottom: 300, left: 10, right: 310 };
  f.window.fire('scroll'); await turn();
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, 1); assert.equal(f.counts.at(-1), 0);
  f.controller.destroy();
});

test('a sent response cannot acknowledge intervening unseen teammates; history is bridged before receipt', async () => {
  let sent = false, resolveHistory;
  const f = fixture({ request: async (url, options) => {
    if (url.endsWith('/read')) return { roomId: 'room-a', unreadCount: 0 };
    if (url.endsWith('/messages')) { sent = true; return { roomId: 'room-a', message: message(9, options.body.body, 'alice') }; }
    if (!sent) return { roomId: 'room-a', messages: [message(1)], hasMore: false, unreadCount: 0 };
    return new Promise(resolve => { resolveHistory = resolve; });
  } });
  f.start(); await turn();
  const initialReads = f.calls.filter(call => call.url.endsWith('/read')).length;
  const input = f.find('tec-team-chat__input'); input.value = 'Mensaje propio'; input.fire('input'); f.find('tec-team-chat__form').fire('submit'); await turn();
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, initialReads);
  resolveHistory({ roomId: 'room-a', messages: [message(1), message(2), message(3), message(9, 'Mensaje propio', 'alice')], unreadCount: 2, hasMore: false });
  await turn();
  assert.equal(f.find('tec-team-chat__list').children.length, 4);
  const read = f.calls.filter(call => call.url.endsWith('/read')).at(-1);
  assert.equal(read.options.body.lastMessageId, message(9).id);
  f.controller.destroy();
});

test('a delayed older receipt does not hide the jump for a newer sent message', async () => {
  let resolveRead;
  const f = fixture({ request: async (url, options) => {
    if (url.endsWith('/read')) return new Promise(resolve => { resolveRead = resolve; });
    if (url.endsWith('/messages')) return { roomId: 'room-a', message: message(5, options.body.body, 'alice') };
    return { roomId: 'room-a', messages: [1, 2, 3, 4].map(n => message(n)), unreadCount: 4, hasMore: false };
  } });
  f.start(); await turn(); const list = f.find('tec-team-chat__list'); list.scrollTop = 0;
  const input = f.find('tec-team-chat__input'); input.value = 'Texto'; input.fire('input');
  f.find('tec-team-chat__form').fire('submit'); await turn();
  assert.equal(f.find('tec-team-chat__jump').hidden, false);
  resolveRead({ roomId: 'room-a', unreadCount: 1 }); await turn();
  assert.equal(f.find('tec-team-chat__jump').hidden, false);
  assert.match(f.find('tec-team-chat__jump').textContent, /1 mensaje nuevo/);
  f.controller.destroy();
});

test('explicit mark-all reads a fresh server cursor while hidden and preserves draft and history', async () => {
  let latest = 2;
  const f = fixture({ hidden: true, request: async (url, options) => url.endsWith('/read') ? { roomId: 'room-a', unreadCount: 0 } : { roomId: 'room-a', messages: [message(latest)], unreadCount: 1, hasMore: false } });
  f.start(); await turn(); const input = f.find('tec-team-chat__input'); input.value = 'Borrador'; input.fire('input');
  latest = 10; const result = await f.controller.markAllRead();
  assert.equal(result, true); assert.equal(f.calls.at(-1).options.body.lastMessageId, message(10).id);
  assert.equal(f.calls.at(-2).options.headers['X-TEC-Account-ID'], 'alice');
  assert.equal(input.value, 'Borrador'); assert.equal(f.find('tec-team-chat__list').children[0].textContent, 'Cargando el chat del equipo…');
  assert.equal(f.counts.at(-1), 0); f.controller.destroy();
});


test('an open dialog outside the group suppresses automatic reading until it closes', async () => {
  const f = fixture();
  let modalOpen = true;
  f.document.querySelectorAll = selector => selector === 'dialog[open]' && modalOpen ? [{ contains: () => false }] : [];
  f.start(); await turn();
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, 0);
  assert.equal(f.counts.at(-1), 2);
  f.window.fire('scroll'); await turn();
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, 0);
  modalOpen = false; f.window.fire('scroll'); await turn();
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, 1);
  assert.equal(f.counts.at(-1), 0);
  f.controller.destroy();
});

test('an outside dialog does not block the explicitly requested mark-all action', async () => {
  const f = fixture();
  f.document.querySelectorAll = selector => selector === 'dialog[open]' ? [{ contains: () => false }] : [];
  f.start(); await turn();
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, 0);
  assert.equal(await f.controller.markAllRead(), true);
  assert.equal(f.calls.filter(call => call.url.endsWith('/read')).length, 1);
  assert.equal(f.counts.at(-1), 0);
  f.controller.destroy();
});


test('a successful retry clears the matching load error', async () => {
  let fail = true;
  const f = fixture({ request: async () => {
    if (fail) throw new Error('Error de carga temporal');
    return { roomId: 'room-a', messages: [], unreadCount: 0, hasMore: false };
  } });
  f.start(); await turn();
  const status = f.find('tec-team-chat__status');
  assert.equal(status.dataset.error, 'true'); assert.match(status.textContent, /Error de carga temporal/);
  fail = false; await f.controller.refresh();
  assert.equal(status.dataset.error, 'false'); assert.equal(status.textContent, '');
  f.controller.destroy();
});

test('a recovered read clears its own warning; a successful load alone preserves that warning', async () => {
  let readFails = true;
  const f = fixture({ request: async url => {
    if (url.endsWith('/read')) { if (readFails) throw new Error('Error de lectura'); return { roomId: 'room-a', unreadCount: 0 }; }
    return { roomId: 'room-a', messages: [message(1), message(2)], unreadCount: 2, hasMore: false };
  } });
  f.start(); await turn(); const status = f.find('tec-team-chat__status');
  assert.match(status.textContent, /No pude actualizar los leídos/);
  readFails = false; await f.controller.refresh();
  assert.match(status.textContent, /No pude actualizar los leídos/, 'loading is not a successful receipt');
  f.advance(15001); f.window.fire('scroll'); await turn();
  assert.equal(status.textContent, ''); assert.equal(status.dataset.error, 'false'); assert.equal(f.counts.at(-1), 0);
  f.controller.destroy();
});

test('load and receipt recovery never erase an uncertain-send warning', async () => {
  let failLoad = false;
  const f = fixture({ request: async url => {
    if (url.endsWith('/messages')) throw new Error('Envío sin confirmar');
    if (url.endsWith('/read')) return { roomId: 'room-a', unreadCount: 0 };
    if (failLoad) throw new Error('Carga interrumpida');
    return { roomId: 'room-a', messages: [message(1)], unreadCount: 0, hasMore: false };
  } });
  f.start(); await turn(); const input = f.find('tec-team-chat__input'); input.value = 'Conservar este envío'; input.fire('input');
  f.find('tec-team-chat__form').fire('submit'); await turn();
  const status = f.find('tec-team-chat__status'); const warning = status.textContent;
  assert.match(warning, /Usa Reintentar envío/);
  failLoad = true; await f.controller.refresh(); assert.equal(status.textContent, warning);
  failLoad = false; await f.controller.refresh(); assert.equal(status.textContent, warning);
  await f.controller.markAllRead(); assert.equal(status.textContent, warning);
  assert.equal(input.value, 'Conservar este envío'); assert.equal(f.find('tec-team-chat__retry').hidden, false);
  f.controller.destroy();
});
