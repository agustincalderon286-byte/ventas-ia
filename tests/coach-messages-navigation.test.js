import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const app = fs.readFileSync(new URL('../public/coach/app.js', import.meta.url), 'utf8');
const navigation = app.slice(app.indexOf('function isCoachWorkspaceAvailable('), app.indexOf('function buildCoachActiveCrmContext('));
const messages = app.slice(app.indexOf('function initCoachMessagesWorkspace('), app.indexOf('function renderCoachRepLeadSummary('));
function fixture({ mode = 'default', shortcut = true, fail = false, readFailure = false, pageHidden = false } = {}) {
  let alreadyRead = false, overviewLoads = 0;
  const node = (dataset = {}, hidden = false, textContent = '') => ({
    dataset, hidden, textContent, attributes: {}, listeners: {},
    classList: { toggle() {} }, hasAttribute() { return false; },
    setAttribute(key, value) { this.attributes[key] = value; },
    getAttribute(key) { return this.attributes[key] ?? null; },
    addEventListener(event, fn) { (this.listeners[event] ||= []).push(fn); },
    click() { for (const fn of this.listeners.click || []) fn(); }
  });
  const tabs = ['cierre', 'audiolibros', 'mensajes', 'crm', 'equipo'].map(id => node({ coachWorkspaceTab: id }, !['cierre', 'audiolibros'].includes(id), id === 'mensajes' ? 'Mensajes' : id));
  const sections = ['cierre', 'audiolibros', 'mensajes', 'mensajes', 'crm', 'equipo'].map(id => node({ coachWorkspaceSection: id }, id !== 'cierre'));
  const header = node({}, !shortcut), title = node(), form = node(), feedback = node();
  const storage = new Map(), requests = [], announcements = [], summaries = [], errors = [], scrolls = [];
  const document = { hidden: pageHidden,
    documentElement: { dataset: { coachPortalMode: mode, coachTeamManager: 'false', coachTeamViewer: 'false' } },
    querySelectorAll(selector) {
      if (selector === '[data-coach-workspace-tab]') return tabs;
      if (selector === '[data-coach-workspace-section]') return sections;
      if (selector === '[data-coach-open-messages]') return [header];
      return [];
    },
    querySelector(selector) {
      if (selector === '[data-coach-open-messages]:not([hidden])') return header.hidden ? null : header;
      if (selector === '[data-coach-workspace-title]') return title;
      if (selector === '[data-support-message-form]') return form;
      if (selector === '[data-messages-feedback]') return feedback;
      const id = selector.match(/^\[data-coach-workspace-tab="([^"]+)"\]$/)?.[1];
      return id ? tabs.find(n => n.dataset.coachWorkspaceTab === id) : null;
    }
  };
  const window = { sessionStorage: { getItem: k => storage.get(k), setItem: (k, v) => storage.set(k, v) },
    location: { href: 'https://totalelitecoaching.com/coach/app/', hash: '' }, history: { replaceState() {} }, dispatchEvent() {}, scrollTo: options => scrolls.push(options) };
  const context = { window, document, URL, COACH_WORKSPACE_TAB_KEY: 'workspace', CustomEvent: class {}, registerCoachDemoEvent() {},
    apiRequest: async (url, options) => {
      requests.push({ url, options });
      if (fail) throw new Error('Sin conexión');
      if (url === '/api/coach/announcements/read') {
        if (readFailure) throw new Error('No pude registrar la lectura');
        alreadyRead = true; return { readIds: options.body.ids };
      }
      return { announcements: [{ id: 'a'.repeat(24), title: `Aviso ${++overviewLoads}`, read: alreadyRead }],
        unread: { announcements: alreadyRead ? 0 : 1, support: 2, direct: 3, total: alreadyRead ? 5 : 6 } };
    },
    renderCoachMessagesSummary: value => summaries.push(value.unread), renderCoachAnnouncements: value => announcements.push(value), renderCoachBulletinComposer() {},
    renderCoachDirectWorkspace: () => '', renderCoachSupportThread() {}, clearMessage: () => errors.push(''), setMessage: (node, message) => errors.push(message) };
  vm.createContext(context); vm.runInContext(navigation + messages, context);
  context.initCoachWorkspaceTabs(); context.initCoachMessagesWorkspace({ id: 'fixture-account' });
  return { ...context, tabs, sections, header, title, requests, announcements, summaries, errors, scrolls, storage, setFailure(value) { fail = value; }, setReadFailure(value) { readFailure = value; } };
}
const settle = () => new Promise(resolve => setImmediate(resolve));

test('the compact header opens Messages, refreshes bulletins and acknowledges only loaded announcements', async () => {
  const f = fixture(); await settle();
  assert.equal(f.tabs.find(n => n.dataset.coachWorkspaceTab === 'mensajes').hidden, true);
  f.header.click(); await settle();
  assert.equal(f.title.textContent, 'Mensajes');
  assert.ok(f.sections.filter(n => n.dataset.coachWorkspaceSection === 'mensajes').every(n => !n.hidden));
  assert.ok(f.sections.filter(n => n.dataset.coachWorkspaceSection !== 'mensajes').every(n => n.hidden));
  assert.equal(f.header.attributes['aria-pressed'], 'true');
  assert.equal(f.storage.get('workspace'), 'mensajes');
  assert.equal(f.scrolls.at(-1).top, 0);
  assert.equal(f.requests.length, 3);
  const receipt = f.requests.find(r => r.options?.method === 'POST');
  assert.equal(receipt.url, '/api/coach/announcements/read'); assert.equal(receipt.options.body.accountId, 'fixture-account');
  assert.deepEqual(Array.from(receipt.options.body.ids), ['a'.repeat(24)]);
  assert.equal(f.summaries.at(-1).announcements, 0); assert.equal(f.summaries.at(-1).direct, 3);
  assert.equal(f.summaries.at(-1).support, 2); assert.equal(f.summaries.at(-1).total, 5);
  assert.equal(f.announcements.at(-1)[0].title, 'Aviso 2');
  assert.equal(f.setCoachWorkspaceTab('cierre'), true);
  assert.equal(f.header.attributes['aria-pressed'], 'false');
  assert.equal(f.sections[0].hidden, false);
});
test('the Messages shortcut does not unlock hidden CRM or team areas, and remains unavailable in telemarketing or without its header entry', async () => {
  const normal = fixture(); await settle();
  for (const target of ['crm', 'equipo', 'missing']) assert.equal(normal.setCoachWorkspaceTab(target), false);
  assert.equal(normal.title.textContent, 'cierre');
  for (const options of [{ mode: 'telemarketing' }, { shortcut: false }]) {
    const f = fixture(options); await settle();
    assert.equal(f.setCoachWorkspaceTab('mensajes'), false);
    assert.ok(f.sections.filter(n => n.dataset.coachWorkspaceSection === 'mensajes').every(n => n.hidden));
  }
});
test('a failed refresh still opens Messages and shows the error without silently sending or clearing anything', async () => {
  const f = fixture({ fail: true }); await settle(); f.header.click(); await settle();
  assert.equal(f.title.textContent, 'Mensajes'); assert.equal(f.errors.at(-1), 'Sin conexión');
  assert.equal(f.requests.length, 2); assert.ok(f.requests.every(r => !r.options));
  f.setFailure(false); f.header.click(); await settle();
  assert.equal(f.errors.at(-1), ''); assert.equal(f.announcements.at(-1)[0].title, 'Aviso 1');
});

test('loading in Cierre or a background page does not acknowledge bulletins; receipt failure preserves the red count until retry succeeds', async () => {
  const normal = fixture(); await settle();
  assert.equal(normal.requests.length, 1); assert.equal(normal.summaries.at(-1).announcements, 1);
  const background = fixture({ pageHidden: true }); await settle(); background.header.click(); await settle();
  assert.ok(background.requests.every(r => !r.options));
  const failed = fixture({ readFailure: true }); await settle(); failed.header.click(); await settle();
  assert.equal(failed.summaries.at(-1).announcements, 1); assert.equal(failed.errors.at(-1), 'No pude registrar la lectura');
  failed.setReadFailure(false); failed.header.click(); await settle();
  assert.equal(failed.summaries.at(-1).announcements, 0); assert.equal(failed.errors.at(-1), '');
  failed.header.click(); await settle();
  assert.equal(failed.requests.filter(r => r.url === '/api/coach/announcements/read').length, 2);
});
