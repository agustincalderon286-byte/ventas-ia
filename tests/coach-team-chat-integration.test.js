import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const server = fs.readFileSync(new URL('../server.js', import.meta.url), 'utf8');
const app = fs.readFileSync(new URL('../public/coach/app.js', import.meta.url), 'utf8');
function between(source, start, end) {
  const from = source.indexOf(start), to = source.indexOf(end, from + start.length);
  assert.ok(from >= 0 && to > from, `Missing source boundaries: ${start}`);
  return source.slice(from, to);
}
function keyFixture(owners = {}) {
  const lookups = [];
  const resolver = between(server, 'function resolverCoachOwnerUserId(', 'async function getCoachTeamChatKey(');
  const key = between(server, 'async function getCoachTeamChatKey(', 'function construirCoachWorkspaceQuery(');
  const getKey = vm.runInNewContext(`${resolver}\n${key}\ngetCoachTeamChatKey`, {
    normalizarCoachAccountType: value => ['seat', 'leader'].includes(value) ? value : 'owner',
    CoachUser: { findById(id) { lookups.push(String(id)); return { select(fields) { assert.equal(fields, '_id accountType'); return { lean: async () => owners[String(id)] || null }; } }; } }
  });
  return { getKey, lookups };
}

test('actual server group resolver keeps owners independent and seats/leaders in their root owner group', async () => {
  const f = keyFixture({ root: { _id: 'root', accountType: 'owner' } });
  assert.equal(await f.getKey({ _id: 'root', accountType: 'owner' }), 'root');
  assert.equal(await f.getKey({ _id: 'other-owner', accountType: 'owner', teamOwnerUserId: 'root', parentUserId: 'root', sponsorUserId: 'root' }), 'other-owner');
  assert.equal(f.lookups.length, 0);
  for (const accountType of ['seat', 'leader']) {
    assert.equal(await f.getKey({ _id: `${accountType}-id`, accountType, teamOwnerUserId: 'root', parentUserId: 'different-parent', sponsorUserId: 'different-sponsor' }), 'root');
  }
  assert.deepEqual(f.lookups, ['root', 'root']);
});

test('actual server group resolver fails closed for absent roots and referenced non-owner accounts', async () => {
  const f = keyFixture({ leader: { _id: 'leader', accountType: 'leader' }, seat: { _id: 'seat', accountType: 'seat' } });
  for (const accountType of ['seat', 'leader']) {
    for (const teamOwnerUserId of [undefined, '', 'deleted', 'leader', 'seat']) {
      assert.equal(await f.getKey({ _id: 'viewer', accountType, teamOwnerUserId, parentUserId: 'valid-looking-parent', sponsorUserId: 'valid-looking-sponsor' }), '');
    }
  }
  assert.equal(await f.getKey({}), '');
});

function badgeFixture() {
  const selectors = ['[data-coach-unread-badge]', '[data-messages-total-unread]', '[data-team-chat-unread-summary]', '[data-messages-total-announcements]', '[data-messages-unread-announcements]', '[data-messages-total-direct]', '[data-messages-unread-direct]', '[data-messages-unread-support]'];
  const nodes = new Map(selectors.map(selector => [selector, [{ hidden: true, textContent: '' }, { hidden: true, textContent: '' }]]));
  const instances = [];
  const stateSource = between(app, 'const coachMessageUnreadState =', 'if ("serviceWorker" in navigator');
  const summarySource = between(app, 'function renderCoachMessagesSummary(', 'function renderCoachAnnouncements(');
  const api = vm.runInNewContext(`${stateSource}\n${summarySource}\n({ init: initCoachTeamChatWorkspace, summary: renderCoachMessagesSummary, state: coachMessageUnreadState })`, {
    document: { querySelectorAll: selector => nodes.get(selector) || [], querySelector: () => null },
    window: { TecTeamChat: { init(options) { const instance = { options, destroyed: false, destroy() { this.destroyed = true; } }; instances.push(instance); return instance; } } },
    apiRequest() {}
  });
  return { ...api, instances, nodes, total: () => nodes.get('[data-messages-total-unread]')[0].textContent };
}
const overview = (announcements = 2, direct = 3, support = 4) => ({ announcements: [{}, {}], direct: { threads: [{}] }, unread: { announcements, direct, support, total: announcements + direct + support } });

test('actual badge combines internal channels and group unread without either read path erasing the other', () => {
  const f = badgeFixture(); f.init({ id: 'alice' });
  f.summary(overview()); f.instances[0].options.onUnreadCount(5);
  assert.equal(f.total(), '14');
  for (const node of f.nodes.get('[data-coach-unread-badge]')) { assert.equal(node.hidden, false); assert.equal(node.textContent, '14'); }
  assert.equal(f.nodes.get('[data-team-chat-unread-summary]')[0].textContent, '5');
  assert.equal(f.nodes.get('[data-messages-unread-announcements]')[0].textContent, '2');
  assert.equal(f.nodes.get('[data-messages-unread-direct]')[0].textContent, '3');
  assert.equal(f.nodes.get('[data-messages-unread-support]')[0].textContent, '4');
  f.summary(overview(0, 0, 0)); assert.equal(f.total(), '5', 'internal automatic read must preserve group unread');
  f.summary(overview(0, 3, 4)); assert.equal(f.total(), '12');
  f.instances[0].options.onUnreadCount(0); assert.equal(f.total(), '7', 'group read must preserve internal unread');
  f.summary(null); assert.equal(f.total(), '0');
  for (const node of f.nodes.get('[data-coach-unread-badge]')) assert.equal(node.hidden, true);
});

test('actual workspace initialization resets badge and ignores callbacks belonging to the prior account', () => {
  const f = badgeFixture(); f.init({ id: 'alice' });
  const alice = f.instances[0]; f.summary(overview()); alice.options.onUnreadCount(5);
  f.init({ id: 'bob' }); assert.equal(alice.destroyed, true); assert.equal(f.total(), '0');
  const bob = f.instances[1]; alice.options.onUnreadCount(99); assert.equal(f.total(), '0');
  assert.equal(bob.options.userId, 'bob'); bob.options.onUnreadCount(4); f.summary(overview(0, 1, 0)); assert.equal(f.total(), '5');
  f.init(null); assert.equal(bob.destroyed, true); assert.equal(f.total(), '0');
  bob.options.onUnreadCount(99); alice.options.onUnreadCount(99); assert.equal(f.total(), '0');
  assert.equal(f.state.accountId, '');
});

test('actual message-center mark-all handler calls the group controller and reports incomplete group receipts', () => {
  const handler = between(app, '  markReadButton?.addEventListener("click", async () => {', '  bulletinScopeSelect?.addEventListener');
  assert.match(handler, /await apiRequest\("\/api\/coach\/messages\/read-all"/);
  assert.match(handler, /await coachTeamChatController\.markAllRead\(\)/);
  assert.match(handler, /teamRead \? "success" : "error"/);
  assert.match(handler, /await loadOverview\(\)/);
});
