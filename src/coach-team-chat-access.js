import { canReadDownline, downlineMembers } from './tec-team-supervision.js';
import { teamChatRoomId } from './coach-team-chat.js';
const validId = /^[a-f0-9]{24}$/i;
const denied = () => Object.assign(new Error('No tienes acceso a ese grupo del equipo.'), { status: 403 });

// Explicit owner request, 2026-11-17: Rigoberto's reserved administrator account
// may participate in the existing shared David/Lorena group. This root was
// verified against the real Eric/Lorena workspace and Veronica's message.
// It is a chat-only delegation, independent of the billing/parent relationship.
const RIGO_SHARED_CHAT_OWNER = '6ad94167006e949af1bc026e';
export function getExplicitChatDelegations(user, isAdministrator) {
  if (!user?._id || typeof isAdministrator !== 'function' || isAdministrator(user) !== true || String(user.email || '').trim().toLowerCase() !== 'calderonrigoberto51@gmail.com') return [];
  return [{ ownerId: RIGO_SHARED_CHAT_OWNER, name: 'Equipo de David Peñaherra', preferred: true }];
}
async function validatedDelegations(user, users, getDelegations) {
  const entries = getDelegations ? await getDelegations(user) : [];
  if (!Array.isArray(entries) || entries.length > 64) throw denied();
  const valid = [];
  for (const entry of entries) {
    if (!entry || typeof entry.ownerId !== 'string' || !validId.test(entry.ownerId)) throw denied();
    const root = await users.findById(entry.ownerId).select('_id name accountType').lean();
    if (!root || (root.accountType || 'owner') !== 'owner') continue;
    valid.push({ ...entry, name: String(entry.name || `Equipo de ${root.name || 'vendedor'}`).slice(0,160) });
  }
  return valid;
}
// Sharing a billing owner, a surname or a territory never grants another branch.
export async function resolveTeamChatGroup(user, requestedOwner, { users, getOwnKey, getDelegations }) {
  const own = await getOwnKey(user);
  if (requestedOwner === undefined || requestedOwner === '') {
    const preferred=(await validatedDelegations(user,users,getDelegations)).find(grant=>grant.preferred===true);
    return preferred?.ownerId || own;
  }
  if (requestedOwner === own) return own;
  if (typeof requestedOwner !== 'string' || !validId.test(requestedOwner)) throw denied();
  const grants = await validatedDelegations(user, users, getDelegations);
  if (!canReadDownline(user) && !grants.length) throw denied();
  let current = await users.findById(requestedOwner).select('_id parentUserId accountType').lean();
  if (!current || (current.accountType || 'owner') !== 'owner') throw denied();
  const delegatedRoots = new Set(grants.map(grant => grant.ownerId));
  if (delegatedRoots.has(String(current._id))) return String(requestedOwner);
  const seen = new Set([String(current._id)]);
  for (let depth = 0; depth < 64 && current?.parentUserId; depth++) {
    const parent = String(current.parentUserId);
    if ((canReadDownline(user) && parent === String(user._id)) || delegatedRoots.has(parent)) return String(requestedOwner);
    if (seen.has(parent)) throw denied();
    seen.add(parent);
    current = await users.findById(parent).select('_id parentUserId accountType').lean();
  }
  throw denied();
}
export async function listTeamChatGroups(user, { users, getOwnKey, getDelegations }) {
  const own = await getOwnKey(user);
  if (!own) throw denied();
  const groups = new Map([[own,{ ownerId: own, roomId: teamChatRoomId(own), name: 'Mi equipo' }]]);
  const addMembers = members => { for (const member of members) if ((member.accountType || 'owner') === 'owner' && !groups.has(String(member._id))) {
    const key=String(member._id);groups.set(key,{ownerId:key,roomId:teamChatRoomId(key),name:`Equipo de ${String(member.name || 'vendedor').slice(0,160)}`});
  } };
  addMembers(await downlineMembers(user,users));
  for (const grant of await validatedDelegations(user,users,getDelegations)) {
    if (grant.ownerId !== own) groups.set(grant.ownerId,{ ownerId:grant.ownerId,roomId:teamChatRoomId(grant.ownerId),name:grant.name,preferred:grant.preferred === true });
    addMembers(await downlineMembers({_id:grant.ownerId,teamSupervisionEnabled:true},users));
  }
  return [...groups.values()];
}
