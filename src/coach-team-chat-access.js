import { canReadDownline, downlineMembers } from './tec-team-supervision.js';
import { teamChatRoomId } from './coach-team-chat.js';
const validId = /^[a-f0-9]{24}$/i;
const denied = () => Object.assign(new Error('No tienes acceso a ese grupo del equipo.'), { status: 403 });
// Sharing a billing owner, a surname or a territory never grants another branch.
export async function resolveTeamChatGroup(user, requestedOwner, { users, getOwnKey }) {
  const own = await getOwnKey(user);
  if (requestedOwner === undefined || requestedOwner === '' || requestedOwner === own) return own;
  if (typeof requestedOwner !== 'string' || !validId.test(requestedOwner) || !canReadDownline(user)) throw denied();
  let current = await users.findById(requestedOwner).select('_id parentUserId accountType').lean();
  if (!current || (current.accountType || 'owner') !== 'owner') throw denied();
  const seen = new Set([String(current._id)]);
  for (let depth = 0; depth < 64 && current?.parentUserId; depth++) {
    const parent = String(current.parentUserId);
    if (parent === String(user._id)) return String(requestedOwner);
    if (seen.has(parent)) throw denied();
    seen.add(parent);
    current = await users.findById(parent).select('_id parentUserId accountType').lean();
  }
  throw denied();
}
export async function listTeamChatGroups(user, { users, getOwnKey }) {
  const own = await getOwnKey(user);
  if (!own) throw denied();
  const descendants = await downlineMembers(user, users);
  const groups = [{ ownerId: own, roomId: teamChatRoomId(own), name: 'Mi equipo' }];
  for (const member of descendants) if ((member.accountType || 'owner') === 'owner' && String(member._id) !== own) {
    groups.push({ ownerId: String(member._id), roomId: teamChatRoomId(String(member._id)), name: `Equipo de ${String(member.name || 'vendedor').slice(0,160)}` });
  }
  return groups;
}
