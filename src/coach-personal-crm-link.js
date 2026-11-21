// Match whole name tokens, including unaccented spellings typed on a phone.
function contactPattern(name) {
  const letters = { a: '[aáàä]', e: '[eéèë]', i: '[iíìï]', o: '[oóòö]', u: '[uúùü]', n: '[nñ]' };
  const input = String(name || '').normalize('NFD').replace(/\p{M}/gu, '').toLowerCase().trim();
  if (!input || input.length > 100 || !/^[\p{L} '\u2019-]+$/u.test(input)) return null;
  const words = input.split(/\s+/).map(word => Array.from(word, c => letters[c] || c).join(''));
  return new RegExp('(?:^|\\s)' + words.join('\\s+') + '(?:$|\\s)', 'i');
}

export function personalCalendarCrm({ users, records, viewerQuery, applySources, invalidate }) {
  async function sellerScope(owner) {
    const user = await users.findById(owner).select('_id name email accountType teamOwnerUserId').lean();
    if (!user) return null;
    const visible = await viewerQuery(user);
    const involved = [{ generatedByUserId: user._id }, { appointmentRepUserId: user._id }, { assignedTelemarketerUserId: user._id }];
    if (String(visible.ownerUserId) === String(user._id)) involved.push({ generatedByUserId: null });
    return { user, filter: { ...visible, $or: involved } };
  }
  return {
    async findLeads({ owner, name, id }) {
      const pattern = id ? null : contactPattern(name);
      if (id ? !/^[\da-f]{24}$/i.test(String(id)) : !pattern) return [];
      const selector = id ? { _id: id } : { leadName: pattern };
      const scope = await sellerScope(owner);
      if (!scope) return [];
      const matches = await records.find({ ...scope.filter, ...selector }).select('_id leadName nextActionAt')
        .sort({ leadName: 1, _id: 1 }).limit(6).lean();
      return matches.map(row => ({ id: String(row._id), name: row.leadName, nextActionAt: row.nextActionAt?.toISOString() || null }));
    },
    async syncLead({ owner, leadId, start, action, expectedStart }) {
      if (!/^[\da-f]{24}$/i.test(String(leadId))) return false;
      const nextActionAt = new Date(start);
      const before = expectedStart ? new Date(expectedStart) : null;
      if (!start || !Number.isFinite(+nextActionAt) || (before && !Number.isFinite(+before))) return false;
      const scope = await sellerScope(owner);
      if (!scope) return false;
      const nextAction = ['llamar', 'cita', 'demo', 'seguimiento'].includes(action) ? action : 'seguimiento';
      const filter = { ...scope.filter, _id: leadId, nextActionAt: { $in: [before, nextActionAt] } };
      // Compare with the date seen in the preview. A retry may already have written the new time.
      const updates = { nextAction, nextActionAt };
      const row = await records.findOneAndUpdate(filter, [{ $set: {
        ...updates, updatedAt: '$$NOW',
        appointmentAt: { $cond: [{ $in: ['$status', ['cita_agendada', 'reagendada']] }, nextActionAt, '$appointmentAt'] }
      } }], { new: true }).lean();
      if (!row) return false;
      await applySources(row, updates, scope.user);
      await invalidate(scope.user, row.ownerUserId);
      return true;
    }
  };
}
