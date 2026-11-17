// Acknowledge only the bulletins actually loaded for this account.
export function registerCoachAnnouncementRead(app, { requireActive, getOverview, receipts }) {
  app.post('/api/coach/announcements/read', async (req, res) => {
    try {
      const auth = await requireActive(req, res);
      if (!auth) return;
      if (req.body?.accountId !== String(auth.user._id)) {
        return res.status(403).json({ error: 'La cuenta cambió. Vuelve a abrir Coach.' });
      }
      const ids = req.body?.ids;
      if (!Array.isArray(ids) || ids.length > 20 || new Set(ids).size !== ids.length ||
          ids.some(id => typeof id !== 'string' || !/^[a-f0-9]{24}$/i.test(id))) {
        return res.status(400).json({ error: 'Los avisos no son válidos. Vuelve a abrir Mensajes.' });
      }
      const overview = await getOverview(auth.user);
      const allowed = new Map((overview.announcements || []).map(item => [item.id, item]));
      if (ids.some(id => !allowed.has(id))) {
        return res.status(403).json({ error: 'Uno de los avisos ya no está disponible para esta cuenta.' });
      }
      const now = new Date();
      await Promise.all(ids.filter(id => !allowed.get(id).read).map(announcementId => receipts.updateOne(
        { announcementId, userId: auth.user._id },
        { $set: { readAt: now }, $setOnInsert: { createdAt: now } },
        { upsert: true }
      )));
      res.set('Cache-Control', 'private, no-store').vary('Cookie').json({ readIds: ids });
    } catch {
      res.status(503).json({ error: 'No pude actualizar los avisos leídos. Vuelve a abrir Mensajes para reintentar.' });
    }
  });
}
