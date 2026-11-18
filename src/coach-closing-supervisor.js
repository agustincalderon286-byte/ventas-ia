const path = '/api/coach/closing-supervisor';

function normalizeContact(value) {
  if (typeof value?.name !== 'string' || typeof value.phone !== 'string' ||
      value.name.length > 120 || value.phone.length > 40) return null;
  const name = value.name.trim().replace(/[^\S\r\n]+/gu, ' ');
  const rawPhone = value.phone.trim();
  if (!/\p{L}/u.test(name) || /[\p{C}\p{Zl}\p{Zp}]/u.test(name) ||
      !/^\+?[\d .()-]+$/.test(rawPhone)) return null;
  const digits = rawPhone.replace(/\D/g, '');
  // A local ten-digit number belongs to the Coach's US/Canada dialing plan.
  let phone = '+' + digits;
  if (!rawPhone.startsWith('+')) {
    if (digits.length === 10) phone = '+1' + digits;
    else if (!/^1\d{10}$/.test(digits)) return null;
  }
  return /^\+[1-9]\d{7,14}$/.test(phone) && (!phone.startsWith('+1') || phone.length === 12) ? { name, phone } : null;
}

export function registerCoachClosingSupervisor(app, { requireActive, users }) {
  function withOwnAccount(handler) {
    return async (req, res) => {
      try {
        const auth = await requireActive(req, res);
        if (!auth) return;
        const accountId = auth.user._id;
        // Never read or change a supervisor using another account's stale tab.
        if (req.get('X-TEC-Account-ID') !== String(accountId)) {
          return res.status(403).json({ error: 'La cuenta cambió. Vuelve a abrir el Coach.' });
        }
        res.set('Cache-Control', 'private, no-store').vary('Cookie');
        await handler(req, res, accountId);
      } catch {
        res.status(503).json({ error: 'No se pudo acceder al contacto. Inténtalo otra vez.' });
      }
    };
  }

  app.get(path, withOwnAccount(async (_req, res, accountId) => {
    const user = await users.findById(accountId).select('+coachClosingSupervisor').lean();
    if (!user) return res.status(404).json({ error: 'La cuenta ya no está disponible.' });
    res.json({ contact: normalizeContact(user.coachClosingSupervisor) });
  }));

  app.put(path, withOwnAccount(async (req, res, accountId) => {
    const contact = normalizeContact(req.body);
    if (!contact) {
      return res.status(400).json({ error: 'Escribe el nombre completo y un número válido. Fuera de EE. UU. y Canadá, incluye el código de país con +.' });
    }
    const result = await users.updateOne({ _id: accountId }, { $set: { coachClosingSupervisor: contact } });
    if (result.matchedCount !== 1) return res.status(404).json({ error: 'La cuenta ya no está disponible.' });
    res.json({ contact });
  }));
}
