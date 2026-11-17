export const COACH_FLOW_CARD_IDS = Object.freeze([
  'paso-conexion', 'paso-regalo', 'paso-4-en-14', 'paso-cita-instantanea',
  'paso-encuesta', 'paso-demostracion', 'prueba-agua', 'paso-cierre',
  'calculadora-pedido', 'balance-decision', 'perfil-cliente', 'oferta-especial',
  'paso-invitacion', 'resultado-visita'
]);
const allowed = new Set(COACH_FLOW_CARD_IDS);
export function readCoachCardOrder(value) {
  return Array.isArray(value) ? [...new Set(value.filter(id => typeof id === 'string' && allowed.has(id)))] : [];
}
export function registerCoachCardOrder(app, { requireActive, users }) {
  app.put('/api/coach/card-order', async (req, res) => {
    try {
      const auth = await requireActive(req, res);
      if (!auth) return;
      const accountId = String(auth.user._id);
      // A stale tab must never save its draft into a different signed-in account.
      if (req.body?.accountId !== accountId) {
        return res.status(403).json({ error: 'La cuenta cambió. Vuelve a abrir Coach antes de guardar este orden.' });
      }
      const order = req.body?.order;
      if (!Array.isArray(order) || order.length > allowed.size ||
          order.some(id => typeof id !== 'string' || !allowed.has(id)) ||
          new Set(order).size !== order.length) {
        return res.status(400).json({ error: 'El orden de las tarjetas no es válido. Vuelve a abrir el editor.' });
      }
      const result = await users.updateOne({ _id: auth.user._id }, { $set: { coachCardOrder: order } });
      if (result.matchedCount !== 1) throw new Error('Account unavailable');
      res.set('Cache-Control', 'private, no-store').vary('Cookie').json({ order });
    } catch {
      res.status(503).json({ error: 'No pude guardar tu orden. Tus cambios siguen en el editor; vuelve a intentarlo.' });
    }
  });
}
