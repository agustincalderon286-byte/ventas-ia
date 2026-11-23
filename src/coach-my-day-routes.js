// Rutas de «Mi día» para la app del Coach. Cada una trabaja solo con la cuenta de la sesión.
import { createHash } from 'node:crypto';
import { validZone } from './coach-my-day-push.js';

const api = '/api/coach/mi-dia';

export function registerMyDayRoutes(app, { requireActive, service, allowed = () => true }) {
  async function actorOf(req, res) {
    res.set('Cache-Control', 'private, no-store').vary('Cookie');
    const auth = await requireActive(req, res);
    if (!auth) return null;
    const accountId = String(auth.user._id);
    const claimed = req.get('X-TEC-Account-ID') || req.body?.accountId || req.query?.accountId;
    if (!claimed || String(claimed) !== accountId) { res.status(403).json({ error: 'La cuenta cambió. Vuelve a abrir TEC.' }); return null; }
    if (!allowed(auth)) { res.status(403).json({ error: 'Mi día todavía no está activo en tu cuenta.', code: 'my_day_off' }); return null; }
    const zone = validZone(req.body?.zone || req.query?.zone) || 'America/Chicago';
    return { owner: auth.user._id, email: String(auth.user.email || '').toLowerCase(), name: String(auth.user.name || '').trim(), zone,
      session: createHash('sha256').update('my-day:' + String(auth.session?.tokenHash || auth.session?._id || '')).digest('hex') };
  }
  const handle = work => async (req, res) => {
    try {
      const actor = await actorOf(req, res);
      if (!actor) return;
      res.json(await work(actor, req.body || {}, req));
    } catch (error) {
      if ([409, 422].includes(error?.status)) return res.status(error.status).json({ error: error.message });
      console.error('[mi-dia] falló', req.path, error?.status || 500);
      res.status(503).json({ error: 'No pude cargar Mi día. Vuelve a intentar.' });
    }
  };
  app.get(api, handle(actor => service.state(actor)));
  app.post(api + '/elegir', handle((actor, body) => service.choose(actor, body.mode)));
  app.post(api + '/llamadas', handle((actor, body) => service.callBlock(actor, { more: body.more === true })));
  app.post(api + '/llamada', handle((actor, body) => service.callResult(actor, { ref: body.ref, result: body.result })));
  app.post(api + '/meta', handle((actor, body) => service.setGoal(actor, body.leads)));
  app.post(api + '/exhibidor', handle((actor, body) => service.addExhibitor(actor, body)));
  app.post(api + '/exhibidor/borrar', handle((actor, body) => service.removeExhibitor(actor, body.id)));
  app.post(api + '/exhibidor/sin', handle((actor, body) => service.skipExhibitor(actor, body.reason)));
  app.post(api + '/ruta', handle((actor, body) => service.route(actor, body)));
}
