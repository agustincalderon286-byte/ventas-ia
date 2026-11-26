import { structuredResult } from './coach-mail-intent.js';

// The one-button concept: the seller taps a single button and talks. Coach Jorge decides from what was
// said whether a visit is starting (so the whole visit is recorded) or it is a message for him.
// No fixed phrases: the model reads the meaning, and anything unclear is treated as a message.
const fold = text => String(text || '').normalize('NFD').replace(/\p{M}/gu, '').replace(/\s+/g, ' ').trim().toLowerCase();
const schema = {
  kind: { type: 'string', enum: ['visita', 'mensaje'] },
  host: { type: 'string' }
};
const system = 'Un vendedor de visitas a domicilio tocó el único botón de su aplicación y dijo algo. Decide qué quiere. ' +
  'kind=visita solo si anuncia que va a empezar, está empezando o quiere grabar una visita, una demostración, una cita o una plática con un cliente o prospecto, con sus propias palabras. ' +
  'kind=mensaje para todo lo demás: una pregunta, un recado, un dato, una respuesta a algo pendiente, o si no queda claro. ' +
  'En host copia tal cual el nombre de la persona con quien es la visita si lo dijo; si no, deja vacío. No inventes.';

export async function classifyButtonSpeech(text, { request = structuredResult } = {}) {
  const said = typeof text === 'string' ? text.trim() : '';
  if (!said || said.length > 8000) return { kind: 'mensaje', host: '' };
  // A long message is something told to Jorge, never the announcement of a visit.
  if (said.length > 400) return { kind: 'mensaje', host: '' };
  let result;
  try { result = await request({ name: 'tec_boton', timeout: 9000, properties: schema, system, instruction: JSON.stringify({ dijo: said }) }); }
  catch { return { kind: 'mensaje', host: '' }; }
  if (result?.kind !== 'visita') return { kind: 'mensaje', host: '' };
  const host = typeof result.host === 'string' ? result.host.replace(/[\p{Cc}<>]/gu, ' ').replace(/\s+/g, ' ').trim().slice(0, 80) : '';
  // The name is shown on screen only if the seller really said it.
  return { kind: 'visita', host: host && fold(said).includes(fold(host)) ? host : '' };
}

export function registerOneButton(app, { active, allowed, classify = classifyButtonSpeech }) {
  // A short address to open the concept: the same app, starting on the single button.
  app.get(['/boton', '/boton/'], (_req, res) => res.redirect('/coach/app/?boton=1'));
  app.post('/api/coach/boton/intent', async (req, res) => {
    res.set('Cache-Control', 'private, no-store').vary('Cookie');
    const auth = await active(req, res);
    if (!auth) return;
    if (!allowed(auth, req)) return res.status(403).json({ error: 'El botón único todavía no está activo en tu cuenta.' });
    if (req.get('Sec-Fetch-Site') ? req.get('Sec-Fetch-Site') !== 'same-origin' : req.get('Origin') !== req.protocol + '://' + req.get('host')) {
      return res.status(403).json({ error: 'Abre el botón desde la página del Coach.' });
    }
    res.json(await classify(req.body?.text));
  });
}
