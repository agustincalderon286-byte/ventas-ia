import { createHash } from 'node:crypto';

// Piloto de WhatsApp Business Cloud API para UNA línea y UN vendedor.
// El número se mantiene en la app de WhatsApp Business por coexistencia; Coach solo
// envía texto libre por Graph /{phone-number-id}/messages tras la confirmación exacta.

const digits = value => String(value || '').replace(/^whatsapp:/i, '').replace(/\D/g, '');
const sha = value => createHash('sha256').update(value).digest('hex');
const email = /^[a-z0-9._%+-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+$/;

export function waCloudConfig(env = {}) {
  const seller = String(env.COACH_WA_CLOUD_SELLER_EMAIL || '').trim().toLowerCase();
  const phone = String(env.COACH_WA_CLOUD_PHONE || '').trim();
  const phoneNumberId = String(env.COACH_WA_CLOUD_PHONE_NUMBER_ID || '').trim();
  const token = String(env.COACH_WA_CLOUD_ACCESS_TOKEN || '').trim();
  const version = String(env.COACH_WA_CLOUD_GRAPH_VERSION || 'v27.0').trim();
  // Twilio y el Chef usan números globales de TEC; nunca pueden ser la línea del vendedor.
  const reserved = [env.TWILIO_WHATSAPP_FROM, env.TWILIO_SMS_FROM, env.WHATSAPP_CHEF_NUMBER].map(digits).filter(Boolean);
  const problems = [];
  if (!email.test(seller)) problems.push('email');
  if (!/^\+[1-9]\d{6,14}$/.test(phone)) problems.push('phone');
  else if (reserved.includes(phone.slice(1))) problems.push('globalPhone');
  if (!/^\d{5,25}$/.test(phoneNumberId)) problems.push('phoneNumberId');
  if (!/^[A-Za-z0-9_-]{20,2048}$/.test(token)) problems.push('token');
  if (!/^v\d{1,2}\.\d$/.test(version)) problems.push('version');
  const ready = problems.length === 0;
  const config = {
    ready, problems, email: email.test(seller) ? seller : '', phone: ready ? phone : '',
    phoneNumberId: ready ? phoneNumberId : '', version,
    // El grant cambia si cambian vendedor, línea, ID, versión de Graph o token: los borradores previos dejan de valer.
    grant: ready ? sha(JSON.stringify(['coach-wa-cloud', seller, phone, phoneNumberId, version, sha(token)])) : ''
  };
  // El token no es enumerable para que no aparezca en JSON ni en registros.
  Object.defineProperty(config, 'token', { value: ready ? token : '', enumerable: false });
  return config;
}

export const waCloudSeller = (config, user) =>
  Boolean(config?.ready && config.email && String(user?.email || '').trim().toLowerCase() === config.email);

const lineReasons = {
  notConfigured: 'TEC todavía no completó la conexión de tu línea con Meta.',
  unreachable: 'No pude comprobar tu línea con Meta en este momento.',
  token: 'La autorización de Meta para tu línea caducó o fue retirada. TEC debe renovarla.',
  rejected: 'Meta no reconoce la línea configurada para tu cuenta.',
  mismatch: 'Meta indica un número distinto al configurado para tu cuenta. TEC debe revisar la conexión.'
};
export const lineProblem = reason => lineReasons[reason] || lineReasons.unreachable;

const sendReasons = {
  template: 'Meta exige una plantilla aprobada porque esta persona no te ha escrito en las últimas 24 horas. Este piloto solo envía texto libre dentro de esa ventana: espera su respuesta o escríbele desde tu app de WhatsApp Business.',
  undeliverable: 'Meta no puede entregar mensajes a ese número. Comprueba que tenga WhatsApp y el prefijo de país.',
  notAllowed: 'Meta todavía no permite enviar a ese número desde esta línea.',
  token: lineReasons.token,
  rateLimit: 'Meta limitó temporalmente los envíos de tu línea. Espera unos minutos y prepara el mensaje de nuevo.',
  account: 'Meta tiene restringida o pendiente de pago la cuenta de WhatsApp Business de tu línea.',
  invalid: 'Meta rechazó el número o el texto del mensaje.',
  rejected: 'Meta rechazó el mensaje.'
};
export const sendProblem = reason => sendReasons[reason] || sendReasons.rejected;

function classify(code, status) {
  if (code === 131047) return 'template';
  if (code === 131026) return 'undeliverable';
  if (code === 131030) return 'notAllowed';
  if (code === 190 || status === 401) return 'token';
  if ([4, 80007, 130429, 131048, 131056].includes(code) || status === 429) return 'rateLimit';
  if ([131031, 131042, 368].includes(code)) return 'account';
  if ([100, 131008, 131009, 131051].includes(code)) return 'invalid';
  return 'rejected';
}

export function createWaCloud(config, { fetch: request = (...args) => globalThis.fetch(...args), base = 'https://graph.facebook.com', verifyMs = 8000, sendMs = 15000 } = {}) {
  const url = suffix => base + '/' + config.version + '/' + encodeURIComponent(config.phoneNumberId) + suffix;
  const headers = () => ({ Authorization: 'Bearer ' + config.token });
  return {
    config,
    async verify() {
      if (!config.ready) return { ok: false, reason: 'notConfigured' };
      let response, data;
      try {
        response = await request(url('?fields=display_phone_number,verified_name'),
          { headers: headers(), redirect: 'error', signal: AbortSignal.timeout(verifyMs) });
        data = await response.json().catch(() => ({}));
      } catch { return { ok: false, reason: 'unreachable' }; }
      if (!response.ok) {
        const code = Number(data?.error?.code);
        return { ok: false, reason: code === 190 || response.status === 401 ? 'token' : response.status >= 500 ? 'unreachable' : 'rejected' };
      }
      if (!digits(data?.display_phone_number) || digits(data.display_phone_number) !== config.phone.slice(1)) return { ok: false, reason: 'mismatch' };
      return { ok: true, phone: config.phone, name: String(data.verified_name || '').slice(0, 120) };
    },
    // Resultado: {id} si Meta aceptó; error.providerRejected si Meta respondió que no;
    // cualquier otro error significa resultado indeterminado y no debe repetirse.
    async send({ to, body }) {
      if (!config.ready || !/^\+[1-9]\d{6,14}$/.test(to) || typeof body !== 'string' || !body.trim() || body.length > 4096) {
        throw Object.assign(new Error('Mensaje no válido.'), { beforeSend: true, reason: 'invalid' });
      }
      const response = await request(url('/messages'), {
        method: 'POST', redirect: 'error', signal: AbortSignal.timeout(sendMs),
        headers: { ...headers(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ messaging_product: 'whatsapp', recipient_type: 'individual', to: to.slice(1), type: 'text', text: { preview_url: false, body } })
      });
      const data = await response.json().catch(() => null);
      if (response.status >= 400 && response.status < 500 && data?.error) {
        const code = Number(data.error.code);
        throw Object.assign(new Error('Meta rechazó el mensaje.'), { providerRejected: true, reason: classify(code, response.status), code });
      }
      const message = response.ok && Array.isArray(data?.messages) ? data.messages[0] : null;
      if (typeof message?.id !== 'string' || !/^[\w.=:+/-]{1,200}$/.test(message.id)) throw new Error('Respuesta indeterminada de Meta.');
      return { id: message.id, metaStatus: typeof message.message_status === 'string' ? message.message_status.slice(0, 40) : 'accepted' };
    }
  };
}
