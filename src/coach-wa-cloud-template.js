import { sendProblem } from './coach-wa-cloud.js';

// Plantilla aprobada OPCIONAL para el piloto de WhatsApp Cloud API: solo sirve para
// escribir fuera de la ventana de 24 horas. Se admite únicamente una plantilla UTILITY
// aprobada cuyo único componente sea un BODY de texto fijo (sin parámetros, header ni
// botones), de modo que el texto revisado por el vendedor sea exactamente el que llega.
// Nunca crea ni edita plantillas en Meta: solo las lee y envía la configurada en servidor.

const e164 = /^\+[1-9]\d{6,14}$/;
const digits = value => String(value || '').replace(/\D/g, '');
const TEMPLATE_FIELDS = 'name,status,category,language,components';
const BODY_MAX = 1024;

export function validTemplateBody(body) {
  return typeof body === 'string' && body.trim().length > 0 && body.length <= BODY_MAX &&
    !/\{\{|\}\}/.test(body) && !/[\u0000-\u0009\u000B-\u001F\u007F]/.test(body);
}

export function waCloudTemplateSettings(config, { wabaId, templateName, language, base = 'https://graph.facebook.com', env = process.env } = {}) {
  const waba = String(wabaId ?? env.COACH_WA_CLOUD_WABA_ID ?? '').trim();
  const name = String(templateName ?? env.COACH_WA_CLOUD_TEMPLATE_NAME ?? '').trim();
  const lang = String(language ?? env.COACH_WA_CLOUD_TEMPLATE_LANGUAGE ?? '').trim();
  const problems = [];
  if (!config?.ready || !config.token || !/^\d{5,25}$/.test(String(config.phoneNumberId || '')) ||
      !/^v\d{1,2}\.\d$/.test(String(config.version || '')) || !e164.test(String(config.phone || ''))) problems.push('cloud');
  if (!/^\d{5,25}$/.test(waba)) problems.push('wabaId');
  if (!/^[a-z0-9_]{1,512}$/.test(name)) problems.push('templateName');
  if (!/^[a-z]{2,3}(?:_[A-Z]{2}|_\d{3})?$/.test(lang)) problems.push('language');
  let origin = '';
  try {
    const url = new URL(base);
    // El token Bearer solo viaja a un origen HTTPS sin ruta, credenciales ni consulta.
    if (url.origin === 'https://graph.facebook.com' && url.origin === String(base).replace(/\/$/, '')) origin = url.origin;
  } catch { /* base no válida */ }
  if (!origin) problems.push('base');
  const ready = problems.length === 0;
  return Object.freeze({ ready, problems: Object.freeze(problems), wabaId: ready ? waba : '', name: ready ? name : '', language: ready ? lang : '', base: origin });
}

const templateReasons = {
  notConfigured: 'TEC todavía no configuró una plantilla aprobada para seguimientos fuera de las 24 horas.',
  unreachable: 'No pude comprobar la plantilla con Meta en este momento.',
  notOwned: 'Meta no vincula tu línea con la cuenta de WhatsApp Business configurada. TEC debe revisar la conexión.',
  notFound: 'Meta no tiene la plantilla de seguimiento configurada en ese idioma.',
  ambiguous: 'Meta devolvió más de una plantilla con ese nombre e idioma. TEC debe revisarla.',
  notApproved: 'La plantilla de seguimiento no está aprobada por Meta en este momento.',
  category: 'La plantilla de seguimiento no está clasificada por Meta como mensaje de utilidad.',
  components: 'La plantilla de seguimiento debe tener solo un texto fijo, sin variables, encabezado ni botones.',
  text: 'El texto de la plantilla de seguimiento no es válido.',
  changed: 'La plantilla cambió desde que la revisaste. Prepárala de nuevo para ver el texto actual antes de enviarla.',
  templateUnavailable: 'Meta ya no permite usar la plantilla de seguimiento. TEC debe revisarla antes de volver a enviarla.'
};
export const templateProblem = reason => templateReasons[reason] || sendProblem(reason);

function classify(code, status) {
  if ([132000, 132001, 132005, 132007, 132012, 132015, 132016].includes(code)) return 'templateUnavailable';
  if (code === 131026) return 'undeliverable';
  if (code === 131030) return 'notAllowed';
  if (code === 190 || status === 401) return 'token';
  if ([4, 80007, 130429, 131048, 131056].includes(code) || status === 429) return 'rateLimit';
  if ([131031, 131042, 368].includes(code)) return 'account';
  if ([100, 131008, 131009, 131051].includes(code)) return 'invalid';
  return 'rejected';
}

export function createWaCloudTemplate(config, { fetch: request = (...args) => globalThis.fetch(...args), verifyMs = 8000, sendMs = 15000, ...options } = {}) {
  const settings = waCloudTemplateSettings(config, options);
  const headers = () => ({ Authorization: 'Bearer ' + config.token });
  const url = path => settings.base + '/' + config.version + path;

  async function list(path) {
    const dataRows = [];
    const cursors = new Set();
    const signal = AbortSignal.timeout(verifyMs);
    let next = path;
    while (next) {
      const page = await getPage(next, signal);
      if (!page.ok) return page;
      if (!Array.isArray(page.data?.data)) return { ok:false, reason:'unreachable' };
      dataRows.push(...page.data.data);
      if (!page.data.paging?.next) break;
      const cursor = page.data.paging.cursors?.after;
      if (typeof cursor !== 'string' || !cursor || cursors.has(cursor) || signal.aborted) return { ok:false, reason:'unreachable' };
      cursors.add(cursor);
      next = path + '&after=' + encodeURIComponent(cursor);
    }
    return { ok:true, rows:dataRows };
  }

  async function getPage(path, signal) {
    let response, data;
    try {
      response = await request(url(path), { method: 'GET', headers: headers(), redirect: 'error', signal });
      data = await response.json().catch(() => null);
    } catch { return { ok: false, reason: 'unreachable' }; }
    if (!response.ok) {
      const code = Number(data?.error?.code);
      return { ok: false, reason: code === 190 || response.status === 401 ? 'token' : response.status >= 500 ? 'unreachable' : 'rejected' };
    }
    return { ok: true, data };
  }

  // Comprueba en Meta, en cada llamada, que la línea pertenece a la WABA y que la
  // plantilla sigue aprobada, UTILITY y con un único BODY fijo. Sin caché a propósito.
  async function inspect() {
    if (!settings.ready) return { ok: false, reason: 'notConfigured' };
    const phones = await list('/' + settings.wabaId + '/phone_numbers?fields=id,display_phone_number');
    if (!phones.ok) return phones;
    const owned = phones.rows.some(line => typeof line?.id === 'string' && line.id === config.phoneNumberId &&
      digits(line.display_phone_number) === config.phone.slice(1));
    if (!owned) return { ok: false, reason: 'notOwned' };

    const found = await list('/' + settings.wabaId + '/message_templates?name=' + encodeURIComponent(settings.name) + '&fields=' + TEMPLATE_FIELDS);
    if (!found.ok) return found;
    // Meta filtra por nombre de forma parcial: solo cuenta la coincidencia exacta de nombre e idioma.
    const matches = found.rows.filter(row => row?.name === settings.name && row?.language === settings.language);
    if (matches.length !== 1) return { ok: false, reason: matches.length ? 'ambiguous' : 'notFound' };
    const row = matches[0];
    if (row.status !== 'APPROVED') return { ok: false, reason: 'notApproved' };
    if (row.category !== 'UTILITY') return { ok: false, reason: 'category' };
    const parts = row.components;
    if (!Array.isArray(parts) || parts.length !== 1 || !parts[0] || typeof parts[0] !== 'object' || Array.isArray(parts[0]) ||
        parts[0].type !== 'BODY' || Object.keys(parts[0]).some(key => key !== 'type' && key !== 'text')) return { ok: false, reason: 'components' };
    const body = parts[0].text;
    if (!validTemplateBody(body)) return { ok: false, reason: typeof body === 'string' && /\{\{|\}\}/.test(body) ? 'components' : 'text' };
    // El texto se conserva tal cual (sin recortar ni normalizar) para la revisión exacta.
    return { ok: true, template: Object.freeze({ name: settings.name, language: settings.language, category: 'UTILITY', body }) };
  }

  return {
    settings,
    inspect,
    async prepare() {
      const result = await inspect();
      return result.ok ? result.template : null;
    },
    // Resultado: {id, metaStatus} si Meta aceptó.
    // error.beforeSend: no se hizo POST (seguro reintentar tras preparar de nuevo).
    // error.providerRejected: Meta respondió que no al POST.
    // Cualquier otro error (error.indeterminate): no se sabe si salió; no repetir.
    async send({ to, template } = {}) {
      const stop = reason => Object.assign(new Error('Plantilla no enviada.'), { beforeSend: true, reason });
      if (!settings.ready) throw stop('notConfigured');
      if (typeof to !== 'string' || !e164.test(to)) throw stop('invalid');
      if (!template || template.name !== settings.name || template.language !== settings.language || !validTemplateBody(template.body)) throw stop('changed');
      const current = await inspect();
      if (!current.ok) throw stop(current.reason);
      if (current.template.body !== template.body) throw stop('changed');

      let response, data;
      try {
        response = await request(url('/' + encodeURIComponent(config.phoneNumberId) + '/messages'), {
          method: 'POST', redirect: 'error', signal: AbortSignal.timeout(sendMs),
          headers: { ...headers(), 'Content-Type': 'application/json' },
          // Solo nombre e idioma configurados en servidor; sin componentes ni parámetros.
          body: JSON.stringify({ messaging_product: 'whatsapp', recipient_type: 'individual', to: to.slice(1), type: 'template',
            template: { name: settings.name, language: { code: settings.language } } })
        });
        data = await response.json().catch(() => null);
      } catch (cause) {
        throw Object.assign(new Error('Respuesta indeterminada de Meta.'), { indeterminate: true, cause });
      }
      if (response.status >= 400 && response.status < 500 && data?.error) {
        const code = Number(data.error.code);
        throw Object.assign(new Error('Meta rechazó la plantilla.'), { providerRejected: true, reason: classify(code, response.status), code });
      }
      const message = response.ok && Array.isArray(data?.messages) ? data.messages[0] : null;
      if (typeof message?.id !== 'string' || !/^[\w.=:+/-]{1,200}$/.test(message.id)) {
        throw Object.assign(new Error('Respuesta indeterminada de Meta.'), { indeterminate: true });
      }
      return { id: message.id, metaStatus: typeof message.message_status === 'string' ? message.message_status.slice(0, 40) : 'accepted' };
    }
  };
}
