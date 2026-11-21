import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';

export const gmailScope = 'https://www.googleapis.com/auth/gmail.readonly';
const api = 'https://gmail.googleapis.com/gmail/v1/users/me';
const tokenUrl = 'https://oauth2.googleapis.com/token';

export function sealToken(value, key, owner) {
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  cipher.setAAD(Buffer.from(String(owner)));
  const content = Buffer.concat([cipher.update(value, 'utf8'), cipher.final()]);
  return [iv, cipher.getAuthTag(), content].map(b => b.toString('base64url')).join('.');
}

export function openToken(value, key, owner) {
  const [iv, tag, content] = value.split('.').map(s => Buffer.from(s, 'base64url'));
  const cipher = createDecipheriv('aes-256-gcm', key, iv);
  cipher.setAAD(Buffer.from(String(owner)));
  cipher.setAuthTag(tag);
  return Buffer.concat([cipher.update(content), cipher.final()]).toString();
}

async function requestJson(url, options) {
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(12000), redirect: 'error' });
  if (!response.ok) {
    const error = new Error('Gmail no respondió. Intenta actualizar más tarde.');
    const body = await response.json().catch(() => ({}));
    if (body.error === 'invalid_grant' || response.status === 401) error.reauthorize = true;
    throw error;
  }
  return response.json();
}

export function gmailClient(config) {
  const redirect_uri = config.origin + '/api/coach/conexiones/gmail/callback';
  const token = fields => requestJson(tokenUrl, {
    method: 'POST', body: new URLSearchParams({
      client_id: config.clientId, client_secret: config.secret, ...fields
    })
  });
  const read = (url, access) => requestJson(url, { headers: { Authorization: 'Bearer ' + access } });
  return {
    authorize(state, challenge) {
      const query = new URLSearchParams({ client_id: config.clientId, redirect_uri, state,
        code_challenge: challenge, code_challenge_method: 'S256', scope: gmailScope,
        response_type: 'code', access_type: 'offline', prompt: 'consent select_account' });
      return 'https://accounts.google.com/o/oauth2/v2/auth?' + query;
    },
    exchange: (code, verifier) => token({ grant_type: 'authorization_code', code, code_verifier: verifier, redirect_uri }),
    refresh: refresh_token => token({ grant_type: 'refresh_token', refresh_token }),
    profile: access => read(api + '/profile', access),
    async revoke(value) {
      const response = await fetch('https://oauth2.googleapis.com/revoke', {
        method: 'POST', body: new URLSearchParams({ token: value }), signal: AbortSignal.timeout(10000)
      });
      return response.ok;
    },
    async notices(access) {
      const query = config.domains.map(domain => 'from:(@' + domain + ')').join(' ');
      const list = await read(api + '/messages?' + new URLSearchParams({
        q: 'newer_than:90d {' + query + '}', maxResults: '100'
      }), access);
      const messages = [];
      const ids = list.messages || [];
      for (let i = 0; i < ids.length; i += 8) {
        const batch = await Promise.all(ids.slice(i, i + 8).map(async ({ id }) => {
          const fields = 'id,internalDate,snippet,payload/headers';
          const params = new URLSearchParams({ format: 'full', fields });
          return read(api + '/messages/' + encodeURIComponent(id) + '?' + params, access);
        }));
        messages.push(...batch);
      }
      return { ...summarizeMessages(messages, config.domains), limited: Boolean(list.nextPageToken) };
    }
  };
}

const normalize = text => String(text || '').normalize('NFD').replace(/\p{M}/gu, '').toLowerCase().trim();
// Require an explicit outcome. Acknowledgements such as "order confirmation"
// can describe receipt of an application before the sale is approved.
const confirmedTitle = /^(?:sale confirmed|order confirmed|venta confirmada|orden confirmada|pedido confirmado|confirmacion de venta)$/;
const idPattern = '[a-z0-9]+(?:-[a-z0-9]+)*';
const confirmationWithId = new RegExp(
  '^(sale confirmed|order confirmed|venta confirmada|orden confirmada|pedido confirmado|confirmacion de venta)' +
  '(?:\\s+|\\s*[:#–-]\\s*)(?:(?:order|orden|pedido)\\s*)?(?:(?:no\\.?|number|numero|n[º°])\\s*)?#?\\s*(' + idPattern + ')$'
);
const reference = new RegExp(
  '\\b(?:(?:numero de )?(?:order|pedido|orden|venta|sale)(?:\\s+(?:number|numero|no\\.?|n[º°]|confirmad[ao]|confirmed|cancelad[ao]|cancelled|canceled|rechazad[ao]|declined|voided|pendiente|pending|devuelt[ao]))?)' +
  '(?:\\s*[:#–]\\s*|\\s+)(?:#\\s*)?(' + idPattern + ')(?![a-z0-9_/-])', 'gi'
);
const uncertain = /pendiente|pending|cancel|declin|rechaz|revers|devolu|devuelt|refund|anulad|void|on hold|suspend|denied|denegad|not (?:yet )?(?:confirmed|approved)|sin (?:confirmar|aprobar)|no (?:confirmad|aprobad)|credit application|solicitud de credito|awaiting approval|en revision/;
const validOrder = value => value && value.length <= 40 && /\d/.test(value);

function parseNotice(message, domains) {
  const headers = message.payload?.headers || [];
  const header = name => headers.find(h => h.name.toLowerCase() === name)?.value || '';
  const from = header('from').trim().toLowerCase();
  const domain = (from.match(/^(?:[^<>]*<)?[\w.+%-]+@([a-z\d.-]+)>?$/) || [])[1];
  if (!domains.includes(domain)) return null;
  const checks = header('authentication-results').toLowerCase();
  const authDomain = checks.match(/(?:^|;)\s*dmarc=pass\b[^;]*?\bheader\.from=([a-z0-9.-]+)/)?.[1];
  if (!/^mx\.google\.com\s*;/.test(checks) || authDomain !== domain) return null;
  const subject = header('subject').trim();
  const normalized = normalize(subject);
  const snippet = normalize(message.snippet);
  const match = normalized.match(confirmationWithId);
  const ids = new Set();
  if (validOrder(match?.[2])) ids.add(match[2].toUpperCase());
  for (const text of [normalized, snippet]) {
    for (const [, value] of text.matchAll(reference)) {
      if (validOrder(value)) ids.add(value.toUpperCase());
    }
  }
  const order = ids.size === 1 ? [...ids][0] : '';
  const time = Number(message.internalDate);
  if (!message.id || !Number.isSafeInteger(time) || time < 0 || time > 8.64e15) return null;
  const accepted = Boolean(order && (match || confirmedTitle.test(normalized)) && !uncertain.test(normalized + ' ' + snippet));
  return { ids, time, row: { id: message.id, subject: subject.slice(0, 220), order,
    date: new Date(time).toISOString(), confirmed: accepted } };
}

export function summarizeMessages(messages, domains) {
  const seen = new Set();
  const confirmed = [], review = [];
  const notices = messages.map(message => parseNotice(message, domains)).filter(Boolean);
  // At the same timestamp, prefer manual review over an ambiguous confirmation.
  notices.sort((a,b) => b.time - a.time || Number(a.row.confirmed) - Number(b.row.confirmed));
  for (const { ids, row } of notices) {
    if (row.order && seen.has(row.order)) continue;
    // A later ambiguous notice covering several orders supersedes all of them.
    for (const order of ids) seen.add(order);
    (row.confirmed ? confirmed : review).push(row);
  }
  return { confirmed, review };
}
