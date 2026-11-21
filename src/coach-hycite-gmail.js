import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';

export const gmailScope = 'https://www.googleapis.com/auth/gmail.readonly';
export const gmailSendScope = 'https://www.googleapis.com/auth/gmail.send';
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
    error.status = response.status;
    error.providerRejected = response.status >= 400 && response.status < 500 && response.status !== 408;
    const body = await response.json().catch(() => ({}));
    if (body.error === 'invalid_grant' || response.status === 401) error.reauthorize = true;
    throw error;
  }
  return response.json();
}

// Each draft has exactly one explicit recipient. Display names and address lists
// are intentionally not interpreted as mailboxes.
export function isGmailAddress(address) {
  if (typeof address !== 'string' || address.length > 254 || /[\s\x00-\x1f\x7f]/u.test(address)) return false;
  const parts = address.split('@');
  if (parts.length !== 2 || parts[0].length > 64) return false;
  const local = /^[a-z\d!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z\d!#$%&'*+\/=?^_`{|}~-]+)*$/i;
  const label = /^[a-z\d](?:[a-z\d-]{0,61}[a-z\d])?$/i;
  const domain = parts[1].split('.');
  return local.test(parts[0]) && domain.length >= 2 && domain.every(part => label.test(part));
}

function encodedSubject(subject) {
  // RFC 2047 encoded words must fit 75 characters and must not split a UTF-8 character.
  const words = [];
  let chunk = '';
  for (const character of subject) {
    if (Buffer.byteLength(chunk + character) > 42) {
      words.push(chunk);
      chunk = '';
    }
    chunk += character;
  }
  words.push(chunk);
  return words.map(word => '=?UTF-8?B?' + Buffer.from(word).toString('base64') + '?=').join('\r\n ');
}

function rawMessage({ from, to, subject, body }) {
  if (!isGmailAddress(from) || !isGmailAddress(to)) throw new Error('Se necesita una dirección de correo válida.');
  if (typeof subject !== 'string' || !subject.trim() || subject.length > 300 || /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/u.test(subject)) {
    throw new Error('Revisa el asunto del correo.');
  }
  if (typeof body !== 'string' || !body.trim() || Buffer.byteLength(body) > 50000) throw new Error('Revisa el texto del correo.');
  const encodedBody = Buffer.from(body.replace(/\r\n|\r|\n/g, '\r\n')).toString('base64');
  const mime = ['Date: ' + new Date().toUTCString(), 'From: ' + from, 'To: ' + to,
    'Subject: ' + encodedSubject(subject), 'MIME-Version: 1.0', 'Content-Type: text/plain; charset=UTF-8',
    'Content-Transfer-Encoding: base64', '', ...encodedBody.match(/.{1,76}/g), ''].join('\r\n');
  return Buffer.from(mime).toString('base64url');
}

function inboxQuery({ unread, from, days, since, before }) {
  const params = new URLSearchParams({ labelIds: 'INBOX', maxResults: '5', fields: 'messages(id)' });
  if (unread !== undefined && typeof unread !== 'boolean') throw new TypeError('Filtro de lectura no válido.');
  if (unread) params.append('labelIds', 'UNREAD');
  const filters = [];
  if (from !== undefined) {
    if (!isGmailAddress(from)) throw new TypeError('Dirección de remitente no válida.');
    filters.push('from:"' + from + '"');
  }
  if (days !== undefined) {
    if (!Number.isInteger(days) || days < 1 || days > 365) throw new TypeError('Elige un período de 1 a 365 días.');
    filters.push('newer_than:' + days + 'd');
  }
  if (since !== undefined || before !== undefined) {
    if (days !== undefined || !Number.isSafeInteger(since) || !Number.isSafeInteger(before) ||
        since < 0 || before > 8640000000000 || before <= since) {
      throw new TypeError('El intervalo de fechas no es válido.');
    }
    filters.push('after:' + since, 'before:' + before);
  }
  if (filters.length) params.set('q', filters.join(' '));
  return params;
}

function inboxText(value, limit) {
  return String(value ?? '').replace(/[\s\p{Cc}\u202a-\u202e\u2066-\u2069]+/gu, ' ').trim().slice(0, limit);
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
    authorize(state, challenge, { send = false, mailbox } = {}) {
      const query = new URLSearchParams({ client_id: config.clientId, redirect_uri, state,
        code_challenge: challenge, code_challenge_method: 'S256', scope: send ? gmailScope + ' ' + gmailSendScope : gmailScope,
        response_type: 'code', access_type: 'offline', prompt: send ? 'consent' : 'consent select_account' });
      if (send) {
        query.set('include_granted_scopes', 'true');
        query.set('login_hint', mailbox);
      }
      return 'https://accounts.google.com/o/oauth2/v2/auth?' + query;
    },
    exchange: (code, verifier) => token({ grant_type: 'authorization_code', code, code_verifier: verifier, redirect_uri }),
    refresh: refresh_token => token({ grant_type: 'refresh_token', refresh_token }),
    profile: access => read(api + '/profile', access),
    async inbox(access, options = {}) {
      const list = await read(api + '/messages?' + inboxQuery(options), access);
      const params = new URLSearchParams({ format: 'metadata', fields: 'id,internalDate,snippet,payload/headers' });
      for (const name of ['From', 'Subject']) params.append('metadataHeaders', name);
      const rows = await Promise.all((list.messages || []).slice(0, 5).map(async ({ id }) => {
        const url = api + '/messages/' + encodeURIComponent(id);
        try {
          const message = await read(url + '?' + params, access);
          // Metadata responses may omit the snippet. Fetch just that scalar in that case.
          const snippet = message.snippet ?? (await read(url + '?format=full&fields=snippet', access)).snippet;
          const headers = message.payload?.headers || [];
          const header = name => headers.find(h => h.name?.toLowerCase() === name)?.value;
          return { id, from: inboxText(header('from'), 320), subject: inboxText(header('subject'), 220),
            date: new Date(Number(message.internalDate)).toISOString(), snippet: inboxText(snippet, 240) };
        } catch (error) {
          if (error.status === 404) return null; // Removed from Gmail during this request.
          throw error;
        }
      }));
      return rows.filter(Boolean).sort((a,b) => b.date.localeCompare(a.date));
    },
    async send(access, message) {
      const raw = rawMessage(message);
      const result = await requestJson(api + '/messages/send', {
        method: 'POST', headers: { Authorization: 'Bearer ' + access, 'Content-Type': 'application/json' },
        body: JSON.stringify({ raw })
      });
      if (!result?.id) throw new Error('Google no devolvió el comprobante de envío.');
      return { id: result.id };
    },
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
