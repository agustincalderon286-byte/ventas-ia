// Google People API client for a seller's personal contacts (Coach TEC).
// Read/search never writes. Writes are additive and guarded by the contact etag.

export const contactsScope = 'https://www.googleapis.com/auth/contacts';
const peopleApi = 'https://people.googleapis.com/v1';
const googleToken = 'https://oauth2.googleapis.com/token';
const personFields = 'names,phoneNumbers,emailAddresses,metadata';
// searchContacts has no pagination; a full page may hide candidates.
export const searchPageSize = 30;
const resourcePattern = /^people\/c[A-Za-z0-9_-]{1,64}$/;

export const fail = (text, status, code, extra = {}) => Object.assign(new Error(text), { status, code, ...extra });
const invalid = text => fail(text, 400, 'contactsInvalid');
const conflict = () => fail('El contacto cambió en Google. Vuelve a buscarlo antes de actualizar.', 409, 'contactsConflict');
const incomplete = () => fail('Google devolvió un contacto incompleto.', 502, 'contactsFailed');

export const isContactResource = value => typeof value === 'string' && resourcePattern.test(value);
export const phoneDigits = value => String(value ?? '').replace(/\D/g, '');
const normalEmail = value => String(value ?? '').trim().toLowerCase();

export function samePhone(a, b) {
  const x = phoneDigits(a), y = phoneDigits(b);
  if (!x || !y) return false;
  if (x === y) return true;
  // The pilot uses US local numbers. Keep other country codes explicit.
  if (String(a).trim().startsWith('+') && String(b).trim().startsWith('+')) return false;
  return (x.length === 10 && y === '1' + x) || (y.length === 10 && x === '1' + y);
}

// person = {name, phone, email}; empty/null fields are ignored. Throws contactsInvalid.
export function cleanPerson(person, { needName = false } = {}) {
  if (!person || typeof person !== 'object') throw invalid('Faltan los datos del contacto.');
  const out = {};
  for (const key of ['name', 'phone', 'email']) {
    const value = person[key];
    if (value == null || value === '') continue;
    if (typeof value !== 'string') throw invalid('Revisa los datos del contacto.');
    const text = value.trim();
    if (!text) continue;
    if (/[\p{Cc}\p{Cf}]/u.test(text)) throw invalid('Revisa los datos del contacto.');
    out[key] = text;
  }
  if (out.name && out.name.length > 200) throw invalid('Revisa el nombre del contacto.');
  if (out.phone) {
    const count = phoneDigits(out.phone).length;
    if (!/^\+?[\d\s().-]{6,32}$/.test(out.phone) || count < 7 || count > 15) throw invalid('Revisa el teléfono del contacto.');
  }
  if (out.email) {
    out.email = normalEmail(out.email);
    if (out.email.length > 254 || !/^[^\s@<>()",;:\\[\]]+@[^\s@<>()",;:\\[\].]+(\.[^\s@<>()",;:\\[\].]+)+$/.test(out.email)) {
      throw invalid('Revisa el correo del contacto.');
    }
  }
  if (needName && !out.name) throw invalid('Falta el nombre del contacto.');
  if (!out.name && !out.phone && !out.email) throw invalid('Indica nombre, teléfono o correo.');
  return out;
}

const unique = list => [...new Set(list)];

// Public card: {resourceName, etag, name, phones, emails}. Never silently drops malformed rows.
export function contactCard(row) {
  if (!row || !isContactResource(row.resourceName) || typeof row.etag !== 'string' || !row.etag) throw incomplete();
  const names = Array.isArray(row.names) ? row.names : [];
  const primary = names.find(item => item?.metadata?.primary) || names[0];
  const strings = list => unique((Array.isArray(list) ? list : []).map(item => item?.value).filter(v => typeof v === 'string' && v.trim()).map(v => v.trim()));
  return {
    resourceName: row.resourceName, etag: row.etag,
    name: primary?.displayName || primary?.unstructuredName || '',
    phones: strings(row.phoneNumbers), emails: strings(row.emailAddresses)
  };
}

export function contactsGoogleClient(config, fetcher = fetch) {
  const redirect = config.origin + '/api/coach/conexiones/gmail/callback';

  async function request(url, init = {}, write = false) {
    let res;
    try {
      res = await fetcher(url, { ...init, redirect: 'error', signal: AbortSignal.timeout(12000) });
    } catch (cause) {
      // Timeout/network: a write may have landed.
      throw fail('Google Contactos no respondió.', 503, write ? 'contactsUncertain' : 'contactsFailed', { uncertain: write, cause });
    }
    if (res.ok) {
      try { return await res.json(); }
      catch (cause) { throw fail('Google Contactos devolvió una respuesta ilegible.', 502, write ? 'contactsUncertain' : 'contactsFailed', { uncertain: write, cause }); }
    }
    const body = await res.json().catch(() => ({}));
    const status = res.status;
    const reasons = [...(body.error?.details || []), ...(body.error?.errors || [])].map(item => item?.reason);
    if (status === 401 || body.error === 'invalid_grant' ||
        (status === 403 && reasons.some(r => ['ACCESS_TOKEN_SCOPE_INSUFFICIENT', 'insufficientPermissions'].includes(r)))) {
      throw fail('Google solicita volver a autorizar Contactos.', status, 'contactsReconnect', { reauthorize: true });
    }
    if (status === 409 || (status === 400 && (body.error?.status === 'FAILED_PRECONDITION' || reasons.includes('failedPrecondition')))) throw conflict();
    if (status === 404 || status === 410) throw fail('Ese contacto ya no existe en Google.', 404, 'contactsNotFound');
    if (status === 429) throw fail('Google Contactos está ocupado. Intenta en un momento.', 429, 'contactsBusy');
    if (status >= 500) {
      throw fail('Google Contactos no pudo completar la operación.', status, write ? 'contactsUncertain' : 'contactsFailed', { uncertain: write });
    }
    if (status === 400) throw invalid('Google rechazó los datos del contacto.');
    throw fail('Google Contactos no pudo completar la operación.', status, 'contactsFailed');
  }

  const token = fields => request(googleToken, { method: 'POST', body: new URLSearchParams({
    client_id: config.clientId, client_secret: config.secret, ...fields
  }) });
  const api = (access, path, { method = 'GET', data } = {}) => request(peopleApi + path, {
    method, headers: { Authorization: 'Bearer ' + access, ...(data && { 'Content-Type': 'application/json' }) },
    ...(data && { body: JSON.stringify(data) })
  }, method !== 'GET');

  async function searchOnce(access, query) {
    const params = new URLSearchParams({ query, pageSize: String(searchPageSize), readMask: personFields });
    const body = await api(access, '/people:searchContacts?' + params);
    if (body?.results !== undefined && !Array.isArray(body.results)) throw incomplete();
    return body?.results || [];
  }

  async function read(access, resourceName) {
    if (!isContactResource(resourceName)) throw invalid('La referencia del contacto no es válida.');
    const row = await api(access, '/' + resourceName + '?personFields=' + personFields);
    if (row?.resourceName !== resourceName) throw conflict();
    return row;
  }

  return {
    authorize(state, challenge, email) {
      const params = new URLSearchParams({
        client_id: config.clientId, redirect_uri: redirect, state, code_challenge: challenge,
        code_challenge_method: 'S256', scope: ['openid', 'email', contactsScope].join(' '), response_type: 'code',
        access_type: 'offline', prompt: 'consent select_account', login_hint: email
      });
      return 'https://accounts.google.com/o/oauth2/v2/auth?' + params;
    },
    exchange: (code, verifier) => token({ grant_type: 'authorization_code', code, code_verifier: verifier, redirect_uri: redirect }),
    refresh: refresh_token => token({ grant_type: 'refresh_token', refresh_token }),
    profile: access => request('https://openidconnect.googleapis.com/v1/userinfo', { headers: { Authorization: 'Bearer ' + access } }),

    // Read-only. Returns every candidate Google matched (deduplicated) with matchedBy: ['name'|'phone'|'email'].
    async search(access, person) {
      const wanted = cleanPerson(person);
      const queries = [];
      if (wanted.name) queries.push(['name', wanted.name]);
      if (wanted.phone) {
        queries.push(['phone', wanted.phone]);
        const digits = phoneDigits(wanted.phone);
        if (digits !== wanted.phone) queries.push(['phone', digits]);
      }
      if (wanted.email) queries.push(['email', wanted.email]);
      // Google requires a warmup request with an empty query to refresh the search cache.
      await searchOnce(access, '');
      const found = new Map();
      for (const [kind, query] of queries) {
        const results = await searchOnce(access, query);
        if (results.length >= searchPageSize) {
          throw fail('Hay demasiados contactos parecidos. Agrega teléfono o correo para buscar.', 409, 'contactsTooMany');
        }
        for (const item of results) {
          const candidate = contactCard(item?.person);
          const seen = found.get(candidate.resourceName);
          if (!seen) found.set(candidate.resourceName, { ...candidate, matchedBy: [kind] });
          else if (!seen.matchedBy.includes(kind)) seen.matchedBy.push(kind);
        }
      }
      return [...found.values()];
    },

    get: async (access, resourceName) => contactCard(await read(access, resourceName)),

    // Not idempotent: on contactsUncertain the caller must search before retrying.
    async create(access, person) {
      const wanted = cleanPerson(person, { needName: true });
      const row = await api(access, '/people:createContact?personFields=' + personFields, { method: 'POST', data: {
        names: [{ unstructuredName: wanted.name }],
        ...(wanted.phone && { phoneNumbers: [{ value: wanted.phone }] }),
        ...(wanted.email && { emailAddresses: [{ value: wanted.email }] })
      } });
      return { ...contactCard(row), changed: Object.keys(wanted) };
    },

    // Additive only: keeps every phone/email and other fields, never renames, compares etag.
    async update(access, expected, person) {
      const wanted = cleanPerson(person);
      if (!expected || !isContactResource(expected.resourceName) || typeof expected.etag !== 'string' || !expected.etag) {
        throw invalid('Falta la versión del contacto que confirmaste.');
      }
      const row = await read(access, expected.resourceName);
      if (row.etag !== expected.etag) throw conflict();
      const sources = (row.metadata?.sources || []).filter(source => source?.type === 'CONTACT');
      if (sources.length !== 1 || !sources[0].id || !sources[0].etag) {
        throw fail('Este contacto no se puede editar desde Coach.', 409, 'contactsNotEditable');
      }
      const sourceId = sources[0].id;
      // Only echo entries owned by the contact source; profile/domain entries are read-only.
      const owned = item => !item?.metadata?.source || (item.metadata.source.type === 'CONTACT' && item.metadata.source.id === sourceId);
      const phones = Array.isArray(row.phoneNumbers) ? row.phoneNumbers : [];
      const emails = Array.isArray(row.emailAddresses) ? row.emailAddresses : [];
      const names = Array.isArray(row.names) ? row.names : [];
      const data = { etag: row.etag, metadata: { sources: [{ type: 'CONTACT', id: sourceId, etag: sources[0].etag }] } };
      const fields = [], changed = [];
      if (wanted.name && !names.length) {
        data.names = [{ unstructuredName: wanted.name }]; fields.push('names'); changed.push('name');
      }
      if (wanted.phone && !phones.some(item => samePhone(item?.value, wanted.phone) || samePhone(item?.canonicalForm, wanted.phone))) {
        data.phoneNumbers = [...phones.filter(owned), { value: wanted.phone }]; fields.push('phoneNumbers'); changed.push('phone');
      }
      if (wanted.email && !emails.some(item => normalEmail(item?.value) === wanted.email)) {
        data.emailAddresses = [...emails.filter(owned), { value: wanted.email }]; fields.push('emailAddresses'); changed.push('email');
      }
      if (!fields.length) return { ...contactCard(row), changed };
      const saved = await api(access, '/' + row.resourceName + ':updateContact?' + new URLSearchParams({
        updatePersonFields: fields.join(','), personFields
      }), { method: 'PATCH', data });
      const result = contactCard(saved);
      if (result.resourceName !== row.resourceName ||
          (changed.includes('phone') && !result.phones.some(value => samePhone(value, wanted.phone))) ||
          (changed.includes('email') && !result.emails.some(value => normalEmail(value) === wanted.email))) {
        throw fail('Google no confirmó la actualización del contacto.', 502, 'contactsUncertain', { uncertain: true });
      }
      return { ...result, changed };
    }
  };
}
