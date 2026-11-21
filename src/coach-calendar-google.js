import { createHash } from 'node:crypto';

export const calendarScope = 'https://www.googleapis.com/auth/calendar.app.created';
export const availabilityScope = 'https://www.googleapis.com/auth/calendar.freebusy';
const calendarApi = 'https://www.googleapis.com/calendar/v3';
const googleToken = 'https://oauth2.googleapis.com/token';
export const calendarName = 'Seguimientos TEC';
const eventFields = 'id,htmlLink,etag,status,summary,start/dateTime,end/dateTime,reminders,extendedProperties/private,attendees(email),attendeesOmitted';

const fail = (text, status) => Object.assign(new Error(text), { status });
const isoTime = /^\d{4}-\d\d-\d\dT\d\d:\d\d(?::\d\d(?:\.\d{1,9})?)?(?:Z|[+-]\d\d:\d\d)$/;
const timestamp = value => typeof value === 'string' && isoTime.test(value) ? Date.parse(value) : NaN;
const unavailableSlot = () => Object.assign(fail('Ese horario ya no está disponible.', 409), {slotBusy:true});
const incompleteAvailability = () => fail('Google no pudo verificar ambos calendarios.', 503);
const calendarPath = id => {
  if (!isSecondaryCalendarId(id)) throw fail('Elige el calendario personal de TEC.', 400);
  return '/calendars/' + encodeURIComponent(id);
};
export function isSecondaryCalendarId(id) {
  return typeof id === 'string' && /^[a-z0-9._%+-]{8,250}@group\.calendar\.google\.com$/i.test(id);
}

export function calendarGoogleClient(config, fetcher = fetch) {
  const redirect = config.origin + '/api/coach/conexiones/gmail/callback';
  async function request(url, init = {}) {
    const res = await fetcher(url, { ...init, redirect: 'error', signal: AbortSignal.timeout(12000) });
    if (res.ok) return res.json();
    const error = fail('Google Calendar no pudo completar la operación.', res.status);
    const body = await res.json().catch(() => ({}));
    if (res.status === 401 || body.error === 'invalid_grant' ||
        (res.status === 403 && (body.error?.errors?.some(item => item.reason === 'insufficientPermissions') ||
          body.error?.details?.some(item => item.reason === 'ACCESS_TOKEN_SCOPE_INSUFFICIENT')))) error.reauthorize = true;
    throw error;
  }
  const token = fields => request(googleToken, { method: 'POST', body: new URLSearchParams({
    client_id: config.clientId, client_secret: config.secret, ...fields
  }) });
  const api = (access, path, { method = 'GET', data, etag } = {}) => request(calendarApi + path, {
    method, headers: { Authorization: 'Bearer ' + access,
      ...(data && { 'Content-Type': 'application/json' }), ...(etag && { 'If-Match': etag }) },
    ...(data && { body: JSON.stringify(data) })
  });
  async function availability(access, calendarId, {start,end}, mailbox) {
    const from = timestamp(start), to = timestamp(end);
    if (!isSecondaryCalendarId(calendarId) || !Number.isFinite(from) || !Number.isFinite(to) || from >= to) {
      throw fail('Revisa el período para buscar horarios libres.', 400);
    }
    const result = await api(access,'/freeBusy',{method:'POST',data:{
      timeMin:start,timeMax:end,items:[{id:'primary'},{id:calendarId}]
    }});
    if (!(timestamp(result?.timeMin) <= from && timestamp(result?.timeMax) >= to)) throw incompleteAvailability();
    const calendars = result.calendars;
    const primary = calendars?.primary ?? (mailbox && calendars?.[mailbox]);
    const blocks = [];
    for (const item of [primary,calendars?.[calendarId]]) {
      if (Array.isArray(item?.errors) && item.errors.some(error => ['forbidden','insufficientPermissions'].includes(error?.reason))) {
        throw Object.assign(fail('Google solicita renovar el permiso de disponibilidad.',403),{reauthorize:true});
      }
      if (!Array.isArray(item?.busy) || (item.errors !== undefined && (!Array.isArray(item.errors) || item.errors.length))) {
        throw incompleteAvailability();
      }
      for (const block of item.busy) {
        const left = timestamp(block?.start), right = timestamp(block?.end);
        if (!(left < right)) throw incompleteAvailability();
        if (left < to && right > from) blocks.push({
          start:new Date(Math.max(from,left)).toISOString(),end:new Date(Math.min(to,right)).toISOString()
        });
      }
    }
    return blocks.sort((a,b) => a.start.localeCompare(b.start));
  }
  return {
    availability,
    authorize(state, challenge, email, withAvailability = false) {
      const params = new URLSearchParams({
        client_id: config.clientId, redirect_uri: redirect, state, code_challenge: challenge,
        code_challenge_method: 'S256', scope: ['openid','email',calendarScope,...(withAvailability ? [availabilityScope] : [])].join(' '), response_type: 'code',
        access_type: 'offline', prompt: 'consent select_account', login_hint: email
      });
      return 'https://accounts.google.com/o/oauth2/v2/auth?' + params;
    },
    exchange: (code, verifier) => token({ grant_type:'authorization_code', code, code_verifier:verifier, redirect_uri:redirect }),
    refresh: refresh_token => token({ grant_type:'refresh_token', refresh_token }),
    profile: access => request('https://openidconnect.googleapis.com/v1/userinfo', { headers: { Authorization:'Bearer ' + access } }),
    createCalendar: (access, marker) => api(access, '/calendars?fields=id', { method:'POST',
      data: { summary:calendarName, description:calendarDescription(marker) } }),
    getCalendar: (access, id) => api(access, calendarPath(id) + '?fields=id,description'),
    // Read-only reconciliation: the event written by exactly this operation, or null if it never landed.
    async findEvent(access, calendarId, event) {
      const payload = eventBody({...event,calendarId});
      let row;
      try { row = await api(access, calendarPath(calendarId) + '/events/' + event.eventId + '?fields=' + eventFields); }
      catch (error) { if ([404,410].includes(error.status)) return null; throw error; }
      const own = row?.extendedProperties?.private;
      if (row?.id !== event.eventId || row.status === 'cancelled' || own?.tecEvent !== payload.extendedProperties.private.tecEvent ||
          own?.tecOperation !== event.operationId) return null;
      if (!matchesConfirmedFields(row,payload)) throw fail('La cita se editó en Calendar.', 409);
      if (!row.etag) throw fail('Google todavía no confirmó la cita.', 503);
      return { id:row.id, htmlLink:row.htmlLink || '', etag:row.etag, operationId:event.operationId };
    },
    async saveEvent(access, calendarId, event) {
      const { eventId, operationId, mode, previousOperationId, previousEtag } = event;
      const payload = eventBody({...event,calendarId});
      const base = calendarPath(calendarId) + '/events';
      const target = base + '/' + eventId;
      const fields = eventFields;
      const exact = async () => {
        const row = await api(access, target + '?fields=' + fields);
        if (row.id !== eventId || row.status === 'cancelled' || row.extendedProperties?.private?.tecEvent !== payload.extendedProperties.private.tecEvent) {
          throw fail('Esta cita cambió en Calendar.', 409);
        }
        return row;
      };
      let row;
      if (mode === 'create') {
        const confirmedEvent = async () => {
          const existing = await exact();
          if (existing.extendedProperties.private.tecOperation !== operationId || !matchesConfirmedFields(existing,payload)) throw fail('Esta cita ya cambió.', 409);
          return existing;
        };
        if (event.checkAvailability && event.retryGoogle) {
          // The previous insert may have succeeded even when its response was lost.
          try { row = await confirmedEvent(); }
          catch (error) { if (error.status !== 404) throw error; }
        }
        if (!row) {
          if (event.checkAvailability) {
            const blocks = await availability(access,calendarId,event,event.mailbox);
            if (blocks.length || timestamp(event.start) <= Date.now()) throw unavailableSlot();
          }
          try {
            row = await api(access, base + '?sendUpdates=none&fields=id,htmlLink,etag', {
              method:'POST', data:{ id:eventId, ...payload }
            });
          } catch (error) {
            if (error.status !== 409) throw error;
            row = await confirmedEvent();
          }
        }
      } else {
        row = await exact();
        const last = row.extendedProperties.private.tecOperation;
        if (last === operationId) {
          if (!matchesConfirmedFields(row,payload)) throw fail('La cita se editó en Calendar.', 409);
        } else {
          if (!previousEtag || row.etag !== previousEtag || last !== previousOperationId || row.attendees?.length || row.attendeesOmitted) {
            throw fail('La cita cambió en Calendar. Revisa la actualización.', 409);
          }
          row = await api(access, target + '?sendUpdates=none&fields=id,htmlLink,etag', {
            method:'PATCH', etag:row.etag, data:payload
          });
        }
      }
      if (!row?.etag || row.id !== eventId) throw fail('Google todavía no confirmó la cita.', 503);
      return { id:row.id, htmlLink:row.htmlLink || '', etag:row.etag, operationId };
    }
  };
}

export const calendarDescription = marker => 'Seguimientos personales de Coach TEC. Referencia TEC ' + marker;

function matchesConfirmedFields(row, payload) {
  const expectedReminder = payload.reminders.overrides[0];
  const reminders = row.reminders?.overrides || [];
  return row.summary === payload.summary && Date.parse(row.start?.dateTime) === Date.parse(payload.start.dateTime) &&
    Date.parse(row.end?.dateTime) === Date.parse(payload.end.dateTime) && row.reminders?.useDefault !== true &&
    (expectedReminder ? reminders.length === 1 && reminders[0].method === expectedReminder.method &&
      reminders[0].minutes === expectedReminder.minutes : reminders.length === 0);
}

function eventBody({ owner, calendarId, eventId, operationId, mode, summary, start, end, timeZone, reminderMinutes }) {
  if (!owner || !/^[a-f0-9]{16,64}$/.test(eventId) || typeof operationId !== 'string' ||
      !/^[a-z0-9_-]{1,128}$/i.test(operationId) || !['create','move'].includes(mode)) {
    throw fail('La referencia de la cita no es válida.', 400);
  }
  if (typeof summary !== 'string' || !summary.trim() || summary.length > 250 || /[\p{Cc}]/u.test(summary)) throw fail('Revisa el título de la cita.', 400);
  const iso = /^\d{4}-\d\d-\d\dT\d\d:\d\d(?::\d\d(?:\.\d{1,3})?)?(?:Z|[+-]\d\d:\d\d)$/;
  const startTime = Date.parse(start), endTime = Date.parse(end);
  if (!iso.test(start) || !iso.test(end) || !Number.isFinite(startTime) || !Number.isFinite(endTime) || startTime >= endTime) {
    throw fail('Revisa la fecha y la duración de la cita.', 400);
  }
  if (!timeZone || typeof timeZone !== 'string') throw fail('Revisa tu zona horaria.', 400);
  new Intl.DateTimeFormat('en', { timeZone });
  if (reminderMinutes != null && (!Number.isInteger(reminderMinutes) || reminderMinutes < 0 || reminderMinutes > 40320)) {
    throw fail('Revisa el recordatorio de la cita.', 400);
  }
  const fingerprint = createHash('sha256').update([owner,calendarId,eventId].join(':')).digest('hex');
  return {
    summary:summary.trim(), start:{dateTime:start,timeZone}, end:{dateTime:end,timeZone},
    reminders:{useDefault:false, overrides:reminderMinutes == null ? [] : [{method:'popup',minutes:reminderMinutes}]},
    extendedProperties:{private:{tecEvent:fingerprint, tecOperation:operationId}}
  };
}
