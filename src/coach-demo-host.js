// Whose house is this demo in? When the seller records without opening the client's record, TEC works it out:
// the host heard in the recording is crossed with the seller's CRM (phone, name, an appointment at this hour)
// and with the appointments of the TEC calendar around the time the recording started.
const fold = text => String(text || '').normalize('NFD').replace(/\p{M}/gu, '').replace(/[^\p{L}\d ]+/gu, ' ')
  .replace(/\s+/g, ' ').trim().toLowerCase();
const connectors = new Set(['de', 'del', 'la', 'las', 'los', 'y', 'e', 'san', 'van', 'da', 'do', 'dos']);
const generic = new Set(['tec', 'demo', 'demostracion', 'cita', 'visita', 'seguimiento', 'llamada', 'referido', 'referida', 'cliente', 'anfitrion', 'anfitriona']);
const tokens = name => fold(name).split(' ').filter(word => word.length >= 2 && !connectors.has(word));
const phoneKey = value => { const digits = String(value || '').replace(/\D/g, ''); return digits.length === 11 && digits[0] === '1' ? digits.slice(1) : digits.length === 10 ? digits : ''; };
const wordIn = (text, word) => new RegExp('(?:^| )' + word + '(?: |$)').test(text);

export const DEMO_HOST_BEFORE_MS = 3 * 3600_000;
export const DEMO_HOST_AFTER_MS = 2 * 3600_000;

// The name in a calendar title: «Demo · María López (4 en 14 de Rosa)» → «María López».
export function nameFromTitle(title) {
  const main = String(title || '').split('·').pop().replace(/\([^)]*\)/g, ' ').replace(/\s+/g, ' ').trim()
    .replace(/^(?:demo|demostraci[oó]n|cita|visita|seguimiento|llamada)\b\s*(?:con |a |de )?[:\-–]?\s*/i, '').trim();
  const words = tokens(main);
  return words.length && words.length <= 5 && !/\d/.test(main) && !words.every(word => generic.has(word)) ? main : '';
}

// Every word of the shorter name is in the longer one: «María» ≈ «María López», «María López» ≈ «María Elena López».
function sameName(a, b) {
  const left = tokens(a), right = tokens(b);
  if (!left.length || !right.length) return false;
  const [short, long] = left.length <= right.length ? [left, right] : [right, left];
  return short.every(word => long.includes(word));
}

const near = (date, start) => {
  const at = Number(new Date(date)), from = Number(new Date(start));
  return Number.isFinite(at) && Number.isFinite(from) && at >= from - DEMO_HOST_BEFORE_MS && at <= from + DEMO_HOST_AFTER_MS;
};

// heard: { name, phone } verified in the recording, or null. records: the seller's CRM records
// ({ id, name, phone, appointmentAt }). events: TEC calendar appointments ({ title, start }).
// Returns { name, phone, crmId?, source } or null when nothing is clear enough.
export function resolveDemoHost({ heard = null, transcript = '', startedAt, records = [], events = [], sellerName = '', referralNames = [] }) {
  const text = fold(transcript);
  const seller = tokens(sellerName);
  const isSeller = name => seller.length && tokens(name).length && tokens(name).every(word => seller.includes(word));
  const isReferral = name => referralNames.some(other => sameName(other, name) && tokens(other).length === tokens(name).length);
  const usable = name => tokens(name).length && !isSeller(name) && !isReferral(name);
  // The first name of a person is said during the visit («gracias, María»).
  const said = name => { const first = tokens(name)[0]; return Boolean(first && first.length >= 3 && wordIn(text, first)); };
  const host = heard && usable(heard.name) ? { name: String(heard.name).trim(), phone: phoneKey(heard.phone) } : null;
  const rows = records.filter(row => row?.id && usable(row.name));
  const unique = list => list.length === 1 ? list[0] : null;
  const fromRecord = (row, source) => ({ name: String(row.name).trim().slice(0, 100), phone: phoneKey(row.phone), crmId: String(row.id), source,
    ...(row.address && { address: String(row.address).slice(0, 240) }) });

  // 1. The host's phone was dictated and is in the CRM.
  if (host?.phone) {
    const match = unique(rows.filter(row => phoneKey(row.phone) === host.phone));
    if (match) return fromRecord(match, 'telefono');
  }
  // 2. The full name heard (name and surname) is one client of the CRM.
  if (host && tokens(host.name).length >= 2) {
    const match = unique(rows.filter(row => sameName(row.name, host.name) && tokens(row.name).length >= 2));
    if (match) return fromRecord(match, 'nombre');
  }
  // 3. A CRM client has an appointment at this hour and is the host heard, or is named in the visit.
  const booked = rows.filter(row => near(row.appointmentAt, startedAt));
  const bookedMatch = unique(booked.filter(row => host ? sameName(row.name, host.name) : said(row.name)));
  if (bookedMatch) return fromRecord(bookedMatch, 'cita');
  // 4. An appointment of the TEC calendar at this hour names the host heard, or someone named in the visit.
  const titled = events.filter(event => near(event.start, startedAt)).map(event => nameFromTitle(event.title))
    .filter(name => usable(name) && (host ? sameName(name, host.name) : said(name)));
  const eventName = unique([...new Map(titled.map(name => [fold(name), name])).values()]);
  if (eventName) {
    const record = unique(rows.filter(row => sameName(row.name, eventName) && tokens(row.name).length >= tokens(eventName).length));
    if (record) return fromRecord(record, 'calendario');
    const name = host && tokens(host.name).length > tokens(eventName).length ? host.name : eventName;
    return { name: name.slice(0, 100), phone: host?.phone || '', source: 'calendario' };
  }
  // 5. Nobody known: the host heard with name and surname is a new client.
  if (host && tokens(host.name).length >= 2) return { name: host.name.slice(0, 100), phone: host.phone, source: 'grabacion' };
  return null;
}
