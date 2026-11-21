// Use the seller pilot's local calendar. A Chicago day can last 23 or 25 hours.
const chicago = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/Chicago', year: 'numeric', month: '2-digit', day: '2-digit',
  timeZoneName: 'longOffset'
});
const oneDay = 86400000;

function chicagoOffset(timestamp) {
  const offset = chicago.formatToParts(timestamp).find(p => p.type === 'timeZoneName').value;
  const [, sign, hours, minutes] = /^GMT([+-])(\d{2}):(\d{2})$/.exec(offset);
  return (sign === '+' ? 1 : -1) * (Number(hours) * 60 + Number(minutes)) * 60000;
}

function midnightSeconds(calendarDate) {
  // Resolve the offset again at the candidate local midnight, including DST transitions.
  const candidate = calendarDate - chicagoOffset(calendarDate);
  return (calendarDate - chicagoOffset(candidate)) / 1000;
}

export function mailPeriod(text, now = Date.now()) {
  const normalized = String(text || '').toLowerCase().normalize('NFD').replace(/\p{M}/gu, '').replace(/\s+/g,' ');
  const matches = new Set(normalized.match(/\b(?:hoy|ayer|esta semana|este mes)\b/g) || []);
  if (matches.size !== 1) return null;
  const [period] = matches;
  const date = Object.fromEntries(chicago.formatToParts(now).map(part => [part.type, part.value]));
  const year = Number(date.year), month = Number(date.month) - 1;
  const today = Date.UTC(year, month, Number(date.day));
  let first = today, next = today + oneDay;
  if (period === 'ayer') {
    first = today - oneDay;
    next = today;
  } else if (period === 'esta semana') {
    const daysSinceMonday = (new Date(today).getUTCDay() + 6) % 7;
    first = today - daysSinceMonday * oneDay;
    next = first + 7 * oneDay;
  } else if (period === 'este mes') {
    first = Date.UTC(year, month, 1);
    next = Date.UTC(year, month + 1, 1);
  }
  return { since: midnightSeconds(first), before: midnightSeconds(next), label: period + ' (hora de Chicago)' };
}
