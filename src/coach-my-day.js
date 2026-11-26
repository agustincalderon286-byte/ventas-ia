// «Mi día»: el Coach le pregunta al vendedor si va a atender citas, llamar, prospectar o si terminó por hoy,
// con lo que sabe de su calendario, su exhibidor, sus leads y su meta de la semana. Cada elección abre pasos cortos.
// Solo lee los datos del propio vendedor; las citas se agendan con el botón de confirmar del Coach, no aquí.
import mongoose from 'mongoose';
import { createHash } from 'node:crypto';
import { resolveInstant } from './coach-followup-input.js';
import { localClock, validZone } from './coach-my-day-push.js';

export const STANDARD_WEEKLY_LEADS = 100;
export const MODES = Object.freeze(['citas', 'llamar', 'prospectar', 'terminar']);
export const CALL_RESULTS = Object.freeze({ cita: 'Sacó cita', no_contesto: 'No contestó', volver: 'Volver a llamar', no_interesa: 'No le interesa' });
// Búsquedas de Google Maps: el teléfono las abre cerca de donde está el vendedor, sin costo para TEC.
export const PROSPECT_PLACES = Object.freeze([
  { id: 'lavanderias', label: 'Lavanderías cerca de mí', query: 'lavanderías cerca de mí' },
  { id: 'fresh-market', label: 'Fresh Market cerca de mí', query: 'Fresh Market cerca de mí' },
  { id: 'supermercados', label: 'Supermercados latinos cerca de mí', query: 'supermercados latinos cerca de mí' }
]);
// Horas en que la gente está en casa: las mejores para demos y para llamar.
const PEAK = { from: 17, to: 20 };
const DAY = 86400000;
const BLOCK = 5;
const fail = (status, message) => Object.assign(new Error(message), { status });
const text = (value, max = 200) => String(value ?? '').replace(/[\p{Cc}\p{Cf}]+/gu, ' ').replace(/\s+/g, ' ').trim().slice(0, max);
const fold = value => text(value).normalize('NFD').replace(/\p{M}/gu, '').toLowerCase();
const civil = day => Date.parse(day + 'T00:00:00Z');
const dayString = ms => new Date(ms).toISOString().slice(0, 10);
const validDay = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && dayString(civil(value)) === value;
const validTime = value => typeof value === 'string' && /^([01]\d|2[0-3]):[0-5]\d$/.test(value);
const hhmm = (ms, zone) => { const c = localClock(ms, zone); return String(c.hour).padStart(2, '0') + ':' + String(c.minute).padStart(2, '0'); };
const firstName = value => text(value, 80).split(' ')[0] || '';
const plural = (n, one, many) => n + ' ' + (n === 1 ? one : many);
// Estados de la hoja que no se llaman en un bloque: ya tienen cita o pidieron no seguir.
const skipStates = /\b(?:no interesad[oa]|numero equivocado|no califica|colgo|cita)\b/;

export function createMyDayModels(mg = mongoose) {
  const { Schema } = mg;
  const exhibitor = new Schema({ day: String, start: String, end: String, place: String, address: String });
  const plan = new Schema({ _id: Schema.Types.ObjectId, weeklyLeadGoal: Number, exhibitors: [exhibitor],
    exhibitorSkip: { week: String, reason: String }, updatedAt: Date });
  // Un registro por vendedor y día: solo elecciones y conteos, sin nombres ni teléfonos.
  const log = new Schema({ owner: { type: Schema.Types.ObjectId, required: true }, day: { type: String, required: true },
    choices: [{ mode: String, at: Date, _id: false }], calls: { type: Schema.Types.Mixed, default: {} }, given: [String],
    route: { place: String, startedAt: Date, finishedAt: Date, places: Number, leads: Number, note: String }, expiresAt: Date });
  log.index({ owner: 1, day: 1 }, { unique: true });
  log.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  return {
    plans: mg.models.CoachMyDayPlan || mg.model('CoachMyDayPlan', plan),
    logs: mg.models.CoachMyDayLog || mg.model('CoachMyDayLog', log)
  };
}

// Modelos de captura de TEC, que define el servidor principal. Se buscan al usarlos.
const capture = name => mongoose.models[name] || null;
const defaultSources = () => ({ leads: capture('CoachLeadInbox'), tickets: capture('CoachPersonalTicketLead'),
  sheets: capture('CoachPersonalProgramSheet'), outcomes: capture('CoachDemoOutcome') });

// Semana de lunes a domingo en la zona del vendedor.
export function calendarOf(at, zone) {
  const { day, hour, minute } = localClock(at, zone);
  const today = civil(day), weekday = new Date(today).getUTCDay();
  const monday = today - ((weekday + 6) % 7) * DAY;
  const instant = ms => resolveInstant(ms, 0, 0, zone) ?? resolveInstant(ms, 1, 0, zone);
  // Días de trabajo que quedan, de hoy al sábado; el domingo cuenta solo él.
  const workDaysLeft = weekday === 0 ? 1 : 7 - weekday;
  return { day, hour, minute, weekday, weekKey: dayString(monday), workDaysLeft,
    todayStart: instant(today), todayEnd: instant(today + DAY), weekStart: instant(monday), weekEnd: instant(monday + 7 * DAY) };
}

export function createMyDay({ config = {}, models = createMyDayModels(), sources = defaultSources, calendar = null, sheets = null, sheetTools = null, now = Date.now }) {
  const { plans, logs } = models;
  const src = () => (typeof sources === 'function' ? sources() : sources) || {};
  const inList = (list, email) => Boolean(list?.some(item => item === '*' || item === String(email || '').toLowerCase()));

  async function leadsBetween(seller, from, to) {
    const { leads, tickets, sheets: programSheets } = src();
    const range = { $gte: new Date(from), $lt: new Date(to) };
    const [a, b, c] = await Promise.all([
      leads ? leads.countDocuments({ generatedByUserId: seller, createdAt: range }) : 0,
      tickets ? tickets.countDocuments({ generatedByUserId: seller, createdAt: range }) : 0,
      programSheets ? programSheets.find({ ownerUserId: seller, createdAt: range }).select('referralCount').lean() : []
    ]);
    return Number(a || 0) + Number(b || 0) + (c || []).reduce((sum, sheet) => sum + (Number(sheet.referralCount) || 0), 0);
  }
  async function demosBetween(seller, from, to) {
    const { outcomes } = src();
    if (!outcomes) return { demos: 0, sales: 0, amount: 0 };
    const rows = await outcomes.find({ reportedByUserId: seller, createdAt: { $gte: new Date(from), $lt: new Date(to) } }).select('resultType saleAmount').lean();
    const demos = rows.filter(r => r.resultType !== 'no_atendio').length;
    const sold = rows.filter(r => r.resultType === 'venta');
    return { demos, sales: sold.length, amount: Math.round(sold.reduce((sum, r) => sum + (Number(r.saleAmount) || 0), 0)) };
  }
  async function callsBetween(seller, fromDay, toDay) {
    const rows = await logs.find({ owner: seller, day: { $gte: fromDay, $lt: toDay } }).select('calls').lean();
    return rows.reduce((sum, row) => sum + (Number(row.calls?.total) || 0), 0);
  }

  // Citas de hoy: las de «Seguimientos TEC», los bloques ocupados del calendario principal y las de la Agenda de la Hoja TEC.
  async function appointmentsToday(actor, cal) {
    const result = { events: [], upcoming: [], busy: 0, agenda: [], calendarConnected: false, notes: [] };
    if (calendar && inList(config.pilot, actor.email)) {
      try {
        const connection = await calendar.getConnection(actor.owner);
        if (connection?.grant) {
          result.calendarConnected = true;
          const read = await calendar.readAgenda({ owner: actor.owner, grant: connection.grant, zone: actor.zone, includeEvents: true, includePrimary: true,
            start: new Date(cal.todayStart).toISOString(), end: new Date(cal.todayEnd).toISOString() });
          if (read.events.status === 'fulfilled') result.events = (read.events.value || []).filter(e => !e.allDay && e.attendance !== 'declined')
            .map(e => ({ title: text(e.title, 120), time: hhmm(e.start, actor.zone), at: new Date(e.start).getTime() })).sort((a, b) => a.at - b.at).slice(0, 12);
          else result.notes.push('No pude leer las citas de «Seguimientos TEC».');
          if (read.busy?.status === 'fulfilled') result.busy = (read.busy.value || []).length;
          // What comes next: the appointments of the following two weeks (those the demos and the Coach booked).
          const next = await calendar.readAgenda({ owner: actor.owner, grant: connection.grant, zone: actor.zone, includeEvents: true, includePrimary: false,
            start: new Date(cal.todayEnd).toISOString(), end: new Date(cal.todayEnd + 14 * DAY).toISOString() }).catch(() => null);
          if (next?.events?.status === 'fulfilled') result.upcoming = (next.events.value || []).filter(e => !e.allDay && e.attendance !== 'declined')
            .map(e => ({ title: text(e.title, 120), at: new Date(e.start).getTime(), when: new Intl.DateTimeFormat('es-US', { timeZone: actor.zone,
              weekday: 'short', day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit' }).format(new Date(e.start)) }))
            .sort((a, b) => a.at - b.at).slice(0, 12);
        }
      } catch { result.notes.push('No pude leer tu calendario.'); }
    }
    const ctx = await sheetContext(actor).catch(() => null);
    if (ctx && sheetTools) {
      try {
        const data = await sheetTools.agenda(ctx, { from: cal.day, to: cal.day });
        const me = fold(firstName(actor.name));
        result.agenda = (data?.rows || []).filter(r => !r.result && (!r.seller || !me || fold(r.seller).includes(me)))
          .map(r => ({ client: text(r.client, 120), time: r.time || '', address: text(r.address, 200), origin: text(r.origin, 60) })).slice(0, 12);
      } catch { result.notes.push('No pude leer la Agenda de tu Hoja TEC.'); }
    }
    return result;
  }

  async function sheetContext(actor) {
    if (!sheets || !sheetTools || !inList(config.sheetsPilot, actor.email) || !config.sheetsReady) return null;
    let book;
    try { book = await sheets.access(actor.owner); } catch { return null; }
    const { token, grant, ...workbook } = book;
    return { owner: actor.owner, session: actor.session, grant, workbook, access: token, sellerName: actor.name,
      isCurrent: async () => await sheets.current(actor.owner, grant) };
  }

  async function ensureLog(owner, day) {
    return logs.findOneAndUpdate({ owner, day }, { $setOnInsert: { owner, day, calls: {}, given: [], choices: [], expiresAt: new Date(now() + 180 * DAY) } },
      { upsert: true, new: true }).lean();
  }

  // callable: si hay gente por llamar. Para no leer la hoja completa en cada vistazo se supone que sí;
  // el bloque de llamadas dice si no quedó nadie.
  function recommend({ cal, citas, exhibitorsToday, callable = true }) {
    const upcoming = [...citas.events.filter(e => e.at > now()).map(e => e.time), ...citas.agenda.map(a => a.time).filter(t => t && t >= hhmm(now(), cal.zone))].sort();
    if (upcoming.length) return { mode: 'citas', reason: 'Tienes ' + plural(upcoming.length, 'cita', 'citas') + ' por atender hoy; la próxima es a las ' + upcoming[0] + '.' };
    const exhibitor = exhibitorsToday.find(e => !e.end || e.end > hhmm(now(), cal.zone));
    if (exhibitor) return { mode: 'prospectar', reason: 'Hoy tienes exhibidor en ' + exhibitor.place + (exhibitor.start ? ' a las ' + exhibitor.start : '') + '.' };
    if (cal.hour >= 21) return { mode: 'terminar', reason: 'Ya es tarde: revisa cómo te fue hoy y descansa para empezar fresco mañana.' };
    if (callable && cal.hour >= PEAK.from && cal.hour < PEAK.to) return { mode: 'llamar', reason: 'Son horas pico: la gente está en casa. Llama para sacar citas.' };
    if (callable && cal.hour >= 9) return { mode: 'llamar', reason: 'No tienes citas por ahora: llama a tus leads para llenar tu agenda.' };
    return { mode: 'prospectar', reason: 'No tienes citas ni llamadas pendientes: sal a sacar leads nuevos.' };
  }

  async function state(actor, { withAppointments = true } = {}) {
    const cal = { ...calendarOf(now(), actor.zone), zone: actor.zone };
    const seller = actor.owner;
    const [plan, log, weekLeads, todayLeads, weekDemos, todayDemos, weekCalls, citas] = await Promise.all([
      plans.findById(seller).lean(), ensureLog(seller, cal.day),
      leadsBetween(seller, cal.weekStart, cal.weekEnd), leadsBetween(seller, cal.todayStart, cal.todayEnd),
      demosBetween(seller, cal.weekStart, cal.weekEnd), demosBetween(seller, cal.todayStart, cal.todayEnd),
      callsBetween(seller, cal.weekKey, dayString(civil(cal.weekKey) + 7 * DAY)),
      withAppointments ? appointmentsToday(actor, cal) : { events: [], busy: 0, agenda: [], calendarConnected: false, notes: [] }
    ]);
    const personal = Number(plan?.weeklyLeadGoal) || null;
    const goal = personal || STANDARD_WEEKLY_LEADS;
    const remaining = Math.max(0, goal - weekLeads);
    const exhibitors = (plan?.exhibitors || []).filter(e => e.day >= cal.day).sort((a, b) => (a.day + a.start).localeCompare(b.day + b.start))
      .map(e => ({ id: String(e._id), day: e.day, start: e.start || '', end: e.end || '', place: text(e.place, 80), address: text(e.address, 200) }));
    const weekEndKey = dayString(civil(cal.weekKey) + 7 * DAY);
    const exhibitorsToday = exhibitors.filter(e => e.day === cal.day);
    const exhibitorsThisWeek = exhibitors.filter(e => e.day < weekEndKey);
    const callsToday = Number(log?.calls?.total) || 0;
    const todayStats = { leads: todayLeads, calls: callsToday, demos: todayDemos.demos, sales: todayDemos.sales, amount: todayDemos.amount };
    const recommendation = recommend({ cal, citas, exhibitorsToday });
    return {
      day: cal.day, time: hhmm(now(), actor.zone), zone: actor.zone, name: firstName(actor.name),
      goal: { standard: STANDARD_WEEKLY_LEADS, personal, weekLeads, remaining, dailyTarget: remaining ? Math.ceil(remaining / cal.workDaysLeft) : 0, workDaysLeft: cal.workDaysLeft },
      today: todayStats, week: { leads: weekLeads, calls: weekCalls, demos: weekDemos.demos, sales: weekDemos.sales, amount: weekDemos.amount },
      citas, exhibitors, exhibitorsToday,
      // Si no hay exhibidor esta semana y el vendedor no dijo por qué, el Coach le pregunta.
      askExhibitor: !exhibitorsThisWeek.length && plan?.exhibitorSkip?.week !== cal.weekKey,
      choices: (log?.choices || []).map(c => c.mode).slice(-10), route: log?.route?.startedAt && !log.route.finishedAt ? { place: log.route.place || '', since: hhmm(log.route.startedAt, actor.zone) } : null,
      recommendation, peak: PEAK, places: PROSPECT_PLACES, callResults: CALL_RESULTS
    };
  }

  // Bloque de cinco personas para llamar, con el motivo de cada una. `ref` no revela a la persona.
  async function callBlock(actor, { more = false } = {}) {
    const cal = calendarOf(now(), actor.zone);
    const log = await ensureLog(actor.owner, cal.day);
    const given = new Set(more ? log.given || [] : []);
    const refOf = key => 'l' + createHash('sha256').update(String(actor.owner) + '\n' + key).digest('hex').slice(0, 12);
    let pool = [], source = 'tec';
    const ctx = await sheetContext(actor).catch(() => null);
    if (ctx) {
      try {
        const ranked = await sheetTools.nextCall(ctx, { today: cal.day, pool: true });
        source = 'sheet';
        pool = ranked.contacts.filter(c => !skipStates.test(fold(c.status))).map(c => ({
          ref: refOf('sheet:' + c.identityKey), name: text(c.fullName, 120), phone: c.phone,
          origin: c.kind === 'tickets' ? 'Boleta' : 'Referido' + (c.host ? ' de ' + text(c.host, 80) : ''), status: text(c.status, 60),
          reason: text(c.reason, 160) || 'Pendiente de llamada.', bucket: c.bucket, rank: 0 }));
      } catch { pool = []; }
    }
    if (!pool.length) pool = await tecLeads(actor, cal, refOf);
    // Preferencias de horario que anotó el telemarketer: de tarde, de mañana o solo fines de semana.
    const weekend = cal.weekday === 0 || cal.weekday === 6;
    for (const person of pool) {
      const s = fold(person.status);
      if (/tardes?/.test(s)) person.rank = cal.hour >= 16 ? -1 : cal.hour < 12 ? 2 : 0;
      if (/mananas?/.test(s)) person.rank = cal.hour < 12 ? -1 : 2;
      if (/fines? de semana/.test(s)) person.rank = weekend ? -1 : 3;
      if (person.rank === -1) person.reason += ' Es buena hora para llamarle.';
      if (person.rank > 0) person.reason += ' Prefiere otro horario; va al final.';
    }
    pool.sort((a, b) => a.rank - b.rank);
    const fresh = pool.filter(p => !given.has(p.ref));
    const block = fresh.slice(0, BLOCK);
    if (block.length) await logs.updateOne({ owner: actor.owner, day: cal.day }, { $addToSet: { given: { $each: block.map(p => p.ref) } } });
    return { source, total: pool.length, left: Math.max(0, fresh.length - block.length),
      people: block.map(({ rank, bucket, ...person }) => person),
      note: source === 'tec' ? 'Tomé los leads que capturaste en TEC. Conecta tu hoja de Google en Mis conexiones para usar la lista del telemarketing.' : '' };
  }

  async function tecLeads(actor, cal, refOf) {
    const { leads, tickets } = src();
    const query = { generatedByUserId: actor.owner, status: { $in: ['nuevo', 'contactado'] }, phone: { $nin: [null, ''] } };
    const fields = 'fullName phone status nextActionAt createdAt source';
    const [a, b] = await Promise.all([leads ? leads.find(query).select(fields).sort({ createdAt: -1 }).limit(300).lean() : [],
      tickets ? tickets.find(query).select(fields).sort({ createdAt: -1 }).limit(300).lean() : []]);
    const end = cal.todayEnd;
    return [...a, ...b].map(lead => {
      const digits = String(lead.phone || '').replace(/\D/g, '');
      const phone = digits.length === 10 ? '+1' + digits : digits.length === 11 && digits.startsWith('1') ? '+' + digits : '';
      const due = lead.nextActionAt && new Date(lead.nextActionAt).getTime() <= end;
      const reason = due ? (new Date(lead.nextActionAt).getTime() < cal.todayStart ? 'Tenía una llamada pendiente que ya se pasó.' : 'Tiene llamada pendiente para hoy.')
        : lead.status === 'nuevo' ? 'Lead nuevo sin contactar.' : 'Ya hubo contacto; falta sacar la cita.';
      return { ref: refOf('tec:' + lead._id), name: text(lead.fullName, 120), phone, origin: lead.source === 'rifa_digital' ? 'Boleta' : lead.source === 'programa_4_en_14' ? 'Referido del 4 en 14' : 'Lead de TEC',
        status: '', reason, rank: 0, order: due ? 0 : lead.status === 'nuevo' ? 1 : 2, created: new Date(lead.createdAt || 0).getTime() };
    }).filter(p => p.phone && p.name).sort((x, y) => x.order - y.order || y.created - x.created).map(({ order, created, ...p }) => p);
  }

  const routeKeys = new Set(PROSPECT_PLACES.map(p => p.id).concat(['exhibidor', 'otro']));
  return {
    models,
    state,
    callBlock,
    async choose(actor, mode) {
      if (!MODES.includes(mode)) throw fail(422, 'Elige citas, llamar, prospectar o terminar.');
      const cal = calendarOf(now(), actor.zone);
      await ensureLog(actor.owner, cal.day);
      await logs.updateOne({ owner: actor.owner, day: cal.day }, { $push: { choices: { $each: [{ mode, at: new Date(now()) }], $slice: -40 } } });
      return { mode };
    },
    async callResult(actor, { ref, result }) {
      if (typeof ref !== 'string' || !/^l[a-f0-9]{12}$/.test(ref) || !Object.hasOwn(CALL_RESULTS, result)) throw fail(422, 'Elige cómo te fue con la llamada.');
      const cal = calendarOf(now(), actor.zone);
      const counted = await logs.updateOne({ owner: actor.owner, day: cal.day, given: ref, ['calls.refs.' + ref]: { $exists: false } },
        { $inc: { 'calls.total': 1, ['calls.' + result]: 1 }, $set: { ['calls.refs.' + ref]: result } });
      // Corregir el resultado de la misma persona no suma otra llamada: solo cambia de tipo.
      if (!counted.matchedCount) {
        const before = (await logs.findOne({ owner: actor.owner, day: cal.day, given: ref }).select('calls').lean())?.calls?.refs?.[ref];
        if (before && before !== result) await logs.updateOne({ owner: actor.owner, day: cal.day, given: ref, ['calls.refs.' + ref]: before },
          { $inc: { ['calls.' + before]: -1, ['calls.' + result]: 1 }, $set: { ['calls.refs.' + ref]: result } });
      }
      const log = await logs.findOne({ owner: actor.owner, day: cal.day }).select('calls').lean();
      return { calls: Number(log?.calls?.total) || 0 };
    },
    async setGoal(actor, leads) {
      const value = Number(leads);
      if (!Number.isInteger(value) || value < 0 || value > 1000) throw fail(422, 'Escribe tu meta de leads por semana, de 1 a 1000, o 0 para usar la estándar.');
      await plans.updateOne({ _id: actor.owner }, value ? { $set: { weeklyLeadGoal: value, updatedAt: new Date(now()) } } : { $unset: { weeklyLeadGoal: 1 }, $set: { updatedAt: new Date(now()) } }, { upsert: true });
      return { personal: value || null };
    },
    async addExhibitor(actor, input) {
      const entry = { day: input?.day, start: input?.start || '', end: input?.end || '', place: text(input?.place, 80), address: text(input?.address, 200) };
      const today = calendarOf(now(), actor.zone).day;
      if (!validDay(entry.day) || entry.day < today || civil(entry.day) - civil(today) > 120 * DAY) throw fail(422, 'Elige el día del exhibidor, de hoy a cuatro meses.');
      if ((entry.start && !validTime(entry.start)) || (entry.end && !validTime(entry.end)) || (entry.start && entry.end && entry.end <= entry.start)) throw fail(422, 'Revisa la hora de inicio y de fin.');
      if (!entry.place || !entry.address) throw fail(422, 'Escribe el nombre del lugar y su dirección.');
      const plan = await plans.findById(actor.owner).select('exhibitors').lean();
      if ((plan?.exhibitors || []).filter(e => e.day >= today).length >= 60) throw fail(422, 'Ya tienes 60 exhibidores próximos. Borra alguno antes de agregar otro.');
      await plans.updateOne({ _id: actor.owner }, { $push: { exhibitors: entry }, $set: { updatedAt: new Date(now()) } }, { upsert: true });
      // Los exhibidores que ya pasaron se borran para que la lista no crezca.
      await plans.updateOne({ _id: actor.owner }, { $pull: { exhibitors: { day: { $lt: today } } } });
      return { added: true };
    },
    async removeExhibitor(actor, id) {
      if (typeof id !== 'string' || !/^[a-f0-9]{24}$/.test(id)) throw fail(422, 'Elige el exhibidor que quieres borrar.');
      await plans.updateOne({ _id: actor.owner }, { $pull: { exhibitors: { _id: new mongoose.Types.ObjectId(id) } }, $set: { updatedAt: new Date(now()) } });
      return { removed: true };
    },
    async skipExhibitor(actor, reason) {
      const value = text(reason, 160);
      if (!value) throw fail(422, 'Cuéntame por qué no hay exhibidor esta semana.');
      const cal = calendarOf(now(), actor.zone);
      await plans.updateOne({ _id: actor.owner }, { $set: { exhibitorSkip: { week: cal.weekKey, reason: value }, updatedAt: new Date(now()) } }, { upsert: true });
      return { saved: true };
    },
    async route(actor, { action, place = '', places = 0, note = '' }) {
      const cal = calendarOf(now(), actor.zone);
      await ensureLog(actor.owner, cal.day);
      if (action === 'start') {
        if (!routeKeys.has(place)) throw fail(422, 'Elige dónde vas a prospectar.');
        await logs.updateOne({ owner: actor.owner, day: cal.day }, { $set: { route: { place, startedAt: new Date(now()) } } });
        return { started: true };
      }
      if (action !== 'finish') throw fail(422, 'Revisa la acción de la ruta.');
      const count = Number(places);
      if (!Number.isInteger(count) || count < 0 || count > 100) throw fail(422, 'Escribe cuántos lugares visitaste.');
      const log = await logs.findOne({ owner: actor.owner, day: cal.day }).select('route').lean();
      const since = log?.route?.startedAt ? new Date(log.route.startedAt).getTime() : cal.todayStart;
      const leads = await leadsBetween(actor.owner, since, now() + 1);
      await logs.updateOne({ owner: actor.owner, day: cal.day }, { $set: { 'route.finishedAt': new Date(now()), 'route.places': count, 'route.leads': leads, 'route.note': text(note, 300) } });
      return { places: count, leads };
    },
    // Texto de los avisos de 8 am, 1 pm y 9 pm.
    async pushMessage({ owner, user, zone, slot }) {
      const actor = { owner, email: user?.email, name: String(user?.name || ''), zone: validZone(zone) || 'America/Chicago', session: 'push' };
      const s = await state(actor, { withAppointments: slot === 8 });
      return dayMessage(s, slot);
    }
  };
}

export function dayMessage(s, slot) {
  const hi = s.name ? ', ' + s.name : '';
  const goal = s.goal.personal || s.goal.standard;
  const week = 'Llevas ' + s.goal.weekLeads + ' de ' + goal + ' leads esta semana';
  if (slot === 8) {
    const citas = s.citas.events.length + s.citas.agenda.length;
    const plan = citas ? 'Hoy tienes ' + plural(citas, 'cita', 'citas') + '.' : s.exhibitorsToday.length ? 'Hoy tienes exhibidor en ' + s.exhibitorsToday[0].place + '.' : 'Hoy no tienes citas agendadas.';
    const today = s.goal.remaining ? ' Meta de hoy: ' + plural(s.goal.dailyTarget, 'lead', 'leads') + '.' : ' Ya cumpliste la meta de la semana.';
    return { title: 'Buenos días' + hi, body: plan + ' ' + week + '.' + today + ' ¿Vas a atender citas, llamar o prospectar?' };
  }
  if (slot === 13) {
    return { title: 'El día ya empezó' + hi, body: 'Hoy llevas ' + plural(s.today.leads, 'lead', 'leads') + ' y ' + plural(s.today.calls, 'llamada', 'llamadas') + '. Busca dónde prospectar o llama para sacar citas: de 5 a 8 de la noche la gente está en casa y son horas pico para tus demos.' };
  }
  const reached = s.goal.remaining === 0;
  return { title: 'Así fue tu día' + hi, body: 'Hoy: ' + plural(s.today.leads, 'lead', 'leads') + ', ' + plural(s.today.calls, 'llamada', 'llamadas') + ', ' + plural(s.today.demos, 'demo', 'demos') + ' y ' + plural(s.today.sales, 'venta', 'ventas') + '. ' + week +
    (reached ? ': ¡llegaste a la meta!' : ': te faltan ' + s.goal.remaining + '.') + ' Mañana empiezas fresco.' };
}
