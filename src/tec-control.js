// Read-only TEC administration. No workspace switching, account edits or billing mutations.
export const actorExpression = { $ifNull: ["$generatedByUserId", "$ownerUserId"] };
export const userActivityQuery = id => ({ $or: [{ generatedByUserId: id }, { generatedByUserId: null, ownerUserId: id }] });
export function controlPeriod(value, now = new Date()) {
  if (value === "all") return { days: "all", from: new Date("2000-01-01T00:00:00Z"), to: now };
  const days = [7, 30, 90].includes(Number(value)) ? Number(value) : 30;
  return { days, from: new Date(now.getTime() - days * 86400000), to: now };
}
export function membershipKind(user, { isTest = () => false, isAdmin = () => false, monthlyPrice = "", annualPrice = "" } = {}) {
  if (isAdmin(user)) return "admin";
  if (isTest(user.email)) return "test";
  if (user.accountType === "seat" || user.subscriptionStatus === "team_access") return "team";
  if (user.stripeSubscriptionId && user.subscriptionStatus === "active") {
    return user.stripePriceId === annualPrice && annualPrice ? "annual"
      : user.stripePriceId === monthlyPrice && monthlyPrice ? "monthly" : "paid_other";
  }
  return user.subscriptionStatus === "trialing" ? "trial" : "inactive";
}
const countStages = (from, match = {}, groupId = actorExpression) => [
  { $match: { createdAt: { $gte: from }, ...match } },
  { $group: { _id: groupId, total: { $sum: 1 }, lastAt: { $max: "$createdAt" } } }
];
const numeric = value => Number.isFinite(Number(value)) ? Number(value) : 0;
const latest = values => values.filter(Boolean).sort((a, b) => new Date(b) - new Date(a))[0] || null;
export async function readTecInvoices(stripe, customerIds, period) {
  if (!stripe) return { available: false, reason: "Stripe no configurado." };
  const eligible = new Set(customerIds.filter(Boolean));
  // No matching paying accounts: avoid reading unrelated businesses' invoices.
  if (!eligible.size) return { available: true, grossUsd: 0, invoices: 0, incomplete: false };
  let gross = 0, invoices = 0, cursor, hasMore = false;
  try {
    for (let page = 0; page < 3; page++) {
      // Fetch by creation date and then check paid_at. Older-created invoices paid in
      // the period are not covered: expose that basis instead of claiming net revenue.
      const batch = await stripe.invoices.list({ status: "paid", limit: 100,
        created: { gte: Math.floor(period.from.getTime() / 1000), lte: Math.floor(period.to.getTime() / 1000) },
        ...(cursor ? { starting_after: cursor } : {}) }, { timeout: 5000, maxNetworkRetries: 0 });
      for (const row of batch.data) {
        const customer = typeof row.customer === "string" ? row.customer : row.customer?.id;
        if (eligible.has(customer) && row.currency === "usd" && row.livemode === true && row.paid_out_of_band !== true && numeric(row.amount_paid) > 0) {
          gross += numeric(row.amount_paid); invoices++;
        }
      }
      hasMore = Boolean(batch.has_more);
      if (!hasMore || !batch.data.length) break;
      cursor = batch.data.at(-1).id;
    }
    return { available: true, grossUsd: gross / 100, invoices, incomplete: hasMore,
      basis: "Facturas USD reales creadas en el período y pagadas; cobro bruto, antes de reembolsos y comisiones. No incluye facturas antiguas pagadas ahora." };
  } catch { return { available: false, reason: "No se pudo consultar Stripe. No se interpreta como cero ingresos." }; }
}

export function registerTecControlRoutes(app, deps) {
  const { models: m, requireAdmin, isTest, isAdmin, monthlyPrice, annualPrice, cleanAnalysis, mongoose, stripe } = deps;
  const options = { isTest, isAdmin, monthlyPrice, annualPrice };
  let billingCache = null;
  const protectedGet = (path, handler) => app.get(path, async (req, res) => {
    res.set("Cache-Control", "private, no-store");
    const auth = await requireAdmin(req, res);
    if (!auth) return;
    try { await handler(req, res, auth); }
    catch (error) { console.error("TEC control:", error.message); res.status(503).json({ error: "No pude cargar estos datos. Intenta nuevamente." }); }
  });
  protectedGet("/api/control/tec/overview", async (req, res) => {
    const period = controlPeriod(req.query.days);
    const page = Math.max(1, Math.min(10000, Number.parseInt(req.query.page, 10) || 1));
    const search = String(req.query.search || "").trim().slice(0, 100);
    const regex = search ? new RegExp(search.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i") : null;
    let filter = regex ? { $or: [{ name: regex }, { email: regex }] } : {};
    if (req.query.team) {
      if (!mongoose.Types.ObjectId.isValid(req.query.team)) return res.status(400).json({ error: "Equipo no válido." });
      const teamId = new mongoose.Types.ObjectId(req.query.team);
      const teamFilter = { $or: [{ _id: teamId }, { teamOwnerUserId: teamId }, { sponsorUserId: teamId }] };
      filter = regex ? { $and: [filter, teamFilter] } : teamFilter;
    }
    const appointmentActor = { $ifNull: ["$appointmentRepUserId", actorExpression] };
    const [users, total, allUsers, profiles, leads, programs, surveys, demos, analyses, visits, appointments] = await Promise.all([
      m.users.find(filter).select("name email accountType teamOwnerUserId sponsorUserId subscriptionStatus subscriptionCancelAtPeriodEnd stripeSubscriptionId stripePriceId lastLoginAt createdAt").sort({ name: 1, _id: 1 }).skip((page - 1) * 30).limit(30).lean(),
      m.users.countDocuments(filter),
      m.users.find({}).select("name email accountType teamOwnerUserId sponsorUserId subscriptionStatus stripeSubscriptionId stripeCustomerId stripePriceId subscriptionCancelAtPeriodEnd").lean(),
      m.profiles.find({}).select("userId lastInteractionAt").lean(),
      m.leads.aggregate(countStages(period.from)),
      m.programs.aggregate(countStages(period.from, { programType: "4_en_14" })),
      m.surveys.aggregate(countStages(period.from)),
      m.outcomes.aggregate([{ $match: { createdAt: { $gte: period.from } } }, { $group: { _id: actorExpression, total: { $sum: 1 },
        sales: { $sum: { $cond: [{ $eq: ["$resultType", "venta"] }, 1, 0] } },
        amount: { $sum: { $cond: [{ $eq: ["$resultType", "venta"] }, "$saleAmount", 0] } }, lastAt: { $max: "$createdAt" } } }]),
      m.analyses.aggregate(countStages(period.from, { status: "completed" }, "$ownerUserId")),
      m.visits ? m.visits.aggregate(countStages(period.from, {}, "$ownerUserId")) : [],
      m.crm ? m.crm.aggregate([{ $match: { appointmentAt: { $gte: period.from, $lte: period.to } } },
        { $group: { _id: appointmentActor, total: { $sum: 1 } } }]) : []
    ]);
    const byUser = new Map(allUsers.map(user => [String(user._id), user]));
    const profileMap = new Map(profiles.map(profile => [String(profile.userId), profile]));
    const activityMaps = [leads, programs, surveys, demos, analyses, visits, appointments].map(rows => new Map(rows.map(row => [String(row._id), row])));
    const sums = rows => rows.reduce((sum, row) => sum + numeric(row.total), 0);
    const membership = { monthly: 0, annual: 0, paid_other: 0, test: 0, admin: 0, team: 0, trial: 0, inactive: 0, cancelScheduled: 0 };
    const customers = [];
    for (const user of allUsers) {
      const kind = membershipKind(user, options); membership[kind]++;
      if (!["test", "admin", "team"].includes(kind) && user.stripeCustomerId) customers.push(user.stripeCustomerId);
      if (user.stripeSubscriptionId && user.subscriptionCancelAtPeriodEnd && !["test", "admin", "team"].includes(kind)) membership.cancelScheduled++;
    }
    const cacheKey = `${period.days}:${[...customers].sort().join(",")}`;
    if (!billingCache || billingCache.key !== cacheKey || Date.now() - billingCache.at > 300000) {
      const promise = readTecInvoices(stripe, customers, period);
      billingCache = { key: cacheKey, at: Date.now(), promise };
    }
    const billing = await billingCache.promise;
    const sales = demos.reduce((sum, row) => sum + numeric(row.sales), 0);
    res.json({ updatedAt: new Date(), period, page, pageSize: 30, total, membership, billing,
      teams: allUsers.filter(user => user.accountType !== "seat").map(user => ({ id: String(user._id), name: user.name || user.email })),
      summary: { users: allUsers.length, leads: sums(leads), programs: sums(programs), surveys: sums(surveys),
        demos: sums(demos), analyses: sums(analyses), visits: sums(visits), appointments: sums(appointments), sales, soldAmount: demos.reduce((sum, row) => sum + numeric(row.amount), 0),
        closeRate: sums(demos) ? Math.round(sales / sums(demos) * 1000) / 10 : null },
      users: users.map(user => {
        const id = String(user._id);
        const teamId = String(user.teamOwnerUserId || user.sponsorUserId || "");
        const activity = activityMaps.map(map => map.get(id) || {});
        return { id, name: user.name, email: user.email, accountType: user.accountType, membership: membershipKind(user, options),
          uplineName: teamId && teamId !== id ? byUser.get(teamId)?.name || "No disponible" : "Cuenta principal",
          lastLoginAt: user.lastLoginAt || null,
          lastActivityAt: latest([profileMap.get(id)?.lastInteractionAt, ...activity.map(row => row.lastAt)]),
          leads: numeric(activity[0].total), programs: numeric(activity[1].total), surveys: numeric(activity[2].total),
          demos: numeric(activity[3].total), sales: numeric(activity[3].sales), soldAmount: numeric(activity[3].amount), analyses: numeric(activity[4].total),
          visits: numeric(activity[5].total), appointments: numeric(activity[6].total) };
      }) });
  });
  protectedGet("/api/control/tec/users/:id/activity", async (req, res) => {
    if (!mongoose.Types.ObjectId.isValid(req.params.id)) return res.status(404).json({ error: "Cuenta no encontrada." });
    const id = new mongoose.Types.ObjectId(req.params.id);
    const user = await m.users.findById(id).select("name email accountType teamOwnerUserId").lean();
    if (!user) return res.status(404).json({ error: "Cuenta no encontrada." });
    const { from } = controlPeriod(req.query.days);
    const match = { ...userActivityQuery(id), createdAt: { $gte: from } };
    const query = (model, filter, fields) => model.find(filter).select(fields).sort({ createdAt: -1 }).limit(30).lean();
    const [leads, programs, surveys, outcomes, analyses, visits, appointments] = await Promise.all([
      query(m.leads, match, "fullName phone email status nextAction nextActionAt createdAt"),
      query(m.programs, { ...match, programType: "4_en_14" }, "hostName programType referrals startDate startWindow deadlineDate rewardUnlockedAt rewardDeliveredAt createdAt"),
      query(m.surveys, match, "fullName name leadName createdAt"),
      query(m.outcomes, match, "resultType saleAmount summary products createdAt"),
      query(m.analyses, { ownerUserId: id, createdAt: { $gte: from } }, "ownerUserId ownerName status report durationSeconds createdAt"),
      m.visits ? query(m.visits, { ownerUserId: id, createdAt: { $gte: from } }, "property status notes nextVisitAt createdAt") : [],
      m.crm ? query(m.crm, { appointmentAt: { $gte: from, $lte: new Date() }, $or: [{ appointmentRepUserId: id },
        { appointmentRepUserId: null, ...userActivityQuery(id) }] }, "leadName phone address city appointmentAt status createdAt") : []
    ]);
    res.json({ user: { id: String(id), name: user.name, email: user.email }, leads, programs, surveys, outcomes, visits, appointments,
      analyses: analyses.map(doc => cleanAnalysis(doc)), limitPerSection: 30,
      note: "Hasta 30 registros recientes por sección del período seleccionado; no representa todo el historial." });
  });
  protectedGet("/api/control/tec/analyses/:id", async (req, res) => {
    if (!mongoose.Types.ObjectId.isValid(req.params.id)) return res.status(404).json({ error: "Análisis no encontrado." });
    const doc = await m.analyses.findById(req.params.id).lean();
    if (!doc) return res.status(404).json({ error: "Análisis no encontrado." });
    res.json({ analysis: cleanAnalysis(doc, { includeDetail: true }) });
  });
}
