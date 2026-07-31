export function registerMetalworksCrmAgendaRoutes(app, dependencies) {
  const {
    requireAuth,
    MetalworksAgendaEvent,
    cleanAgendaEvent,
    cleanText,
    normalizeAgendaEventType,
    getMetalworksCrmProfile,
    parseCrmDatetimeInput,
    mongoose,
    respondError,
  } = dependencies;

  app.get("/api/metalworks-crm/agenda-events", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) return;

    try {
      const docs = await MetalworksAgendaEvent.find({
        status: { $ne: "canceled" },
        startsAt: { $gte: new Date(Date.now() - 14 * 24 * 60 * 60 * 1000) },
      })
        .sort({ startsAt: 1, updatedAt: -1 })
        .limit(160)
        .lean();

      res.json({ ok: true, agendaEvents: docs.map(cleanAgendaEvent).filter(Boolean) });
    } catch (error) {
      console.error("Error loading Metal Works agenda events:", error.message);
      respondError(res, 500, "No pude cargar los eventos de agenda.");
    }
  });

  app.post("/api/metalworks-crm/agenda-events", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) return;

    const title = cleanText(req.body?.title || "", 120);
    const eventType = normalizeAgendaEventType(req.body?.eventType || "internal");
    const owner = cleanText(req.body?.owner || getMetalworksCrmProfile(auth.email)?.displayName || "", 80);
    const notes = cleanText(req.body?.notes || "", 1000);
    const startsAt = parseCrmDatetimeInput(req.body?.startsAt || "");
    const endsAt = req.body?.endsAt ? parseCrmDatetimeInput(req.body.endsAt) : null;
    const relatedLeadId = String(req.body?.relatedLeadId || "").trim();

    if (!title) return respondError(res, 400, "El titulo del evento es requerido.");
    if (!(startsAt instanceof Date) || Number.isNaN(startsAt.getTime())) {
      return respondError(res, 400, "La fecha/hora del evento es requerida.");
    }
    if (endsAt && endsAt.getTime() <= startsAt.getTime()) {
      return respondError(res, 400, "La hora de fin debe ser despues de la hora de inicio.");
    }
    if (relatedLeadId && !mongoose.Types.ObjectId.isValid(relatedLeadId)) {
      return respondError(res, 400, "Lead relacionado invalido.");
    }

    try {
      const now = new Date();
      const doc = await MetalworksAgendaEvent.create({
        title,
        eventType,
        owner,
        notes,
        startsAt,
        endsAt,
        status: "scheduled",
        source: cleanText(req.body?.source || "manual", 80),
        relatedLeadId: relatedLeadId || null,
        createdByEmail: auth.email,
        updatedByEmail: auth.email,
        updatedAt: now,
        createdAt: now,
      });

      res.status(201).json({
        ok: true,
        agendaEvent: cleanAgendaEvent(doc.toObject ? doc.toObject() : doc),
      });
    } catch (error) {
      console.error("Error creating Metal Works agenda event:", error.message);
      respondError(res, 500, "No pude crear el evento de agenda.");
    }
  });
}
