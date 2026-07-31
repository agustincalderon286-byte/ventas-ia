export function registerMetalworksCrmLeadRoutes(app, dependencies) {
  const {
    requireAuth,
    cleanText,
    normalizePhone,
    normalizeEmail,
    normalizeStatus,
    normalizeLeadSourceGroup,
    getSourceGroupOverrideFields,
    MetalworksLead,
    appendActivity,
    labelLeadSourceGroup,
    MetalworksLeadActivity,
    listLeadAssets,
    cleanLead,
    cleanActivity,
    respondError,
  } = dependencies;

  app.post("/api/metalworks-crm/leads", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) return;

    const fullName = cleanText(req.body?.fullName || "", 120);
    const phoneDisplay = cleanText(req.body?.phoneDisplay || "", 40);
    const phone = normalizePhone(phoneDisplay);
    const email = normalizeEmail(req.body?.email || "");
    const projectType = cleanText(req.body?.projectType || "", 120);
    const location = cleanText(req.body?.location || "", 160);
    const details = cleanText(req.body?.details || "", 3000);
    const status = normalizeStatus(req.body?.status || "new");
    const sourceGroup = normalizeLeadSourceGroup(req.body?.sourceGroup || "") || "manual";
    const sourceFields = getSourceGroupOverrideFields(sourceGroup) || getSourceGroupOverrideFields("manual");

    if (!fullName) return respondError(res, 400, "El nombre es requerido.");

    try {
      const now = new Date();
      const leadDoc = await MetalworksLead.create({
        fullName,
        phone,
        phoneDisplay,
        email,
        projectType,
        location,
        details,
        status,
        sourceType: sourceFields.sourceType,
        sourceExternalSystem: sourceFields.sourceExternalSystem,
        tracking: sourceFields.tracking,
        lastContactAt: now,
        updatedAt: now,
        createdAt: now,
      });

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "lead_created",
        title: "Lead creado manualmente",
        body: `${fullName} se agrego manualmente al CRM.`,
        meta: {
          adminEmail: auth.email,
          sourceGroup,
          sourceLabel: labelLeadSourceGroup(sourceGroup),
          sourceType: sourceFields.sourceType,
          projectType,
          location,
        },
        req,
      });

      const [activityDocs, assets] = await Promise.all([
        MetalworksLeadActivity.find({ leadId: leadDoc._id })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
        listLeadAssets(leadDoc._id),
      ]);

      res.status(201).json({
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc, { includeConversation: true }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error creating manual Metal Works lead:", error.message);
      respondError(res, 500, "No pude crear ese lead manual.");
    }
  });
}
