export function registerMetalworksCrmLeadOperationRoutes(app, dependencies) {
  const {
    METALWORKS_CALLBACK_TIME_ZONE,
    METALWORKS_ASSISTANT_PLACEHOLDER_NAME,
    METALWORKS_WEBSITE_CHAT_SOURCE_TYPE,
    METALWORKS_LEAD_ASSET_MAX_FILES,
    METALWORKS_LEAD_ASSET_MAX_TOTAL_BYTES,
    requireAuth,
    mongoose,
    respondError,
    MetalworksLead,
    MetalworksLeadActivity,
    MetalworksLeadAsset,
    cleanText,
    cleanMultilineText,
    normalizeEmail,
    normalizePhone,
    normalizeStatus,
    normalizeAppointmentType,
    normalizeAppointmentStatus,
    normalizeAppointmentDurationMinutes,
    inferLeadAppointmentType,
    inferLeadAppointmentStatus,
    inferAppointmentDurationMinutes,
    normalizeLeadSourceGroup,
    labelLeadSourceGroup,
    getSourceGroupOverrideFields,
    normalizeClientDocumentType,
    normalizeMoney,
    parseDateOnly,
    syncAndSaveLeadGoogleCalendarEvent,
    parseCrmDatetimeInput,
    formatMoneyLabel,
    formatDateTimeLabel,
    buildMetalworksClientDocumentSnapshot,
    sendMetalworksEstimateEmail,
    labelStatus,
    labelAppointmentType,
    labelAppointmentStatus,
    mergeLeadTextImportIntoPrivateNotes,
    normalizeLeadReminderOffsets,
    formatLeadReminderOffsetLabel,
    resolveBookedReminderOffsets,
    pruneLeadReminderSentKeys,
    mergeAssistantUniqueValues,
    mergeConversationHistory,
    sanitizeAssistantStoredName,
    getLeadSourceGroup,
    isWebsiteLiveChatLead,
    sanitizeLeadAssetFileName,
    normalizeLeadAssetMimeType,
    parseAssistantLeadAssetUpload,
    cleanLead,
    cleanActivity,
    appendActivity,
    listLeadAssets,
    sendWebsiteLiveChatReplyPushAlert,
  } = dependencies;

  app.get("/api/metalworks-crm/leads/:leadId", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    try {
      const [leadDoc, activityDocs] = await Promise.all([
        MetalworksLead.findById(leadId).lean(),
        MetalworksLeadActivity.find({ leadId })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
      ]);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      const assets = await listLeadAssets(leadId);

      res.json({
        lead: cleanLead(leadDoc, { includeConversation: true }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error loading Metal Works lead detail:", error.message);
      respondError(res, 500, "No pude cargar ese lead.");
    }
  });

  app.patch("/api/metalworks-crm/leads/:leadId", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    try {
      const leadDoc = await MetalworksLead.findById(leadId);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      const changes = [];
      const fullName = Object.prototype.hasOwnProperty.call(req.body || {}, "fullName")
        ? cleanText(req.body?.fullName || "", 120)
        : null;
      const phoneDisplayRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "phoneDisplay")
        ? cleanText(req.body?.phoneDisplay || "", 40)
        : null;
      const phone = phoneDisplayRaw !== null ? normalizePhone(phoneDisplayRaw) : null;
      const email = Object.prototype.hasOwnProperty.call(req.body || {}, "email")
        ? normalizeEmail(req.body?.email || "")
        : null;
      const projectType = Object.prototype.hasOwnProperty.call(req.body || {}, "projectType")
        ? cleanText(req.body?.projectType || "", 120)
        : null;
      const location = Object.prototype.hasOwnProperty.call(req.body || {}, "location")
        ? cleanText(req.body?.location || "", 160)
        : null;
      const sourceGroupRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "sourceGroup")
        ? cleanText(req.body?.sourceGroup || "", 40)
        : null;
      const sourceGroup = sourceGroupRaw ? normalizeLeadSourceGroup(sourceGroupRaw) : null;
      const details = Object.prototype.hasOwnProperty.call(req.body || {}, "details")
        ? cleanText(req.body?.details || "", 3000)
        : null;
      const bestContactDay = Object.prototype.hasOwnProperty.call(req.body || {}, "bestContactDay")
        ? cleanText(req.body?.bestContactDay || "", 80)
        : null;
      const bestContactTime = Object.prototype.hasOwnProperty.call(req.body || {}, "bestContactTime")
        ? cleanText(req.body?.bestContactTime || "", 80)
        : null;
      const nextStatus = Object.prototype.hasOwnProperty.call(req.body || {}, "status")
        ? normalizeStatus(req.body?.status || "new")
        : null;
      const nextAction = Object.prototype.hasOwnProperty.call(req.body || {}, "nextAction")
        ? cleanText(req.body?.nextAction || "", 160)
        : null;
      const nextActionAtRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "nextActionAt")
        ? String(req.body?.nextActionAt || "").trim()
        : null;
      const nextActionAt = nextActionAtRaw
        ? parseCrmDatetimeInput(nextActionAtRaw)
        : nextActionAtRaw === ""
          ? null
          : undefined;
      const appointmentType = Object.prototype.hasOwnProperty.call(req.body || {}, "appointmentType")
        ? normalizeAppointmentType(req.body?.appointmentType || "")
        : null;
      const appointmentStatus = Object.prototype.hasOwnProperty.call(
        req.body || {},
        "appointmentStatus",
      )
        ? normalizeAppointmentStatus(req.body?.appointmentStatus || "")
        : null;
      const appointmentDurationMinutes = Object.prototype.hasOwnProperty.call(
        req.body || {},
        "appointmentDurationMinutes",
      )
        ? normalizeAppointmentDurationMinutes(req.body?.appointmentDurationMinutes || 0)
        : null;
      const appointmentAssignedTo = Object.prototype.hasOwnProperty.call(
        req.body || {},
        "appointmentAssignedTo",
      )
        ? cleanText(req.body?.appointmentAssignedTo || "", 80)
        : null;
      const nextActionReminderOffsetsProvided = Object.prototype.hasOwnProperty.call(
        req.body || {},
        "nextActionReminderOffsets",
      );
      const nextActionReminderOffsets = nextActionReminderOffsetsProvided
        ? normalizeLeadReminderOffsets(req.body?.nextActionReminderOffsets || [])
        : null;
      const privateNotes = Object.prototype.hasOwnProperty.call(req.body || {}, "privateNotes")
        ? cleanMultilineText(req.body?.privateNotes || "", 12000)
        : null;
      const textThreadImportSource = Object.prototype.hasOwnProperty.call(
        req.body || {},
        "textThreadImportSource",
      )
        ? cleanText(req.body?.textThreadImportSource || "", 60)
        : "";
      const textThreadImport = Object.prototype.hasOwnProperty.call(req.body || {}, "textThreadImport")
        ? cleanMultilineText(req.body?.textThreadImport || "", 8000)
        : null;
      const estimateAmount = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateAmount")
        ? normalizeMoney(req.body?.estimateAmount || 0)
        : null;
      const invoiceDepositAmount = Object.prototype.hasOwnProperty.call(req.body || {}, "invoiceDepositAmount")
        ? normalizeMoney(req.body?.invoiceDepositAmount || 0)
        : null;
      const estimateTitle = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateTitle")
        ? cleanText(req.body?.estimateTitle || "", 160)
        : null;
      const estimateScope = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateScope")
        ? cleanText(req.body?.estimateScope || "", 2400)
        : null;
      const estimateMaterialsCost = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateMaterialsCost")
        ? normalizeMoney(req.body?.estimateMaterialsCost || 0)
        : null;
      const estimateLaborCost = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateLaborCost")
        ? normalizeMoney(req.body?.estimateLaborCost || 0)
        : null;
      const estimateCoatingCost = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateCoatingCost")
        ? normalizeMoney(req.body?.estimateCoatingCost || 0)
        : null;
      const estimateMiscCost = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateMiscCost")
        ? normalizeMoney(req.body?.estimateMiscCost || 0)
        : null;
      const estimateDiscount = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateDiscount")
        ? normalizeMoney(req.body?.estimateDiscount || 0)
        : null;
      const estimateValidUntilRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateValidUntil")
        ? String(req.body?.estimateValidUntil || "").trim()
        : null;
      const estimateValidUntil = estimateValidUntilRaw === null
        ? null
        : estimateValidUntilRaw
          ? parseDateOnly(estimateValidUntilRaw)
          : null;
      const estimateNotes = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateNotes")
        ? cleanText(req.body?.estimateNotes || "", 2400)
        : null;
      const clientDocumentType = Object.prototype.hasOwnProperty.call(req.body || {}, "clientDocumentType")
        ? normalizeClientDocumentType(req.body?.clientDocumentType || "estimate")
        : null;
      const clientDocumentDescription = Object.prototype.hasOwnProperty.call(req.body || {}, "clientDocumentDescription")
        ? cleanText(req.body?.clientDocumentDescription || "", 3200)
        : null;
      const clientDocumentWorkDateRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "clientDocumentWorkDate")
        ? String(req.body?.clientDocumentWorkDate || "").trim()
        : null;
      const clientDocumentWorkDate = clientDocumentWorkDateRaw === null
        ? null
        : clientDocumentWorkDateRaw
          ? parseDateOnly(clientDocumentWorkDateRaw)
          : null;
      const clientDocumentWarranty = Object.prototype.hasOwnProperty.call(req.body || {}, "clientDocumentWarranty")
        ? cleanText(req.body?.clientDocumentWarranty || "", 2400)
        : null;
      const note = cleanText(req.body?.note || "", 600);
      let estimateChanged = false;
      let estimateMoneyChanged = false;
      let clientDocumentChanged = false;
      let invoiceDepositChanged = false;
      let profileChanged = false;

      if (fullName !== null) {
        const nextFullName =
          fullName || sanitizeAssistantStoredName(leadDoc.fullName || "") || METALWORKS_ASSISTANT_PLACEHOLDER_NAME;

        if (leadDoc.fullName !== nextFullName) {
          leadDoc.fullName = nextFullName;
          profileChanged = true;
        }
      }

      if (phoneDisplayRaw !== null) {
        const nextPhoneDisplay = phoneDisplayRaw || "";

        if (leadDoc.phone !== phone || leadDoc.phoneDisplay !== nextPhoneDisplay) {
          leadDoc.phone = phone || "";
          leadDoc.phoneDisplay = nextPhoneDisplay;
          profileChanged = true;
        }
      }

      if (email !== null && leadDoc.email !== email) {
        leadDoc.email = email;
        profileChanged = true;
      }

      if (projectType !== null && leadDoc.projectType !== projectType) {
        leadDoc.projectType = projectType;
        profileChanged = true;
      }

      if (location !== null && leadDoc.location !== location) {
        leadDoc.location = location;
        profileChanged = true;
      }

      if (sourceGroupRaw && !sourceGroup) {
        return respondError(res, 400, "Fuente invalida.");
      }

      if (sourceGroup) {
        const currentLeadSnapshot = leadDoc.toObject ? leadDoc.toObject() : leadDoc;
        const currentSourceGroup = getLeadSourceGroup(currentLeadSnapshot);

        if (currentSourceGroup !== sourceGroup) {
          const sourceFields = getSourceGroupOverrideFields(sourceGroup, currentLeadSnapshot);

          if (sourceFields) {
            leadDoc.sourceType = sourceFields.sourceType;
            leadDoc.sourceExternalSystem = sourceFields.sourceExternalSystem;
            leadDoc.tracking = sourceFields.tracking;
            changes.push(
              `Fuente: ${labelLeadSourceGroup(currentSourceGroup)} -> ${labelLeadSourceGroup(sourceGroup)}`,
            );
            profileChanged = true;
          }
        }
      }

      if (details !== null && leadDoc.details !== details) {
        leadDoc.details = details;
        profileChanged = true;
      }

      if (bestContactDay !== null && leadDoc.bestContactDay !== bestContactDay) {
        leadDoc.bestContactDay = bestContactDay;
        profileChanged = true;
      }

      if (bestContactTime !== null && leadDoc.bestContactTime !== bestContactTime) {
        leadDoc.bestContactTime = bestContactTime;
        profileChanged = true;
      }

      if (nextStatus && leadDoc.status !== nextStatus) {
        changes.push(`Estado: ${labelStatus(leadDoc.status)} -> ${labelStatus(nextStatus)}`);
        leadDoc.status = nextStatus;

        if (nextStatus === "won") {
          leadDoc.soldAt = new Date();
        }
      }

      if (nextAction !== null && leadDoc.nextAction !== nextAction) {
        changes.push(`Proxima accion: ${nextAction || "Sin accion"}`);
        leadDoc.nextAction = nextAction;
      }

      if (nextActionAt !== undefined && String(leadDoc.nextActionAt || "") !== String(nextActionAt || "")) {
        changes.push(
          `Seguimiento: ${
            nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())
              ? formatDateTimeLabel(nextActionAt, METALWORKS_CALLBACK_TIME_ZONE)
              : "Sin fecha"
          }`,
        );
        leadDoc.nextActionAt =
          nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())
            ? nextActionAt
            : null;
      }

      if (appointmentType !== null) {
        const currentAppointmentType = normalizeAppointmentType(leadDoc.appointmentType || "");

        if (appointmentType && currentAppointmentType !== appointmentType) {
          leadDoc.appointmentType = appointmentType;
          changes.push(`Tipo de cita: ${labelAppointmentType(appointmentType) || "Sin tipo"}`);
        } else if (!appointmentType && !leadDoc.nextActionAt && currentAppointmentType) {
          leadDoc.appointmentType = "";
          changes.push("Tipo de cita: Sin tipo");
        }
      }

      if (appointmentStatus !== null) {
        const currentAppointmentStatus = normalizeAppointmentStatus(leadDoc.appointmentStatus || "");

        if (appointmentStatus && currentAppointmentStatus !== appointmentStatus) {
          leadDoc.appointmentStatus = appointmentStatus;
          changes.push(
            `Estado de cita: ${labelAppointmentStatus(appointmentStatus) || "Sin estado"}`,
          );
        } else if (!appointmentStatus && !leadDoc.nextActionAt && currentAppointmentStatus) {
          leadDoc.appointmentStatus = "";
          changes.push("Estado de cita: Sin estado");
        }
      }

      if (appointmentDurationMinutes !== null) {
        const currentAppointmentDuration = normalizeAppointmentDurationMinutes(
          leadDoc.appointmentDurationMinutes || 0,
        );

        if (appointmentDurationMinutes && currentAppointmentDuration !== appointmentDurationMinutes) {
          leadDoc.appointmentDurationMinutes = appointmentDurationMinutes;
          changes.push(`Duracion de cita: ${appointmentDurationMinutes} min`);
        } else if (!appointmentDurationMinutes && !leadDoc.nextActionAt && currentAppointmentDuration) {
          leadDoc.appointmentDurationMinutes = 0;
          changes.push("Duracion de cita: Sin duracion");
        }
      }

      if (
        appointmentAssignedTo !== null &&
        cleanText(leadDoc.appointmentAssignedTo || "", 80) !== appointmentAssignedTo
      ) {
        leadDoc.appointmentAssignedTo = appointmentAssignedTo;
        changes.push(`Asignado a: ${appointmentAssignedTo || "Sin asignar"}`);
      }

      if (leadDoc.nextActionAt) {
        const inferredAppointmentType =
          normalizeAppointmentType(leadDoc.appointmentType || "") || inferLeadAppointmentType(leadDoc);

        if (
          inferredAppointmentType &&
          normalizeAppointmentType(leadDoc.appointmentType || "") !== inferredAppointmentType
        ) {
          leadDoc.appointmentType = inferredAppointmentType;
        }

        const inferredAppointmentStatus =
          normalizeAppointmentStatus(leadDoc.appointmentStatus || "") ||
          inferLeadAppointmentStatus(leadDoc);

        if (
          inferredAppointmentStatus &&
          normalizeAppointmentStatus(leadDoc.appointmentStatus || "") !== inferredAppointmentStatus
        ) {
          leadDoc.appointmentStatus = inferredAppointmentStatus;
        }

        const resolvedDurationMinutes =
          normalizeAppointmentDurationMinutes(leadDoc.appointmentDurationMinutes || 0) ||
          inferAppointmentDurationMinutes(leadDoc.appointmentType || "");

        if (
          resolvedDurationMinutes &&
          normalizeAppointmentDurationMinutes(leadDoc.appointmentDurationMinutes || 0) !==
            resolvedDurationMinutes
        ) {
          leadDoc.appointmentDurationMinutes = resolvedDurationMinutes;
        }
      }

      if (
        !nextActionReminderOffsetsProvided &&
        nextActionAt !== undefined &&
        leadDoc.nextActionAt &&
        normalizeStatus(leadDoc.status || "new") === "booked"
      ) {
        const currentReminderOffsets = normalizeLeadReminderOffsets(
          leadDoc.nextActionReminderOffsets || [],
        );
        const autoReminderOffsets = resolveBookedReminderOffsets(
          currentReminderOffsets,
          leadDoc.nextActionAt,
          new Date(),
        );

        if (
          JSON.stringify(currentReminderOffsets) !== JSON.stringify(autoReminderOffsets) &&
          autoReminderOffsets.length
        ) {
          leadDoc.nextActionReminderOffsets = autoReminderOffsets;
          changes.push(
            `Reminders: ${autoReminderOffsets
              .map((value) => formatLeadReminderOffsetLabel(value))
              .filter(Boolean)
              .join(", ")}`,
          );
        }
      }

      if (nextActionReminderOffsets !== null) {
        const currentReminderOffsets = normalizeLeadReminderOffsets(
          leadDoc.nextActionReminderOffsets || [],
        );

        if (
          JSON.stringify(currentReminderOffsets) !== JSON.stringify(nextActionReminderOffsets)
        ) {
          changes.push(
            nextActionReminderOffsets.length
              ? `Reminders: ${nextActionReminderOffsets
                  .map((value) => formatLeadReminderOffsetLabel(value))
                  .filter(Boolean)
                  .join(", ")}`
              : "Reminders: Off",
          );
        }

        leadDoc.nextActionReminderOffsets = nextActionReminderOffsets;
      }

      if (privateNotes !== null || textThreadImport) {
        const nextPrivateNotesBase =
          privateNotes !== null
            ? privateNotes
            : cleanMultilineText(leadDoc.privateNotes || "", 12000);
        const nextPrivateNotes = textThreadImport
          ? mergeLeadTextImportIntoPrivateNotes(nextPrivateNotesBase, textThreadImport, {
              sourceLabel: textThreadImportSource || "Text thread",
            })
          : nextPrivateNotesBase;

        if (leadDoc.privateNotes !== nextPrivateNotes) {
          leadDoc.privateNotes = nextPrivateNotes;
          changes.push(
            textThreadImport ? "Text thread guardado en notas privadas" : "Notas privadas actualizadas",
          );
        }
      }

      if (estimateTitle !== null) {
        if (leadDoc.estimateTitle !== estimateTitle) {
          estimateChanged = true;
        }
        leadDoc.estimateTitle = estimateTitle;
      }

      if (estimateScope !== null) {
        if (leadDoc.estimateScope !== estimateScope) {
          estimateChanged = true;
        }
        leadDoc.estimateScope = estimateScope;
      }

      if (estimateMaterialsCost !== null) {
        if (normalizeMoney(leadDoc.estimateMaterialsCost || 0) !== estimateMaterialsCost) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateMaterialsCost = estimateMaterialsCost;
      }

      if (estimateLaborCost !== null) {
        if (normalizeMoney(leadDoc.estimateLaborCost || 0) !== estimateLaborCost) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateLaborCost = estimateLaborCost;
      }

      if (estimateCoatingCost !== null) {
        if (normalizeMoney(leadDoc.estimateCoatingCost || 0) !== estimateCoatingCost) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateCoatingCost = estimateCoatingCost;
      }

      if (estimateMiscCost !== null) {
        if (normalizeMoney(leadDoc.estimateMiscCost || 0) !== estimateMiscCost) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateMiscCost = estimateMiscCost;
      }

      if (estimateDiscount !== null) {
        if (normalizeMoney(leadDoc.estimateDiscount || 0) !== estimateDiscount) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateDiscount = estimateDiscount;
      }

      if (estimateValidUntilRaw !== null) {
        if (String(leadDoc.estimateValidUntil || "") !== String(estimateValidUntil || "")) {
          estimateChanged = true;
        }
        leadDoc.estimateValidUntil = estimateValidUntil;
      }

      if (estimateNotes !== null) {
        if (leadDoc.estimateNotes !== estimateNotes) {
          estimateChanged = true;
        }
        leadDoc.estimateNotes = estimateNotes;
      }

      if (clientDocumentType !== null) {
        if (normalizeClientDocumentType(leadDoc.clientDocumentType || "") !== clientDocumentType) {
          clientDocumentChanged = true;
        }
        leadDoc.clientDocumentType = clientDocumentType;
      }

      if (clientDocumentDescription !== null) {
        if (leadDoc.clientDocumentDescription !== clientDocumentDescription) {
          clientDocumentChanged = true;
        }
        leadDoc.clientDocumentDescription = clientDocumentDescription;
      }

      if (clientDocumentWorkDateRaw !== null) {
        if (String(leadDoc.clientDocumentWorkDate || "") !== String(clientDocumentWorkDate || "")) {
          clientDocumentChanged = true;
        }
        leadDoc.clientDocumentWorkDate = clientDocumentWorkDate;
      }

      if (clientDocumentWarranty !== null) {
        if (leadDoc.clientDocumentWarranty !== clientDocumentWarranty) {
          clientDocumentChanged = true;
        }
        leadDoc.clientDocumentWarranty = clientDocumentWarranty;
      }

      if (estimateMoneyChanged || estimateAmount !== null) {
        const nextEstimateAmount = estimateMoneyChanged
          ? normalizeMoney(
              (estimateMaterialsCost !== null ? estimateMaterialsCost : leadDoc.estimateMaterialsCost || 0) +
                (estimateLaborCost !== null ? estimateLaborCost : leadDoc.estimateLaborCost || 0) +
                (estimateCoatingCost !== null ? estimateCoatingCost : leadDoc.estimateCoatingCost || 0) +
                (estimateMiscCost !== null ? estimateMiscCost : leadDoc.estimateMiscCost || 0) -
                (estimateDiscount !== null ? estimateDiscount : leadDoc.estimateDiscount || 0),
            )
          : normalizeMoney(estimateAmount || 0);

        if (normalizeMoney(leadDoc.estimateAmount || 0) !== nextEstimateAmount) {
          estimateChanged = true;
        }

        leadDoc.estimateAmount = nextEstimateAmount;
      }

      const nextInvoiceDepositAmount =
        invoiceDepositAmount !== null
          ? invoiceDepositAmount
          : normalizeMoney(leadDoc.invoiceDepositAmount || 0);

      if (nextInvoiceDepositAmount > 0 && normalizeMoney(leadDoc.estimateAmount || 0) <= 0) {
        return respondError(res, 400, "Add the total before recording the amount collected.");
      }

      if (nextInvoiceDepositAmount > normalizeMoney(leadDoc.estimateAmount || 0)) {
        return respondError(res, 400, "Amount collected can't be higher than the total.");
      }

      if (invoiceDepositAmount !== null) {
        if (normalizeMoney(leadDoc.invoiceDepositAmount || 0) !== invoiceDepositAmount) {
          clientDocumentChanged = true;
          invoiceDepositChanged = true;
        }

        leadDoc.invoiceDepositAmount = invoiceDepositAmount;
      }

      if (estimateChanged) {
        changes.push(`Estimate: ${formatMoneyLabel(leadDoc.estimateAmount || 0)}`);
      }

      if (invoiceDepositChanged) {
        changes.push(`Amount collected: ${formatMoneyLabel(leadDoc.invoiceDepositAmount || 0)}`);
      }

      if (clientDocumentChanged) {
        changes.push("Documento para cliente actualizado");
      }

      if (profileChanged) {
        changes.push("Perfil del cliente actualizado");
      }

      if (nextActionAt !== undefined || nextActionReminderOffsets !== null) {
        leadDoc.nextActionReminderSentKeys = pruneLeadReminderSentKeys(
          leadDoc.nextActionReminderSentKeys || [],
          leadDoc.nextActionAt,
          leadDoc.nextActionReminderOffsets || [],
        );
      }

      if (changes.length || note) {
        leadDoc.lastContactAt = new Date();
      }

      if (normalizeStatus(leadDoc.status || "new") === "won" && !leadDoc.soldAt) {
        leadDoc.soldAt = leadDoc.estimateSentAt || leadDoc.updatedAt || leadDoc.clientDocumentWorkDate || new Date();
      }

      leadDoc.updatedAt = new Date();
      await leadDoc.save();

      await syncAndSaveLeadGoogleCalendarEvent(leadDoc);

      if (changes.length) {
        await appendActivity({
          leadId: leadDoc._id,
          activityType: "lead_updated",
          title: "Lead actualizado",
          body: changes.join(". "),
          meta: {
            adminEmail: auth.email,
          },
          req,
        });
      }

      if (note) {
        await appendActivity({
          leadId: leadDoc._id,
          activityType: "note_added",
          title: "Nota privada",
          body: note,
          meta: {
            adminEmail: auth.email,
          },
          req,
        });
      }

      if (textThreadImport) {
        await appendActivity({
          leadId: leadDoc._id,
          activityType: "text_thread_imported",
          title: "Text thread imported",
          body: cleanText(
            `${textThreadImportSource || "Text thread"} saved to private notes.`,
            280,
          ),
          meta: {
            adminEmail: auth.email,
            source: textThreadImportSource || "Text thread",
            charCount: textThreadImport.length,
          },
          req,
        });
      }

      const [updatedLead, activityDocs] = await Promise.all([
        MetalworksLead.findById(leadId).lean(),
        MetalworksLeadActivity.find({ leadId })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
      ]);
      const assets = await listLeadAssets(leadId);

      res.json({
        lead: cleanLead(updatedLead, { includeConversation: true }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error updating Metal Works lead:", error.message);
      respondError(res, 500, "No pude guardar ese lead.");
    }
  });

  app.delete("/api/metalworks-crm/leads/:leadId", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    try {
      const leadDoc = await MetalworksLead.findById(leadId).select("fullName");

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      const now = new Date();

      await Promise.all([
        MetalworksLeadActivity.deleteMany({ leadId: leadDoc._id }),
        MetalworksLeadAsset.deleteMany({ leadId: leadDoc._id }),
        MetalworksPublicChatWebPushDevice.updateMany(
          { leadId: leadDoc._id },
          {
            $set: {
              leadId: null,
              updatedAt: now,
            },
          },
        ),
        MetalworksLead.deleteOne({ _id: leadDoc._id }),
      ]);

      res.json({
        ok: true,
        deletedLeadId: leadId,
        deletedLeadName: cleanText(leadDoc.fullName || "", 120),
        deletedBy: auth.email,
      });
    } catch (error) {
      console.error("Error deleting Metal Works lead:", error.message);
      respondError(res, 500, "No pude borrar ese lead.");
    }
  });

  app.post("/api/metalworks-crm/leads/:leadId/assets", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    const filePayloads = Array.isArray(req.body?.files) ? req.body.files : [];

    if (!filePayloads.length) {
      return respondError(res, 400, "Add at least one image.");
    }

    if (filePayloads.length > METALWORKS_LEAD_ASSET_MAX_FILES) {
      return respondError(
        res,
        400,
        `Upload up to ${METALWORKS_LEAD_ASSET_MAX_FILES} images at a time.`,
      );
    }

    try {
      const leadDoc = await MetalworksLead.findById(leadId);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      const parsedFiles = filePayloads.map((item) => parseAssistantLeadAssetUpload(item));
      const totalBytes = parsedFiles.reduce((sum, item) => sum + (item.sizeBytes || 0), 0);

      if (totalBytes > METALWORKS_LEAD_ASSET_MAX_TOTAL_BYTES) {
        return respondError(res, 400, "Total image upload is too large.");
      }

      const now = new Date();
      const existingAssets = await MetalworksLeadAsset.find({ leadId: leadDoc._id })
        .select("fileName sizeBytes")
        .lean();
      const existingKeys = new Set(
        existingAssets.map(
          (item) =>
            `${sanitizeLeadAssetFileName(item?.fileName || "")}:${Number(item?.sizeBytes || 0)}`,
        ),
      );
      const newFiles = parsedFiles.filter((item) => {
        const key = `${sanitizeLeadAssetFileName(item.fileName || "")}:${Number(
          item.sizeBytes || 0,
        )}`;

        if (existingKeys.has(key)) {
          return false;
        }

        existingKeys.add(key);
        return true;
      });
      const assetDocs = await Promise.all(
        newFiles.map((item) =>
          MetalworksLeadAsset.create({
            leadId: leadDoc._id,
            sourceType: "crm_manual_photo",
            fileName: item.fileName,
            mimeType: item.mimeType,
            sizeBytes: item.sizeBytes,
            fileData: item.fileData,
            uploadedAt: now,
            updatedAt: now,
            createdAt: now,
          }),
        ),
      );

      if (assetDocs.length) {
        leadDoc.photoFileNames = mergeAssistantUniqueValues(
          leadDoc.photoFileNames || [],
          newFiles.map((item) => item.fileName),
        );
        leadDoc.updatedAt = now;
        await leadDoc.save();

        await appendActivity({
          leadId: leadDoc._id,
          activityType: "crm_photo_uploaded",
          title: "Fotos agregadas manualmente",
          body: `Se agregaron ${assetDocs.length} foto${assetDocs.length === 1 ? "" : "s"} desde el CRM.`,
          meta: {
            adminEmail: auth.email,
            fileNames: newFiles.map((item) => item.fileName),
          },
          req,
        });
      }

      const [updatedLead, activityDocs, assets] = await Promise.all([
        MetalworksLead.findById(leadId).lean(),
        MetalworksLeadActivity.find({ leadId })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
        listLeadAssets(leadId),
      ]);

      res.json({
        ok: true,
        uploadedCount: assetDocs.length,
        lead: cleanLead(updatedLead, { includeConversation: true }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error saving manual Metal Works photos:", error.message);
      respondError(res, 500, error?.message || "No pude guardar esas fotos.");
    }
  });

  app.post("/api/metalworks-crm/leads/:leadId/live-chat-reply", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    const message = cleanText(req.body?.message || "", 500);

    if (!message) {
      return respondError(res, 400, "El mensaje es requerido.");
    }

    try {
      const leadDoc = await MetalworksLead.findById(leadId);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      if (!isWebsiteLiveChatLead(leadDoc)) {
        return respondError(res, 400, "Este lead no usa el chat web conectado.");
      }

      const now = new Date();
      const currentStatus = normalizeStatus(leadDoc.status || "new");

      leadDoc.conversationHistory = mergeConversationHistory(leadDoc.conversationHistory || [], [
        {
          role: "assistant",
          content: message,
          createdAt: now,
        },
      ]);
      leadDoc.lastAssistantMessage = message;
      leadDoc.lastContactAt = now;
      leadDoc.updatedAt = now;

      if (currentStatus === "new") {
        leadDoc.status = "contacted";
      }

      await leadDoc.save();

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "website_live_chat_reply",
        title: "Respuesta del CRM",
        body: message,
        meta: {
          adminEmail: auth.email,
          sourceType: leadDoc.sourceType || METALWORKS_WEBSITE_CHAT_SOURCE_TYPE,
        },
        req,
      });

      try {
        await sendWebsiteLiveChatReplyPushAlert({
          lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
          message,
        });
      } catch (error) {
        console.error("Error sending website live chat reply push:", error.message);
      }

      const [activityDocs, assets] = await Promise.all([
        MetalworksLeadActivity.find({ leadId: leadDoc._id })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
        listLeadAssets(leadDoc._id),
      ]);

      res.json({
        ok: true,
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc, {
          includeConversation: true,
        }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error replying to Metal Works website chat:", error.message);
      respondError(res, 500, "No pude mandar la respuesta al chat web.");
    }
  });

  app.get("/api/metalworks-crm/assets/:assetId/content", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const assetId = String(req.params?.assetId || "").trim();

    if (!assetId || !mongoose.Types.ObjectId.isValid(assetId)) {
      return respondError(res, 400, "Asset invalido.");
    }

    try {
      const assetDoc = await MetalworksLeadAsset.findById(assetId).select(
        "mimeType fileName fileData",
      );

      if (!assetDoc?.fileData) {
        return respondError(res, 404, "No encontre esa foto.");
      }

      res.setHeader(
        "Content-Type",
        normalizeLeadAssetMimeType(assetDoc.mimeType || "") || "application/octet-stream",
      );
      res.setHeader(
        "Content-Disposition",
        `inline; filename="${sanitizeLeadAssetFileName(assetDoc.fileName || "project-photo.jpg")}"`,
      );
      res.setHeader("Cache-Control", "private, max-age=3600");
      res.send(assetDoc.fileData);
    } catch (error) {
      console.error("Error loading Metal Works asset:", error.message);
      respondError(res, 500, "No pude cargar esa foto.");
    }
  });

  app.post("/api/metalworks-crm/leads/:leadId/send-estimate", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    try {
      const leadDoc = await MetalworksLead.findById(leadId);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      const clientDocument = buildMetalworksClientDocumentSnapshot(leadDoc);

      if (!normalizeEmail(leadDoc.email || "")) {
        return respondError(res, 400, "Este lead no tiene correo todavia.");
      }

      if (
        !cleanText(clientDocument.description || "", 3200) &&
        !normalizeMoney(leadDoc.estimateAmount || 0)
      ) {
        return respondError(
          res,
          400,
          `Primero guarda la descripcion o el total del ${clientDocument.documentType === "invoice" ? "invoice" : "estimate"} para poder enviarlo.`,
        );
      }

      const delivery = await sendMetalworksEstimateEmail(leadDoc, auth.email);
      let activityDocs = [];

      if (delivery.delivered) {
        const sentAt = new Date();
        const statusBefore = normalizeStatus(leadDoc.status || "new");
        let statusLine = "";
        const documentLabel = clientDocument.documentLabel;

        leadDoc.estimateSentAt = sentAt;
        leadDoc.estimateSentTo = normalizeEmail(leadDoc.email || "");
        leadDoc.lastContactAt = sentAt;
        leadDoc.updatedAt = sentAt;

        if (["new", "contacted"].includes(statusBefore)) {
          leadDoc.status = "quoted";
          statusLine = " Status changed to Quoted.";
        }

        await leadDoc.save();

        await appendActivity({
          leadId: leadDoc._id,
          activityType: "estimate_sent",
          title: `${documentLabel} sent`,
          body: `${documentLabel} sent to ${leadDoc.estimateSentTo}.${statusLine}`.trim(),
          meta: {
            adminEmail: auth.email,
            sentTo: leadDoc.estimateSentTo,
            documentType: clientDocument.documentType,
          },
          req,
        });
      }

      const updatedLead = await MetalworksLead.findById(leadId).lean();
      activityDocs = await MetalworksLeadActivity.find({ leadId })
        .sort({ createdAt: -1 })
        .limit(80)
        .lean();
      const assets = await listLeadAssets(leadId);

      res.json({
        ok: true,
        delivered: Boolean(delivery.delivered),
        fallbackUsed: !delivery.delivered,
        message: delivery.delivered
          ? `${clientDocument.documentLabel} sent to the client.`
          : delivery.error || "I could not send it from the system.",
        lead: cleanLead(updatedLead, { includeConversation: true }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error sending Metal Works estimate:", error.message);
      respondError(res, 500, "No pude enviar ese documento.");
    }
  });

}
