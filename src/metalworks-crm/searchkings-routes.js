import { buildSearchKingsWebhookEvent } from "../searchkings-webhook.js";
import { buildSearchKingsSmsEmailCandidate } from "../searchkings-sms-email.js";

const SEARCHKINGS_SMS_CONVERSATION_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;

function mergeSearchKingsSmsDetails(existingDetails = "", incomingDetails = "") {
  const existing = String(existingDetails || "").trim();
  const incoming = String(incomingDetails || "").trim();

  if (!existing) return incoming;
  if (!incoming || existing.includes(incoming)) return existing;

  return `${existing}\n\n---\n\n${incoming}`.slice(0, 12000);
}

export function registerMetalworksSearchKingsRoutes(app, dependencies) {
  const {
    searchKingsWebhookConfigured,
    searchKingsSmsEmailConfigured,
    respondError,
    requestHasSearchKingsWebhookAccess,
    requestHasSearchKingsSmsEmailAccess,
    parseSearchKingsWebhookBody,
    cleanText,
    normalizePhone,
    normalizeEmail,
    buildTrackingPayload,
    MetalworksLead,
    getClientIp,
    appendActivity,
    sendMetalworksPushAlert,
    cleanExternalLeadReceipt,
  } = dependencies;

  app.post(
    ["/integrations/searchkings/webhook", "/api/integrations/searchkings/webhook"],
    async (req, res) => {
      try {
        if (!searchKingsWebhookConfigured()) {
          return respondError(res, 503, "SearchKings webhook auth is not configured yet.");
        }

        if (!requestHasSearchKingsWebhookAccess(req)) {
          return respondError(res, 401, "Unauthorized SearchKings webhook request.");
        }

        const webhookPayload = parseSearchKingsWebhookBody(req.body || {});
        const parsedEvent = buildSearchKingsWebhookEvent(webhookPayload);
        const leadCandidate = parsedEvent.leadCandidate || {};
        const externalLeadId = cleanText(leadCandidate.externalLeadId || "", 120);
        const fullName = cleanText(leadCandidate.fullName || "", 120);
        const phoneDisplay = cleanText(leadCandidate.phoneDisplay || leadCandidate.phone || "", 40);
        const phone = normalizePhone(leadCandidate.phone || phoneDisplay);
        const email = normalizeEmail(leadCandidate.email || "");
        const projectType = cleanText(leadCandidate.projectType || "", 120);
        const location = cleanText(leadCandidate.location || "", 160);
        const addressLine = cleanText(leadCandidate.addressLine || "", 160);
        const zipCode = cleanText(leadCandidate.zipCode || "", 20);
        const city = cleanText(leadCandidate.city || "", 120);
        const details = cleanText(leadCandidate.details || "", 3000);
        const sourceType = cleanText(leadCandidate.sourceType || "", 80) || "searchkings_call";
        const externalSystem = cleanText(leadCandidate.externalSystem || "", 80) || "searchkings";
        const pagePath = "/integrations/searchkings/webhook";
        const pageUrl = "https://www.searchkings.com/";
        const tracking = buildTrackingPayload({
          ...(parsedEvent.tracking || {}),
          utmSource: "search_kings",
          utmMedium: "call_intelligence",
        });

        if (!externalLeadId) return respondError(res, 400, "Missing SearchKings call id.");
        if (!phone) return respondError(res, 400, "The phone number is required.");

        const now = new Date();
        let leadDoc = await MetalworksLead.findOne({ sourceExternalId: externalLeadId }).sort({
          updatedAt: -1,
          createdAt: -1,
        });
        const duplicate = Boolean(leadDoc);

        if (leadDoc) {
          leadDoc.fullName = fullName || leadDoc.fullName;
          leadDoc.phone = phone;
          leadDoc.phoneDisplay = phoneDisplay || phone;
          leadDoc.email = email;
          leadDoc.projectType = projectType || leadDoc.projectType;
          leadDoc.location = location || leadDoc.location;
          leadDoc.addressLine = addressLine || leadDoc.addressLine;
          leadDoc.zipCode = zipCode || leadDoc.zipCode;
          leadDoc.city = city || leadDoc.city;
          leadDoc.details = details || leadDoc.details;
          leadDoc.sourceType = sourceType;
          leadDoc.sourceExternalId = externalLeadId;
          leadDoc.sourceExternalSystem = externalSystem;
          leadDoc.pageTitle = "SearchKings";
          leadDoc.pagePath = pagePath;
          leadDoc.pageUrl = pageUrl;
          leadDoc.referrer = pageUrl;
          leadDoc.ipAddress = cleanText(getClientIp(req), 120);
          leadDoc.userAgent = cleanText(req.headers["user-agent"] || "", 400);
          leadDoc.tracking = tracking;
          leadDoc.updatedAt = now;
          await leadDoc.save();
        } else {
          leadDoc = await MetalworksLead.create({
            fullName: fullName || "SearchKings caller",
            phone,
            phoneDisplay: phoneDisplay || phone,
            email,
            projectType: projectType || "Google Ads phone call",
            location,
            addressLine,
            zipCode,
            city,
            details,
            status: cleanText(leadCandidate.crmStatus || "", 24) || "new",
            sourceType,
            sourceExternalId: externalLeadId,
            sourceExternalSystem: externalSystem,
            pageTitle: "SearchKings",
            pagePath,
            pageUrl,
            referrer: pageUrl,
            ipAddress: cleanText(getClientIp(req), 120),
            userAgent: cleanText(req.headers["user-agent"] || "", 400),
            tracking,
            updatedAt: now,
            createdAt: now,
          });

          await appendActivity({
            leadId: leadDoc._id,
            activityType: "lead_created",
            title: "Lead creado",
            body: `${leadDoc.fullName} entro desde SearchKings.`,
            meta: {
              externalLeadId,
              externalSystem: "searchkings",
              eventType: cleanText(parsedEvent.eventType || "", 80),
              campaign: cleanText(leadCandidate?.meta?.campaign || "", 160),
              source: cleanText(leadCandidate?.meta?.source || "", 120),
            },
            req,
            pagePath,
            pageUrl,
            tracking,
          });
        }

        await appendActivity({
          leadId: leadDoc._id,
          activityType: cleanText(parsedEvent?.activity?.activityType || "", 80) || "searchkings_call",
          title: cleanText(parsedEvent?.activity?.title || "", 160) || "Llamada de SearchKings",
          body: cleanText(parsedEvent?.activity?.body || "", 2000) || "SearchKings call data received.",
          meta: { ...(parsedEvent?.activity?.meta || {}), duplicate, webhookPayload },
          req,
          pagePath,
          pageUrl,
          tracking,
        });

        let pushDelivery = { attempted: false, delivered: false };

        if (!duplicate) {
          try {
            pushDelivery = await sendMetalworksPushAlert({
              lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
              alertType: "website_lead",
            });
          } catch (error) {
            console.error("Error sending SearchKings webhook push:", error.message);
          }
        }

        return res.json({
          ok: true,
          eventType: parsedEvent.eventType || "",
          entityType: parsedEvent.entityType || "call",
          duplicate,
          notified: Boolean(pushDelivery.delivered),
          lead: cleanExternalLeadReceipt(leadDoc.toObject ? leadDoc.toObject() : leadDoc),
        });
      } catch (error) {
        console.error("Error handling SearchKings webhook:", error.message);
        return respondError(
          res,
          error?.statusCode || 500,
          error?.statusCode ? error.message : "No pude procesar el webhook de SearchKings.",
        );
      }
    },
  );

  app.post("/api/integrations/searchkings/sms-email", async (req, res) => {
    try {
      if (!searchKingsSmsEmailConfigured()) {
        return respondError(res, 503, "SearchKings SMS email sync is not configured yet.");
      }
      if (!requestHasSearchKingsSmsEmailAccess(req)) {
        return respondError(res, 401, "Unauthorized SearchKings SMS email request.");
      }

      const candidate = buildSearchKingsSmsEmailCandidate(req.body || {});
      // Gmail searches return every message in a matching thread, including replies and drafts.
      // A non-alert must not stop the scheduled sync from reaching later SMS alerts.
      if (!candidate) return res.json({ ok: true, ignored: true });

      const now = new Date();
      const pagePath = "/api/integrations/searchkings/sms-email";
      const pageUrl = "https://calls.searchkings.com/";
      const tracking = buildTrackingPayload(candidate.tracking);
      let leadDoc = await MetalworksLead.findOne({
        sourceExternalSystem: candidate.externalSystem,
        sourceExternalId: candidate.externalLeadId,
      }).sort({ updatedAt: -1, createdAt: -1 });

      // SearchKings sends each SMS and media item as a separate Gmail message.
      // Keep those messages together under the caller's existing SMS lead.
      if (!leadDoc && candidate.phone) {
        leadDoc = await MetalworksLead.findOne({
          sourceExternalSystem: candidate.externalSystem,
          sourceType: candidate.sourceType,
          phone: candidate.phone,
          createdAt: { $gte: new Date(now.getTime() - SEARCHKINGS_SMS_CONVERSATION_WINDOW_MS) },
        }).sort({ createdAt: 1, updatedAt: -1 });
      }

      const duplicate = Boolean(leadDoc);

      if (leadDoc) {
        Object.assign(leadDoc, {
          fullName: candidate.fullName || leadDoc.fullName,
          phone: candidate.phone,
          phoneDisplay: candidate.phoneDisplay || leadDoc.phoneDisplay || candidate.phone,
          projectType: candidate.projectType || leadDoc.projectType,
          location: candidate.location || leadDoc.location,
          zipCode: candidate.zipCode || leadDoc.zipCode,
          city: candidate.city || leadDoc.city,
          details: mergeSearchKingsSmsDetails(leadDoc.details, candidate.details),
          sourceType: candidate.sourceType,
          tracking,
          updatedAt: now,
        });
        await leadDoc.save();
      } else {
        leadDoc = await MetalworksLead.create({
          fullName: candidate.fullName, phone: candidate.phone, phoneDisplay: candidate.phoneDisplay || candidate.phone,
          projectType: candidate.projectType, location: candidate.location, zipCode: candidate.zipCode, city: candidate.city,
          details: candidate.details, status: candidate.crmStatus, sourceType: candidate.sourceType,
          sourceExternalId: candidate.externalLeadId, sourceExternalSystem: candidate.externalSystem,
          pageTitle: "SearchKings SMS", pagePath, pageUrl, referrer: pageUrl,
          ipAddress: cleanText(getClientIp(req), 120), userAgent: cleanText(req.headers["user-agent"] || "", 400),
          tracking, updatedAt: now, createdAt: now,
        });
        await appendActivity({
          leadId: leadDoc._id, activityType: "lead_created", title: "Lead creado",
          body: "Nuevo SMS de SearchKings guardado en el CRM.",
          meta: { externalLeadId: candidate.externalLeadId, externalSystem: candidate.externalSystem },
          externalEventKey: `searchkings:sms:${candidate.externalLeadId}:lead_created`,
          req, pagePath, pageUrl, tracking,
        });
      }

      await appendActivity({
        leadId: leadDoc._id, activityType: candidate.activity.activityType, title: candidate.activity.title,
        body: candidate.activity.body, meta: { ...candidate.activity.meta, duplicate },
        externalEventKey: `searchkings:sms:${candidate.externalLeadId}`,
        req, pagePath, pageUrl, tracking,
      });

      let pushDelivery = { attempted: false, delivered: false };
      if (!duplicate) {
        try {
          pushDelivery = await sendMetalworksPushAlert({
            lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc, alertType: "website_lead",
          });
        } catch (error) {
          console.error("Error sending SearchKings SMS email push:", error.message);
        }
      }

      return res.json({ ok: true, entityType: candidate.entityType, duplicate,
        notified: Boolean(pushDelivery.delivered),
        lead: cleanExternalLeadReceipt(leadDoc.toObject ? leadDoc.toObject() : leadDoc),
      });
    } catch (error) {
      console.error("Error importing SearchKings SMS email:", error.message);
      return respondError(res, error?.statusCode || 500,
        error?.statusCode ? error.message : "No pude importar el SMS de SearchKings.");
    }
  });
}
