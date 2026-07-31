export function registerMetalworksCrmCalendarRoutes(app, dependencies) {
  const {
    requireAuth,
    MetalworksLead,
    getGoogleCalendarConfig,
    getGoogleCalendarAccessToken,
    leadHasAtlasCommercialOutreachAttribution,
    syncAndSaveLeadGoogleCalendarEvent,
    respondError,
    waitMs,
  } = dependencies;

  app.post("/api/metalworks-crm/google-calendar/cleanup-atlas-outreach", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) return;

    try {
      const leads = await MetalworksLead.find({
        googleCalendarEventId: { $exists: true, $nin: ["", null] },
      });
      const calendarConfig = getGoogleCalendarConfig();

      if (!calendarConfig.configured) {
        return respondError(res, 503, calendarConfig.reason);
      }

      const accessToken = await getGoogleCalendarAccessToken(calendarConfig);
      let removed = 0;
      let errors = 0;

      for (const leadDoc of leads) {
        if (!leadHasAtlasCommercialOutreachAttribution(leadDoc)) continue;

        const result = await syncAndSaveLeadGoogleCalendarEvent(leadDoc, {
          configOverride: calendarConfig,
          accessTokenOverride: accessToken,
        });

        if (result?.status === "skipped") removed += 1;
        else if (result?.status === "error") errors += 1;

        // Avoid Google Calendar write limits when cleaning historical events.
        await waitMs(350);
      }

      res.json({
        ok: errors === 0,
        removed,
        errors,
        message: `${removed} Atlas Outreach event${removed === 1 ? "" : "s"} removed from Google Calendar.`,
      });
    } catch (error) {
      console.error("Error cleaning Atlas Outreach Google Calendar events:", error.message);
      respondError(res, 500, "No pude limpiar los eventos de Atlas Outreach.");
    }
  });
}
