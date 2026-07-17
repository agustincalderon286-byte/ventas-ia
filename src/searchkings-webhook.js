function cleanText(value = "", maxLength = 0) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return maxLength > 0 ? text.slice(0, maxLength) : text;
}

function normalizeEmail(value = "") {
  return cleanText(value).toLowerCase();
}

function normalizePhone(value = "") {
  const digits = String(value || "").replace(/\D/g, "");

  if (digits.length === 11 && digits.startsWith("1")) {
    return digits.slice(1);
  }

  if (digits.length === 10) {
    return digits;
  }

  return digits.slice(0, 15);
}

function isRecord(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function getPath(target, path = []) {
  return path.reduce((current, segment) => {
    if (!isRecord(current) && !Array.isArray(current)) {
      return undefined;
    }

    return current?.[segment];
  }, target);
}

function pickFirst(target, paths = []) {
  for (const path of paths) {
    const value = getPath(target, path);

    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return undefined;
}

function pickFirstText(target, paths = [], maxLength = 0) {
  return cleanText(pickFirst(target, paths) || "", maxLength);
}

function pickFirstRecord(target, paths = []) {
  for (const path of paths) {
    const value = getPath(target, path);

    if (isRecord(value)) {
      return value;
    }
  }

  return null;
}

function uniqueStrings(values = [], maxLength = 0) {
  return Array.from(
    new Set(
      values
        .map((value) => cleanText(value, maxLength))
        .filter(Boolean),
    ),
  );
}

function joinLabel(parts = [], maxLength = 0) {
  return cleanText(parts.filter(Boolean).join(", "), maxLength);
}

function getSearchKingsEventType(payload = {}) {
  return (
    pickFirstText(
      payload,
      [
        ["eventType"],
        ["type"],
        ["event"],
        ["event_type"],
        ["data", "eventType"],
        ["data", "type"],
        ["call", "eventType"],
        ["call", "type"],
        ["lead", "eventType"],
        ["lead", "type"],
      ],
      80,
    ) || "searchkings_call"
  );
}

function getSearchKingsCallRecord(payload = {}) {
  return (
    pickFirstRecord(payload, [["call"], ["data", "call"], ["lead"], ["data", "lead"]]) ||
    (isRecord(payload) ? payload : {})
  );
}

function buildLocationParts(payload = {}) {
  const record =
    pickFirstRecord(payload, [
      ["location"],
      ["caller", "location"],
      ["data", "location"],
      ["call", "location"],
      ["call", "caller", "location"],
    ]) || {};

  const city = cleanText(
    record.city ||
      record.locality ||
      pickFirst(payload, [["city"], ["callerCity"], ["data", "city"]]) ||
      "",
    80,
  );
  const state = cleanText(
    record.state ||
      record.stateCode ||
      record.region ||
      pickFirst(payload, [["state"], ["callerState"], ["data", "state"]]) ||
      "",
    40,
  );
  const zipCode = cleanText(
    record.zipCode ||
      record.zip ||
      record.postalCode ||
      pickFirst(payload, [["zipCode"], ["zip"], ["callerZip"], ["data", "zipCode"]]) ||
      "",
    20,
  );

  return {
    city,
    state,
    zipCode,
    location: joinLabel([city, state, zipCode], 160),
  };
}

function buildSearchKingsLeadCandidate(payload = {}) {
  const call = getSearchKingsCallRecord(payload);
  const eventType = getSearchKingsEventType(payload);
  const externalLeadId =
    pickFirstText(
      call,
      [
        ["callId"],
        ["callID"],
        ["leadId"],
        ["leadID"],
        ["eventId"],
        ["eventID"],
        ["id"],
        ["sid"],
      ],
      120,
    ) ||
    pickFirstText(
      payload,
      [
        ["callId"],
        ["callID"],
        ["leadId"],
        ["leadID"],
        ["eventId"],
        ["eventID"],
        ["id"],
        ["sid"],
      ],
      120,
    );

  const phoneDisplay =
    pickFirstText(
      call,
      [
        ["callerPhone"],
        ["callerNumber"],
        ["fromNumber"],
        ["phoneNumber"],
        ["phone"],
        ["caller", "phone"],
        ["contact", "phone"],
      ],
      40,
    ) ||
    pickFirstText(
      payload,
      [
        ["callerPhone"],
        ["callerNumber"],
        ["fromNumber"],
        ["phoneNumber"],
        ["phone"],
        ["caller", "phone"],
        ["contact", "phone"],
      ],
      40,
    );
  const phone = normalizePhone(phoneDisplay);

  const firstName = pickFirstText(
    call,
    [["callerFirstName"], ["contact", "firstName"], ["caller", "firstName"]],
    80,
  );
  const lastName = pickFirstText(
    call,
    [["callerLastName"], ["contact", "lastName"], ["caller", "lastName"]],
    80,
  );
  const joinedName = cleanText([firstName, lastName].filter(Boolean).join(" "), 120);
  const fullName =
    pickFirstText(
      call,
      [["callerName"], ["contactName"], ["name"], ["contact", "name"], ["caller", "name"]],
      120,
    ) ||
    pickFirstText(
      payload,
      [["callerName"], ["contactName"], ["name"], ["contact", "name"], ["caller", "name"]],
      120,
    ) ||
    joinedName ||
    "SearchKings caller";

  const email = normalizeEmail(
    pickFirstText(call, [["email"], ["contact", "email"], ["caller", "email"]], 160) ||
      pickFirstText(payload, [["email"], ["contact", "email"], ["caller", "email"]], 160),
  );

  const campaign = cleanText(
    pickFirst(call, [
      ["campaignName"],
      ["campaign"],
      ["sourceCampaign"],
      ["googleAdsCampaign"],
      ["tracking", "campaign"],
      ["utm", "campaign"],
    ]) || "",
    160,
  );
  const adGroup = cleanText(
    pickFirst(call, [["adGroup"], ["adGroupName"], ["tracking", "adGroup"], ["utm", "content"]]) ||
      "",
    160,
  );
  const source = cleanText(
    pickFirst(call, [["source"], ["sourceName"], ["channel"], ["network"], ["trafficSource"]]) ||
      pickFirst(payload, [["source"], ["sourceName"], ["channel"], ["network"], ["trafficSource"]]) ||
      "",
    120,
  );
  const keyword = cleanText(
    pickFirst(call, [["keyword"], ["searchKeyword"], ["tracking", "keyword"], ["utm", "term"]]) ||
      "",
    160,
  );
  const recordingUrl = cleanText(
    pickFirst(call, [["recordingUrl"], ["recordingURL"], ["recording"], ["recordingLink"]]) ||
      pickFirst(payload, [["recordingUrl"], ["recordingURL"], ["recording"], ["recordingLink"]]) ||
      "",
    1200,
  );
  const transcript = cleanText(
    pickFirst(call, [["transcript"], ["callTranscript"], ["notesTranscript"]]) ||
      pickFirst(payload, [["transcript"], ["callTranscript"], ["notesTranscript"]]) ||
      "",
    2400,
  );
  const summary = cleanText(
    pickFirst(call, [["summary"], ["callSummary"], ["aiSummary"], ["description"]]) ||
      pickFirst(payload, [["summary"], ["callSummary"], ["aiSummary"], ["description"]]) ||
      "",
    800,
  );
  const outcome = cleanText(
    pickFirst(call, [["outcome"], ["callOutcome"], ["status"], ["callStatus"], ["disposition"]]) ||
      pickFirst(payload, [["outcome"], ["callOutcome"], ["status"], ["callStatus"], ["disposition"]]) ||
      "",
    120,
  );
  const objections = uniqueStrings(
    [
      ...(Array.isArray(call?.objections) ? call.objections : []),
      ...(Array.isArray(payload?.objections) ? payload.objections : []),
      pickFirstText(call, [["objection"]], 160),
      pickFirstText(payload, [["objection"]], 160),
    ],
    160,
  );
  const tags = uniqueStrings(
    [
      ...(Array.isArray(call?.tags) ? call.tags : []),
      ...(Array.isArray(payload?.tags) ? payload.tags : []),
      pickFirstText(call, [["tag"]], 80),
      pickFirstText(payload, [["tag"]], 80),
    ],
    80,
  );
  const callTimestamp = cleanText(
    pickFirst(call, [["callTime"], ["timestamp"], ["receivedAt"], ["createdAt"], ["startTime"]]) ||
      pickFirst(payload, [["callTime"], ["timestamp"], ["receivedAt"], ["createdAt"], ["startTime"]]) ||
      "",
    80,
  );
  const duration = cleanText(
    pickFirst(call, [["duration"], ["durationSeconds"], ["callDuration"], ["billableMinutes"]]) ||
      pickFirst(payload, [["duration"], ["durationSeconds"], ["callDuration"], ["billableMinutes"]]) ||
      "",
    40,
  );
  const landingUrl = cleanText(
    pickFirst(call, [["landingUrl"], ["landingURL"], ["pageUrl"], ["pageURL"]]) ||
      pickFirst(payload, [["landingUrl"], ["landingURL"], ["pageUrl"], ["pageURL"]]) ||
      "",
    500,
  );
  const locationParts = buildLocationParts(call);
  const projectType = campaign
    ? `Google Ads phone call / ${campaign}`
    : "Google Ads phone call";

  const detailsLines = [
    externalLeadId ? `Call ID: ${externalLeadId}` : "",
    eventType ? `Event Type: ${eventType}` : "",
    callTimestamp ? `Call Time: ${callTimestamp}` : "",
    source ? `Source: ${source}` : "",
    campaign ? `Campaign: ${campaign}` : "",
    adGroup ? `Ad Group: ${adGroup}` : "",
    keyword ? `Keyword: ${keyword}` : "",
    duration ? `Duration: ${duration}` : "",
    outcome ? `Outcome: ${outcome}` : "",
    recordingUrl ? `Recording: ${recordingUrl}` : "",
    summary ? `Summary: ${summary}` : "",
    objections.length ? `Objections: ${objections.join(", ")}` : "",
    tags.length ? `Tags: ${tags.join(", ")}` : "",
    transcript ? `Transcript: ${transcript}` : "",
  ].filter(Boolean);

  return {
    eventType,
    entityType: "call",
    externalLeadId:
      externalLeadId || cleanText(`searchkings-${phone || "unknown"}-${callTimestamp || Date.now()}`, 120),
    externalSystem: "searchkings",
    sourceType: "searchkings_call",
    crmStatus: "new",
    fullName,
    phone,
    phoneDisplay,
    email,
    projectType,
    city: locationParts.city,
    zipCode: locationParts.zipCode,
    location: locationParts.location,
    details: cleanText(detailsLines.join("\n"), 3000),
    meta: {
      callId: externalLeadId,
      eventType,
      source,
      campaign,
      adGroup,
      keyword,
      recordingUrl,
      summary,
      transcript,
      outcome,
      callTimestamp,
      duration,
      tags,
      objections,
      landingUrl,
    },
  };
}

function buildSearchKingsActivity(payload = {}, leadCandidate = null) {
  const meta = leadCandidate?.meta || {};
  const summary = cleanText(meta.summary || "", 400);
  const transcript = cleanText(meta.transcript || "", 1200);
  const outcome = cleanText(meta.outcome || "", 120);
  const source = cleanText(meta.source || "", 120);
  const campaign = cleanText(meta.campaign || "", 160);

  return {
    activityType: "searchkings_call",
    title: "Llamada de SearchKings",
    body:
      summary ||
      transcript ||
      cleanText(
        [
          "Call data received from SearchKings.",
          campaign ? `Campaign: ${campaign}.` : "",
          source ? `Source: ${source}.` : "",
          outcome ? `Outcome: ${outcome}.` : "",
        ]
          .filter(Boolean)
          .join(" "),
        1200,
      ),
    meta: {
      externalSystem: "searchkings",
      eventType: leadCandidate?.eventType || getSearchKingsEventType(payload),
      callId: cleanText(meta.callId || "", 120),
      source,
      campaign,
      adGroup: cleanText(meta.adGroup || "", 160),
      keyword: cleanText(meta.keyword || "", 160),
      recordingUrl: cleanText(meta.recordingUrl || "", 1200),
      summary,
      transcript,
      outcome,
      callTimestamp: cleanText(meta.callTimestamp || "", 80),
      duration: cleanText(meta.duration || "", 40),
      tags: Array.isArray(meta.tags) ? meta.tags : [],
      objections: Array.isArray(meta.objections) ? meta.objections : [],
      landingUrl: cleanText(meta.landingUrl || "", 500),
    },
  };
}

function buildSearchKingsTrackingPayload(leadCandidate = null) {
  const meta = leadCandidate?.meta || {};

  return {
    utmSource: "search_kings",
    utmMedium: "call_intelligence",
    utmCampaign: cleanText(meta.campaign || "search_kings_google_ads", 120),
    utmTerm: cleanText(meta.keyword || "", 120),
    utmContent: cleanText(meta.adGroup || "", 120),
    landingUrl: cleanText(meta.landingUrl || "", 500),
  };
}

function buildSearchKingsWebhookEvent(payload = {}) {
  const leadCandidate = buildSearchKingsLeadCandidate(payload);

  return {
    eventType: leadCandidate.eventType || getSearchKingsEventType(payload),
    entityType: "call",
    leadCandidate,
    tracking: buildSearchKingsTrackingPayload(leadCandidate),
    activity: buildSearchKingsActivity(payload, leadCandidate),
  };
}

export {
  buildSearchKingsLeadCandidate,
  buildSearchKingsWebhookEvent,
  getSearchKingsEventType,
};
