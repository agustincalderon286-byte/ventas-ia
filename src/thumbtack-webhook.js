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

function pickFirstRecord(target, paths = [], predicate = () => true) {
  for (const path of paths) {
    const value = getPath(target, path);

    if (predicate(value)) {
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

function looksLikeNegotiation(value = null) {
  if (!isRecord(value)) {
    return false;
  }

  return Boolean(
    value.negotiationID ||
      value.requestID ||
      value.jobStatus ||
      value.status ||
      value.customer ||
      value.description ||
      value.category ||
      value.services ||
      value.request,
  );
}

function looksLikeMessage(value = null) {
  if (!isRecord(value)) {
    return false;
  }

  return Boolean(
    value.messageID ||
      value.negotiationID ||
      value.text ||
      value.sentAt ||
      value.from ||
      value.attachments,
  );
}

function looksLikeReview(value = null) {
  if (!isRecord(value)) {
    return false;
  }

  return Boolean(value.reviewID || value.reviewText || value.rating || value.reviewerName);
}

function inferEntityType(eventType = "", payload = {}) {
  const normalized = cleanText(eventType, 80).toLowerCase();

  if (/review/.test(normalized)) {
    return "review";
  }

  if (/message/.test(normalized)) {
    return "message";
  }

  if (/negotiation|lead/.test(normalized)) {
    return "negotiation";
  }

  if (getThumbtackReview(payload)) {
    return "review";
  }

  if (getThumbtackMessage(payload)) {
    return "message";
  }

  if (getThumbtackNegotiation(payload)) {
    return "negotiation";
  }

  return "unknown";
}

function joinLabel(parts = [], maxLength = 0) {
  return cleanText(parts.filter(Boolean).join(", "), maxLength);
}

function buildLocationLabel(...sources) {
  for (const source of sources) {
    if (!source) {
      continue;
    }

    if (typeof source === "string") {
      const direct = cleanText(source, 160);

      if (direct) {
        return direct;
      }

      continue;
    }

    if (!isRecord(source)) {
      continue;
    }

    const direct = cleanText(
      source.fullAddress ||
        source.formattedAddress ||
        source.address ||
        source.locationLabel ||
        "",
      160,
    );

    if (direct) {
      return direct;
    }

    const parts = [
      source.addressLine,
      source.city,
      source.state,
      source.zipCode || source.zip,
    ];
    const joined = joinLabel(parts, 160);

    if (joined) {
      return joined;
    }
  }

  return "";
}

function buildProjectType(negotiation = {}, payload = {}) {
  const categoryName = cleanText(
    negotiation?.category?.name ||
      negotiation?.request?.category?.name ||
      payload?.category?.name ||
      "",
    80,
  );
  const serviceNames = uniqueStrings(
    [
      ...(Array.isArray(negotiation?.services)
        ? negotiation.services.map((item) => item?.name || item?.label || "")
        : []),
      ...(Array.isArray(negotiation?.request?.services)
        ? negotiation.request.services.map((item) => item?.name || item?.label || "")
        : []),
    ],
    60,
  );

  return cleanText(
    [categoryName, serviceNames.join(", ")].filter(Boolean).join(" / ") || "Thumbtack lead",
    120,
  );
}

function mapThumbtackStatusToCrmStatus(status = "", entityType = "") {
  const normalized = cleanText(status, 80).toLowerCase();

  if (!normalized) {
    return entityType === "message" ? "contacted" : "new";
  }

  if (/(won|hired|booked|completed|complete|closed_won|closed-won)/.test(normalized)) {
    return "won";
  }

  if (/(lost|declin|cancel|archiv|closed_lost|closed-lost)/.test(normalized)) {
    return "lost";
  }

  if (/(quoted|estimate|proposal)/.test(normalized)) {
    return "quoted";
  }

  if (/(contacted|responded|replied|active|in_progress|in-progress)/.test(normalized)) {
    return "contacted";
  }

  return entityType === "message" ? "contacted" : "new";
}

function isAutomatedMessage(message = {}) {
  const typeLabel = cleanText(
    message?.messageType || message?.type || message?.kind || message?.category || "",
    80,
  ).toLowerCase();

  return /(quick_reply|lead_description|system|automation|auto)/.test(typeLabel);
}

function isCustomerMessage(message = {}) {
  const sender = cleanText(
    message?.from || message?.senderType || message?.authorType || "",
    40,
  ).toLowerCase();

  if (!sender) {
    return true;
  }

  return /customer/.test(sender);
}

function buildNegotiationDetails({
  eventType = "",
  negotiation = {},
  customer = {},
  location = "",
  projectType = "",
} = {}) {
  const description = cleanText(
    negotiation?.description || negotiation?.request?.description || "",
    2200,
  );
  const status = cleanText(
    negotiation?.jobStatus || negotiation?.status || negotiation?.negotiationStatus || "",
    120,
  );
  const lines = [
    eventType ? `Thumbtack event: ${eventType}` : "Thumbtack lead received",
    negotiation?.negotiationID ? `Negotiation ID: ${cleanText(negotiation.negotiationID, 120)}` : "",
    negotiation?.requestID ? `Request ID: ${cleanText(negotiation.requestID, 120)}` : "",
    customer?.customerID ? `Customer ID: ${cleanText(customer.customerID, 120)}` : "",
    status ? `Status: ${status}` : "",
    projectType ? `Project type: ${projectType}` : "",
    location ? `Location: ${location}` : "",
    description ? `Description: ${description}` : "",
  ];

  return lines.filter(Boolean).join("\n").slice(0, 3000);
}

function buildMessageDetails({
  eventType = "",
  message = {},
  customer = {},
  projectType = "",
} = {}) {
  const text = cleanText(message?.text || "", 2200);
  const sender = cleanText(message?.from || message?.senderType || message?.authorType || "", 80);
  const lines = [
    eventType ? `Thumbtack event: ${eventType}` : "Thumbtack message received",
    message?.negotiationID ? `Negotiation ID: ${cleanText(message.negotiationID, 120)}` : "",
    message?.messageID ? `Message ID: ${cleanText(message.messageID, 120)}` : "",
    customer?.customerID ? `Customer ID: ${cleanText(customer.customerID, 120)}` : "",
    sender ? `From: ${sender}` : "",
    projectType ? `Project type: ${projectType}` : "",
    text ? `Message: ${text}` : "",
  ];

  return lines.filter(Boolean).join("\n").slice(0, 3000);
}

function buildReviewBody(review = {}, eventType = "") {
  const rating =
    review?.rating === 0 || review?.rating ? `Rating: ${String(review.rating).trim()}` : "";
  const reviewText = cleanText(review?.reviewText || review?.text || "", 800);

  return [
    eventType ? `Thumbtack event: ${eventType}` : "Thumbtack review received",
    review?.reviewID ? `Review ID: ${cleanText(review.reviewID, 120)}` : "",
    review?.reviewerName ? `Reviewer: ${cleanText(review.reviewerName, 120)}` : "",
    rating,
    reviewText ? `Review: ${reviewText}` : "",
  ]
    .filter(Boolean)
    .join("\n")
    .slice(0, 1200);
}

export function getThumbtackEventType(payload = {}) {
  const directType = cleanText(
    pickFirst(payload, [
      ["eventType"],
      ["type"],
      ["event", "type"],
      ["webhook", "eventType"],
      ["payload", "eventType"],
      ["data", "eventType"],
    ]) || "",
    80,
  );

  if (directType) {
    return directType;
  }

  if (getThumbtackReview(payload)) {
    return "ReviewCreatedV4";
  }

  if (getThumbtackMessage(payload)) {
    return "MessageCreatedV4";
  }

  if (getThumbtackNegotiation(payload)) {
    return "NegotiationCreatedV4";
  }

  return "";
}

export function getThumbtackNegotiation(payload = {}) {
  if (looksLikeNegotiation(payload)) {
    return payload;
  }

  return pickFirstRecord(
    payload,
    [
      ["negotiation"],
      ["lead"],
      ["data", "negotiation"],
      ["data", "lead"],
      ["event", "negotiation"],
      ["event", "lead"],
      ["payload", "negotiation"],
      ["payload", "lead"],
      ["resource", "negotiation"],
      ["resource", "lead"],
      ["data"],
      ["event", "data"],
      ["payload", "data"],
      ["resource"],
    ],
    looksLikeNegotiation,
  );
}

export function getThumbtackMessage(payload = {}) {
  if (looksLikeMessage(payload)) {
    return payload;
  }

  return pickFirstRecord(
    payload,
    [
      ["message"],
      ["data", "message"],
      ["event", "message"],
      ["payload", "message"],
      ["resource", "message"],
      ["data"],
      ["event", "data"],
      ["payload", "data"],
      ["resource"],
    ],
    looksLikeMessage,
  );
}

export function getThumbtackReview(payload = {}) {
  if (looksLikeReview(payload)) {
    return payload;
  }

  return pickFirstRecord(
    payload,
    [
      ["review"],
      ["data", "review"],
      ["event", "review"],
      ["payload", "review"],
      ["resource", "review"],
      ["data"],
      ["event", "data"],
      ["payload", "data"],
      ["resource"],
    ],
    looksLikeReview,
  );
}

export function buildThumbtackLeadCandidate(payload = {}) {
  const eventType = getThumbtackEventType(payload);
  const entityType = inferEntityType(eventType, payload);

  if (entityType === "review" || entityType === "unknown") {
    return null;
  }

  const negotiation = getThumbtackNegotiation(payload) || {};
  const message = getThumbtackMessage(payload) || {};
  const customer = negotiation?.customer || message?.customer || payload?.customer || {};
  const externalLeadId = cleanText(
    negotiation?.negotiationID ||
      message?.negotiationID ||
      negotiation?.requestID ||
      payload?.negotiationID ||
      message?.messageID ||
      "",
    120,
  );

  if (!externalLeadId) {
    return null;
  }

  if (entityType === "message" && (!isCustomerMessage(message) || isAutomatedMessage(message))) {
    return null;
  }

  const phoneDisplay = cleanText(
    customer?.phone ||
      customer?.phoneNumber ||
      customer?.formattedPhone ||
      payload?.customer?.phone ||
      "",
    40,
  );
  const fullName = cleanText(
    customer?.displayName ||
      customer?.name ||
      customer?.fullName ||
      payload?.customer?.displayName ||
      payload?.customer?.name ||
      (entityType === "message" ? "Thumbtack conversation" : "Thumbtack lead"),
    120,
  );
  const location = buildLocationLabel(
    negotiation?.location,
    negotiation?.request?.location,
    payload?.location,
    customer?.location,
    {
      addressLine: payload?.addressLine,
      city: payload?.city,
      state: payload?.state,
      zipCode: payload?.zipCode,
    },
  );
  const projectType = buildProjectType(negotiation, payload);
  const statusLabel = cleanText(
    negotiation?.jobStatus || negotiation?.status || negotiation?.negotiationStatus || "",
    80,
  );
  const details =
    entityType === "message"
      ? buildMessageDetails({
          eventType,
          message,
          customer,
          projectType,
        })
      : buildNegotiationDetails({
          eventType,
          negotiation,
          customer,
          location,
          projectType,
        });

  return {
    eventType,
    entityType,
    externalLeadId,
    externalSystem: "thumbtack",
    sourceType: entityType === "message" ? "thumbtack_message" : "thumbtack_negotiation",
    crmStatus: mapThumbtackStatusToCrmStatus(statusLabel, entityType),
    fullName,
    phone: normalizePhone(phoneDisplay),
    phoneDisplay,
    email: normalizeEmail(
      customer?.email || customer?.emailAddress || payload?.customer?.email || "",
    ),
    projectType,
    location,
    details,
    meta: {
      customerId: cleanText(customer?.customerID || "", 120),
      negotiationId: cleanText(
        negotiation?.negotiationID || message?.negotiationID || payload?.negotiationID || "",
        120,
      ),
      messageId: cleanText(message?.messageID || "", 120),
      statusLabel,
    },
  };
}

export function buildThumbtackWebhookEvent(payload = {}) {
  const eventType = getThumbtackEventType(payload);
  const entityType = inferEntityType(eventType, payload);
  const leadCandidate = buildThumbtackLeadCandidate(payload);
  const message = getThumbtackMessage(payload) || {};
  const review = getThumbtackReview(payload) || {};
  const activityType =
    entityType === "negotiation"
      ? "thumbtack_negotiation"
      : entityType === "message"
        ? "thumbtack_message"
        : entityType === "review"
          ? "thumbtack_review"
          : "thumbtack_webhook";
  const title =
    entityType === "negotiation"
      ? "Lead de Thumbtack"
      : entityType === "message"
        ? "Mensaje de Thumbtack"
        : entityType === "review"
          ? "Review de Thumbtack"
          : "Webhook de Thumbtack";
  let body = leadCandidate?.details || "";

  if (!body && entityType === "review") {
    body = buildReviewBody(review, eventType);
  }

  if (!body && entityType === "message") {
    body = cleanText(message?.text || "", 1200) || "Thumbtack message received.";
  }

  if (!body) {
    body = cleanText(`Thumbtack webhook received${eventType ? ` (${eventType})` : ""}.`, 1200);
  }

  return {
    eventType,
    entityType,
    leadCandidate,
    activity: {
      activityType,
      title,
      body,
      meta: {
        eventType,
        entityType,
        externalLeadId: leadCandidate?.externalLeadId || "",
        negotiationId: leadCandidate?.meta?.negotiationId || "",
        messageId: leadCandidate?.meta?.messageId || cleanText(message?.messageID || "", 120),
        reviewId: cleanText(review?.reviewID || "", 120),
      },
    },
  };
}
