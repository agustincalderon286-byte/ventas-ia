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

function joinNameParts(firstName = "", lastName = "", maxLength = 0) {
  return cleanText([firstName, lastName].filter(Boolean).join(" "), maxLength);
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

    const parts = [
      source.addressLine || source.address1 || source.line1 || source.streetAddress || source.street1,
      source.city || source.locality,
      source.state || source.stateCode || source.administrativeArea || source.region,
      source.zipCode || source.zip || source.postalCode || source.postcode,
    ];
    const joined = joinLabel(parts, 160);

    if (joined) {
      return joined;
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
  }

  return "";
}

function extractLocationParts(...sources) {
  for (const source of sources) {
    if (!source) {
      continue;
    }

    if (typeof source === "string") {
      const direct = cleanText(source, 160);

      if (direct) {
        return {
          addressLine: "",
          city: "",
          state: "",
          zipCode: "",
          location: direct,
        };
      }

      continue;
    }

    if (!isRecord(source)) {
      continue;
    }

    const addressLine = cleanText(
      source.addressLine ||
        source.address1 ||
        source.line1 ||
        source.streetAddress ||
        source.street1 ||
        "",
      160,
    );
    const city = cleanText(source.city || source.locality || "", 120);
    const state = cleanText(
      source.state || source.stateCode || source.administrativeArea || source.region || "",
      40,
    );
    const zipCode = cleanText(source.zipCode || source.zip || source.postalCode || source.postcode || "", 20);
    const location = buildLocationLabel(source);

    if (addressLine || city || state || zipCode || location) {
      return {
        addressLine,
        city,
        state,
        zipCode,
        location,
      };
    }
  }

  return {
    addressLine: "",
    city: "",
    state: "",
    zipCode: "",
    location: "",
  };
}

function buildThumbtackCustomerName(customer = {}, negotiation = {}, message = {}, payload = {}, entityType = "") {
  const directName = cleanText(
    customer?.displayName ||
      customer?.name ||
      customer?.fullName ||
      pickFirst(payload, [
        ["customer", "displayName"],
        ["customer", "name"],
        ["customer", "fullName"],
        ["negotiation", "customer", "displayName"],
        ["negotiation", "customer", "name"],
        ["negotiation", "customer", "fullName"],
        ["data", "customer", "displayName"],
        ["data", "customer", "name"],
        ["data", "customer", "fullName"],
      ]) ||
      "",
    120,
  );

  if (directName) {
    return directName;
  }

  const composedName =
    joinNameParts(customer?.firstName, customer?.lastName, 120) ||
    joinNameParts(customer?.first_name, customer?.last_name, 120) ||
    joinNameParts(
      pickFirst(payload, [["customer", "firstName"], ["negotiation", "customer", "firstName"]]) || "",
      pickFirst(payload, [["customer", "lastName"], ["negotiation", "customer", "lastName"]]) || "",
      120,
    ) ||
    joinNameParts(
      pickFirst(payload, [["customer", "first_name"], ["negotiation", "customer", "first_name"]]) || "",
      pickFirst(payload, [["customer", "last_name"], ["negotiation", "customer", "last_name"]]) || "",
      120,
    );

  if (composedName) {
    return composedName;
  }

  return entityType === "message" ? "Thumbtack conversation" : "Thumbtack lead";
}

function buildThumbtackPhoneDisplay(customer = {}, negotiation = {}, message = {}, payload = {}) {
  return cleanText(
    customer?.phone ||
      customer?.formattedPhone ||
      customer?.formattedNumber ||
      customer?.phoneNumber?.formattedNumber ||
      customer?.phoneNumber?.nationalNumber ||
      (typeof customer?.phoneNumber === "string" ? customer.phoneNumber : "") ||
      pickFirst(payload, [
        ["customer", "phone"],
        ["customer", "phoneNumber"],
        ["customer", "formattedPhone"],
        ["customer", "formattedNumber"],
        ["customer", "phoneNumber", "formattedNumber"],
        ["customer", "phoneNumber", "nationalNumber"],
        ["negotiation", "customer", "phone"],
        ["negotiation", "customer", "phoneNumber"],
        ["negotiation", "customer", "formattedPhone"],
        ["negotiation", "customer", "formattedNumber"],
        ["negotiation", "customer", "phoneNumber", "formattedNumber"],
        ["negotiation", "customer", "phoneNumber", "nationalNumber"],
        ["message", "customer", "phone"],
        ["message", "customer", "phoneNumber"],
        ["message", "customer", "formattedPhone"],
        ["data", "customer", "phone"],
        ["data", "customer", "phoneNumber"],
        ["data", "customer", "formattedPhone"],
        ["data", "customer", "formattedNumber"],
        ["data", "customer", "phoneNumber", "formattedNumber"],
        ["data", "customer", "phoneNumber", "nationalNumber"],
        ["lead", "customer", "phone"],
        ["lead", "customer", "phoneNumber"],
        ["request", "customer", "phone"],
        ["request", "customer", "phoneNumber"],
        ["contact", "phone"],
        ["contact", "phoneNumber"],
      ]) ||
      "",
    40,
  );
}

function buildThumbtackEmail(customer = {}, negotiation = {}, message = {}, payload = {}) {
  return normalizeEmail(
    customer?.email ||
      customer?.emailAddress ||
      pickFirst(payload, [
        ["customer", "email"],
        ["customer", "emailAddress"],
        ["negotiation", "customer", "email"],
        ["negotiation", "customer", "emailAddress"],
        ["message", "customer", "email"],
        ["message", "customer", "emailAddress"],
        ["data", "customer", "email"],
        ["data", "customer", "emailAddress"],
        ["lead", "customer", "email"],
        ["lead", "customer", "emailAddress"],
        ["contact", "email"],
        ["contact", "emailAddress"],
      ]) ||
      "",
  );
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

  const phoneDisplay = buildThumbtackPhoneDisplay(customer, negotiation, message, payload);
  const fullName = buildThumbtackCustomerName(customer, negotiation, message, payload, entityType);
  const locationParts = extractLocationParts(
    negotiation?.location,
    negotiation?.request?.location,
    negotiation?.request?.address,
    payload?.location,
    customer?.location,
    payload?.customer?.location,
    payload?.request?.location,
    payload?.request?.address,
    {
      addressLine:
        pickFirstText(payload, [
          ["addressLine"],
          ["address1"],
          ["line1"],
          ["streetAddress"],
          ["street1"],
          ["request", "addressLine"],
          ["request", "address1"],
          ["request", "line1"],
          ["request", "streetAddress"],
          ["request", "street1"],
        ], 160),
      city: pickFirstText(payload, [["city"], ["locality"], ["request", "city"], ["request", "locality"]], 120),
      state: pickFirstText(
        payload,
        [["state"], ["stateCode"], ["administrativeArea"], ["region"], ["request", "state"], ["request", "stateCode"]],
        40,
      ),
      zipCode: pickFirstText(
        payload,
        [["zipCode"], ["zip"], ["postalCode"], ["postcode"], ["request", "zipCode"], ["request", "postalCode"]],
        20,
      ),
    },
  );
  const location = cleanText(locationParts.location || "", 160);
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
    email: buildThumbtackEmail(customer, negotiation, message, payload),
    projectType,
    location,
    addressLine: cleanText(locationParts.addressLine || "", 160),
    city: cleanText(locationParts.city || "", 120),
    zipCode: cleanText(locationParts.zipCode || "", 20),
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
