function cleanText(value = "", maxLength = 0) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return maxLength > 0 ? text.slice(0, maxLength) : text;
}

function cleanMultilineText(value = "", maxLength = 0) {
  const text = String(value || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .join("\n")
    .trim();
  return maxLength > 0 ? text.slice(0, maxLength) : text;
}

function normalizePhone(value = "") {
  const digits = String(value || "").replace(/\D/g, "");
  if (digits.length === 11 && digits.startsWith("1")) return digits.slice(1);
  return digits.length === 10 ? digits : digits.slice(0, 15);
}

function normalizeSender(value = "") {
  const match = String(value || "").match(/<([^>]+)>/);
  return cleanText(match?.[1] || value, 160).toLowerCase();
}

function findSection(body = "", label = "", nextLabel = "") {
  const escapedLabel = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const escapedNextLabel = nextLabel.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = nextLabel
    ? new RegExp(`${escapedLabel}\\s*:?\\s*([\\s\\S]*?)\\n\\s*${escapedNextLabel}\\s*:`, "i")
    : new RegExp(`${escapedLabel}\\s*:?\\s*([^\\n]+)`, "i");
  const match = String(body || "").match(pattern);
  return cleanMultilineText(match?.[1] || "", 2400);
}

function extractUrls(value = "") {
  return Array.from(String(value || "").matchAll(/https?:\/\/[^\s)\]]+/gi))
    .map((match) => cleanText(match[0], 1200))
    .filter(Boolean)
    .filter((url, index, values) => values.indexOf(url) === index)
    .slice(0, 8);
}

export function buildSearchKingsSmsEmailCandidate(email = {}) {
  const gmailMessageId = cleanText(email.gmailMessageId || email.messageId || "", 120);
  const sender = normalizeSender(email.from || email.sender || "");
  const subject = cleanText(email.subject || "", 240);
  const body = cleanMultilineText(email.body || email.plainBody || "", 8000);
  const receivedAt = cleanText(email.receivedAt || email.date || "", 80);
  const subjectPhone = subject.match(/new\s+sms\s+lead\s+from\s+([+()\-\s\d.]{10,})/i)?.[1] || "";
  const fromPhone = findSection(body, "From") || subjectPhone;
  const toPhone = findSection(body, "To");
  const message = findSection(body, "Message", "Media") || findSection(body, "Message");
  const mediaUrls = extractUrls(findSection(body, "Media", "SearchKings") || body);
  const phone = normalizePhone(fromPhone);
  const zipCode = message.match(/\b\d{5}(?:-\d{4})?\b/)?.[0] || "";
  const city = /\bchicago\b/i.test(message) ? "Chicago" : "";

  if (!gmailMessageId || sender !== "calls@searchkings.com" || !/new\s+sms\s+lead/i.test(subject)) return null;
  if (!phone || !message) return null;

  const details = [
    `Gmail Message ID: ${gmailMessageId}`,
    receivedAt ? `Received At: ${receivedAt}` : "",
    `SMS From: ${cleanText(fromPhone, 40)}`,
    toPhone ? `SMS To: ${cleanText(toPhone, 40)}` : "",
    `Message: ${message}`,
    ...mediaUrls.map((url) => `Media: ${url}`),
  ].filter(Boolean).join("\n");

  return {
    eventType: "sms.received", entityType: "sms", externalLeadId: `gmail-${gmailMessageId}`,
    externalSystem: "searchkings", sourceType: "searchkings_sms", crmStatus: "new",
    fullName: "SearchKings SMS lead", phone, phoneDisplay: cleanText(fromPhone, 40),
    projectType: "Google Ads SMS lead", location: [city, "IL", zipCode].filter(Boolean).join(", "),
    zipCode, city, details: cleanMultilineText(details, 3000),
    tracking: { utmSource: "search_kings", utmMedium: "sms_lead", utmCampaign: "search_kings_google_ads" },
    activity: {
      activityType: "searchkings_sms", title: "SMS de SearchKings", body: cleanMultilineText(message, 2000),
      meta: { gmailMessageId, sender, receivedAt, toPhone: cleanText(toPhone, 40), mediaUrls },
    },
  };
}
