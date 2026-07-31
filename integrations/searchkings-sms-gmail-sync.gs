// Google Apps Script: imports only SearchKings SMS alert emails into the CRM.
const SEARCHKINGS_SMS_SYNC = {
  endpoint: "https://cmwf-crm-api.onrender.com/api/integrations/searchkings/sms-email",
  query: 'from:(calls@searchkings.com) subject:(New SMS Lead) newer_than:30d',
  tokenProperty: "SEARCHKINGS_SMS_EMAIL_TOKEN",
  processedPrefix: "searchkings-sms-imported:",
};

function syncSearchKingsSmsLeads() {
  const properties = PropertiesService.getScriptProperties();
  const token = properties.getProperty(SEARCHKINGS_SMS_SYNC.tokenProperty);
  if (!token) throw new Error("Set SEARCHKINGS_SMS_EMAIL_TOKEN in Script Properties first.");

  GmailApp.search(SEARCHKINGS_SMS_SYNC.query, 0, 50).forEach((thread) => {
    thread.getMessages().forEach((message) => {
      const messageId = message.getId();
      const processedKey = `${SEARCHKINGS_SMS_SYNC.processedPrefix}${messageId}`;
      if (properties.getProperty(processedKey)) return;

      const response = UrlFetchApp.fetch(SEARCHKINGS_SMS_SYNC.endpoint, {
        method: "post", contentType: "application/json",
        headers: { "x-searchkings-sms-email-token": token },
        payload: JSON.stringify({ gmailMessageId: messageId, from: message.getFrom(),
          subject: message.getSubject(), body: message.getPlainBody(),
          receivedAt: message.getDate().toISOString() }),
        muteHttpExceptions: true,
      });
      const status = response.getResponseCode();
      if (status < 200 || status >= 300) {
        throw new Error(`CRM import failed for ${messageId}: ${status} ${response.getContentText()}`);
      }
      properties.setProperty(processedKey, new Date().toISOString());
    });
  });
}
