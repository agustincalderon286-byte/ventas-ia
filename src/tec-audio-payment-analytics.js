import crypto from 'node:crypto';
import { MEASUREMENT_ID, CAMPAIGN_TTL, safeCampaignFields, campaignEventParams } from '../public/audiolibros/analytics-events.js';

const hash = value => crypto.createHash('sha256').update(value).digest('hex');
const id = value => typeof value === 'string' ? value : value?.id;
const audience = 'tec-audiolibros';
export const AUDIO_ANALYTICS_CONSENT = 2;

export function normalizeAudioMeasurement(value) {
  if (value?.consentVersion !== AUDIO_ANALYTICS_CONSENT ||
    !/^\d{1,20}\.\d{1,20}$/.test(value.clientId || '') || !/^\d{1,16}$/.test(String(value.sessionId || ''))) return null;
  const campaign = safeCampaignFields(value.campaign);
  // QA visits never create commercial revenue measurements.
  if (campaign.campaign_source === 'tec_test') return null;
  return { consentVersion: AUDIO_ANALYTICS_CONSENT, clientId: value.clientId,
    sessionId: String(value.sessionId), campaign };
}

export function createAudioPaymentAnalyticsModels(mongoose) {
  const model = (name, fields) => mongoose.models[name] || mongoose.model(name, new mongoose.Schema(fields));
  return {
    contexts: model('TecAudioAnalyticsContext', { _id: String, clientId: String, sessionId: String,
      consentVersion: Number, campaign: mongoose.Schema.Types.Mixed, updatedAt: Date,
      expiresAt: { type: Date, expires: 0 } }),
    deliveries: model('TecAudioAnalyticsDelivery', { _id: String, status: String, leaseUntil: Date,
      createdAt: Date, sentAt: Date, expiresAt: { type: Date, expires: 0 } })
  };
}

// Only called with Stripe events whose signature and live/test mode were verified by the server.
export function createAudioPaymentAnalytics({ contexts, deliveries, accounts, stripe, priceId,
  apiSecret = '', fetchImpl = fetch, now = () => Date.now() }) {
  const key = customer => hash('audio-analytics:' + customer);
  async function saveContext(user, raw) {
    const context = normalizeAudioMeasurement(raw);
    if (!user?.stripeCustomerId || user.isTest || user.analyticsExcluded || !context) return false;
    const time = now();
    await contexts.updateOne({ _id: key(user.stripeCustomerId) }, { $set: { ...context,
      updatedAt: new Date(time), expiresAt: new Date(time + CAMPAIGN_TTL) } }, { upsert: true });
    return true;
  }
  async function revoke(user) {
    if (user?.stripeCustomerId) await contexts.deleteOne({ _id: key(user.stripeCustomerId) });
  }
  async function deliver(invoice) {
    const customer = id(invoice?.customer);
    const subscriptionId = id(invoice?.parent?.subscription_details?.subscription || invoice?.subscription);
    if (!apiSecret || !priceId || !customer || !subscriptionId || !invoice.id || invoice.livemode !== true ||
      invoice.status !== 'paid' || invoice.paid_out_of_band === true ||
      !Number.isSafeInteger(invoice.amount_paid) || invoice.amount_paid <= 0 || invoice.currency !== 'usd') return false;
    const user = await accounts.findOne({ stripeCustomerId: customer }).lean();
    if (!user || user.isTest || user.analyticsExcluded) return false;
    const time = now();
    const context = await contexts.findById(key(customer)).lean();
    if (!context || new Date(context.expiresAt).getTime() <= time || !normalizeAudioMeasurement(context)) return false;
    // Modern Stripe invoices omit paid_out_of_band. Require settled Stripe
    // payments so a manually marked-paid invoice cannot create revenue.
    if (invoice.paid_out_of_band !== false) {
      let paid = 0;
      const settled = payment => payment.status === 'paid' && payment.livemode === true &&
        payment.currency === invoice.currency && id(payment.invoice) === invoice.id &&
        ['payment_intent', 'charge'].includes(payment.payment?.type) &&
        Boolean(id(payment.payment[payment.payment.type])) && Number.isSafeInteger(payment.amount_paid) && payment.amount_paid > 0;
      if (invoice.payments?.data && invoice.payments.has_more === false) {
        for (const payment of invoice.payments.data) if (settled(payment)) paid += payment.amount_paid;
      } else {
        for await (const payment of stripe.invoicePayments.list({ invoice: invoice.id, status: 'paid', limit: 100 })) {
          if (settled(payment)) paid += payment.amount_paid;
        }
      }
      if (paid < invoice.amount_paid) return false;
    }
    const subscription = await stripe.subscriptions.retrieve(subscriptionId);
    if (subscription.metadata?.audience !== audience ||
      !subscription.items?.data?.some(item => id(item.price) === priceId)) return false;
    const paidAt = invoice.status_transitions?.paid_at;
    if (!Number.isSafeInteger(paidAt) || paidAt * 1000 > time || time - paidAt * 1000 >= 72 * 3600000) return false;
    const deliveryId = hash('audio-invoice:' + invoice.id);
    let claim;
    try {
      claim = await deliveries.findOneAndUpdate({ _id: deliveryId, status: { $ne: 'sent' },
        $or: [{ leaseUntil: { $exists: false } }, { leaseUntil: { $lte: new Date(time) } }] },
      { $set: { status: 'sending', leaseUntil: new Date(time + 60000) },
        $setOnInsert: { createdAt: new Date(time), expiresAt: new Date(time + 400 * 86400000) } },
      { upsert: true, new: true }).lean();
    } catch (error) { if (error.code === 11000) return false; throw error; }
    if (!claim) return false;
    const tax = (invoice.total_taxes || invoice.total_tax_amounts || []).reduce((sum, item) => sum + (item.amount || 0), 0);
    const value = Math.max(0, invoice.amount_paid - tax) / 100;
    const campaign = safeCampaignFields(context.campaign);
    const dimensions = campaignEventParams(campaign);
    const params = { transaction_id: 'tec_audio_' + deliveryId.slice(0, 40), currency: 'USD', value,
      tax: tax / 100, engagement_time_msec: 1, tec_area: 'audiolibros',
      tec_audio_payment_type: invoice.billing_reason === 'subscription_create' ? 'new_subscription' : 'renewal',
      // Preserve reviewed acquisition labels for reporting without inventing a
      // new campaign interaction on an automatic renewal. GA joins the original
      // online session using client_id/session_id when it is still eligible.
      ...(campaign.campaign_source ? { tec_audio_source: campaign.campaign_source,
        tec_audio_medium: campaign.campaign_medium, tec_audio_campaign: campaign.campaign_name,
        tec_audio_content: dimensions.tec_audio_content, tec_audio_piece: dimensions.tec_audio_piece } : {}),
      items: [{ item_id: 'tec_audio_monthly', item_name: 'Membresía Audiolibros TEC', price: value, quantity: 1 }] };
    // Never attach a renewal to a month-old online session.
    if (time - Number(context.sessionId) * 1000 < 24 * 3600000 && Number(context.sessionId) * 1000 <= time) {
      params.session_id = context.sessionId;
    }
    const payload = { client_id: context.clientId, timestamp_micros: paidAt * 1000000,
      consent: { ad_user_data: 'DENIED', ad_personalization: 'DENIED' }, events: [{ name: 'purchase', params }] };
    try {
      // Recheck revocation immediately before the external request.
      const latest = await contexts.findById(key(customer)).lean();
      if (!latest || latest.clientId !== context.clientId || new Date(latest.expiresAt).getTime() <= now()) {
        await deliveries.deleteOne({ _id: deliveryId }); return false;
      }
      const url = new URL('https://www.google-analytics.com/mp/collect');
      url.searchParams.set('measurement_id', MEASUREMENT_ID); url.searchParams.set('api_secret', apiSecret);
      const response = await fetchImpl(url, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload), signal: AbortSignal.timeout(5000) });
      if (!response.ok) throw new Error('Audio analytics delivery failed');
      await deliveries.updateOne({ _id: deliveryId }, { $set: { status: 'sent', sentAt: new Date(now()) }, $unset: { leaseUntil: 1 } });
      return true;
    } catch {
      await deliveries.updateOne({ _id: deliveryId }, { $set: { status: 'pending' }, $unset: { leaseUntil: 1 } });
      // No URL, secret, customer, email or invoice data in errors/logs.
      throw new Error('Audio analytics delivery failed');
    }
  }
  async function handleStripeEvent(event) {
    if (!apiSecret || event?.livemode !== true) return false;
    if (['invoice.paid', 'invoice.payment_succeeded'].includes(event.type)) return deliver(event.data.object);
    if (event.type === 'checkout.session.completed' && event.data.object.metadata?.audience === audience) {
      const session = event.data.object;
      if (session.payment_status !== 'paid' || !id(session.invoice)) return false;
      return deliver(await stripe.invoices.retrieve(id(session.invoice)));
    }
    return false;
  }
  return { saveContext, revoke, handleStripeEvent, configured: Boolean(apiSecret) };
}
