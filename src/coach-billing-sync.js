// Only billing operations call Stripe. Ordinary Coach requests read MongoDB.
// Every notification re-reads authoritative Stripe state; event delivery order
// is not an ordering guarantee. A MongoDB revision prevents slower concurrent
// synchronizers from overwriting a newer snapshot (including across processes).
export function createCoachBillingSync({ stripe, users, priceIds }) {
  const allowed = new Set(priceIds.filter(Boolean));
  const idOf = value => typeof value === 'string' ? value : value?.id || '';
  const itemsOf = sub => (sub?.items?.data || []).filter(item => allowed.has(idOf(item.price)));

  async function syncUser(userId, checkoutSessionId = '') {
    const reserved = await users.findOneAndUpdate(
      { _id: userId }, { $inc: { stripeSyncRevision: 1 } }, { new: true }
    );
    if (!reserved?.stripeCustomerId) return null;
    const subscriptions = [];
    let cursor;
    // Include canceled subscriptions: their current state must revoke access.
    for (;;) {
      const page = await stripe.subscriptions.list({ customer: reserved.stripeCustomerId,
        status: 'all', limit: 100, ...(cursor ? { starting_after: cursor } : {}) });
      subscriptions.push(...page.data.filter(sub =>
        idOf(sub.customer) === reserved.stripeCustomerId && itemsOf(sub).length > 0));
      if (!page.has_more) break;
      const next = page.data.at(-1)?.id;
      if (!next || next === cursor) throw new Error('Stripe subscription pagination did not advance');
      cursor = next;
    }
    const rank = sub => ['active', 'trialing'].includes(sub.status) ? 2
      : ['past_due', 'unpaid', 'paused', 'incomplete'].includes(sub.status) ? 1 : 0;
    subscriptions.sort((a, b) => rank(b) - rank(a) || (b.created || 0) - (a.created || 0)
      || (b.id === reserved.stripeSubscriptionId ? 1 : 0) - (a.id === reserved.stripeSubscriptionId ? 1 : 0)
      || a.id.localeCompare(b.id));
    const sub = subscriptions[0];
    const items = itemsOf(sub);
    const ends = items.map(item => Number(item.current_period_end)).filter(value => value > 0);
    // TEC sells one recurring item; use the earliest entitlement end if more
    // than one supported item is ever present. Keep old API compatibility.
    const end = ends.length ? Math.min(...ends) : Number(sub?.current_period_end);
    const changes = {
      stripeSubscriptionId: sub?.id || '',
      stripePriceId: idOf(items[0]?.price),
      subscriptionStatus: sub?.status || 'inactive',
      subscriptionActive: ['active', 'trialing'].includes(sub?.status),
      subscriptionCurrentPeriodEnd: end > 0 ? new Date(end * 1000) : null,
      subscriptionCancelAtPeriodEnd: Boolean(sub?.cancel_at_period_end ||
        (sub?.cancel_at && sub.cancel_at > Date.now() / 1000)),
      ...(checkoutSessionId ? { lastCheckoutSessionId: checkoutSessionId } : {}),
      updatedAt: new Date()
    };
    const updated = await users.findOneAndUpdate(
      { _id: reserved._id, stripeSyncRevision: reserved.stripeSyncRevision,
        stripeCustomerId: reserved.stripeCustomerId },
      { $set: changes }, { new: true }
    );
    return updated || users.findById(reserved._id);
  }

  async function fromCheckout(sessionId) {
    const session = await stripe.checkout.sessions.retrieve(sessionId);
    const customer = idOf(session.customer);
    const userId = session.metadata?.coachUserId || session.client_reference_id;
    const user = userId ? await users.findById(userId)
      : customer ? await users.findOne({ stripeCustomerId: customer }) : null;
    if (!user) return null;
    // Never link a session by email or trust metadata against another customer.
    if (!customer || user.stripeCustomerId !== customer) throw new Error('Checkout customer does not match Coach account');
    if (session.mode !== 'subscription' || session.status !== 'complete') return user;
    return syncUser(user._id, session.id);
  }

  async function fromSubscription(subscription) {
    const customer = idOf(subscription.customer);
    if (!customer) return null;
    const user = await users.findOne({ stripeCustomerId: customer });
    if (!user) return null;
    // The payload identifies the customer only. Its status may already be stale.
    return syncUser(user._id);
  }

  return { fromCheckout, fromSubscription, syncUser };
}
