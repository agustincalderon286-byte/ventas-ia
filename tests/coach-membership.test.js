import test from "node:test";
import assert from "node:assert/strict";

process.env.NODE_ENV = "test";
process.env.STRIPE_PRICE_ID_MONTHLY = "price_tec_monthly_test";
process.env.STRIPE_PRICE_ID_ANNUAL = "price_tec_annual_test";

const { construirCheckoutCoach, normalizarPlanCoach, obtenerPlanCoach } = await import("../server.js");

test("acepta solamente los ciclos mensual y anual", () => {
  assert.equal(normalizarPlanCoach("monthly"), "monthly");
  assert.equal(normalizarPlanCoach("annual"), "annual");
  assert.equal(normalizarPlanCoach("trial"), "monthly");
  assert.equal(normalizarPlanCoach("cualquier-otro"), "monthly");
});

test("asocia cada membresia TEC con su precio de Stripe", () => {
  assert.deepEqual(obtenerPlanCoach("monthly"), {
    code: "monthly",
    label: "plan mensual",
    priceId: "price_tec_monthly_test",
    amountUsd: 50,
    interval: "month"
  });

  assert.deepEqual(obtenerPlanCoach("annual"), {
    code: "annual",
    label: "plan anual",
    priceId: "price_tec_annual_test",
    amountUsd: 500,
    interval: "year"
  });
});

test("construye Checkout de suscripcion sin prueba gratis", () => {
  const req = {
    protocol: "https",
    headers: { "x-forwarded-proto": "https" },
    get(name) {
      return name === "host" ? "totalelitecoaching.com" : "";
    }
  };
  const userDoc = {
    _id: "671000000000000000000001",
    stripeCustomerId: "cus_tec_test"
  };

  const { selectedPlan, checkoutConfig } = construirCheckoutCoach(req, userDoc, "annual");

  assert.equal(selectedPlan.code, "annual");
  assert.equal(checkoutConfig.mode, "subscription");
  assert.equal(checkoutConfig.customer, "cus_tec_test");
  assert.equal(checkoutConfig.line_items[0].price, "price_tec_annual_test");
  assert.equal(checkoutConfig.subscription_data.trial_period_days, undefined);
  assert.equal(checkoutConfig.success_url, "https://totalelitecoaching.com/coach/success/?session_id={CHECKOUT_SESSION_ID}");
});
