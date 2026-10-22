(() => {
  "use strict";
  function billingState(user = {}) {
    const test = user.subscriptionStatus === "test_access";
    const team = user.accountType === "seat";
    const access = Boolean(user.accessGranted || user.subscriptionActive);
    return {
      portal: Boolean(user.managesTeam && !team && user.stripeCustomerId),
      label: test ? "Acceso de prueba sin vencimiento" : team ? (access ? "Acceso por equipo" : "Acceso de equipo pausado")
        : user.subscriptionStatus === "trialing" ? "Prueba temporal activa" : access ? "Membresía activa"
          : user.subscriptionStatus === "past_due" ? "Pago pendiente" : "Sin activar",
      period: test ? "Sin vencimiento" : team ? "Administrado por tu equipo" : null
    };
  }

  function alignFlow(container) {
    if (!container) return;
    const children = Array.from(container.children);
    const order = node => {
      const value = node.style.getPropertyValue("--coach-flow-order");
      return value.trim() ? Number(value) : node.classList.contains("dashboard-card") ? 100 : 0;
    };
    children.sort((a, b) => order(a) - order(b)).forEach(node => container.append(node));
  }

  function offerStorageKey(accountId, visitId) {
    if (!String(accountId || "").trim() || !String(visitId || "").trim()) throw new Error("Falta identificar la cuenta y la visita de esta oferta.");
    return `tec-offer-v2:${encodeURIComponent(accountId)}:${encodeURIComponent(visitId)}`;
  }
  window.TecUi = Object.freeze({ billingState, alignFlow, offerStorageKey });
})();
