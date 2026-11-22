(() => {
  "use strict";

  const surveyKeys = ["cooksForCount", "foodSpendWeekly", "mealPrepTime", "drinkingWaterType",
    "cookingWaterType", "waterSpendWeekly", "familyPriority", "productInterest"];
  const outcomeKeys = ["outcome", "reason", "saleAmount"];
  const calcKeys = ["calcProduct", "calcPrice", "calcDown"];
  const calcDraftKey = "tec-visit-close-calculator";
  const named = (form, name) => form?.elements.namedItem(name);
  const valueOf = input => String(input?.value ?? "").trim();
  const fold = value => String(value || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
  const currentVisit = () => window.sessionStorage.getItem("tec-current-visit") || "";
  const activeClient = () => window.getActiveCoachCrmContext?.() || null;

  function formBelongs(form, visitId, client) {
    if (!form || !visitId || form.dataset.tecVisitId !== visitId) return false;
    const controller = form._tecOfflineController;
    if (!controller) return false;
    if (controller.hasDraft()) {
      const context = controller.getDraftContext?.();
      if (context?.visitId !== visitId || (context.activeCrmRecordId || "") !== (client?.id || "")) return false;
    }
    const savedSurvey = valueOf(named(form, "surveyId"));
    if (savedSurvey && client?.linkedHealthSurveyId !== savedSurvey) return false;
    const formPhone = valueOf(named(form, "phone"));
    const formName = valueOf(named(form, "fullName"));
    if (client?.phone && formPhone && window.normalizeLeadPhone(client.phone) !== window.normalizeLeadPhone(formPhone)) return false;
    return !client?.leadName || !formName || fold(client.leadName) === fold(formName);
  }

  function findSlot(slots, product, aliases, isEmpty) {
    const matching = product ? slots.filter(slot => aliases(slot).some(value => value && fold(value) === fold(product))) : [];
    if (matching.length) return matching.length === 1 ? matching[0] : null;
    return slots.find(isEmpty) || null;
  }

  function tools(fields, visitId, client) {
    const survey = document.querySelector("[data-health-survey-form]");
    const outcome = document.querySelector("[data-demo-outcome-form]");
    const calc = document.querySelector("[data-order-calc]");
    const nodes = {};
    let selectedSurveyCode = "", selectedCalcCode = "";
    if (formBelongs(survey, visitId, client)) {
      for (const key of surveyKeys) nodes[key] = named(survey, key);
      const slots = [1, 2, 3].map(i => named(survey, "topProduct" + i)).filter(Boolean);
      const code = slot => valueOf(named(survey, "topProductCode" + slot.name.slice(-1)));
      nodes.productInterest = findSlot(slots, fields.productInterest, slot => [valueOf(slot), code(slot)], slot => !valueOf(slot));
      if (nodes.productInterest) selectedSurveyCode = code(nodes.productInterest);
    }
    if (formBelongs(outcome, visitId, client)) {
      nodes.outcome = named(outcome, "resultType");
      nodes.reason = named(outcome, "privateReason");
      nodes.saleAmount = named(outcome, "saleAmount");
    }
    if (calc?.dataset.orderCalcSyncReady === "true") {
      const cards = Array.from(calc.querySelectorAll("[data-order-calc-product]"));
      const name = card => card.querySelector("[data-order-calc-name]");
      const slot = findSlot(cards, fields.calcProduct, card => [valueOf(name(card)), card.dataset?.orderCalcCatalogCode],
        card => !Array.from(card.querySelectorAll("input")).some(input => valueOf(input)));
      nodes.calcProduct = slot && name(slot);
      selectedCalcCode = slot?.dataset?.orderCalcCatalogCode || "";
      // A price without a product must never land on an arbitrary row.
      nodes.calcPrice = fields.calcProduct && slot?.querySelector("[data-order-calc-price]");
      // Applying a global down payment on top of item-level payments would count it twice.
      if (!cards.some(card => Number(valueOf(card.querySelector("[data-order-calc-down]"))) > 0)) {
        nodes.calcDown = calc.querySelector("[data-order-calc-down-total]");
      }
    }
    return { survey, outcome, nodes, selectedSurveyCode, selectedCalcCode };
  }

  function readExisting(key, field, outcome) {
    const value = valueOf(field);
    if (key === "outcome" && value === "venta" && !outcome?.dataset.closeOutcomeChosen &&
        !outcome?._tecOfflineController?.hasDraft() && !valueOf(named(outcome, "saleAmount")) &&
        !valueOf(named(outcome, "privateReason")) && !valueOf(named(outcome, "products"))) return "";
    return value;
  }

  function snapshot(fields = {}) {
    const { nodes, outcome } = tools(fields, currentVisit(), activeClient());
    return Object.fromEntries(Object.entries(nodes).filter(([, input]) => input)
      .map(([key, input]) => [key, readExisting(key, input, outcome)]));
  }

  function write(input, value) {
    input.value = value;
    input.dispatchEvent(new Event(input.tagName === "SELECT" ? "change" : "input", { bubbles: true }));
  }

  function usableValue(key, field, value) {
    if (!value || value.length > 320 || (field.maxLength > 0 && value.length > field.maxLength)) return "";
    if (field.tagName === "SELECT") {
      return Array.from(field.options).find(option => fold(option.value) === fold(value))?.value || "";
    }
    if (key === "cooksForCount") return /^\d+$/.test(value) && Number(value) >= 1 && Number(value) <= 50 ? value : "";
    if (["saleAmount", "calcPrice", "calcDown"].includes(key)) {
      return /^\d+(?:\.\d{1,2})?$/.test(value) && Number.isSafeInteger(Math.round(Number(value) * 100)) &&
        (key === "calcDown" || Number(value) > 0) ? value : "";
    }
    return value;
  }

  function validScenarioWith(field, value) {
    const calc = document.querySelector("[data-order-calc]");
    if (!calc || !field || !window.TecOrderCalculator) return false;
    const read = (root, name) => {
      const input = root.querySelector("[data-order-calc-" + name + "]");
      return input === field ? value : valueOf(input);
    };
    const products = Array.from(calc.querySelectorAll("[data-order-calc-product]"), card => ({
      basePrice: read(card, "price"), down: read(card, "down"),
      descFixed: read(card, "desc-fixed"), descPercent: read(card, "desc-percent")
    }));
    return window.TecOrderCalculator.calculateOrder({ products, summary: {
      extraDown: read(calc, "down-total"), extraDescFixed: read(calc, "desc-fixed-total"),
      extraDescPercent: read(calc, "desc-percent-total")
    } }).valid;
  }

  function toolInputs(form) {
    return Array.from(form?.elements || []).filter(input => input.name && !input.disabled &&
      !["button", "submit", "reset", "file"].includes(input.type) &&
      (!["checkbox", "radio"].includes(input.type) || input.checked) && valueOf(input));
  }

  // Only send reviewed fields. The normal survey endpoint replaces a survey on edit.
  // Edits to existing surveys therefore remain in their own editor.
  function surveyPayload({ survey, nodes }, applied, client, options) {
    const selected = surveyKeys.filter(key => applied.includes(key));
    if (!selected.length || !client?.id || options.crmId !== client.id || client.linkedHealthSurveyId ||
        window.getActiveCoachHealthSurveyContext?.()?.id || valueOf(named(survey, "surveyId"))) return null;
    const fullName = valueOf(named(survey, "fullName"));
    const phone = window.normalizeLeadPhone?.(valueOf(named(survey, "phone"))) || "";
    if (fold(fullName) !== fold(client.leadName) || !fullName || !/^\d{10}$/.test(phone) ||
        phone !== window.normalizeLeadPhone(client.phone)) return null;
    const allowed = new Set([named(survey, "fullName"), named(survey, "phone"), ...selected.map(key => nodes[key])]);
    if (toolInputs(survey).some(input => !allowed.has(input))) return null;
    const payload = { fullName, phone };
    for (const key of selected) {
      if (key === "productInterest") payload.topProducts = [valueOf(nodes[key])];
      else payload[key] = valueOf(nodes[key]);
    }
    return payload;
  }

  function outcomePayload({ outcome, nodes }, applied, client, options) {
    if (!applied.includes("outcome") || client?.latestDemoOutcomeId || window.getActiveCoachDemoOutcomeContext?.()?.id) return null;
    const resultType = valueOf(nodes.outcome);
    const selected = outcomeKeys.filter(key => applied.includes(key));
    if (toolInputs(outcome).some(input => !selected.some(key => nodes[key] === input))) return null;
    if (resultType === "venta" && !(options.saveSale === true && selected.includes("saleAmount") && Number(valueOf(nodes.saleAmount)) > 0)) return null;
    if (resultType !== "venta" && !selected.includes("reason")) return null;
    const payload = { resultType };
    if (selected.includes("reason")) payload.privateReason = valueOf(nodes.reason);
    if (resultType === "venta") payload.saleAmount = valueOf(nodes.saleAmount);
    return payload;
  }

  const samePayload = (a, b) => a && Object.keys(a).length === Object.keys(b).length &&
    Object.keys(b).every(key => JSON.stringify(a[key]) === JSON.stringify(b[key]));

  async function saveTool(kind, form, payload, options) {
    const { analysisId, accountId, visitId, crmId } = options;
    const id = `close-${analysisId}-${kind}`;
    const body = { ...payload, visitId, activeCrmRecordId: crmId || "", accountId };
    const stamp = () => JSON.stringify(toolInputs(form).map(input => [input.name, input.value]));
    const before = stamp();
    const viewer = await window.getTecCachedViewer?.();
    if (String(viewer?.user?.id || viewer?.user?._id || "") !== accountId) throw new Error("account");
    const email = String(viewer.user.storageIdentity || viewer.user.email || "").trim().toLowerCase();
    const receiptKey = `form-result:${email}:${id}`;
    const [receipt, queued] = await Promise.all([window.tecOfflineGet("cache", receiptKey), window.tecOfflineGet("outbox", id)]);
    const earlier = receipt || queued;
    if (earlier && !samePayload(earlier.body, body)) return "previous";
    if (!receipt && !queued) {
      if (visitId !== currentVisit() || (crmId || "") !== (activeClient()?.id || "") || before !== stamp()) throw new Error("visit");
      const route = kind === "health-survey" ? "health-surveys" : "demo-outcomes";
      await window.apiRequest("/api/coach/" + route, { method: "POST", requestId: id,
        body, offlineKind: kind, offlineQueue: true, queueFirst: true });
    }
    if (visitId === currentVisit() && (crmId || "") === (activeClient()?.id || "") && before === stamp()) {
      if (kind === "health-survey" && receipt?.value?.survey) form.showClosedSurvey?.(receipt.value.survey);
      else if (!receipt || kind === "demo-outcome") form.reset();
    }
    if (receipt) return "saved";
    await window.flushTecOfflineOutbox().catch(() => null);
    const confirmed = await window.tecOfflineGet("cache", receiptKey);
    if (confirmed) return "saved";
    const pending = await window.tecOfflineGet("outbox", id);
    return pending?.lastError ? "review" : "queued";
  }

  async function apply(fields = {}, options = {}) {
    const { previous = {}, visitId = currentVisit(), crmId, proposed = fields } = options;
    const applied = [], skipped = [], messages = [];
    const keys = [...surveyKeys, ...outcomeKeys, ...calcKeys].filter(key => Object.hasOwn(fields, key));
    const client = activeClient();
    if (!visitId || visitId !== currentVisit() || (crmId !== undefined && crmId !== (client?.id || ""))) {
      return { applied, skipped: keys, message: "La visita activa cambió. Tus datos siguen en el cierre de la demo." };
    }
    const forms = [document.querySelector("[data-health-survey-form]"), document.querySelector("[data-demo-outcome-form]")];
    await Promise.all(forms.map(form => form?._tecOfflineController?.ready));
    if (currentVisit() !== visitId || (activeClient()?.id || "") !== (client?.id || "")) {
      return { applied, skipped: keys, message: "Cambió la visita. Los datos siguen en el cierre." };
    }
    if (options.save && options.accountId) {
      const viewer = await window.getTecCachedViewer?.();
      if (String(viewer?.user?.id || viewer?.user?._id || "") !== options.accountId || currentVisit() !== visitId ||
          (activeClient()?.id || "") !== (client?.id || "")) {
        return { applied, skipped: keys, message: "La cuenta o la visita cambió. Los datos siguen en el cierre." };
      }
    }
    const targets = tools({ ...proposed, ...fields }, visitId, client);
    const { survey, outcome, nodes } = targets;
    for (const key of keys) {
      const field = nodes[key];
      const current = readExisting(key, field, outcome);
      const aliases = key === "productInterest" ? [current, targets.selectedSurveyCode]
        : key === "calcProduct" ? [current, targets.selectedCalcCode] : [];
      const sameProduct = current && aliases.some(alias => alias && fold(alias) === fold(fields[key]));
      const value = sameProduct ? current : field && usableValue(key, field, String(fields[key] ?? "").trim());
      const changedSinceReview = Object.hasOwn(previous, key) ? previous[key] !== current : !!current;
      let blocked = !value || (current !== value && changedSinceReview);
      if (key === "outcome" && value !== "venta" && valueOf(named(outcome, "saleAmount"))) blocked = true;
      if (key === "saleAmount" && (valueOf(nodes.outcome) !== "venta" || skipped.includes("outcome"))) blocked = true;
      if (["calcPrice", "calcDown"].includes(key) && !validScenarioWith(field, value)) blocked = true;
      if (key === "calcPrice" && (!valueOf(nodes.calcProduct) || ![valueOf(nodes.calcProduct), targets.selectedCalcCode]
          .some(text => text && fold(text) === fold(fields.calcProduct || proposed.calcProduct)))) blocked = true;
      if (blocked) { skipped.push(key); continue; }
      if (current !== value) write(field, value);
      applied.push(key);
    }
    const updates = [];
    if (applied.some(key => surveyKeys.includes(key))) {
      if (client?.id && crmId === client.id) {
        for (const [name, val] of [["fullName", client.leadName], ["phone", window.normalizeLeadPhone?.(client.phone)]]) {
          const input = named(survey, name);
          if (input && !valueOf(input) && val) write(input, val);
        }
      }
      updates.push(reportSave("health-survey", "Encuesta", survey, surveyPayload(targets, applied, client, options),
        "Encuesta en borrador. Revisa la casa y pulsa Guardar encuesta."));
    }
    if (applied.some(key => outcomeKeys.includes(key))) {
      updates.push(reportSave("demo-outcome", "Resultado", outcome, outcomePayload(targets, applied, client, options),
        valueOf(nodes.outcome) === "venta" && !Number(valueOf(nodes.saleAmount))
        ? "El resultado queda en borrador hasta que confirmes el importe de la venta."
        : "Resultado preparado. Revísalo y pulsa Guardar resultado."));
    }
    const calcValues = Object.fromEntries(calcKeys.filter(key => applied.includes(key)).map(key => [key, fields[key]]));
    if (Object.keys(calcValues).length) {
      messages.push("Revisa el escenario en la calculadora.");
      if (options.save && options.accountId) {
        try {
          window.sessionStorage.setItem(calcDraftKey, JSON.stringify({
            accountId: options.accountId, visitId, crmId: crmId || "", values: calcValues,
            product: fields.calcProduct || proposed.calcProduct || ""
          }));
        } catch { /* The confirmed copy is still available in the visit's closing record. */ }
      }
    }
    await Promise.all(updates);
    if (skipped.length) messages.push("Hay datos que debes revisar en la herramienta antes de aplicarlos.");
    return { applied, skipped, message: messages.join(" ") };

    async function reportSave(kind, label, form, payload, fallback) {
      if (!options.save || !/^[a-f\d]{24}$/i.test(options.analysisId || "") || !options.accountId || !payload) {
        messages.push(fallback); return;
      }
      try {
        const result = await saveTool(kind, form, payload, options);
        const status = { saved: "confirmado en TEC", queued: "en cola de sincronización",
          review: "pendiente de revisión en la lista de envíos de la herramienta",
          previous: "ya enviado con otros datos; revisa el registro anterior antes de editar" };
        messages.push(`${label} · ${status[result]}.`);
      } catch {
        messages.push(`${label} pendiente de comprobar. Reintenta desde este cierre para evitar duplicados.`);
      }
    }
  }

  function mountCalculator(root) {
    const clear = () => { try { window.sessionStorage.removeItem(calcDraftKey); } catch { /* Storage unavailable. */ } };
    root.addEventListener("input", clear);
    root.addEventListener("click", event => { if (event.target.closest("[data-order-calc-suggestion]")) clear(); });
    window.addEventListener("coach:new-client", clear);
    // The form and authenticated viewer finish initialization after the calculator mounts.
    queueMicrotask(async () => {
      try {
        const draft = JSON.parse(window.sessionStorage.getItem(calcDraftKey));
        if (!draft || draft.visitId !== currentVisit() || draft.crmId !== (activeClient()?.id || "")) return;
        const viewer = await window.getTecCachedViewer?.();
        if (!draft.accountId || draft.accountId !== String(viewer?.user?.id || viewer?.user?._id || "") ||
            window.sessionStorage.getItem(calcDraftKey) !== JSON.stringify(draft)) return;
        const result = await apply(draft.values, { visitId: draft.visitId, crmId: draft.crmId,
          proposed: { ...draft.values, calcProduct: draft.product } });
        // The normal input handlers also run during restoration. Retain only unchanged receipts.
        if (!result.skipped.length && draft.visitId === currentVisit() && draft.crmId === (activeClient()?.id || "")) {
          window.sessionStorage.setItem(calcDraftKey, JSON.stringify(draft));
        }
      } catch { /* The normal calculator remains available. */ }
    });
  }

  window.TecDemoCloseTools = Object.freeze({ snapshot, apply, mountCalculator });
})();
