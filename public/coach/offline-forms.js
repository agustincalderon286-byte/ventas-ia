(() => {
  "use strict";
  const fieldsOf = form => {
    const fields = {};
    for (const [key, value] of new FormData(form)) {
      if (typeof value === "string") (fields[key] ||= []).push(value);
    }
    return fields;
  };
  const restoreFields = (form, fields) => {
    for (const field of Array.from(form.elements)) {
      if (!field.name || ["file", "password", "submit", "reset", "button"].includes(field.type)) continue;
      const values = fields[field.name] || [];
      if (["checkbox", "radio"].includes(field.type)) field.checked = values.includes(field.value);
      else field.value = values[0] ?? "";
    }
  };

  function attach(form, { user, kind, slot = "main", collect, contextFields = [], onRestore, onSynced, storage }) {
    const email = String(user.email || "").trim().toLowerCase();
    const accountId = String(user.id || user._id || "");
    const draftKey = `form-draft:${email}:${accountId}:${kind}:${slot}`;
    const status = document.createElement("p");
    status.className = "tec-form-status";
    status.setAttribute("role", "status");
    status.setAttribute("aria-live", "polite");
    const pending = document.createElement("details");
    pending.className = "tec-form-pending";
    pending.hidden = true;
    form.append(status, pending);
    let revision = 0, draft = null, chain = Promise.resolve(), restored = false, refreshId = 0;
    const seen = new Set();
    const say = (text, state = "idle") => { status.textContent = text; status.dataset.state = state; };
    const serialize = () => {
      const payload = collect();
      const context = draft?.context || Object.fromEntries(contextFields.map(key => [key, payload[key]]));
      return { fields: fieldsOf(form), context, payload: { ...payload, ...context, accountId },
        requestId: draft?.requestId || storage.id(kind), revision, savedAt: new Date().toISOString() };
    };
    const belongs = item => item?.ownerEmail === email && String(item.body?.accountId || "") === accountId && item.kind === kind;
    const refresh = async (initial = false) => {
      const current = ++refreshId;
      const [items, cache] = await Promise.all([storage.all("outbox"), storage.all("cache")]);
      if (current !== refreshId) return;
      const receipts = cache.filter(item => item.key?.startsWith(`form-result:${email}:`) && belongs(item));
      if (initial) receipts.forEach(item => seen.add(item.requestId));
      for (const receipt of receipts) {
        if (seen.has(receipt.requestId)) continue;
        seen.add(receipt.requestId);
        await onSynced?.(receipt.value, receipt.body);
      }
      const own = items.filter(belongs).sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));
      pending.replaceChildren();
      pending.hidden = !own.length;
      if (own.length) {
        const title = document.createElement("summary");
        title.textContent = `${own.length} pendiente${own.length === 1 ? "" : "s"} de este formulario`;
        pending.append(title);
        own.forEach(item => {
          const row = document.createElement("p");
          const label = item.body.fullName || item.body.products?.join?.(", ") || item.body.resultType || "Registro";
          row.textContent = `${label} · ${item.lastError || "Copia local guardada; esperando confirmación del servidor."}`;
          pending.append(row);
          if ([400, 422].includes(item.lastStatus) && item.formSnapshot) {
            const revise = document.createElement("button");
            revise.type = "button";
            revise.className = "secondary-button";
            revise.textContent = "Recuperar para corregir";
            revise.addEventListener("click", async () => {
              if (draft) return say("Primero guarda o limpia el borrador actual para no reemplazarlo.", "error");
              try {
                revision++;
                const corrected = { ...item.formSnapshot, revision, requestId: storage.id(kind) };
                await storage.put("cache", { key: draftKey, ownerEmail: email, accountId, kind, value: corrected });
                draft = corrected;
                restoreFields(form, corrected.fields || {});
                onRestore?.();
                // Only validation rejections can be replaced; uncertain writes cannot.
                await storage.remove("outbox", item.id);
                say("Copia recuperada. Corrige los datos y vuelve a guardar.", "draft");
                await refresh();
              } catch { say("No pude preparar la corrección. La copia local se conserva.", "error"); }
            });
            pending.append(revise);
          }
        });
      }
      if (!draft) {
        if (own.some(item => item.lastError)) say("Hay un envío que necesita atención. Abre los pendientes para ver el motivo; tu copia local se conserva.", "error");
        else if (own.length) say("Copia local guardada. Hay envíos pendientes; revisa la lista de abajo.", "pending");
        else if (receipts.length) say("Último envío confirmado por el servidor. Puedes comenzar otro formulario.", "success");
        else say("El borrador se guardará en este dispositivo cuando escribas. Todavía no has enviado este formulario.");
      }
    };
    const ready = (async () => {
      try {
        const saved = await storage.get("cache", draftKey);
        if (saved?.ownerEmail === email && saved.accountId === accountId && revision === 0 && saved.value) {
          draft = saved.value;
          revision = draft.revision || 0;
          restoreFields(form, draft.fields || {});
          onRestore?.();
          say("Borrador recuperado de este dispositivo. Aún no se ha enviado.", "draft");
          restored = true;
        }
        await refresh(true);
      } catch {
        say("No pude abrir la memoria local. No cierres el formulario hasta confirmar su guardado.", "error");
      }
    })();
    const saveDraft = () => {
      revision++;
      const snapshot = serialize();
      draft = snapshot;
      say("Guardando borrador en este dispositivo…", "saving");
      chain = chain.catch(() => null).then(async () => {
        await storage.put("cache", { key: draftKey, ownerEmail: email, accountId, kind, value: snapshot });
        if (revision === snapshot.revision) say("Borrador guardado en este dispositivo. Aún no se ha enviado.", "draft");
      }).catch(error => {
        say("No pude guardar el borrador. No cierres esta página; revisa el espacio del dispositivo.", "error");
        throw error;
      });
      chain.catch(() => null); // Reported in the form; submit still awaits the rejected promise.
    };
    const clearDraft = () => {
      revision++;
      draft = null;
      chain = chain.catch(() => null).then(() => storage.remove("cache", draftKey));
      chain.catch(() => say("No pude limpiar el borrador local. Tu copia se conserva.", "error"));
      return chain;
    };
    form.addEventListener("input", saveDraft);
    form.addEventListener("change", saveDraft);
    form.addEventListener("reset", () => { clearDraft().then(() => refresh()).catch(() => null); });
    window.addEventListener("tec:offline-status", () => { ready.then(() => refresh()).catch(() => null); });
    return {
      ready, hasDraft: () => Boolean(draft), wasRestored: () => restored,
      async submit() {
        await ready;
        await chain;
        const viewer = await storage.viewer();
        if (String(viewer?.user?.email || "").toLowerCase() !== email || String(viewer?.user?.id || viewer?.user?._id || "") !== accountId) {
          throw new Error("La cuenta cambió. Tu borrador sigue aquí; entra con la cuenta que lo creó antes de enviarlo.");
        }
        const snapshot = serialize();
        const submittedRevision = revision;
        // Queue first: a closing tab or dropped response must not lose this operation.
        await storage.queue({ id: snapshot.requestId, kind, url: `/api/coach/${({ "health-survey": "health-surveys", recruitment: "recruitment-applications", "demo-outcome": "demo-outcomes" })[kind]}`,
          method: "POST", headers: { "X-TEC-Request-ID": snapshot.requestId }, body: snapshot.payload,
          formSnapshot: snapshot, draftKey });
        if (revision === submittedRevision) { revision++; draft = null; }
        await refresh().catch(() => say("Copia local guardada. No pude actualizar la lista de pendientes.", "pending"));
        storage.flush().catch(() => refresh().catch(() => null));
        return { offlineQueued: true, requestId: snapshot.requestId, preserveDraft: Boolean(draft) };
      }
    };
  }
  window.TecOfflineForms = Object.freeze({ attach });
})();
