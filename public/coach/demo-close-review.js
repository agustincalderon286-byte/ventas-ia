(function () {
  "use strict";

  var OUTCOMES = { venta: "Venta", follow_up: "Seguimiento", no_venta: "No venta" };
  var FIELDS = {
    outcome: ["Resultado", "select"],
    reason: ["Motivo", "area"],
    followUp: ["Seguimiento acordado", "area"],
    cooksForCount: ["Personas para las que cocina", "num"],
    foodSpendWeekly: ["Gasto semanal en comida", "num"],
    mealPrepTime: ["Tiempo para preparar comidas", "text"],
    drinkingWaterType: ["Agua para beber", "text"],
    cookingWaterType: ["Agua para cocinar", "text"],
    waterSpendWeekly: ["Gasto semanal en agua", "num"],
    familyPriority: ["Prioridad de la familia", "text"],
    productInterest: ["Producto de interés", "text"],
    waterReading: ["Lectura del agua", "text"],
    calcProduct: ["Producto cotizado", "text"],
    calcPrice: ["Precio", "num"],
    calcDown: ["Enganche", "num"],
    quotedMonthly: ["Mensualidad cotizada", "num"],
    saleAmount: ["Monto de venta", "num"],
    sellerNote: ["Nota final (opcional)", "area"]
  };
  var seq = 0;

  function str(v) { return v === undefined || v === null ? "" : String(v).trim(); }
  function show(key, v) { return key === "outcome" && OUTCOMES[v] ? OUTCOMES[v] : v; }
  function errText(e) { return (e && e.message) || "error desconocido"; }
  function isConflict(e) { return !!e && (e.status === 409 || (e.response && e.response.status === 409)); }

  function mount(opts) {
    var root = opts.root;
    var doc = root.ownerDocument || document;
    var uid = "demo-close-" + (++seq) + "-";
    var url = "/api/coach/demo-analyses/" + encodeURIComponent(opts.analysisId) + "/close";
    var alive = true, busy = false, close = null, previous = {}, renderedSame = false, rows = {}, ui = {};

    function make(tag, cls, text) {
      var n = doc.createElement(tag);
      if (cls) n.className = cls;
      if (text !== undefined && text !== null) n.textContent = text;
      return n;
    }
    function clear() { while (root.firstChild) root.removeChild(root.firstChild); }
    function say(msg) { if (ui.status) ui.status.textContent = msg; }
    function sameVisit() {
      var v = str(opts.getVisitId && opts.getVisitId()), c = str(opts.getCrmId && opts.getCrmId());
      return !!close && !!v && v === str(close.visitId) && c === str(close.crmId);
    }
    function snapshot(values) {
      var t = opts.tools;
      if (!t || typeof t.snapshot !== "function") return {};
      return Object.assign({}, t.snapshot(values || close.fields || {}) || {});
    }
    function unwrap(r) {
      if (!r || !r.close) throw new Error("respuesta sin datos de cierre");
      return r.close;
    }

    function load() {
      if (busy || !alive) return;
      busy = true;
      if (close) say("Recargando…");
      else { clear(); root.appendChild(make("p", "demo-close", "Cargando cierre de la visita…")); }
      Promise.resolve().then(function () { return opts.request(url, { method: "GET" }); }).then(function (r) {
        busy = false;
        if (!alive || !root.isConnected) return;
        close = unwrap(r);
        render();
      }).catch(function (e) {
        busy = false;
        if (!alive || !root.isConnected) return;
        if (close && ui.form) { say("No se pudo recargar: " + errText(e) + ". Tus cambios siguen aquí."); return; }
        clear();
        var box = make("div", "demo-close");
        box.setAttribute("role", "alert");
        box.appendChild(make("p", null, "No se pudo cargar el cierre: " + errText(e)));
        var b = make("button", null, "Reintentar");
        b.type = "button";
        b.addEventListener("click", load);
        box.appendChild(b);
        root.appendChild(box);
      });
    }

    function addRow(key, parent) {
      var def = FIELDS[key];
      var audio = str((close.fields || {})[key]), formV = str(previous[key]), conf = str((close.confirmed || {})[key]);
      var declared = key === "outcome" ? str(close.declaredOutcome) : "";
      var sellerDraft = (close.sellerFields || []).includes(key);
      if (!audio) audio = declared;
      var other = formV || declared;
      var conflict = !!(audio && other && audio !== other && (!conf || sellerDraft));
      var initial = conflict ? "" : (sellerDraft ? audio : conf || audio || formV);
      var row = make("fieldset", "demo-close-row");
      var legend = make("legend", null, def[0]);
      legend.id = uid + key + "-label";
      row.appendChild(legend);
      if (conf) row.appendChild(make("p", null, "Guardado: " + show(key, conf)));

      var input;
      if (def[1] === "select") {
        input = make("select");
        input.appendChild(make("option", null, "Sin definir")).value = "";
        Object.keys(OUTCOMES).forEach(function (k) { input.appendChild(make("option", null, OUTCOMES[k])).value = k; });
      } else if (def[1] === "area") {
        input = make("textarea");
        input.rows = 2;
      } else {
        input = make("input");
        input.type = "text";
        if (def[1] === "num") input.inputMode = "decimal";
      }
      input.setAttribute("aria-labelledby", legend.id);
      input.value = initial;

      var check = make("input");
      check.type = "checkbox";
      check.checked = !!initial && initial !== conf;
      var state = { input: input, check: check, chosen: !conflict, label: def[0] };

      if (conflict) {
        var group = make("div");
        group.setAttribute("role", "radiogroup");
        group.setAttribute("aria-label", "Elige el valor de " + def[0]);
        group.appendChild(make("p", null, "Hay dos valores distintos. Elige cuál guardar."));
        [[sellerDraft ? "Aclaración" : "Grabación", audio], [formV ? "Herramienta" : "Resultado indicado", other]].forEach(function (o) {
          var l = make("label"), r = make("input");
          r.type = "radio";
          r.name = uid + key;
          r.addEventListener("change", function () {
            input.value = o[1];
            state.chosen = true;
            check.checked = o[1] !== conf;
          });
          l.appendChild(r);
          l.appendChild(doc.createTextNode(" " + o[0] + ": " + show(key, o[1])));
          group.appendChild(l);
        });
        row.appendChild(group);
      }
      function onEdit() { state.chosen = true; check.checked = str(input.value) !== conf; }
      input.addEventListener("input", onEdit);
      input.addEventListener("change", onEdit);
      row.appendChild(input);

      var cl = make("label");
      cl.appendChild(check);
      cl.appendChild(doc.createTextNode(" Guardar este campo"));
      row.appendChild(cl);
      parent.appendChild(row);
      rows[key] = state;
      return input;
    }

    function render() {
      renderedSame = sameVisit();
      previous = renderedSame ? snapshot() : {};
      rows = {};
      var confirmed = close.confirmed || {};
      var form = make("form", "demo-close");
      form.noValidate = true;
      var title = make("h2", null, "Cierre de la visita");
      title.id = uid + "title";
      form.setAttribute("aria-labelledby", title.id);
      form.appendChild(title);
      if (opts.onRespond) {
        var voice = make("button", "secondary-button", "Aclarar con el Coach");
        voice.type = "button"; voice.addEventListener("click", function () { opts.onRespond(close); }); form.appendChild(voice);
      }
      if (close.partial) form.appendChild(make("p", null, "Aviso: el audio se procesó de forma parcial. Revisa y completa lo que falte."));
      if (!renderedSame) form.appendChild(make("p", null, "Esta ficha es de otra visita o cliente. Puedes revisarla y guardarla, pero no se aplicará a las herramientas abiertas."));
      form.appendChild(make("p", null, "Marca solo lo que quieras guardar. Lo ya guardado que no cambió queda sin marcar."));

      var list = make("div");
      form.appendChild(list);
      Object.keys(FIELDS).forEach(function (k) {
        if (k !== "sellerNote" && (str((close.fields || {})[k]) || (k === "outcome" && close.declaredOutcome) || str(confirmed[k]))) addRow(k, list);
      });

      var pick = make("label", "demo-close-row", "Añadir otro campo ");
      var sel = make("select");
      sel.appendChild(make("option", null, "Elige un campo…")).value = "";
      Object.keys(FIELDS).forEach(function (k) {
        if (!rows[k] && k !== "sellerNote") sel.appendChild(make("option", null, FIELDS[k][0])).value = k;
      });
      sel.addEventListener("change", function () {
        var k = sel.value;
        if (!k || rows[k]) return;
        sel.removeChild(sel.options[sel.selectedIndex]);
        sel.value = "";
        addRow(k, list).focus();
        if (sel.options.length < 2) pick.hidden = true;
      });
      pick.appendChild(sel);
      pick.hidden = sel.options.length < 2;
      form.appendChild(pick);
      addRow("sellerNote", form);

      var saleChoice = make("label", "demo-close-sale");
      var saleConsent = make("input"); saleConsent.type = "checkbox";
      saleChoice.append(saleConsent, doc.createTextNode(" Registrar también la venta con su importe y el aviso habitual al equipo."));
      function saleVisibility() {
        saleChoice.hidden = !renderedSame || rows.outcome?.input.value !== "venta";
        if (saleChoice.hidden) saleConsent.checked = false;
      }
      form.addEventListener("change", saleVisibility); saleVisibility();
      ui.saleConsent = saleConsent; form.appendChild(saleChoice);
      var actions = make("div", "demo-close-actions");
      ui.save = make("button", null, "Guardar lo seleccionado");
      ui.save.type = "submit";
      actions.appendChild(ui.save);
      var reload = make("button", null, "Recargar ficha (descarta lo no guardado)");
      reload.type = "button";
      reload.addEventListener("click", load);
      actions.appendChild(reload);
      if (renderedSame && str(confirmed.followUp) && typeof opts.onFollowUp === "function") {
        var fu = make("button", null, "Preparar recordatorio");
        fu.type = "button";
        fu.addEventListener("click", function () {
          if (!sameVisit()) { say("La visita abierta cambió; no se preparó el recordatorio."); return; }
          opts.onFollowUp(str((close.confirmed || {}).followUp));
        });
        actions.appendChild(fu);
      }
      form.appendChild(actions);
      ui.status = make("p");
      ui.status.setAttribute("aria-live", "polite");
      form.appendChild(ui.status);
      form.addEventListener("submit", function (e) { e.preventDefault(); save(); });
      ui.form = form;
      clear();
      root.appendChild(form);
    }

    function applyTools(fields, prev, wasSame, msgs, saveSale) {
      var t = opts.tools;
      if (!wasSame || !sameVisit()) { msgs.push("No se aplicó a las herramientas abiertas porque son de otra visita."); return Promise.resolve(); }
      if (!t || typeof t.apply !== "function") return Promise.resolve();
      var cur = snapshot(fields), apply = {}, skipped = [];
      Object.keys(fields).forEach(function (k) {
        if (str(cur[k]) !== str(prev[k])) skipped.push(FIELDS[k][0]);
        else apply[k] = fields[k];
      });
      if (skipped.length) msgs.push("No se tocaron en herramientas porque cambiaron mientras revisabas: " + skipped.join(", ") + ".");
      if (!Object.keys(apply).length) return Promise.resolve();
      return Promise.resolve().then(function () {
        return t.apply(apply, { previous: prev, proposed: Object.assign({},close.fields,fields), save: true, saveSale: saveSale, analysisId: opts.analysisId, accountId: opts.accountId, visitId: close.visitId, crmId: close.crmId });
      }).then(function (res) {
        if (res && res.message) msgs.push(String(res.message));
      }, function (e) { msgs.push("No se pudo aplicar en herramientas: " + errText(e) + "."); });
    }

    function save() {
      if (busy || !close) return;
      var fields = {}, count = 0;
      for (var k in rows) {
        var s = rows[k];
        if (!s.check.checked) continue;
        if (!s.chosen) { say("Elige audio o formulario en «" + s.label + "» antes de guardar."); s.input.focus(); return; }
        var v = str(s.input.value);
        if (!v) { say("Completa o desmarca «" + s.label + "»."); return; }
        fields[k] = v;
        count++;
      }
      if (!count) { say("No hay campos marcados para guardar."); return; }
      var saveSale = ui.saleConsent.checked, prev = previous, wasSame = renderedSame, msgs = ["Guardado en la ficha de esta visita."];
      busy = true;
      ui.save.disabled = true;
      say("Guardando…");
      Promise.resolve().then(function () {
        return opts.request(url, { method: "PATCH", body: { visitId: close.visitId, version: close.version, fields: fields } });
      }).then(function (r) {
        close = unwrap(r);
        if (!alive || !root.isConnected) { busy = false; return; }
        return applyTools(fields, prev, wasSame, msgs, saveSale).then(function () {
          busy = false;
          if (!alive || !root.isConnected) return;
          render();
          say(msgs.join(" "));
          if (typeof opts.onConfirmed === "function") opts.onConfirmed(close);
        });
      }, function (e) {
        busy = false;
        if (!alive || !root.isConnected) return;
        ui.save.disabled = false;
        say(isConflict(e)
          ? "La ficha cambió en otro lugar y no se guardó. Tus cambios siguen aquí; usa Recargar para revisar la versión nueva."
          : "No se pudo guardar: " + errText(e) + ". Tus cambios siguen aquí.");
      });
    }

    load();
    return {
      reload: load,
      destroy: function () { alive = false; clear(); }
    };
  }

  window.TecDemoCloseReview = { mount: mount };
})();
