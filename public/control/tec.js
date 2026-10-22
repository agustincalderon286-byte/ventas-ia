const $ = selector => document.querySelector(selector);
const escape = value => String(value ?? "").replace(/[&<>"']/g, character => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[character]));
const date = value => value && Number.isFinite(new Date(value).getTime()) ? new Date(value).toLocaleString("es-US", { dateStyle: "medium", timeStyle: "short" }) : "Sin registro";
const money = value => new Intl.NumberFormat("es-US", { style: "currency", currency: "USD" }).format(Number(value) || 0);
const labels = { monthly: "Mensual", annual: "Anual", paid_other: "Otro plan activo", test: "Prueba sin cobro", admin: "Administrador", team: "Acceso de equipo", inactive: "Sin plan activo", trial: "Prueba temporal" };
let page = 1, pages = 1, overviewRequest = 0, detailRequest = 0, analysisRequest = 0;
async function api(url, options = {}) {
  const response = await fetch(url, { credentials: "same-origin", cache: "no-store", ...options });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(response.status === 401 ? "Tu sesión terminó. Vuelve a entrar al Coach." : data.error || "No pude completar la consulta.");
  return data;
}
function message(selector, text, error = false) { $(selector).textContent = text; $(selector).dataset.error = String(error); }
const metric = (value, label) => `<article class="metric"><strong>${escape(value)}</strong><span>${escape(label)}</span></article>`;
async function loadOverview() {
  const ticket = ++overviewRequest;
  $("#dashboard").hidden = true;
  message("#feedback", "Consultando actividad registrada…");
  try {
    const params = new URLSearchParams({ days: $("#days").value, page, search: $("#search").value.trim(), team: $("#team").value });
    const data = await api(`/api/control/tec/overview?${params}`);
    if (ticket !== overviewRequest) return;
    const s = data.summary, m = data.membership;
    const selectedTeam = $("#team").value;
    $("#team").innerHTML = '<option value="">Todos los equipos</option>' + (data.teams || []).map(team => `<option value="${escape(team.id)}">${escape(team.name)}</option>`).join("");
    $("#team").value = selectedTeam;
    $("#activity-metrics").innerHTML = [[s.users, "Usuarios registrados"], [s.leads, "Leads captados"], [s.appointments, "Citas en el período"],
      [s.programs, "Programas 4 en 14"], [s.demos, "Resultados de demostraciones"], [s.sales, "Ventas registradas"],
      [s.closeRate == null ? "—" : `${s.closeRate}%`, "Tasa de cierre registrada"], [s.analyses, "Análisis completados"],
      [money(s.soldAmount), "Ventas reportadas por representantes"], [s.visits, "Seguimientos del mapa"], [s.surveys, "Encuestas registradas"]].map(item => metric(...item)).join("");
    $("#membership-metrics").innerHTML = [[m.monthly, "Mensuales activas"], [m.annual, "Anuales activas"], [m.test, "Cuentas de prueba"],
      [m.admin, "Administradores"], [m.team, "Accesos de equipo"], [m.trial, "Pruebas temporales"], [m.inactive, "Sin plan activo"],
      [m.paid_other, "Otros planes activos"], [m.cancelScheduled, "Cancelaciones programadas"]].map(item => metric(...item)).join("");
    $("#billing").textContent = data.billing.available
      ? `${data.billing.incomplete ? "Consulta parcial: " : ""}${money(data.billing.grossUsd)} en ${data.billing.invoices} facturas pagadas. ${data.billing.basis || "Sin facturas elegibles en este período."}`
      : `Cobros no disponibles: ${data.billing.reason}`;
    $("#updated").textContent = `Consultado ${date(data.updatedAt)} · cobros con caché de hasta 5 minutos`;
    $("#users").innerHTML = data.users.length ? data.users.map(user => `<tr>
      <td><strong>${escape(user.name || "Sin nombre")}</strong><small>${escape(user.email)}</small><small>Upline / equipo: ${escape(user.uplineName)}</small></td>
      <td><span class="badge">${escape(labels[user.membership] || user.membership)}</span></td><td>${user.leads}</td><td>${user.appointments}</td><td>${user.programs}</td>
      <td>${user.demos} / ${user.sales}<small>${escape(money(user.soldAmount))} reportados</small></td><td>${user.analyses}</td>
      <td><small>Actividad: ${escape(date(user.lastActivityAt))}</small><small>Ingreso: ${escape(date(user.lastLoginAt))}</small></td>
      <td><button class="button secondary" data-user="${escape(user.id)}">Ver actividad</button></td></tr>`).join("") : '<tr><td colspan="9">No se encontraron cuentas con esa búsqueda.</td></tr>';
    pages = Math.max(1, Math.ceil(data.total / data.pageSize));
    $("#page-label").textContent = `Página ${page} de ${pages} · ${data.total} cuentas`;
    $("#previous").disabled = page <= 1; $("#next").disabled = page >= pages;
    $("#dashboard").hidden = false;
    message("#feedback", "");
  } catch (error) { if (ticket === overviewRequest) message("#feedback", error.message, true); }
}
function record(title, body, when) {
  return `<article class="record"><strong>${escape(title)}</strong>${body}${when ? `<small>${escape(date(when))}</small>` : ""}</article>`;
}
function section(title, records) { return `<section class="record-section"><h3>${escape(title)}</h3>${records.join("") || '<p class="empty">Sin datos en esta sección.</p>'}</section>`; }
function reveal(selector) { $(selector).hidden = false; $(selector).focus(); $(selector).scrollIntoView({ behavior: "smooth", block: "start" }); }
async function openPerson(id) {
  const ticket = ++detailRequest;
  $("#person-title").textContent = "Consultando actividad…"; $("#person-content").replaceChildren(); $("#person-note").textContent = "";
  reveal("#person");
  try {
    const data = await api(`/api/control/tec/users/${encodeURIComponent(id)}/activity?days=${$("#days").value}`);
    if (ticket !== detailRequest) return;
    $("#person-title").textContent = `${data.user.name} · ${data.user.email}`;
    $("#person-note").textContent = data.note;
    $("#person-content").innerHTML = `<div class="records-grid">${[
      section("Leads y seguimientos", data.leads.map(row => record(row.fullName || "Sin nombre", `<p>${escape(row.phone)} ${escape(row.email)}</p><p>${escape(row.status)} · ${escape(row.nextAction || "Sin siguiente acción")}</p><p>${escape(row.nextActionAt ? date(row.nextActionAt) : "")}</p>`, row.createdAt))),
      section("Citas", data.appointments.map(row => record(row.fullName || "Cita", `<p>${escape([row.address, row.city].filter(Boolean).join(", "))}</p><p>Cita: ${escape(date(row.appointmentAt))} · ${escape(row.status)}</p>`, row.createdAt))),
      section("Programas 4 en 14", data.programs.map(row => record(row.hostName || "Programa", `<p>${escape(row.startWindow)} · ${row.referrals?.length || 0} referencias</p><p>${row.rewardDeliveredAt ? "Regalo marcado como entregado" : row.rewardUnlockedAt ? "Recompensa desbloqueada" : "Sin recompensa registrada"}</p><details><summary>Ver referencias</summary>${(row.referrals || []).map(ref => `<p>${escape(ref.fullName)} · ${escape(ref.phone)}<br>${escape(ref.instantCallStatus || "Sin resultado de llamada")} · ${escape(ref.appointmentDetails || "")}</p>`).join("") || "Sin referencias"}</details>`, row.createdAt))),
      section("Resultados de demostraciones", data.outcomes.map(row => record(row.resultType === "venta" ? "Venta registrada" : row.resultType || "Resultado", `<p>${escape(money(row.saleAmount))} · ${escape((row.products || []).join(", "))}</p><p>${escape(row.summary)}</p>`, row.createdAt))),
      section("Análisis de grabaciones", data.analyses.map(row => record(row.report?.title || "Demostración", `<p>${escape(row.status)} · ${Math.round(row.durationSeconds / 60)} min${row.report ? ` · ${escape(row.report.overallScore)}/100` : ""}</p><p>${escape(row.report?.summary || "Sin reporte todavía")}</p><button class="button secondary" data-analysis="${escape(row.id)}">Abrir reporte y transcripción</button><p class="note">El audio original de este historial no está archivado.</p>`, row.createdAt))),
      section("Seguimientos del mapa", data.visits.map(row => record(row.property?.formattedAddress || "Propiedad", `<p>${escape(row.status)} · ${escape(row.notes)}</p><p>${row.nextVisitAt ? `Volver: ${escape(date(row.nextVisitAt))}` : ""}</p>`, row.createdAt))),
      section("Encuestas registradas", data.surveys.map(row => record(row.fullName || row.name || row.leadName || "Encuesta de demostración", "", row.createdAt)))
    ].join("")}</div>`;
  } catch (error) { if (ticket === detailRequest) $("#person-note").textContent = error.message; }
}
async function openAnalysis(id) {
  const ticket = ++analysisRequest;
  $("#analysis-title").textContent = "Consultando análisis…"; $("#analysis-content").replaceChildren(); reveal("#analysis");
  try {
    const { analysis } = await api(`/api/control/tec/analyses/${encodeURIComponent(id)}`);
    if (ticket !== analysisRequest) return;
    const r = analysis.report || {};
    $("#analysis-title").textContent = `${analysis.ownerName || "Representante"} · ${r.title || "Análisis"}`;
    const list = items => `<ul>${(Array.isArray(items) ? items : []).map(item => `<li>${escape(item)}</li>`).join("")}</ul>`;
    $("#analysis-content").innerHTML = `<p>${escape(r.summary || "El reporte todavía no está disponible.")}</p><p>Resultado indicado: ${escape(analysis.knownOutcomeLabel)} · ${escape(date(analysis.createdAt))} · ${Math.round(analysis.durationSeconds / 60)} minutos</p>
      ${r.overallScore == null ? "" : `<strong>Puntuación: ${escape(r.overallScore)}/100</strong>`}<p>Evaluación IA: ${escape(r.outcomeAssessment || "Sin evaluación")} · Confianza: ${escape(r.outcomeConfidence ?? "Sin dato")}</p>
      <div class="metrics compact">${Object.entries(r.scores || {}).map(([key, value]) => metric(`${value}/100`, key)).join("")}</div>
      <div class="records-grid">${section("Fortalezas", (r.strengths || []).map(row => record(row.title, `<p>${escape(row.evidence)}</p><p>${escape(row.whyItMatters)}</p>`, null)))}
      ${section("Mejoras", (r.improvements || []).map(row => record(row.title, `<p>${escape(row.evidence)}</p><p>${escape(row.recommendation)}</p>`, null)))}
      ${section("Objeciones", (r.objections || []).map(row => record(row.objection, `<p>${escape(row.responseAssessment)}</p><p>${escape(row.suggestedResponse)}</p>`, null)))}
      ${section("Momentos importantes", (r.keyMoments || []).map(row => record(`Minuto aproximado ${row.approxMinute}: ${row.label}`, `<p>${escape(row.detail)}</p>`, null)))}</div>
      <h3>Razones probables</h3>${list(r.probableReasons)}<h3>Próximos pasos</h3>${list(r.nextSteps)}<h3>Práctica recomendada</h3><p>${escape(r.practiceScript)}</p>
      <details><summary>Ver transcripción guardada</summary><pre>${escape(analysis.transcript || "Sin transcripción disponible.")}</pre></details>
      <p class="note">${escape(r.disclaimer || "La IA puede equivocarse. Los minutos son aproximados; revisa el contexto.")}</p><p class="note">Los audios anteriores no se reconstruyen a partir del texto. La reproducción original se está probando por separado, sin archivar grabaciones de representantes.</p>`;
  } catch (error) { if (ticket === analysisRequest) $("#analysis-content").textContent = error.message; }
}

let testBlob = null, testUrl = "", unsaved = false, recorder = null, stream = null, timer = null, uploading = false;
function preview(blob) {
  if (testUrl) URL.revokeObjectURL(testUrl);
  testBlob = blob; testUrl = URL.createObjectURL(blob); unsaved = true;
  $("#local-audio").src = testUrl; $("#local-audio").hidden = false;
  $("#download-test").href = testUrl; $("#download-test").download = `TEC-mi-prueba.${blob.type.includes("mp4") || blob.type.includes("m4a") ? "m4a" : blob.type.includes("wav") ? "wav" : blob.type.includes("mpeg") ? "mp3" : "webm"}`;
  $("#download-test").hidden = false;
}
async function loadAudioLab() {
  try {
    const data = await api("/api/control/audio-lab");
    $("#audio-lab").hidden = !data.enabled;
    if (!data.enabled) return;
    $("#audio-files").innerHTML = data.files.map(file => `<article class="record"><strong>${escape(file.label)}</strong><small>${escape(date(file.createdAt))} · ${(file.size / 1048576).toFixed(2)} MB · guardado privado en MongoDB</small><audio controls preload="none" src="${escape(file.url)}"></audio>${file.analysisId ? `<button class="button secondary" data-analysis="${escape(file.analysisId)}">Ver análisis vinculado</button>` : ""}<button class="button danger" data-delete-audio="${escape(file.id)}">Eliminar esta prueba</button></article>`).join("") || '<p class="empty">Todavía no hay audios guardados en este espacio de prueba.</p>';
    if (!$("#audio-analysis").dataset.loaded) {
      const own = await api("/api/coach/demo-analyses");
      const option = document.createElement("option");
      // Server enforces ownership again: team analyses cannot be linked by guessing IDs.
      for (const row of (own.analyses || []).filter(item => item.canDelete)) {
        const item = option.cloneNode(); item.value = row.id; item.textContent = `${row.ownerName || "Demo"} · ${date(row.createdAt)}`;
        $("#audio-analysis").append(item);
      }
      $("#audio-analysis").dataset.loaded = "true";
    }
  } catch (error) { message("#audio-feedback", error.message, true); }
}
$("#filters").addEventListener("submit", event => { event.preventDefault(); page = 1; ++detailRequest; ++analysisRequest; $("#person").hidden = true; $("#analysis").hidden = true; loadOverview(); });
$("#search-form").addEventListener("submit", event => { event.preventDefault(); page = 1; loadOverview(); });
$("#previous").addEventListener("click", () => { if (page > 1) { page--; loadOverview(); } });
$("#next").addEventListener("click", () => { if (page < pages) { page++; loadOverview(); } });
$("#close-person").addEventListener("click", () => { ++detailRequest; $("#person").hidden = true; });
$("#close-analysis").addEventListener("click", () => { ++analysisRequest; $("#analysis").hidden = true; });
document.addEventListener("click", async event => {
  const user = event.target.closest("[data-user]"), analysis = event.target.closest("[data-analysis]"), remove = event.target.closest("[data-delete-audio]");
  if (user) openPerson(user.dataset.user);
  if (analysis) openAnalysis(analysis.dataset.analysis);
  if (remove && confirm("¿Eliminar este audio propio de prueba? No se podrá recuperar desde TEC. Tu archivo original no se modifica.")) {
    remove.disabled = true;
    try { await api(`/api/control/audio-lab/${encodeURIComponent(remove.dataset.deleteAudio)}`, { method: "DELETE", headers: { "X-TEC-Audio-Test": "own-test-only" } }); await loadAudioLab(); }
    catch (error) { message("#audio-feedback", error.message, true); remove.disabled = false; }
  }
});
$("#audio-file").addEventListener("change", () => {
  const file = $("#audio-file").files[0];
  if (file) {
    if (file.size > 16 * 1048576) return message("#audio-feedback", "La prueba admite hasta 16 MB.", true);
    if (unsaved && !confirm("Hay una prueba sin guardar. Descárgala primero si quieres conservarla. ¿Reemplazarla?")) { $("#audio-file").value = ""; return; }
    preview(file);
  }
});
$("#record-test").addEventListener("click", async () => {
  if (!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) return message("#audio-feedback", "Este navegador no permite grabar aquí. Puedes subir un audio propio desde Archivos.", true);
  if (!$("#own-audio").checked) return message("#audio-feedback", "Confirma primero que usarás solo tu voz de prueba, sin clientes reales.", true);
  if (unsaved && !confirm("Hay una prueba sin guardar. Descárgala primero si quieres conservarla. ¿Reemplazarla?")) return;
  $("#record-test").disabled = true; $("#audio-file").disabled = true; $("#upload-audio").disabled = true;
  try {
    stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const mimeType = ["audio/mp4", "audio/webm;codecs=opus", "audio/webm"].find(type => MediaRecorder.isTypeSupported(type));
    recorder = new MediaRecorder(stream, { ...(mimeType ? { mimeType } : {}), audioBitsPerSecond: 32000 });
    const parts = [];
    recorder.addEventListener("dataavailable", event => { if (event.data.size) parts.push(event.data); });
    recorder.addEventListener("stop", () => {
      clearTimeout(timer); stream?.getTracks().forEach(track => track.stop());
      if (parts.length) preview(new Blob(parts, { type: recorder.mimeType || parts[0].type }));
      recorder = null; $("#stop-test").disabled = true; $("#record-test").disabled = false; $("#audio-file").disabled = false; $("#upload-audio").disabled = false;
      message("#audio-feedback", "Prueba terminada en este navegador. Guárdala para escuchar la copia del servidor o descarga tu copia antes de cerrar.");
    });
    recorder.addEventListener("error", () => { if (recorder?.state === "recording") recorder.stop(); });
    recorder.start(1000); $("#stop-test").disabled = false;
    timer = setTimeout(() => { if (recorder?.state === "recording") recorder.stop(); }, 300000);
    message("#audio-feedback", "Grabando tu prueba. Termina cuando quieras; se detendrá a los cinco minutos.");
  } catch (error) {
    stream?.getTracks().forEach(track => track.stop()); $("#record-test").disabled = false; $("#audio-file").disabled = false; $("#upload-audio").disabled = false;
    message("#audio-feedback", "No pude abrir el micrófono. Revisa el permiso o selecciona un audio propio.", true);
  }
});
$("#stop-test").addEventListener("click", () => { if (recorder?.state === "recording") recorder.stop(); });
$("#audio-form").addEventListener("submit", async event => {
  event.preventDefault();
  if (!testBlob?.size) return message("#audio-feedback", "Primero graba o selecciona tu audio de prueba.", true);
  if (testBlob.size > 16 * 1048576) return message("#audio-feedback", "La prueba admite hasta 16 MB.", true);
  uploading = true; $("#upload-audio").disabled = true; $("#record-test").disabled = true; $("#audio-file").disabled = true;
  message("#audio-feedback", "Guardando tu prueba privada… No cierres esta página.");
  try {
    const params = new URLSearchParams({ label: $("#audio-label").value, analysisId: $("#audio-analysis").value });
    await api(`/api/control/audio-lab?${params}`, { method: "POST", headers: { "Content-Type": testBlob.type || "audio/mp4", "X-TEC-Audio-Test": "own-test-only" }, body: testBlob });
    unsaved = false;
    await loadAudioLab(); message("#audio-feedback", "Guardada. Usa el reproductor de la lista de abajo: esa copia se lee desde el servidor, no desde la memoria del teléfono.");
  } catch (error) { message("#audio-feedback", `${error.message} Tu copia local sigue disponible para descargar.`, true); }
  finally { uploading = false; $("#upload-audio").disabled = false; $("#record-test").disabled = false; $("#audio-file").disabled = false; }
});
window.addEventListener("beforeunload", event => { if (unsaved || uploading || recorder?.state === "recording") { event.preventDefault(); event.returnValue = ""; } });
await loadOverview();
if (!$("#dashboard").hidden) await loadAudioLab();
