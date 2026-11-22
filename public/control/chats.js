// Pantalla del administrador para revisar preguntas y respuestas del Coach. Todo el texto de las
// conversaciones se escribe con textContent: nada de lo que dijo un vendedor o el Coach se interpreta como HTML.
const $ = selector => document.querySelector(selector);
const date = value => new Date(value).toLocaleString("es-US", { dateStyle: "medium", timeStyle: "short" });
const make = (tag, text = "", className = "") => { const node = document.createElement(tag); if (text) node.textContent = text; if (className) node.className = className; return node; };
const toolLabels = { consultar_agenda: "agenda", buscar_horarios_libres: "horarios libres", leer_bandeja: "correo", leer_hilo: "conversación de correo", revisar_ventas_hycite: "ventas de Hy Cite",
  buscar_referidos: "hoja", priorizar_llamadas: "llamadas por prioridad", leads_por_zona: "leads por zona", leads_por_distancia: "leads por distancia", buscar_contactos: "contactos",
  estado_conexiones: "conexiones", preparar_nota: "preparó una nota", preparar_cita: "preparó una cita", preparar_alta: "preparó un alta", preparar_mensaje: "preparó un mensaje" };
let page = 1, pages = 1, request = 0, sellersLoaded = false;

function message(text, error = false) { $("#feedback").textContent = text; $("#feedback").dataset.error = String(error); }
async function api(url, options = {}) {
  const response = await fetch(url, { credentials: "same-origin", cache: "no-store", ...options });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(response.status === 401 ? "Tu sesión terminó. Vuelve a entrar al Coach." : data.error || "No pude completar la consulta.");
  return data;
}

function turn(label, text, className) {
  const box = make("div", "", "turn " + className);
  box.append(make("strong", label), make("pre", text || "Sin respuesta registrada."));
  return box;
}

function entryCard(entry) {
  const card = make("article", "", "chat-entry");
  card.dataset.flagged = String(Boolean(entry.flag));
  const header = make("header");
  header.append(make("h3", entry.sellerName), make("small", date(entry.at)), make("span", entry.source === "asistente" ? "Asistente de cuentas" : "Coach", "badge"));
  card.append(header, turn("Pregunta", entry.question, "question"), turn("Respuesta", entry.answer, "answer"));
  if (entry.tools?.length) {
    const used = entry.tools.map(step => { const [name, state] = step.split(":"); return (toolLabels[name] || name) + (["ok", "preparado"].includes(state) ? "" : " (" + state.replaceAll("_", " ") + ")"); });
    card.append(make("p", "Lo que hizo: " + used.join(", ") + ".", "note"));
  }
  const mark = make("div", "", "mark");
  const note = make("input");
  note.type = "text"; note.maxLength = 500; note.placeholder = "Qué estuvo mal o qué debió responder"; note.value = entry.flag?.note || "";
  note.setAttribute("aria-label", "Nota sobre esta respuesta");
  const save = make("button", entry.flag ? "Guardar nota" : "Marcar como mala", "button secondary");
  save.type = "button";
  save.addEventListener("click", () => setFlag(entry.id, { nota: note.value }, save));
  mark.append(note, save);
  if (entry.flag) {
    const clear = make("button", "Quitar marca", "button secondary");
    clear.type = "button";
    clear.addEventListener("click", () => setFlag(entry.id, { quitar: true }, clear));
    mark.append(clear);
  }
  card.append(mark);
  return card;
}

async function setFlag(id, change, button) {
  button.disabled = true;
  try {
    await api("/api/control/coach-chats/marca", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id, ...change }) });
    await load();
  } catch (error) { message(error.message, true); button.disabled = false; }
}

async function load() {
  const ticket = ++request;
  message("Cargando conversaciones…");
  const query = new URLSearchParams({ pagina: String(page) });
  for (const [name, selector] of [["vendedor", "#seller"], ["origen", "#source"], ["desde", "#from"], ["hasta", "#to"], ["buscar", "#search"]]) if ($(selector).value) query.set(name, $(selector).value.trim());
  if ($("#flagged").checked) query.set("marcadas", "1");
  try {
    const data = await api("/api/control/coach-chats?" + query);
    if (ticket !== request) return;
    page = data.page; pages = data.pages;
    if (!sellersLoaded) {
      for (const person of data.sellers) { const option = make("option", person.name + (person.email ? " · " + person.email : "")); option.value = person.id; $("#seller").append(option); }
      sellersLoaded = true;
    }
    $("#retention").textContent = `Las conversaciones del Coach se conservan ${Math.round(data.retention.coachDays / 30)} meses y las del asistente de cuentas ${data.retention.assistantDays} días, sin teléfonos, direcciones ni correos de clientes. Después se borran solas.`;
    $("#chats").replaceChildren(...data.entries.map(entryCard));
    $("#count").textContent = data.total + (data.total === 1 ? " conversación" : " conversaciones");
    $("#page-label").textContent = `Página ${data.page} de ${data.pages}`;
    $("#previous").disabled = data.page <= 1; $("#next").disabled = data.page >= data.pages;
    message(data.total ? (data.limited ? "Se revisaron las 600 conversaciones más recientes del período. Acorta las fechas para ver las anteriores." : "") : "No hay conversaciones con esos filtros. Sin fechas se muestran los últimos 30 días.");
  } catch (error) { if (ticket === request) { $("#chats").replaceChildren(); message(error.message, true); } }
}

$("#filters").addEventListener("submit", event => { event.preventDefault(); page = 1; load(); });
$("#previous").addEventListener("click", () => { if (page > 1) { page--; load(); } });
$("#next").addEventListener("click", () => { if (page < pages) { page++; load(); } });
load();
