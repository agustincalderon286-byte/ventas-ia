export function initTecAddressSuggestions({ form, request, onChoose = () => {} }) {
  const input = form.elements.address, city = form.elements.city;
  const popup = form.querySelector("[data-address-popup]");
  const list = form.querySelector("[data-address-options]");
  const status = form.querySelector("[data-address-status]");
  if (!input || !city || !popup || !list || !status) return { close() {} };
  let timer, generation = 0, options = [], active = -1, composing = false;
  const cache = new Map();

  function close() {
    clearTimeout(timer);
    generation++;
    popup.hidden = true;
    input.setAttribute("aria-expanded", "false");
    input.removeAttribute("aria-activedescendant");
    options = []; active = -1;
  }
  function show(text) {
    status.textContent = text;
    popup.hidden = false;
    input.setAttribute("aria-expanded", "true");
  }
  function choose(index) {
    const option = options[index];
    if (!option) return;
    input.value = option.address;
    city.value = option.city;
    close();
    input.focus({ preventScroll: true });
    onChoose(option);
  }
  function render(payload) {
    options = payload.suggestions || [];
    active = -1;
    input.removeAttribute("aria-activedescendant");
    list.replaceChildren();
    options.forEach((option, index) => {
      const row = document.createElement("div");
      row.id = `${list.id}-${index}`;
      row.className = "tec-address-option";
      row.setAttribute("role", "option");
      row.setAttribute("aria-selected", "false");
      row.textContent = `${option.approximate ? "Quizás: " : ""}${option.formattedAddress}`;
      // Keep focus on the input so a tap can choose before the keyboard closes.
      row.addEventListener("pointerdown", event => event.preventDefault());
      row.addEventListener("click", () => choose(index));
      list.append(row);
    });
    show(options.length ? `Elige la dirección de tu cita.${payload.truncated ? " Escribe más letras para precisar." : ""}` : "Sin coincidencias. Revisa el número, la calle y la ciudad. Cobertura: Cook County.");
  }
  function schedule() {
    close();
    if (composing) return;
    const address = input.value.trim(), locality = city.value.trim();
    if (!/^\d[\dA-Z/-]*\s+[\p{L} .'-]{2,}/iu.test(address)) return;
    if (!locality) { list.replaceChildren(); show("Escribe también la ciudad de tu cita para ver direcciones sugeridas."); return; }
    if (!navigator.onLine) { show("Las sugerencias necesitan internet. Puedes seguir escribiendo la dirección."); return; }
    const token = generation;
    const key = `${locality.toUpperCase()}:${address.toUpperCase()}`;
    timer = setTimeout(async () => {
      if (token !== generation) return;
      show("Buscando direcciones…");
      list.replaceChildren();
      try {
        let payload = cache.get(key);
        if (!payload) {
          payload = await request(`/api/coach/properties/suggest?${new URLSearchParams({ address, city: locality })}`);
          if (cache.size >= 30) cache.delete(cache.keys().next().value);
          cache.set(key, payload);
        }
        if (token !== generation) return;
        if (payload.needsMore) { close(); return; }
        render(payload);
      } catch (error) {
        if (token !== generation) return;
        show(error.status === 429 ? "Espera unos momentos para ver más sugerencias. Puedes buscar la dirección completa." : "No pude cargar las sugerencias. Puedes escribir la dirección completa y pulsar Buscar propiedades.");
      }
    }, 450);
  }
  input.addEventListener("input", schedule);
  city.addEventListener("input", schedule);
  input.addEventListener("compositionstart", () => { composing = true; close(); });
  input.addEventListener("compositionend", () => { composing = false; schedule(); });
  input.addEventListener("keydown", event => {
    if (event.key === "Escape") { close(); return; }
    if (popup.hidden || !options.length) return;
    if (event.key === "Enter" && active >= 0) { event.preventDefault(); choose(active); return; }
    if (!["ArrowDown", "ArrowUp"].includes(event.key)) return;
    event.preventDefault();
    active = active < 0 ? (event.key === "ArrowDown" ? 0 : options.length - 1) : (active + (event.key === "ArrowDown" ? 1 : -1) + options.length) % options.length;
    [...list.children].forEach((row, index) => row.setAttribute("aria-selected", String(index === active)));
    const selected = list.children[active];
    input.setAttribute("aria-activedescendant", selected.id);
    selected.scrollIntoView({ block: "nearest" });
  });
  form.addEventListener("submit", close);
  document.addEventListener("pointerdown", event => { if (!popup.parentElement.contains(event.target)) close(); });
  form.addEventListener("focusout", event => { if (!popup.parentElement.contains(event.relatedTarget)) close(); });
  window.addEventListener("offline", close);
  return { close, refresh: schedule };
}
