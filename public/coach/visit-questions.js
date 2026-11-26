/* Coach Jorge's questions about a recorded visit. They stay pinned above the chat box while the seller
   answers with a voice message or text; one answer can settle several, and each is saved where it belongs. */
(function () {
  "use strict";
  function init({ accountId, request, addMessage, openChat, forms }) {
    let items = [], loading = null;
    const cards = forms.filter(Boolean).map(form => {
      const card = document.createElement("section");
      card.className = "visit-questions-card";
      card.hidden = true;
      card.setAttribute("aria-live", "polite");
      form.parentNode.insertBefore(card, form);
      return card;
    });
    const el = (tag, text, className) => {
      const node = document.createElement(tag);
      if (text) node.textContent = text;
      if (className) node.className = className;
      return node;
    };
    const told = id => { try { return sessionStorage.getItem("tec-visit-told:" + accountId + ":" + id) === "1"; } catch { return false; } };
    const tell = id => { try { sessionStorage.setItem("tec-visit-told:" + accountId + ":" + id, "1"); } catch {} };
    const total = () => items.reduce((sum, item) => sum + item.questions.length, 0);

    function render() {
      const count = total();
      for (const card of cards) {
        card.replaceChildren();
        card.hidden = !items.length;
        if (!items.length) continue;
        card.append(el("h4", count ? "Coach Jorge necesita " + count + (count === 1 ? " dato" : " datos") + " de tu visita"
          : "Coach Jorge tiene algo listo para guardar"));
        for (const item of items) {
          if (items.length > 1 || item.host) card.append(el("p", item.host ? "Visita con " + item.host : "Visita del " +
            new Date(item.startedAt).toLocaleString("es-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }), "visit-questions-host"));
          if (item.questions.length) {
            const list = el("ol");
            for (const question of item.questions) list.append(el("li", question.ask));
            card.append(list);
          }
          for (const note of item.waiting) card.append(el("p", note, "visit-questions-waiting"));
        }
        if (count) card.append(el("p", "Contéstame con un audio o escríbelo aquí abajo. Puedes contestar varias de una vez.", "visit-questions-hint"));
      }
    }

    // A short account of what the demo left saved, once per visit.
    function announce(news) {
      for (const item of news || []) {
        if (told(item.id)) continue;
        tell(item.id);
        const who = item.host ? " con " + item.host : "";
        const saved = item.lines.length ? " Ya guardé: " + item.lines.join(" ") : "";
        const missing = item.questions ? " Me " + (item.questions === 1 ? "falta 1 dato" : "faltan " + item.questions + " datos") +
          "; " + (item.questions === 1 ? "está" : "están") + " arriba de donde escribes." : " No me falta nada.";
        addMessage("Listo, revisé tu demo" + who + "." + saved + missing);
        void request("/api/coach/visits/" + encodeURIComponent(item.id) + "/seen", { method: "POST", body: {} }).catch(() => null);
      }
    }

    async function refresh({ open = false } = {}) {
      if (loading) return loading;
      loading = (async () => {
        try {
          const result = await request("/api/coach/visits/pending");
          items = Array.isArray(result.items) ? result.items : [];
          render();
          if (result.news?.length) { if (open) openChat(); announce(result.news); }
        } catch { /* The next refresh tries again. */ }
        finally { loading = null; }
      })();
      return loading;
    }

    // Returns the text Jorge says, or null when the message was not an answer (the normal chat takes it).
    async function answer(text) {
      for (const item of items.filter(found => found.questions.length).slice(0, 2)) {
        let result;
        try { result = await request("/api/coach/visits/" + encodeURIComponent(item.id) + "/answer", { method: "POST", body: { text } }); }
        catch { continue; }
        if (!result?.understood) continue;
        if (result.later) { items = items.filter(found => found.id !== item.id); render(); return "Va, lo dejamos para después. Te lo vuelvo a preguntar más tarde."; }
        const index = items.findIndex(found => found.id === item.id);
        const next = { ...item, questions: result.questions || [], waiting: result.waiting || [] };
        if (next.questions.length || next.waiting.length) items[index] = next; else items.splice(index, 1);
        render();
        const saved = result.saved?.length ? "Guardé: " + result.saved.join(" ") : "";
        const doubts = (result.clarify || []).join(" ");
        const left = next.questions.length ? "Me " + (next.questions.length === 1 ? "falta 1 dato" : "faltan " + next.questions.length + " datos") + "."
          : "Con eso quedó completa la visita.";
        return [saved || (doubts ? "" : "Anotado."), doubts, left].filter(Boolean).join(" ");
      }
      return null;
    }

    window.addEventListener("tec:offline-status", event => { if (event.detail?.analysis?.status === "completed") void refresh({ open: true }); });
    window.addEventListener("tec:coach-session-change", () => { items = []; render(); });
    document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh(); });
    void refresh();
    return { answer, refresh, hasPending: () => total() > 0 };
  }
  window.TecVisitQuestions = { init };
})();
