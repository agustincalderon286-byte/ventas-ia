/* TEC · one-button concept. A single button on screen: tap and talk. Jorge decides whether a visit is
   starting (the whole visit is recorded with the app's own recorder) or it is a message for him.
   A long press starts a visit without saying anything. Everything else is the app that already exists. */
(function () {
  "use strict";
  const MAX_MESSAGE_SECONDS = 180;
  const HOLD_MS = 650;
  const clock = seconds => {
    const s = Math.max(0, Math.floor(seconds)), h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60);
    return (h ? String(h).padStart(2, "0") + ":" : "") + String(m).padStart(2, "0") + ":" + String(s % 60).padStart(2, "0");
  };
  const greeting = hour => hour < 12 ? "Buenos días" : hour < 19 ? "Buenas tardes" : "Buenas noches";
  const mime = () => ["audio/webm;codecs=opus", "audio/mp4", "audio/webm"].find(type => window.MediaRecorder?.isTypeSupported?.(type)) || "";

  function init({ name = "", accountId = "", request }) {
    if (document.querySelector(".tec-one")) return;
    const el = (tag, className, text) => { const node = document.createElement(tag); if (className) node.className = className; if (text) node.textContent = text; return node; };
    const root = el("section", "tec-one");
    root.setAttribute("aria-label", "TEC, un solo botón");
    const hello = el("p", "tec-one__hello", greeting(new Date().getHours()));
    const who = el("p", "tec-one__name", name || "");
    const time = el("p", "tec-one__clock");
    const button = el("button", "tec-one__button");
    button.type = "button";
    const icon = el("span", "tec-one__icon");
    button.append(icon);
    const title = el("p", "tec-one__title");
    const hint = el("p", "tec-one__hint");
    const reply = el("div", "tec-one__reply");
    reply.setAttribute("aria-live", "polite");
    const asks = el("div", "tec-one__asks");
    const full = el("button", "tec-one__full", "Abrir TEC completo");
    full.type = "button";
    root.append(hello, who, time, button, title, hint, reply, asks, full);
    document.body.append(root);
    document.body.classList.add("tec-one-open");

    let phase = "idle", recorder = null, stream = null, chunks = [], started = 0, ticker = 0, hold = 0, held = false, host = "", note = "";
    const demo = () => window.TecDemoRecorder;
    const vibrate = pattern => { try { navigator.vibrate?.(pattern); } catch {} };

    function render() {
      if (demo()?.recording() && phase !== "stopping") phase = "visit";
      root.dataset.phase = phase;
      const texts = {
        idle: ["Toca y habla", "Cuéntale a Jorge qué necesitas, o dile que empiezas una visita. Mantén presionado para grabar una visita de una vez."],
        listening: ["Te escucho", "Toca otra vez cuando termines."],
        thinking: ["Un momento…", "Estoy oyendo lo que dijiste."],
        visit: ["Grabando la visita" + (host ? " con " + host : ""), "Avisa que estás grabando. Puedes apagar la pantalla. Toca para terminar."],
        stopping: ["Guardando la visita…", "No cierres TEC todavía."]
      }[phase];
      title.textContent = texts[0];
      hint.textContent = note || texts[1];
      button.setAttribute("aria-label", phase === "visit" ? "Terminar la visita" : phase === "listening" ? "Enviar a Jorge" : "Hablar con Jorge");
      button.disabled = phase === "thinking" || phase === "stopping";
      time.textContent = phase === "visit" ? clock(demo()?.seconds() || 0) : phase === "listening" ? clock((Date.now() - started) / 1000) : "";
      hello.hidden = who.hidden = phase !== "idle";
    }

    function showAsks() {
      asks.replaceChildren();
      const list = window.tecVisitQuestions?.list?.() || [];
      const total = list.reduce((sum, item) => sum + item.questions.length, 0);
      if (!list.length) return;
      asks.append(el("h4", "", total ? "Jorge necesita " + total + (total === 1 ? " dato" : " datos") : "Jorge tiene algo listo para guardar"));
      for (const item of list) {
        if (item.host) asks.append(el("p", "tec-one__asks-host", "Visita con " + item.host));
        if (item.questions.length) { const ol = el("ol"); for (const ask of item.questions) ol.append(el("li", "", ask)); asks.append(ol); }
        for (const waiting of item.waiting) asks.append(el("p", "", waiting));
      }
      if (total) asks.append(el("p", "tec-one__asks-hint", "Toca el botón y contéstale. Puedes contestar varias de una vez."));
    }

    function say(text) {
      reply.replaceChildren();
      if (text) reply.append(el("p", "", text));
    }

    function tick() {
      window.clearInterval(ticker);
      ticker = window.setInterval(() => {
        if (phase === "listening" && Date.now() - started >= MAX_MESSAGE_SECONDS * 1000) void finishMessage();
        if (phase === "visit" && !demo()?.recording()) { phase = "idle"; host = ""; note = "Visita guardada. Jorge la revisa y te avisa aquí cuando esté lista."; }
        render();
      }, 500);
    }

    async function startMessage() {
      note = "";
      if (!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) { note = "Este teléfono no deja grabar desde la página. Abre TEC completo."; return render(); }
      try { stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true, channelCount: 1 }, video: false }); }
      catch { note = "TEC necesita permiso para usar el micrófono. Actívalo en los permisos del sitio."; return render(); }
      chunks = [];
      const type = mime();
      recorder = new MediaRecorder(stream, { ...(type && { mimeType: type }), audioBitsPerSecond: 32000 });
      recorder.addEventListener("dataavailable", event => { if (event.data?.size) chunks.push(event.data); });
      recorder.start(1000);
      started = Date.now();
      phase = "listening";
      vibrate(20);
      render();
    }

    const stopRecorder = () => new Promise(resolve => {
      if (!recorder || recorder.state === "inactive") return resolve();
      recorder.addEventListener("stop", resolve, { once: true });
      recorder.stop();
    }).then(() => { stream?.getTracks().forEach(track => track.stop()); stream = null; });

    async function finishMessage() {
      if (phase !== "listening") return;
      phase = "thinking";
      render();
      await stopRecorder();
      const type = (recorder?.mimeType || mime() || "audio/webm").split(";")[0];
      const audio = new Blob(chunks, { type });
      recorder = null;
      try {
        if (audio.size < 800) throw new Error("No alcancé a oírte. Toca y habla otra vez.");
        const response = await fetch("/api/coach/voice/transcribe", { method: "POST", credentials: "same-origin",
          headers: { "Content-Type": type, ...(accountId && { "X-TEC-Account-ID": accountId }) }, body: audio });
        const data = await response.json().catch(() => ({}));
        if (!response.ok || !data.text) throw new Error(data.error || "No pude oírte bien. Inténtalo otra vez.");
        const text = String(data.text).trim();
        // Jorge decides: is a visit starting, or is this a message for him?
        const intent = await request("/api/coach/boton/intent", { method: "POST", body: { text } }).catch(() => ({ kind: "mensaje" }));
        if (intent.kind === "visita") return await startVisit(intent.host || "");
        note = "Jorge está pensando su respuesta…";
        render();
        const result = await window.tecCoachTalk.send(text);
        note = "";
        say(result.reply || (result.ok ? "Jorge contestó en el chat. Toca «Abrir TEC completo» para verlo." : "No pude completar la respuesta. Inténtalo otra vez."));
      } catch (error) {
        note = error.message || "Algo falló. Inténtalo otra vez.";
      }
      phase = "idle";
      showAsks();
      render();
    }

    async function startVisit(who) {
      note = "";
      say("");
      if (!demo()) { note = "La grabación de visitas no está disponible en esta cuenta."; phase = "idle"; return render(); }
      if (demo().busy()) { note = "TEC todavía está guardando la visita anterior. Espera un momento."; phase = "idle"; return render(); }
      phase = "thinking";
      render();
      const ok = await demo().start().catch(() => false);
      if (!ok) { note = "No pude empezar a grabar. Revisa el permiso del micrófono o abre TEC completo."; phase = "idle"; return render(); }
      host = who;
      phase = "visit";
      vibrate([30, 60, 30]);
      render();
    }

    async function stopVisit() {
      phase = "stopping";
      render();
      vibrate(40);
      await demo()?.stop().catch(() => null);
      phase = "idle";
      host = "";
      note = "Visita guardada. Jorge la revisa y te avisa aquí cuando esté lista.";
      render();
    }

    function tap() {
      // Inside the tap: lets Jorge's spoken answer play when it arrives.
      try { window.tecCoachTalk?.prepareVoice?.(); } catch {}
      if (phase === "idle") void startMessage();
      else if (phase === "listening") void finishMessage();
      else if (phase === "visit") void stopVisit();
    }

    // One button, two gestures: a tap talks to Jorge; holding it starts a visit right away.
    button.addEventListener("pointerdown", () => {
      held = false;
      if (phase !== "idle") return;
      hold = window.setTimeout(() => { held = true; void startVisit(""); }, HOLD_MS);
    });
    const release = () => window.clearTimeout(hold);
    button.addEventListener("pointerup", release);
    button.addEventListener("pointerleave", release);
    button.addEventListener("pointercancel", release);
    button.addEventListener("click", () => { if (held) { held = false; return; } tap(); });
    button.addEventListener("contextmenu", event => event.preventDefault());

    full.addEventListener("click", () => {
      if (phase === "listening") void stopRecorder();
      window.clearInterval(ticker);
      root.remove();
      document.body.classList.remove("tec-one-open");
    });
    window.addEventListener("tec:visit-questions", showAsks);
    window.addEventListener("tec:visit-news", event => { note = ""; say(event.detail?.text || ""); render(); });
    window.addEventListener("tec:offline-status", event => {
      if (event.detail?.analysis?.status === "completed") { note = "Tu visita ya está revisada."; render(); }
    });
    showAsks();
    render();
    tick();
  }
  window.TecOneButton = { init };
})();
