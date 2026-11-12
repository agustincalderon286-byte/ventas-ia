(() => {
  'use strict';
  const message = status => status === 401 ? 'Tu sesión terminó. Vuelve a ingresar a TEC.'
    : status === 403 ? 'Necesitas una membresía activa para escuchar el audiolibro completo.'
    : status === 410 ? 'Esta grabación ya no está disponible en TEC. Elige otro audiolibro.'
    : 'No se pudo cargar el audio. Revisa tu conexión e inténtalo de nuevo.';
  class Controller {
    constructor(audio, { onFailure = () => {}, fetchImpl = fetch, now = () => Date.now() } = {}) {
      this.audio = audio; this.onFailure = onFailure; this.fetch = fetchImpl.bind(window); this.now = now;
      this.generation = 0; this.source = ''; this.position = 0; this.rate = 1; this.lastRetry = -Infinity;
      this.native = Boolean(window.webkit?.messageHandlers?.tecNative || window.TecNativeOffline || window.TecNative || window.TECNative || window.Capacitor);
      audio.addEventListener('loadedmetadata', () => {
        if (!this.source || audio.getAttribute('src') !== this.url) return;
        audio.playbackRate = this.rate;
        const end = Number.isFinite(audio.duration) ? Math.max(0, audio.duration - .1) : this.position;
        audio.currentTime = Math.min(this.position, end);
        this.onReady?.();
        if (this.playWanted) this.resume();
      });
      audio.addEventListener('timeupdate', () => {
        if (!this.loading && !audio.error && Number.isFinite(audio.currentTime)) this.position = audio.currentTime;
      });
      audio.addEventListener('ratechange', () => { if (!this.loading) this.rate = audio.playbackRate; });
      audio.addEventListener('play', () => {
        this.playWanted = true;
        if (!this.loading && this.expired()) this.refresh(this.position, true).catch(error => this.fail(error));
      });
      audio.addEventListener('pause', () => { if (!this.loading && !audio.error) this.playWanted = false; });
      audio.addEventListener('error', () => {
        if (!this.source || this.loading) return;
        if (navigator.onLine === false || this.now() - this.lastRetry < 10000) return this.fail(new Error(message()));
        this.lastRetry = this.now();
        this.refresh(this.position, this.playWanted).catch(error => this.fail(error));
      });
    }
    expired() { return (!this.native || this.external) && (!this.expiresAt || this.expiresAt <= this.now() + 5000); }
    fail(error) { if (error.name !== 'AbortError' && this.source) this.onFailure(error.message || message()); }
    stop() {
      ++this.generation; this.request?.abort(); this.source = ''; this.url = ''; this.onReady = null;
      this.loading = false; this.playWanted = false; this.position = 0; this.task = null;
      this.audio.pause(); this.audio.removeAttribute('src'); this.audio.load();
    }
    async load(source, { position = 0, rate = 1, onReady, external = false } = {}) {
      this.stop();
      const canonical = new URL(source, location.href);
      if (canonical.origin !== location.origin) throw new Error(message());
      this.external = external; this.source = canonical.href; this.onReady = onReady; this.rate = rate;
      this.lastRetry = -Infinity;
      await this.refresh(position, false);
    }
    async refresh(position, playWanted) {
      this.position = Math.max(0, Number(position) || 0); this.playWanted = Boolean(playWanted);
      if (this.task) return this.task;
      const generation = this.generation;
      this.loading = true; this.audio.pause();
      const task = (async () => {
        if (this.native && !this.external) {
          // Native offline storage owns the canonical media URL. Online requests
          // still follow the server's authorized redirect straight to R2.
          this.url = this.source; this.expiresAt = Infinity;
        } else {
          const requestURL = new URL(this.source); requestURL.searchParams.set('delivery', '1');
          const abort = new AbortController(); this.request = abort;
          const timer = setTimeout(() => abort.abort(), 15000);
          let response, delivery;
          try {
            response = await this.fetch(requestURL.href, { credentials: 'same-origin', cache: 'no-store',
              headers: { Accept: 'application/json' }, signal: abort.signal });
            if (!response.ok) throw new Error(message(response.status));
            delivery = await response.json();
          } finally { clearTimeout(timer); }
          if (generation !== this.generation) return;
          let target;
          try { target = new URL(delivery.url); } catch { throw new Error(message()); }
          const archive = false; // External recordings require a new editorial approval.
          const r2 = /^[a-f0-9]{32}\.r2\.cloudflarestorage\.com$/.test(target.hostname) &&
            target.pathname.startsWith('/tec-audiolibros/');
          if (target.protocol !== 'https:' || (this.external ? !archive : !r2) || !Number.isFinite(delivery.expiresAt) ||
            delivery.expiresAt <= this.now() || delivery.expiresAt > this.now() + 600000) throw new Error(message());
          this.url = delivery.url; this.expiresAt = delivery.expiresAt;
        }
        if (generation !== this.generation || !this.source) return;
        if (this.external) this.audio.removeAttribute('crossorigin');
        else this.audio.crossOrigin = 'anonymous';
        this.audio.src = this.url; this.audio.load();
      })();
      this.task = task;
      try { await task; }
      finally { if (generation === this.generation) { this.loading = false; this.task = null; } }
    }
    resume() {
      this.audio.play().catch(() => { if (this.source) this.onFailure('Toca Reproducir para continuar la escucha.'); });
    }
    play() {
      if (!this.source) return;
      this.playWanted = true;
      if (this.expired() || this.audio.error) this.refresh(this.position, true).catch(error => this.fail(error));
      else this.resume();
    }
    pause() { this.playWanted = false; this.audio.pause(); }
    seek(position, play = false) {
      this.position = Math.max(0, Number(position) || 0);
      this.playWanted = play || this.playWanted;
      if (this.expired() || this.loading || this.audio.error) {
        this.refresh(this.position, this.playWanted).catch(error => this.fail(error));
      } else {
        this.audio.currentTime = this.position;
        if (play) this.play();
      }
    }
  }
  window.TecAudioDelivery = { Controller };
})();
