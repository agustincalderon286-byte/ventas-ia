(() => {
  'use strict';
  const clock = seconds => {
    const value = Math.max(0, Math.floor(seconds));
    return [Math.floor(value / 3600) || null, String(Math.floor(value / 60) % 60).padStart(2, '0'), String(value % 60).padStart(2, '0')].filter(x => x !== null).join(':');
  };
  function seconds(value) {
    if (!/^\d{1,3}:\d{2}(:\d{2})?$/.test(value)) return NaN;
    const parts = value.split(':').map(Number);
    if (parts.slice(1).some(n => n > 59)) return NaN;
    return parts.reduce((sum, part) => sum * 60 + part, 0);
  }
  const textFor = cues => cues.map(c => c.text).join('').replace(/\s+/g, ' ').trim();
  function passageRange(cues, index) {
    let first = Math.max(0, index), last = first;
    const fits = (a, b) => b - a < 6 && [...textFor(cues.slice(a, b + 1))].length <= 600;
    while (first > 0 && first > index - 3 && !/[.!?][”"']?\s*$/.test(cues[first - 1].text) && fits(first - 1, last)) first--;
    while (last + 1 < cues.length && !/[.!?][”"']?\s*$/.test(cues[last]?.text || '') && fits(first, last + 1)) last++;
    return { first, last };
  }
  class Controller {
    constructor({ audio, reader, context }) {
      this.audio = audio; this.reader = reader; this.context = context;
      this.dialog = document.querySelector('#share-fragment');
      this.$ = selector => this.dialog.querySelector(selector);
      this.revision = 0; this.objectURLs = [];
      this.$('[data-share-close]').addEventListener('click', () => this.dialog.close());
      this.dialog.addEventListener('close', () => {
        this.invalidate();
        if (this.resume && this.context() === this.book) audio.play().catch(() => {});
      });
      this.$('[data-share-kind]').addEventListener('change', () => this.render());
      this.$('[data-share-layout]').addEventListener('change', () => this.render());
      this.$('[data-share-prev]').addEventListener('click', () => { this.first = Math.max(0, this.first - 1); this.last = this.first; this.render(); });
      this.$('[data-share-next]').addEventListener('click', () => { this.first = Math.min(reader.cues.length - 1, this.first + 1); this.last = this.first; this.render(); });
      this.$('[data-share-more]').addEventListener('click', () => {
        if (this.last + 1 < reader.cues.length) this.last++;
        this.render();
      });
      this.$('[data-share-less]').addEventListener('click', () => { this.last = Math.max(this.first, this.last - 1); this.render(); });
      for (const selector of ['[data-share-start]', '[data-share-duration]']) this.$(selector).addEventListener('change', () => this.render());
      this.$('[data-share-generate]').addEventListener('click', () => this.prepare());
      this.$('[data-share-video]').addEventListener('click', () => this.prepareVideo());
      this.$('[data-share-native]').addEventListener('click', () => this.shareFile());
      this.$('[data-share-copy]').addEventListener('click', () => this.copyLink());
      this.$('[data-share-system]').addEventListener('click', () => {
        if (!this.result) return;
        if (navigator.share) navigator.share({ title: this.book.title + ' · Audiolibros TEC', text: this.shareText(), url: this.result.url }).catch(error => { if (error.name !== 'AbortError') this.status('Puedes copiar el enlace para compartirlo.'); });
        else this.copyLink();
      });
    }
    status(text) { this.$('[data-share-status]').textContent = text; }
    invalidate() {
      this.revision++; this.pending?.abort(); this.pending = null; this.result = null; this.file = null;
      this.$('[data-share-audio-preview]').pause();
      this.$('[data-share-audio-preview]').removeAttribute('src');
      for (const url of this.objectURLs) URL.revokeObjectURL(url);
      this.objectURLs = []; this.$('[data-share-results]').hidden = true;
      this.$('[data-share-generate]').disabled = false;
    }
    open() {
      const book = this.context(); if (!book) return;
      const selection = window.getSelection();
      const passage = node => (node?.nodeType === 3 ? node.parentElement : node)?.closest?.('.ab-passage');
      const a = passage(selection?.anchorNode), b = passage(selection?.focusNode);
      this.book = book;
      const index = Math.max(0, this.reader.cues.findLastIndex(c => c.start <= this.audio.currentTime));
      let { first, last } = passageRange(this.reader.cues, index);
      if (!selection?.isCollapsed && a && b && this.reader.body.contains(a) && this.reader.body.contains(b)) {
        first = Math.min(Number(a.dataset.cue), Number(b.dataset.cue));
        last = Math.max(Number(a.dataset.cue), Number(b.dataset.cue));
      }
      this.first = first; this.last = last;
      this.resume = !this.audio.paused; this.audio.pause();
      this.$('[data-share-kind]').value = 'quote';
      this.$('[data-share-start]').value = clock(this.audio.currentTime);
      this.$('[data-share-duration]').value = '30';
      this.render();
      this.dialog.showModal();
    }
    render() {
      this.invalidate(); this.status('');
      const quote = this.$('[data-share-kind]').value === 'quote';
      this.$('[data-share-quote-controls]').hidden = !quote;
      this.$('[data-share-audio-controls]').hidden = quote;
      this.$('[data-share-book]').textContent = this.book.title;
      this.$('[data-share-author]').textContent = this.book.author;
      this.$('[data-share-cover]').src = this.book.cover;
      const text = textFor(this.reader.cues.slice(this.first, this.last + 1));
      this.$('[data-share-quote]').textContent = quote ? text : 'Un momento de esta historia, para llevarlo contigo.';
      this.$('[data-share-count]').textContent = `${[...text].length}/600 caracteres · ${this.first + 1}–${this.last + 1}`;
      this.$('[data-share-quote]').classList.toggle('long-quote', text.length > 300);
      this.$('[data-share-preview]').classList.toggle('story-card', this.$('[data-share-layout]').value === 'story');
      this.$('[data-share-prev]').disabled = this.first <= 0;
      this.$('[data-share-next]').disabled = this.first >= this.reader.cues.length - 1;
      this.$('[data-share-less]').disabled = this.last <= this.first;
      const nextText = textFor(this.reader.cues.slice(this.first, this.last + 2));
      this.$('[data-share-more]').disabled = this.last + 1 >= this.reader.cues.length || this.last - this.first >= 5 || [...nextText].length > 600;
      if (quote && (!text || [...text].length > 600 || this.last - this.first >= 6)) {
        this.status('Elige una cita de hasta 600 caracteres y seis pasajes.');
        this.$('[data-share-generate]').disabled = true;
      }
    }
    shareText() { return `${this.book.title} · ${this.book.author}\n${this.result?.quote || ''}`; }
    async response(url, options) {
      const res = await fetch(url, { ...options, credentials: 'same-origin', signal: this.pending?.signal });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || 'No se pudo preparar el archivo. Inténtalo de nuevo.');
      }
      return res;
    }
    async prepare() {
      const kind = this.$('[data-share-kind]').value;
      const start = seconds(this.$('[data-share-start]').value);
      const end = Math.min(this.book.duration, start + Number(this.$('[data-share-duration]').value));
      if (kind === 'audio' && (!Number.isFinite(start) || start < 0 || end - start < 3)) { this.status('Escribe el inicio como 02:15 o 1:02:15, dentro del libro.'); return; }
      this.invalidate(); const revision = this.revision;
      this.pending = new AbortController(); this.$('[data-share-generate]').disabled = true;
      this.status(kind === 'audio' ? 'Preparando tu fragmento de audio…' : 'Preparando tu tarjeta…');
      try {
        const body = { bookId: this.book.id, kind, ...(kind === 'quote' ? { first: this.first, last: this.last } : { start, end }) };
        const result = await (await this.response('/api/audiolibros/compartir', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })).json();
        if (revision !== this.revision) return;
        this.result = result;
        this.$('[data-share-quote]').textContent = result.quote || 'Un fragmento de ' + this.book.title;
        this.$('[data-share-quote]').classList.toggle('long-quote', result.quote.length > 300);
        const source = kind === 'audio' ? result.audio : result.image + '?formato=' + this.$('[data-share-layout]').value;
        const blob = await (await this.response(source)).blob();
        if (revision !== this.revision) return;
        this.setFile(blob, kind === 'audio' ? 'fragmento-tec.mp3' : 'cita-tec.png');
        this.$('[data-share-link]').value = result.url;
        this.$('[data-share-facebook]').href = 'https://www.facebook.com/sharer/sharer.php?u=' + encodeURIComponent(result.url);
        this.$('[data-share-sms]').href = 'sms:?body=' + encodeURIComponent(this.shareText() + '\n' + result.url);
        this.$('[data-share-audio-preview]').hidden = kind !== 'audio';
        if (kind === 'audio') this.$('[data-share-audio-preview]').src = this.objectURLs.at(-1);
        this.$('[data-share-video]').hidden = kind !== 'audio';
        this.$('[data-share-video]').disabled = false;
        this.$('[data-share-results]').hidden = false;
        this.status('Listo. Comparte el enlace o guarda el archivo.');
      } catch (error) { if (revision === this.revision && error.name !== 'AbortError') this.status(error.message); }
      finally { if (revision === this.revision) this.$('[data-share-generate]').disabled = false; }
    }
    setFile(blob, name) {
      this.file = new File([blob], name, { type: blob.type });
      const url = URL.createObjectURL(blob); this.objectURLs.push(url);
      const download = this.$('[data-share-download]'); download.href = url; download.download = name;
      download.textContent = name.endsWith('.mp4') ? 'Guardar video MP4' : name.endsWith('.mp3') ? 'Guardar audio MP3' : 'Guardar tarjeta PNG';
    }
    async prepareVideo() {
      if (!this.result) return;
      const revision = this.revision;
      this.$('[data-share-video]').disabled = true; this.status('Creando el video con tu fragmento…');
      try {
        const blob = await (await this.response(this.result.video)).blob();
        if (revision !== this.revision) return;
        this.setFile(blob, 'fragmento-tec.mp4');
        this.status('Video listo para guardar o compartir en Instagram y TikTok. Copia también el enlace.');
      } catch (error) { if (revision === this.revision && error.name !== 'AbortError') this.status(error.message); }
      finally { if (revision === this.revision) this.$('[data-share-video]').disabled = false; }
    }
    shareFile() {
      if (!this.file) return;
      const data = { files: [this.file], title: this.book.title, text: this.shareText() + '\n' + this.result.url };
      if (navigator.canShare?.({ files: data.files })) navigator.share(data).catch(error => { if (error.name !== 'AbortError') this.status('Guarda el archivo y adjúntalo desde tu aplicación.'); });
      else this.status('Guarda el archivo y adjúntalo desde tu aplicación. También puedes copiar el enlace.');
    }
    async copyLink() {
      try { await navigator.clipboard.writeText(this.result.url); this.status('Enlace copiado.'); }
      catch { this.$('[data-share-link]').select(); this.status('Selecciona y copia el enlace.'); }
    }
  }
  window.TecAudioSharing = { Controller, clock, seconds, textFor, passageRange };
})();
