(() => {
  'use strict';
  function cueIndexAt(cues, time) {
    if (!Number.isFinite(time) || !cues.length) return -1;
    let low = 0, high = cues.length;
    while (low < high) { const mid = (low + high) >>> 1; if (cues[mid].start <= time) low = mid + 1; else high = mid; }
    const index = low - 1;
    return index >= 0 && time < cues[index].end ? index : -1;
  }
  class Reader {
    constructor(root, audio, onSeek) {
      this.root = root; this.audio = audio; this.onSeek = onSeek || (position => { audio.currentTime = position; audio.play().catch(() => {}); }); this.cues = []; this.nodes = []; this.active = -1; this.fontSize = 19;
      this.body = root.querySelector('[data-ab-reader-text]'); this.follow = root.querySelector('[data-ab-follow]');
      this.follow.addEventListener('change', () => { if (this.follow.checked) this.update(true); });
      for (const event of ['wheel', 'touchmove']) this.body.addEventListener(event, () => { this.follow.checked = false; }, { passive: true });
      root.querySelector('[data-ab-font-smaller]').addEventListener('click', () => this.resize(-1));
      root.querySelector('[data-ab-font-larger]').addEventListener('click', () => this.resize(1));
      const play = root.querySelector('[data-ab-read-play]');
      play.addEventListener('click', () => { if (audio.paused) audio.play().catch(() => {}); else audio.pause(); });
      for (const event of ['play', 'pause', 'ended']) audio.addEventListener(event, () => { play.textContent = audio.paused ? 'Reproducir' : 'Pausar'; });
      audio.addEventListener('timeupdate', () => this.update()); audio.addEventListener('seeked', () => this.update(true));
    }
    resize(delta) { this.fontSize = Math.max(16, Math.min(28, this.fontSize + delta)); this.body.style.setProperty('--ab-reading-size', `${this.fontSize}px`); }
    clear() { this.cues = []; this.nodes = []; this.active = -1; this.follow.checked = true; this.body.replaceChildren(); this.root.hidden = true; }
    set(data) {
      this.clear(); this.cues = data.cues; const fragment = document.createDocumentFragment();
      for (const [index, cue] of this.cues.entries()) {
        if (cue.separator) fragment.append(document.createTextNode(cue.separator));
        const passage = document.createElement('span'); passage.setAttribute('role', 'button'); passage.tabIndex = 0; passage.className = 'ab-passage'; passage.textContent = cue.text;
        passage.title = 'Escuchar desde este pasaje'; passage.dataset.cue = index;
        const seek = () => {
          const selection = globalThis.getSelection?.();
          if (selection && !selection.isCollapsed && this.body.contains(selection.anchorNode)) return;
          this.onSeek(cue.start);
        };
        passage.addEventListener('click', seek); passage.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); seek(); } });
        this.nodes.push(passage); fragment.append(passage);
      }
      fragment.append(document.createTextNode(data.trailingSeparator || '')); this.body.append(fragment);
      this.root.hidden = false; this.update(true);
    }
    update(force = false) {
      const index = cueIndexAt(this.cues, this.audio.currentTime);
      const changed = index !== this.active;
      if (changed) {
        if (this.active >= 0) this.nodes[this.active]?.removeAttribute('aria-current');
        this.active = index; if (index >= 0) this.nodes[index].setAttribute('aria-current', 'true');
      }
      if (index >= 0 && this.follow.checked && (force || (changed && !this.audio.paused))) {
        const node = this.nodes[index], bounds = this.body.getBoundingClientRect();
        const top = this.body.scrollTop + node.getBoundingClientRect().top - bounds.top - this.body.clientHeight * .35;
        this.body.scrollTo({ top: Math.max(0, top), behavior: force || matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
      }
    }
  }
  globalThis.TecBookReader = { Reader, cueIndexAt };
})();
