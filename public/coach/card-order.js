(() => {
  'use strict';
  function reconcile(saved, defaults) {
    const allowed = new Set(defaults);
    const known = Array.isArray(saved) ? saved.filter(id => allowed.has(id)) : [];
    return [...new Set([...known, ...defaults])];
  }
  function init({ userId, order = [], request, offline = false }) {
    const sidebar = document.querySelector('.coach-sidebar');
    const toolbar = document.querySelector('[data-coach-order-toolbar]');
    const dialog = document.querySelector('[data-coach-order-dialog]');
    if (!userId || !sidebar || !toolbar || !dialog || toolbar.dataset.initialized) return;
    toolbar.dataset.initialized = 'true';
    const cards = Array.from(sidebar.children).filter(node => node.dataset.coachCardId)
      .sort((a, b) => Number(a.style.getPropertyValue('--coach-flow-order')) - Number(b.style.getPropertyValue('--coach-flow-order')));
    const defaults = cards.map(node => node.dataset.coachCardId);
    const nodes = new Map(cards.map(node => [node.dataset.coachCardId, node]));
    const titles = new Map(cards.map(node => [node.dataset.coachCardId, node.querySelector('h3').textContent.trim()]));
    const labels = new Map(cards.map(node => [node.dataset.coachCardId, node.querySelector('.eyebrow')?.textContent.trim() || '']));
    const list = dialog.querySelector('[data-coach-order-list]');
    const form = dialog.querySelector('form');
    const edit = toolbar.querySelector('[data-coach-order-edit]');
    const feedback = toolbar.querySelector('[data-coach-order-feedback]');
    const message = dialog.querySelector('[data-coach-order-message]');
    const save = dialog.querySelector('[data-coach-order-save]');
    const reset = dialog.querySelector('[data-coach-order-reset]');
    const cancel = dialog.querySelector('[data-coach-order-cancel]');
    const cacheKey = `tec-coach-card-order:v1:${encodeURIComponent(userId)}`;
    const same = (a, b) => a.length === b.length && a.every((id, i) => id === b[i]);
    function cache(value) { try { window.localStorage.setItem(cacheKey, JSON.stringify(value)); } catch {} }
    let initial = order;
    if (offline) {
      try { const cached = JSON.parse(window.localStorage.getItem(cacheKey)); if (Array.isArray(cached)) initial = cached; } catch {}
    } else cache(order);
    let saved = reconcile(initial, defaults), draft = [...saved], busy = false;
    function apply(value) {
      let step = 0;
      value.forEach((id, index) => {
        const card = nodes.get(id);
        // Keep presentation cards ahead of account panels (default flow order 100).
        card.style.setProperty('--coach-flow-order', String(index + 1));
        const label = labels.get(id), match = label.match(/^Paso\s+\d+\s*·\s*(.+)$/u);
        if (match) card.querySelector('.eyebrow').textContent = `Paso ${++step} · ${match[1]}`;
      });
      // Move existing elements, retaining tool inputs, handlers and visit state.
      window.TecUi.alignFlow(sidebar);
    }
    function render(focusId, delta) {
      const fragment = document.createDocumentFragment();
      draft.forEach((id, index) => {
        const row = document.createElement('li');
        const number = document.createElement('span'); number.className = 'coach-order-position'; number.textContent = index + 1; number.setAttribute('aria-hidden', 'true');
        const title = document.createElement('span'); title.className = 'coach-order-title'; title.textContent = titles.get(id);
        row.append(number, title);
        for (const [direction, text, label] of [[-1, '↑', 'Subir'], [1, '↓', 'Bajar']]) {
          const button = document.createElement('button'); button.type = 'button'; button.className = 'coach-order-move'; button.textContent = text;
          button.dataset.cardId = id; button.dataset.direction = String(direction);
          button.setAttribute('aria-label', `${label} ${titles.get(id)}`); button.title = `${label} ${titles.get(id)}`;
          button.disabled = busy || (direction < 0 ? index === 0 : index === draft.length - 1);
          button.addEventListener('click', () => {
            if (busy) return;
            const position = draft.indexOf(id), target = position + direction;
            if (target < 0 || target >= draft.length) return;
            [draft[position], draft[target]] = [draft[target], draft[position]];
            render(id, direction);
            message.textContent = `${titles.get(id)}, posición ${target + 1} de ${draft.length}.`;
          });
          row.append(button);
        }
        fragment.append(row);
      });
      list.replaceChildren(fragment);
      save.disabled = busy || same(saved, draft);
      reset.disabled = busy || same(defaults, draft);
      cancel.disabled = busy;
      if (focusId) {
        const buttons = Array.from(list.querySelectorAll('button')).filter(button => button.dataset.cardId === focusId && !button.disabled);
        (buttons.find(button => Number(button.dataset.direction) === delta) || buttons[0])?.focus();
      }
    }
    edit.addEventListener('click', () => {
      if (dialog.open) return;
      draft = [...saved]; message.textContent = ''; render();
      dialog.showModal(); document.body.classList.add('coach-order-editing');
      list.querySelector('button:not(:disabled)')?.focus();
    });
    cancel.addEventListener('click', () => { if (!busy) dialog.close(); });
    dialog.addEventListener('cancel', event => { if (busy) event.preventDefault(); });
    dialog.addEventListener('close', () => { document.body.classList.remove('coach-order-editing'); edit.focus({ preventScroll: true }); });
    reset.addEventListener('click', () => { if (!busy) { draft = [...defaults]; render(); message.textContent = 'Orden estándar preparado. Pulsa Guardar para aplicarlo.'; } });
    form.addEventListener('submit', async event => {
      event.preventDefault();
      if (busy || same(saved, draft)) return;
      busy = true; render(); message.textContent = 'Guardando tu orden…';
      try {
        const response = await request('/api/coach/card-order', { method: 'PUT', timeoutMs: 12000,
          body: { accountId: String(userId), order: same(defaults, draft) ? [] : [...draft] } });
        if (!Array.isArray(response.order)) throw new Error('No pude confirmar el guardado. Vuelve a intentarlo.');
        saved = reconcile(response.order, defaults); cache(response.order); apply(saved);
        feedback.textContent = same(saved, defaults) ? 'Orden estándar guardado.' : 'Tu orden quedó guardado.';
        dialog.close();
      } catch (error) {
        message.textContent = window.navigator.onLine === false
          ? 'Conéctate a internet para guardar. Tus cambios siguen aquí.'
          : error.message || 'No pude guardar. Tus cambios siguen en el editor.';
      } finally { busy = false; render(); }
    });
    apply(saved);
    edit.hidden = false;
  }
  window.TecCardOrder = Object.freeze({ init, reconcile });
})();
