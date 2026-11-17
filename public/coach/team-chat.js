(() => {
  'use strict';
  const endpoint = '/api/coach/team-chat';
  let current = null;
  const storage = {
    get(key) { try { return JSON.parse(window.localStorage.getItem(key) || 'null'); } catch { return null; } },
    set(key, value) { try { window.localStorage.setItem(key, JSON.stringify(value)); } catch {} }
  };
  const uuid = () => window.crypto.randomUUID?.() || '10000000-1000-4000-8000-100000000000'.replace(/[018]/g, c => (Number(c) ^ window.crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> Number(c) / 4).toString(16));
  function init({ userId, request, teamOwnerId = '', enableTeamRooms = false, root = document.querySelector('[data-team-chat-root]'), onUnreadCount = () => {} } = {}) {
    current?.destroy();
    current = null;
    if (!root || !userId || typeof request !== 'function') return null;
    userId = String(userId);
    const transport = request;
    if (teamOwnerId) request = (url, options = {}) => transport(`${url}${url.includes('?') ? '&' : '?'}teamOwnerId=${encodeURIComponent(teamOwnerId)}`, { ...options, ...(options.body ? { body: { ...options.body, teamOwnerId } } : {}) });
    let destroyed = false, roomId = '', blocked = false, loaded = false, hasMore = false;
    let sending = false, fetching = false, loadingEarlier = false, marking = false, timer = null;
    let pending = null, unread = 0, lastReadId = '', retryReadAt = 0, lastActive = false, noticeCause = '';
    let messages = [], newCount = 0, continuousLatestId = '';
    let media = null, pushControl = null, restoring = false;
    const rowCache = new Map();
    const listeners = [];
    const make = (tag, className, text) => { const node = document.createElement(tag); if (className) node.className = className; if (text !== undefined) node.textContent = text; return node; };
    const listen = (node, type, handler) => { node.addEventListener(type, handler); listeners.push(() => node.removeEventListener(type, handler)); };
    root.classList.add('tec-team-chat');
    root.setAttribute('aria-label', 'Chat privado de tu equipo');
    const header = make('div', 'tec-team-chat__header');
    const titleGroup = make('div');
    const title = make('h2', '', 'Chat de mi equipo');
    const description = make('p', 'tec-team-chat__description', 'Un grupo privado para conversar con tu equipo.');
    titleGroup.append(title, description); header.append(titleGroup);
    const groupLabel = make('label','tec-team-chat__group','Grupo'); groupLabel.hidden = true;
    const groupSelect = make('select','tec-team-chat__group-select'); groupSelect.setAttribute('aria-label','Grupo del equipo'); groupLabel.append(groupSelect); header.append(groupLabel);
    const unreadLabel = make('span', 'tec-team-chat__unread'); unreadLabel.hidden = true; header.append(unreadLabel);
    const older = make('button', 'tec-team-chat__older', 'Cargar mensajes anteriores'); older.type = 'button'; older.hidden = true;
    const list = make('div', 'tec-team-chat__list'); list.tabIndex = 0; list.setAttribute('role', 'region'); list.setAttribute('aria-label', 'Historial del equipo');
    const empty = make('p', 'tec-team-chat__empty', 'Cargando el chat del equipo…'); list.append(empty);
    const jump = make('button', 'tec-team-chat__jump', 'Ver mensajes nuevos'); jump.type = 'button'; jump.hidden = true;
    const form = make('form', 'tec-team-chat__form');
    const label = make('label', '', 'Mensaje para mi equipo');
    const input = make('textarea', 'tec-team-chat__input'); input.rows = 2; input.maxLength = 2000; input.placeholder = 'Escribe a tu equipo…'; input.setAttribute('aria-label', 'Mensaje para mi equipo');
    label.append(input);
    const actions = make('div', 'tec-team-chat__actions');
    const hint = make('small', '', 'Foto, GIF o audio · Mantén el micrófono para grabar');
    const send = make('button', 'tec-team-chat__send', 'Enviar'); send.type = 'submit';
    actions.append(hint, send); form.append(label, actions);
    const status = make('p', 'tec-team-chat__status'); status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite');
    const retry = make('button', 'tec-team-chat__retry', 'Reintentar envío'); retry.type = 'button'; retry.hidden = true;
    const reload = make('button', 'tec-team-chat__retry', 'Volver a cargar'); reload.type = 'button'; reload.hidden = true;
    root.replaceChildren(header, older, list, jump, form, status, retry, reload);
    const key = () => `tec-team-chat:v1:${encodeURIComponent(userId)}:${encodeURIComponent(roomId)}`;
    const active = () => !destroyed && !document.hidden && document.visibilityState !== 'hidden' && !root.closest('[hidden]') && root.getClientRects().length > 0;
    const nearBottom = () => list.scrollHeight - list.scrollTop - list.clientHeight < 48;
    function latestVisible() {
      const row = list.children[list.children.length - 1];
      if (!row || !messages.length) return false;
      const rect = row.getBoundingClientRect(), clip = list.getBoundingClientRect();
      const viewport = window.visualViewport;
      const top = viewport?.offsetTop || 0, left = viewport?.offsetLeft || 0;
      const bottom = top + (viewport?.height || window.innerHeight || document.documentElement.clientHeight);
      const right = left + (viewport?.width || window.innerWidth || document.documentElement.clientWidth);
      return rect.bottom > Math.max(clip.top, top) && rect.top < Math.min(clip.bottom, bottom) &&
        rect.right > Math.max(clip.left, left) && rect.left < Math.min(clip.right, right) &&
        rect.bottom <= Math.min(clip.bottom, bottom) + 2;
    }
    const save = () => { if (roomId) storage.set(key(), { draft: input.value, pending: pending ? { body: pending.body, clientMessageId: pending.clientMessageId, ...(pending.media || pending.mediaPending ? { mediaPending: true } : {}) } : null }); };
    function notice(text, error = false, cause = '') {
      if ((blocked && noticeCause === 'blocked' && cause !== 'blocked') ||
          (pending && noticeCause === 'send' && cause !== 'send' && cause !== 'blocked')) return;
      noticeCause = cause; status.textContent = text; status.dataset.error = error ? 'true' : 'false';
      controls();
    }
    function clearNotice(...causes) { if (causes.includes(noticeCause)) notice(''); }
    function publishUnread(value) {
      unread = Math.max(0, Number(value) || 0); unreadLabel.hidden = !unread;
      unreadLabel.textContent = unread === 1 ? '1 sin leer' : `${unread} sin leer`;
      try { onUnreadCount(unread); } catch {}
    }
    function controls() {
      input.disabled = !roomId || sending || Boolean(pending) || blocked || restoring;
      send.disabled = !roomId || sending || Boolean(pending) || blocked || restoring || Boolean(media?.busy()) || (!input.value.trim() && !media?.selected());
      send.textContent = sending ? 'Enviando…' : 'Enviar';
      retry.hidden = !pending || sending || blocked;
      retry.disabled = restoring || Boolean(pending?.mediaPending && !pending?.media);
      media?.sync(!roomId || sending || Boolean(pending) || blocked || restoring);
      older.disabled = loadingEarlier || blocked;
      groupSelect.disabled = sending || Boolean(media?.busy());
      older.hidden = !hasMore || !loaded;
      reload.hidden = blocked || (loaded && status.dataset.error !== 'true');
    }
    function freeze(error) {
      if (![401, 403].includes(Number(error?.status))) return false;
      blocked = true; save();
      notice('La sesión o el equipo cambió. Tu mensaje sigue aquí, asociado a esta cuenta. Vuelve a abrir Mensajes desde la cuenta correcta.', true, 'blocked');
      controls(); return true;
    }
    function setRoom(next) {
      if (!next) throw new Error('No pude identificar el equipo. Vuelve a cargar.');
      if (roomId && roomId !== String(next)) {
        save(); blocked = true;
        notice('Tu equipo cambió. El borrador del equipo anterior sigue aquí. Vuelve a abrir Mensajes para entrar a tu grupo actual.', true, 'blocked'); controls(); return false;
      }
      if (!roomId) {
        roomId = String(next);
        const saved = storage.get(key());
        input.value = typeof saved?.draft === 'string' ? saved.draft.slice(0, 2000) : '';
        if (saved?.pending && typeof saved.pending.body === 'string' && /^[\da-f]{8}-[\da-f]{4}-4[\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/i.test(saved.pending.clientMessageId)) {
          pending = { body: saved.pending.body.slice(0, 2000), clientMessageId: saved.pending.clientMessageId, ...(saved.pending.mediaPending ? { mediaPending: true } : {}) };
          input.value = pending.body;
          notice('Hay un envío sin confirmar. Reinténtalo para comprobarlo sin duplicarlo.', true, 'send');
        }
        if (media) {
          restoring = true; controls();
          void media.restore(key()).then(recovered => {
            if (destroyed || blocked) return;
            if (recovered?.media && typeof recovered.body === 'string' && typeof recovered.clientMessageId === 'string') { pending = recovered; input.value = pending.body; save(); notice('Hay un archivo sin confirmar. Reinténtalo para comprobarlo sin duplicarlo.',true,'send'); }
            else if (pending?.mediaPending) notice('No pude recuperar el archivo pendiente. Conserva esta pantalla y vuelve a cargar para comprobar el envío.',true,'send');
          }).catch(() => { if (!destroyed && pending?.mediaPending) notice('No pude recuperar el archivo pendiente. Vuelve a abrir el chat con este navegador.',true,'send'); })
            .finally(() => { if (!destroyed) { restoring = false; controls(); } });
        }
      }
      return true;
    }
    function merge(incoming) {
      const existing = new Map(messages.map(message => [String(message.id), message]));
      for (const item of incoming || []) {
        if (!item?.id) continue;
        existing.set(String(item.id), item);
      }
      messages = [...existing.values()].sort((a, b) => String(a.id).localeCompare(String(b.id)));
    }
    function render({ bottom = false, preserve = false } = {}) {
      const top = list.scrollTop, height = list.scrollHeight;
      const nodes = messages.map(message => {
        let row = rowCache.get(String(message.id));
        if (row) { media?.render(row,message); return row; }
        row = make('div', `tec-team-chat__message${String(message.senderId) === userId ? ' tec-team-chat__message--own' : ''}`);
        row.dataset.messageId = String(message.id);
        const meta = make('div', 'tec-team-chat__meta');
        const name = make('strong', '', String(message.senderId) === userId ? 'Tú' : String(message.senderName || 'Compañero'));
        const time = make('time');
        const date = new Date(message.createdAt);
        if (!Number.isNaN(date.getTime())) { time.dateTime = date.toISOString(); time.textContent = date.toLocaleString('es-MX', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }); }
        meta.append(name, time);
        const body = make('p', 'tec-team-chat__body', String(message.body || ''));
        row.append(meta, body);
        if (message.kind === 'sale') { row.classList.add('tec-team-chat__message--sale'); const badge = make('small','tec-team-chat__sale-label','Reporte de Cierre'); row.append(badge); }
        media?.render(row,message); rowCache.set(String(message.id),row); return row;
      });
      if (!nodes.length) nodes.push(make('p', 'tec-team-chat__empty', 'El chat está listo. Envía el primer mensaje a tu equipo.'));
      if (typeof list.insertBefore === 'function') {
        if (!messages.length) list.replaceChildren(...nodes);
        else { if (list.firstElementChild?.classList.contains('tec-team-chat__empty')) list.replaceChildren(); nodes.forEach((node,index) => { if (list.children[index] !== node) list.insertBefore(node,list.children[index] || null); }); }
      } else list.replaceChildren(...nodes);
      if (bottom) list.scrollTop = list.scrollHeight;
      else list.scrollTop = preserve ? top + list.scrollHeight - height : top;
      controls();
    }
    function showNew(count) {
      newCount += count;
      jump.textContent = newCount === 1 ? 'Ver 1 mensaje nuevo ↓' : `Ver ${newCount} mensajes nuevos ↓`;
      jump.hidden = !newCount;
    }
    function clearNew() { newCount = 0; jump.hidden = true; }
    async function markRead() {
      const coveredByDialog = [...document.querySelectorAll('dialog[open]')].some(dialog => !dialog.contains(root));
      if (coveredByDialog || destroyed || blocked || marking || !active() || !nearBottom() || !loaded || !messages.length || !latestVisible() || Date.now() < retryReadAt) return;
      // At the bottom, this is the latest cursor actually displayed in the visible history.
      const lastMessageId = String(messages[messages.length - 1].id);
      if (lastMessageId !== continuousLatestId || (lastReadId === lastMessageId && !unread)) return;
      marking = true;
      try {
        const data = await request(`${endpoint}/read`, { method: 'POST', body: { accountId: userId, roomId, lastMessageId }, timeoutMs: 10000 });
        if (destroyed) return;
        if (String(data.roomId) !== roomId) throw Object.assign(new Error('El equipo cambió.'), { status: 403 });
        lastReadId = lastReadId > lastMessageId ? lastReadId : lastMessageId; retryReadAt = 0; publishUnread(data.unreadCount); clearNotice('read');
        if (String(messages.at(-1)?.id) === lastMessageId && nearBottom() && active() && latestVisible()) clearNew();
      } catch (error) {
        if (!destroyed && !freeze(error)) { retryReadAt = Date.now() + 15000; notice('No pude actualizar los leídos. Lo intentaré de nuevo al ver el chat.', true, 'read'); }
      } finally { marking = false; }
    }
    async function refresh() {
      if (destroyed || blocked || fetching) return;
      fetching = true;
      const wasLoaded = loaded, previousLatest = continuousLatestId;
      try {
        const data = await request(endpoint, { headers: { 'X-TEC-Account-ID': userId }, timeoutMs: 10000 });
        if (destroyed || !setRoom(data.roomId)) return;
        publishUnread(data.unreadCount);
        if (!active()) { controls(); return; }
        let incoming = Array.isArray(data.messages) ? [...data.messages] : [];
        let more = Boolean(data.hasMore);
        // Bridge bursts larger than one page before joining them to loaded history.
        while (wasLoaded && previousLatest && incoming.length && !incoming.some(item => String(item.id) === String(previousLatest)) && String(incoming[0].id) > String(previousLatest) && more) {
          const page = await request(`${endpoint}?before=${encodeURIComponent(incoming[0].id)}`, { headers: { 'X-TEC-Account-ID': userId }, timeoutMs: 10000 });
          if (destroyed || !setRoom(page.roomId)) return;
          const olderMessages = Array.isArray(page.messages) ? page.messages : [];
          if (!olderMessages.length) break;
          incoming = [...olderMessages, ...incoming]; more = Boolean(page.hasMore);
        }
        const wasBottom = nearBottom();
        const updated = incoming.some(item => { const previous = messages.find(old => String(old.id) === String(item.id)); return previous && JSON.stringify(previous) !== JSON.stringify(item); });
        const added = incoming.filter(item => !messages.some(old => String(old.id) === String(item.id))).length;
        if (!wasLoaded) hasMore = Boolean(data.hasMore);
        merge(incoming); loaded = true; continuousLatestId = String(incoming.at(-1)?.id || '');
        if (!wasLoaded || added || updated) render({ bottom: !wasLoaded || (wasBottom && active()) });
        if (wasLoaded && added && !wasBottom) showNew(added);
        clearNotice('load', '');
        controls();
        await markRead();
      } catch (error) {
        if (!destroyed && !freeze(error)) {
          if (!loaded) empty.textContent = 'El chat aún no está disponible.';
          notice(error?.message || 'No pude cargar el chat. Vuelve a intentar.', true, 'load'); controls();
        }
      } finally { fetching = false; }
    }
    async function markAllRead() {
      if (destroyed || blocked) return false;
      try {
        // This explicit user action may acknowledge the latest server cursor without scrolling.
        const latest = await request(endpoint, { headers: { 'X-TEC-Account-ID': userId }, timeoutMs: 10000 });
        if (destroyed || !setRoom(latest.roomId)) return false;
        const lastMessageId = String(latest.messages?.at(-1)?.id || '');
        if (!lastMessageId) { publishUnread(latest.unreadCount); clearNotice('read', 'bulk-read'); return true; }
        const data = await request(`${endpoint}/read`, { method: 'POST', body: { accountId: userId, roomId, lastMessageId }, timeoutMs: 10000 });
        if (destroyed) return false;
        if (String(data.roomId) !== roomId) throw Object.assign(new Error('El equipo cambió.'), { status: 403 });
        lastReadId = lastReadId > lastMessageId ? lastReadId : lastMessageId;
        retryReadAt = 0; publishUnread(data.unreadCount); clearNotice('read', 'bulk-read');
        if (String(messages.at(-1)?.id || '') <= lastMessageId) clearNew();
        return true;
      } catch (error) {
        if (!destroyed && !freeze(error)) { notice('No pude marcar el chat del equipo como leído. Inténtalo de nuevo.', true, 'bulk-read'); controls(); }
        return false;
      }
    }
    async function earlier() {
      if (!hasMore || !messages.length || loadingEarlier || blocked) return;
      loadingEarlier = true; controls(); older.textContent = 'Cargando anteriores…';
      try {
        const data = await request(`${endpoint}?before=${encodeURIComponent(messages[0].id)}`, { headers: { 'X-TEC-Account-ID': userId }, timeoutMs: 10000 });
        if (destroyed || !setRoom(data.roomId)) return;
        hasMore = Boolean(data.hasMore); merge(data.messages); render({ preserve: true }); clearNotice('history');
      } catch (error) { if (!destroyed && !freeze(error)) notice(error.message || 'No pude cargar los mensajes anteriores.', true, 'history'); }
      finally { loadingEarlier = false; older.textContent = 'Cargar mensajes anteriores'; controls(); }
    }
    async function submit() {
      if (destroyed || blocked || sending || !roomId || restoring || media?.busy() || (pending?.mediaPending && !pending.media)) return;
      const body = pending ? pending.body : input.value.trim();
      const selected = pending?.media || media?.selected();
      if ((!body && !selected) || body.length > 2000) return;
      try { if (!pending) pending = { body, clientMessageId: uuid(), ...(selected ? { media: selected } : {}) }; }
      catch { notice('Este navegador no puede preparar el envío. Actualiza el navegador e inténtalo de nuevo.', true, 'send'); return; }
      // Persist before the first network attempt; every explicit retry reuses this exact identity.
      save(); sending = true; notice('Enviando mensaje…', false, 'send'); controls();
      const attempt = pending, attemptRoom = roomId;
      let requestAttempted = false;
      try {
        if (attempt.media) await media.persist(key(),attempt);
        if (destroyed || blocked) return;
        requestAttempted = true;
        const data = await request(`${endpoint}/messages`, { method: 'POST', body: { accountId: userId, roomId: attemptRoom, ...attempt }, timeoutMs: attempt.media ? 60000 : 15000 });
        if (destroyed) return;
        if (String(data.roomId) !== attemptRoom || !data.message?.id) throw Object.assign(new Error('No pude confirmar el mensaje en este equipo.'), { status: data.roomId !== attemptRoom ? 403 : 503 });
        const atBottom = nearBottom();
        merge([data.message]); pending = null; input.value = ''; media?.clear(); save();
        if (attempt.media) void media.forget(key()).catch(() => {});
        render({ bottom: active() && atBottom }); if (!atBottom) showNew(1);
        notice('Mensaje enviado.', false, 'send'); await refresh(); await markRead();
      } catch (error) {
        if (!destroyed && !freeze(error)) {
          if (!requestAttempted || [400,413,415,422].includes(Number(error?.status))) {
            pending = null; save(); if (attempt.media) void media.forget(key()).catch(() => {});
            notice(`${error?.message || 'No pude preparar el archivo.'} Revisa tu mensaje y vuelve a enviarlo.`,true,'send');
          } else notice(`${error?.message || 'No pude confirmar el envío.'} Tu mensaje sigue aquí. Usa Reintentar envío.`, true, 'send');
        }
      } finally { sending = false; if (!destroyed) controls(); }
    }
    function schedule(delay) {
      window.clearTimeout(timer);
      if (destroyed || blocked) return;
      timer = window.setTimeout(async () => {
        const nowActive = active();
        if (nowActive || !document.hidden) await refresh();
        lastActive = nowActive; schedule(nowActive ? 3000 : 30000);
      }, delay);
    }
    function visibilityChanged() {
      const nowActive = active();
      if (!nowActive) media?.pause();
      if (nowActive && !lastActive) { void refresh(); schedule(3000); }
      lastActive = nowActive;
    }
    listen(input, 'input', () => { save(); controls(); });
    listen(form, 'submit', event => { event.preventDefault(); void submit(); });
    listen(input, 'keydown', event => { if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) { event.preventDefault(); if (!send.disabled) void submit(); } });
    listen(retry, 'click', () => void submit()); listen(reload, 'click', () => void refresh());
    listen(older, 'click', () => void earlier());
    listen(jump, 'click', () => { list.scrollTop = list.scrollHeight; clearNew(); void markRead(); });
    listen(list, 'scroll', () => { if (nearBottom()) { clearNew(); void markRead(); } });
    listen(document, 'visibilitychange', visibilityChanged);
    listen(window, 'scroll', () => void markRead());
    listen(window, 'resize', () => void markRead());
    listen(window, 'online', () => { void refresh(); schedule(active() ? 3000 : 30000); });
    listen(window, 'pagehide', save);
    const observer = new MutationObserver(visibilityChanged);
    // Workspace navigation changes hidden/style/class on the section or one of its ancestors.
    for (let node = root; node; node = node.parentElement) observer.observe(node, { attributes: true, attributeFilter: ['hidden', 'style', 'class'] });
    media = window.TecTeamChatMedia?.init({ form, actions, send, input, notify: (text,error=false) => notice(text,error,'media'), changed: controls, submit });
    pushControl = window.TecTeamChatPush?.init({root:header,userId,request});
    if (enableTeamRooms) {
      listen(groupSelect,'change',() => { if (groupSelect.value) init({userId,request:transport,root,onUnreadCount,enableTeamRooms:true,teamOwnerId:groupSelect.value}); });
      void transport(`${endpoint}/rooms`,{headers:{'X-TEC-Account-ID':userId},timeoutMs:10000}).then(data => {
        if (destroyed || !Array.isArray(data.rooms) || data.rooms.length < 2) return;
        for (const room of data.rooms) { const option=make('option','',room.name);option.value=room.ownerId;groupSelect.append(option); }
        groupSelect.value=teamOwnerId || data.rooms[0].ownerId;groupLabel.hidden=false;
      }).catch(() => {});
    }
    current = {
      refresh, markAllRead, getTeamOwnerId: () => teamOwnerId,
      destroy() { save(); destroyed = true; pushControl?.destroy(); media?.destroy(); rowCache.clear(); window.clearTimeout(timer); observer.disconnect(); listeners.forEach(remove => remove()); }
    };
    controls(); void refresh(); lastActive = active(); schedule(lastActive ? 3000 : 30000);
    return current;
  }
  window.TecTeamChat = { init, getTeamOwnerId: () => current?.getTeamOwnerId() || '' };
})();
