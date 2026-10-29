/* External tools keep their own origin. TEC passes no account or lead data. */
(() => {
  'use strict';
  let dialog, frame, viewport, opener, bodyStyle, scrollY, minFrameWidth = 0, active = false;
  // Konverta renders its sign-in form only above its 1023px breakpoint.
  // Fit that layout into tablets/narrow windows without changing other tools.
  function fitFrame() {
    if (!active) return;
    const width = viewport.clientWidth, height = viewport.clientHeight;
    if (!width || !height) return;
    const scale = minFrameWidth ? Math.min(1, width / minFrameWidth) : 1;
    frame.style.width = `${Math.max(width, minFrameWidth)}px`;
    frame.style.height = `${Math.ceil(height / scale)}px`;
    frame.style.transform = `scale(${scale})`;
  }
  function restorePosition() {
    opener?.focus({preventScroll:true});
    window.scrollTo({left:0,top:scrollY,behavior:'instant'});
  }
  function close(fromHistory = false) {
    if (!active) return;
    active = false;
    dialog.close(); frame.removeAttribute('src');
    document.body.style.cssText = bodyStyle;
    if (!fromHistory && history.state?.tecExternalTool) history.replaceState({...history.state,tecExternalTool:false},'',location.href);
    restorePosition();
    window.requestAnimationFrame?.(()=>{if(!active)restorePosition();});
  }
  async function open(config) {
    if (active) return;
    if (!dialog) {
      dialog = document.createElement('dialog'); dialog.className = 'tec-external-tool';
      dialog.innerHTML = `<header><button type="button" data-konverta-close>← Volver a TEC</button><div><strong data-external-title></strong><span data-external-host></span></div><a data-external-fallback target="_blank" rel="noopener noreferrer">Abrir aparte ↗</a></header><div class="tec-external-tool-viewport"><iframe referrerpolicy="no-referrer" sandbox="allow-forms allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox allow-downloads"></iframe></div><p data-external-note></p>`;
      document.body.append(dialog); frame = dialog.querySelector('iframe');
      viewport = dialog.querySelector('.tec-external-tool-viewport');
      if (window.ResizeObserver) new window.ResizeObserver(fitFrame).observe(viewport);
      window.addEventListener('resize',fitFrame);
      dialog.querySelector('[data-konverta-close]').addEventListener('click',()=>close());
      dialog.addEventListener('cancel',event=>{event.preventDefault();close();});
      window.addEventListener('popstate',()=>{if(active&&!history.state?.tecExternalTool)close(true);});
    }
    dialog.setAttribute('aria-label',config.title);
    dialog.querySelector('[data-external-title]').textContent=config.title;
    dialog.querySelector('[data-external-host]').textContent=config.host;
    dialog.querySelector('[data-external-note]').textContent=config.note;
    dialog.querySelector('[data-external-fallback]').href=config.url;
    frame.title=config.title;
    opener = document.activeElement; bodyStyle = document.body.style.cssText; scrollY = window.scrollY;
    document.body.style.position='fixed';document.body.style.top=`-${scrollY}px`;document.body.style.width='100%';
    minFrameWidth = config.minWidth || 0;
    active = true; dialog.showModal(); fitFrame();
    const state={...history.state,tecExternalTool:true,tecExternalToolSlot:true};
    if(history.state?.tecExternalToolSlot)history.replaceState(state,'',location.href);else history.pushState(state,'',location.href);
    frame.src = config.url;
  }
  const KONVERTA_URL = 'https://dash.konverta.app/signin';
  // A top-level, user-initiated HTTPS link lets iOS use Konverta's verified
  // universal link. A sandboxed iframe cannot launch the installed app.
  const phone = () => /iPhone|iPod|Android.*Mobile/i.test(window.navigator?.userAgent || '');
  const openKonverta=()=>phone()
    ? window.open(KONVERTA_URL,'_blank','noopener,noreferrer')
    : open({title:'Konverta',host:'dash.konverta.app',url:KONVERTA_URL,minWidth:1024,note:'Toca la imagen de bienvenida de Konverta para iniciar sesión. Si necesitas, usa «Abrir aparte».'});
  const openWater=zip=>{
    if(zip != null && !/^\d{5}$/.test(String(zip)))return false;
    return open({title:zip ? `Consulta de agua · ${zip}` : 'Consulta de agua',host:'www.ewg.org',url:zip ? `https://www.ewg.org/tapwater/search-results.php?zip5=${zip}` : 'https://www.ewg.org/tapwater/',note:'Resultados de EWG. Si el sitio limita esta vista, usa «Abrir aparte».'});
  };
  window.TecExternalTools = Object.freeze({openKonverta,openWater});
  document.addEventListener('click',event=>{
    const link=event.target.closest('[data-open-konverta], [data-open-water]');
    if(!link||event.defaultPrevented||event.metaKey||event.ctrlKey||event.shiftKey||event.altKey)return;
    if(link.hasAttribute('data-open-water')) { event.preventDefault(); openWater(); return; }
    // Keep the actual anchor navigation in the original tap. No timers or
    // guessed custom schemes; TEC stays open and receives no Konverta data.
    if(phone())return;
    event.preventDefault(); openKonverta();
  });
})();
