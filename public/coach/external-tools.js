/* External tools keep their own origin. TEC passes no account or lead data. */
(() => {
  'use strict';
  let dialog, frame, opener, bodyStyle, scrollY, active = false;
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
      dialog.innerHTML = `<header><button type="button" data-konverta-close>← Volver a TEC</button><div><strong data-external-title></strong><span data-external-host></span></div><a data-external-fallback target="_blank" rel="noopener noreferrer">Abrir aparte ↗</a></header><iframe referrerpolicy="no-referrer" sandbox="allow-forms allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox allow-downloads"></iframe><p data-external-note></p>`;
      document.body.append(dialog); frame = dialog.querySelector('iframe');
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
    active = true; dialog.showModal();
    const state={...history.state,tecExternalTool:true,tecExternalToolSlot:true};
    if(history.state?.tecExternalToolSlot)history.replaceState(state,'',location.href);else history.pushState(state,'',location.href);
    frame.src = config.url;
  }
  const openKonverta=()=>open({title:'Konverta',host:'dash.konverta.app',url:'https://dash.konverta.app/signin',note:'Si tu inicio de sesión no funciona en esta vista, usa «Abrir aparte».'});
  const openWater=zip=>{
    if(!/^\d{5}$/.test(String(zip)))return false;
    return open({title:`Consulta de agua · ${zip}`,host:'www.ewg.org',url:`https://www.ewg.org/tapwater/search-results.php?zip5=${zip}`,note:'Resultados de EWG. Si el sitio limita esta vista, usa «Abrir aparte».'});
  };
  window.TecExternalTools = Object.freeze({openKonverta,openWater});
  document.addEventListener('click',event=>{
    const link=event.target.closest('[data-open-konverta]');
    if(!link||event.defaultPrevented||event.metaKey||event.ctrlKey||event.shiftKey||event.altKey)return;
    event.preventDefault(); openKonverta();
  });
})();
