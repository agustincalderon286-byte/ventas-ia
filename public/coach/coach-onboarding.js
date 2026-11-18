(() => {
  'use strict';
  const VERSION='v1';
  const steps=Object.freeze([
    {title:'Empieza por Cierre',copy:'Tu presentación está organizada por pasos. Revisa la tarjeta en la que vas y abre una herramienta cuando la necesites.',route:'Cierre → Tu presentación → Herramientas',hint:'Usa Nuevo cliente cuando empieces otra casa y quieras preparar la siguiente visita.'},
    {title:'Tus herramientas, en un solo lugar',copy:'En Cierre encontrarás la calculadora y los apoyos de la visita. Usa Audiolibros para escuchar y leer, Mi actividad para retomar tus registros y Mensajes para conversar con tu equipo.',route:'Cierre · Audiolibros · Mi actividad · Mensajes',hint:'Puedes volver a Cierre para continuar tu presentación.'},
    {title:'Aprende a tu ritmo',copy:'En Mi cuenta, abre Tutorial y ayuda. El recorrido tiene capítulos para ir directo a una herramienta; también están los videos para añadir TEC a tu teléfono.',route:'Mi cuenta → Tutorial y ayuda',hint:'Si necesitas repasar, la Guía de bienvenida seguirá disponible en Mi cuenta.'}
  ]);
  let current=null;
  function init({user,root=document,storage}={}){
    const id=typeof user?.id==='string'?user.id.trim():'';
    const app=root.querySelector('[data-coach-app-page]');
    const dialog=root.querySelector('[data-coach-onboarding]');
    if(!id || id.length>128 || !app || app.hidden || !dialog){current?.destroy();current=null;return null;}
    if(current?.id===id && current.root===root)return current;
    current?.destroy();current=null;
    const openers=Array.from(root.querySelectorAll('[data-open-coach-onboarding]'));
    const next=dialog.querySelector('[data-onboarding-next]'),back=dialog.querySelector('[data-onboarding-back]'),skip=dialog.querySelector('[data-onboarding-skip]'),tutorial=dialog.querySelector('[data-onboarding-tutorial]');
    const title=dialog.querySelector('[data-onboarding-title]'),copy=dialog.querySelector('[data-onboarding-copy]'),progress=dialog.querySelector('[data-onboarding-progress]'),route=dialog.querySelector('[data-onboarding-route]'),hint=dialog.querySelector('[data-onboarding-hint]');
    if(!next || !back || !skip || !tutorial || !title || !copy || !progress || !route || !hint)return null;
    let store=storage,step=0,opener=null,disposed=false;
    try{if(store===undefined)store=window.localStorage;}catch{store=null;}
    const key=`tec-coach-welcome-${VERSION}:${encodeURIComponent(id)}`;
    const bindings=[];
    const bind=(node,type,fn)=>{node.addEventListener(type,fn);bindings.push(()=>node.removeEventListener(type,fn));};
    const seen=()=>{try{return Boolean(store?.getItem(key));}catch{return false;}};
    const remember=outcome=>{try{store?.setItem(key,JSON.stringify({version:VERSION,outcome}));}catch{/* A storage failure must not prevent access to the app. */}};
    const render=()=>{
      const data=steps[step];title.textContent=data.title;copy.textContent=data.copy;route.textContent=data.route;hint.textContent=data.hint;progress.textContent=`Paso ${step+1} de ${steps.length}`;
      back.hidden=step===0;next.textContent=step===steps.length-1?'Listo, empezar':'Siguiente';tutorial.hidden=step!==steps.length-1;
    };
    const restore=()=>{dialog.hidden=true;if(!disposed)opener?.focus({preventScroll:true});};
    const close=()=>{if(typeof dialog.close==='function' && dialog.open)dialog.close();else{dialog.removeAttribute('open');restore();}};
    const open=button=>{
      if(disposed || app.hidden)return false;
      if(root.querySelector('dialog[open]') && !dialog.open)return false;
      const menu=button?.closest('[data-visit-account-menu]');opener=menu?.querySelector('summary') || button || root.querySelector('[data-visit-account-menu] > summary');if(menu)menu.open=false;
      root.querySelectorAll('audio,video').forEach(media=>media.pause());step=0;render();dialog.hidden=false;
      if(typeof dialog.showModal==='function')dialog.showModal();else dialog.setAttribute('open','');
      next.focus({preventScroll:true});return true;
    };
    for(const button of openers){button.hidden=false;bind(button,'click',()=>open(button));}
    bind(back,'click',()=>{if(step>0){step--;render();next.focus({preventScroll:true});}});
    bind(next,'click',()=>{if(step===steps.length-1){remember('completed');close();}else{step++;render();next.focus({preventScroll:true});}});
    bind(skip,'click',()=>{remember('skipped');close();});
    bind(tutorial,'click',()=>{remember('completed');opener=null;close();root.querySelector('[data-open-coach-tutorial]')?.click();});
    bind(dialog,'close',restore);
    // Escape closes without recording completion. The account can return later.
    bind(dialog,'cancel',event=>{event.preventDefault();close();});
    bind(window,'pagehide',close);
    const controller={id,root,open:()=>open(openers[0]),destroy(){disposed=true;if(current===controller)current=null;for(const unbind of bindings)unbind();close();dialog.hidden=true;for(const button of openers)button.hidden=true;}};
    current=controller;
    const editing=Boolean(root.activeElement?.matches?.('input,textarea,select,[contenteditable="true"]'));
    const playing=Array.from(root.querySelectorAll('audio,video')).some(media=>!media.paused);
    if(!seen() && !root.hidden && !editing && !playing)open();
    return controller;
  }
  window.TecCoachOnboarding=Object.freeze({init,destroy(){current?.destroy();current=null;}});
})();
