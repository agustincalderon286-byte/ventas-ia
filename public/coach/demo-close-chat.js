/* Private visit closure messages. The microphone and Jorge use the normal Coach chat. */
(function () {
  "use strict";
  function init({accountId,request,addMessage,openChat,visitId,crmId,prepareReminder}) {
    let pending=null,dialog=null,openedId='',review=null;
    const seen=new Set(), loading=new Set();
    const current = close => close.visitId===visitId() && (close.crmId || '')===crmId();
    const api = (id,suffix='') => '/api/coach/demo-analyses/'+encodeURIComponent(id)+'/close'+suffix;
    function button(text,act) {
      const b=document.createElement('button');
      b.type='button'; b.className='secondary-button';b.textContent=text;b.addEventListener('click',act);return b;
    }
    function later(id) {
      if (pending?.id===id) pending=null;
      try { sessionStorage.setItem('tec-close-later:'+accountId+':'+id,'1'); } catch {}
    }
    function deferred(id) {
      try { return sessionStorage.getItem('tec-close-later:'+accountId+':'+id)==='1'; } catch { return false; }
    }
    function dismissDialog() { if (dialog?.open) dialog.close(); }
    function controls(handles,id,withReply=true) {
      for (const {card} of handles) {
        const actions=document.createElement('div'); actions.className='demo-close-actions';
        if (withReply) actions.append(button('Responder al Coach',async () => {
          try { const r=await request(api(id)); activate(id,r,true); }
          catch (e) { addMessage(e.message); }
        }));
        actions.append(button('Revisar cierre',() => openReview(id)));
        if (withReply) actions.append(button('Ahora no',()=>{later(id);actions.textContent='Puedes retomarlo desde Mis demostraciones.';}));
        card.append(actions);
      }
    }
    const questionText=q=>q.map(item=>item.question).join(' ');
    function reviewQuestions(r) {
      const questions=[...(r.questions || [])];
      const close=r.close;
      if (!current(close) || !window.TecDemoCloseTools) return questions;
      const values=window.TecDemoCloseTools.snapshot(close.fields);
      const labels={cooksForCount:'Para cuántas personas cocina',waterSpendWeekly:'Gasto semanal en agua',
        familyPriority:'Prioridad familiar',calcPrice:'Precio cotizado',calcDown:'Anticipo',saleAmount:'Total de la venta'};
      const normalize=v=>String(v??'').trim().toLocaleLowerCase('es').replace(/^(\d+)\.00$/,'$1');
      const key=Object.keys(labels).find(k=>!close.confirmed[k] && !close.sellerFields.includes(k) &&
        values[k] && close.fields[k] && normalize(values[k])!==normalize(close.fields[k]));
      if (key) {
        const conflict={field:key,conflict:true,question:labels[key]+': en la herramienta aparece «'+values[key]+'» y en la grabación «'+close.fields[key]+'». ¿Cuál dejamos?'};
        const prior=questions.filter(q=>q.field!==key);
        questions.splice(0,questions.length,...prior.slice(0,1),conflict);
      }
      return questions;
    }
    function activate(id,r,explicit=false) {
      const questions=reviewQuestions(r);
      pending={id,visit:r.close.visitId,version:r.close.version,explicit,conflictField:questions.find(q=>q.conflict)?.field};
      openChat();
      const msg=questionText(questions) || '¿Qué quieres añadir o corregir de esta visita?';
      controls(addMessage(msg+' Puedes responder por audio o por escrito.'),id,false);
    }
    function mount(root,id) {
      return window.TecDemoCloseReview.mount({root,analysisId:id,accountId,request,getVisitId:visitId,getCrmId:crmId,
        tools:window.TecDemoCloseTools,
        onConfirmed() { if (pending?.id===id) pending=null; },
        onRespond:async()=>{try {const r=await request(api(id));dismissDialog();activate(id,r,true);} catch(e){addMessage(e.message);}},
        onFollowUp:text=>{dismissDialog();pending=null;openChat();prepareReminder('Prepara un recordatorio personal para este seguimiento: '+text);}
      });
    }
    function openReview(id) {
      dismissDialog();
      const d=document.createElement('dialog');d.className='demo-close-dialog';
      d.setAttribute('aria-label','Revisar el cierre de la visita');
      const container=document.createElement('div');d.append(button('Cerrar',()=>d.close()),container);
      document.body.append(d);dialog=d;openedId=id;d.showModal();const view=mount(container,id);review=view;
      d.addEventListener('close',()=>{view.destroy();d.remove();if(dialog===d){dialog=null;openedId='';review=null;}},{once:true});
    }
    function announce(id,r) {
      if (!id || seen.has(id) || !r.close || deferred(id)) return;
      seen.add(id);
      const questions=reviewQuestions(r), active=current(r.close);
      const date=r.startedAt && new Date(r.startedAt).toLocaleString('es-US',{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});
      const intro='Ya preparé el cierre de '+(active?'esta demo':date?'la demo del '+date:'una demo anterior')+'. ';
      if (!pending && active && questions.length) pending={id,visit:r.close.visitId,version:r.close.version,explicit:false,conflictField:questions.find(q=>q.conflict)?.field};
      controls(addMessage(intro+(questions.length?questionText(questions)+' Puedes contestarme con un audio.':'Puedes revisar lo que quedó preparado.')),id,!!questions.length);
    }
    async function notify(id) {
      if (!id || loading.has(id) || seen.has(id)) return;
      loading.add(id);
      try {announce(id,await request(api(id)));} catch { /* Recover through the inbox or the report. */ }
      finally {loading.delete(id);}
    }
    async function inbox() {
      try {const r=await request('/api/coach/demo-close/inbox');for (const item of r.items || []) announce(item.id,item);} catch {}
    }
    async function answer(text) {
      const target=pending;
      if (!target || (!target.explicit && target.visit!==visitId())) return null;
      if (/^(?:ahora no|luego|despu[eé]s|omitir|d[eé]jalo pendiente)[.! ]*$/i.test(text.trim())) {
        later(target.id); return {id:target.id,text:'El cierre quedó pendiente en Mis demostraciones.'};
      }
      let r;
      try {
        r=await request(api(target.id,'/clarify'),{method:'POST',body:{visitId:target.visit,version:target.version,text,conflictField:target.conflictField}});
      } catch(e) {
        if(e.status===409) {pending=null;return {id:target.id,text:'La ficha cambió. Revisa el cierre antes de continuar.'};}
        throw e;
      }
      if (pending!==target) return {id:target.id,text:'Puedes consultar la ficha de esa visita en Mis demostraciones.'};
      if (!r.understood) {
        if (/^(?:no s[eé]|no (?:lo |la )?(?:acordamos|recuerdo|s[eé])|qued[oó] pendiente)[.! ]*$/i.test(text.trim())) {
          later(target.id);return {id:target.id,text:'Dejé ese dato pendiente. Puedes revisar los demás campos.'};
        }
        if (!target.explicit) { pending=null;return null; }
        return {id:target.id,text:'No identifiqué un dato nuevo para el cierre. Puedes decirme el resultado o el seguimiento, o decir «ahora no».'};
      }
      const next=reviewQuestions(r).filter(item=>!Object.hasOwn(r.changed,item.field));
      pending=next.length?{...target,explicit:true,version:r.close.version,conflictField:next.find(q=>q.conflict)?.field}:null;
      if (openedId===target.id) review?.reload();
      const values=Object.entries(r.changed).map(([k,v])=>k==='outcome'?({venta:'venta',follow_up:'seguimiento',no_venta:'sin venta'})[v]:v);
      return {id:target.id,text:'Anoté en el borrador: '+values.join(' · ')+'. '+(next.length?questionText(next):'Revisa el cierre y elige qué guardar.')};
    }
    window.addEventListener('tec:offline-status',e=>{const a=e.detail?.analysis;if(a?.closeAvailable) void notify(a.id);});
    window.addEventListener('tec:coach-session-change',()=>{pending=null;dismissDialog();});
    void inbox();
    return {mount,answer,controls,openReview,notify,hasPending:()=>Boolean(pending && (pending.explicit || pending.visit===visitId()))};
  }
  window.TecDemoCloseChat={init};
})();
