(() => {
  'use strict';
  const $=selector=>document.querySelector(selector);
  let userId='',shareUrl='',scope='mine',loading=false,blocked=false,timer;
  const node=(tag,cls,text)=>{const item=document.createElement(tag);if(cls)item.className=cls;if(text)item.textContent=text;return item;};
  const status=(text,error=false)=>{$('#workspace-status').textContent=text;$('#workspace-status').dataset.error=String(error);};
  function lock(){blocked=true;shareUrl='';$('#chef-share-url').value='';$('#copy-chef-url').disabled=true;$('#share-chef-url').hidden=true;$('#open-customer-chef').hidden=true;$('#chef-personal-qr').hidden=true;$('#chef-leads').replaceChildren();$('#chef-leads-count').textContent='';$('#chef-workspace-user').textContent='Mi cuenta';clearTimeout(timer);}
  function renderLeads(rows){
    const list=$('#chef-leads');list.replaceChildren();
    if(!rows.length)list.append(node('p','chef-workspace-empty',scope==='team'?'Todavía no hay solicitudes del Chef en tu red.':'Todavía no tienes solicitudes del Chef. Comparte tu enlace o QR con tus clientes.'));
    for(const row of rows){
      const card=node('article','chef-lead'),header=node('div','chef-lead-header');header.append(node('h3','',row.fullName),node('span','chef-lead-badge',row.status==='nuevo'?'Nuevo':row.status));card.append(header);
      if(scope==='team')card.append(node('p','',`Vendedor: ${row.sellerName}`));
      card.append(node('p','',row.interest||'Información de productos'));if(row.city)card.append(node('p','',row.city));if(row.notes)card.append(node('p','',row.notes));
      if(row.appointmentRequested)card.append(node('p','','Demostración solicitada · '+row.appointmentPreferredTime+' · Pendiente de confirmar'));
      const contacts=node('div','chef-lead-contact');
      const phone=String(row.phone||'').replace(/[^+\d]/g,'');if(phone){const link=node('a','',row.phone);link.href='tel:'+phone;contacts.append(link);}
      if(row.email){const link=node('a','',row.email);link.href='mailto:'+encodeURIComponent(row.email);contacts.append(link);}card.append(contacts);
      const date=new Date(row.createdAt);if(!Number.isNaN(date.getTime())){const time=node('time','',date.toLocaleString('es',{dateStyle:'medium',timeStyle:'short'}));time.dateTime=date.toISOString();card.append(time);}list.append(card);
    }
    const fresh=rows.filter(row=>row.status==='nuevo').length;$('#chef-leads-count').textContent=rows.length+` ${rows.length===1?'solicitud':'solicitudes'} · `+fresh+` ${fresh===1?'nueva':'nuevas'}`+(rows.length===100?' · Se muestran las 100 más recientes':'');
  }
  async function refresh(){
    if(loading||blocked)return;loading=true;$('#refresh-chef-leads').disabled=true;clearTimeout(timer);
    try{
      const response=await fetch('/api/chef/workspace?scope='+scope,{credentials:'same-origin',cache:'no-store',headers:userId?{'X-TEC-Account-ID':userId}:{},signal:AbortSignal.timeout(30000)}),data=await response.json().catch(()=>({}));
      if(!response.ok){if([401,403].includes(response.status))lock();throw Error(data.error||'No pude cargar los contactos. Vuelve a intentarlo.');}
      if(userId&&data.user.id!==userId){lock();throw Error('La cuenta cambió en otra ventana. Recarga esta página para continuar.');}userId=data.user.id;
      document.title='Mi Chef · '+(data.user.name||'TEC');$('#chef-workspace-user').textContent='Mi cuenta · '+(data.user.name||'TEC');
      shareUrl=data.shareUrl;$('#chef-share-url').value=shareUrl;$('#copy-chef-url').disabled=!shareUrl;
      $('#open-customer-chef').href=shareUrl;$('#open-customer-chef').hidden=!shareUrl;
      $('#share-chef-url').hidden=!shareUrl||typeof navigator.share!=='function';
      const qr=$('#chef-personal-qr'),qrUrl='/api/chef/workspace/qr.svg?account='+encodeURIComponent(userId);if(shareUrl&&qr.getAttribute('src')!==qrUrl)qr.src=qrUrl;qr.hidden=!shareUrl;
      $('[data-chef-scope="team"]').hidden=!data.canViewTeam;
      $('#chef-scope-note').textContent=scope==='team'?'Tu red autorizada incluye tus leads y los de las personas debajo de ti. No muestra otras ramas ni a tu upline.':'Solo se muestran tus contactos. La lista se actualiza mientras esta página está abierta.';
      renderLeads(data.leads);status(data.sharingEnabled?'':'El acceso para compartir tu Chef no está disponible en este momento.',!data.sharingEnabled);
    }catch(error){status(error.message||'No pude actualizar. Los contactos que ya habías cargado se conservan.',true);}
    finally{loading=false;$('#refresh-chef-leads').disabled=false;if(!blocked&&!document.hidden)timer=setTimeout(refresh,20000);}
  }
  $('#refresh-chef-leads').addEventListener('click',()=>{if(blocked){location.reload();return;}refresh();});
  document.querySelectorAll('[data-chef-scope]').forEach(button=>button.addEventListener('click',()=>{
    if(loading||blocked)return;scope=button.dataset.chefScope;document.querySelectorAll('[data-chef-scope]').forEach(tab=>tab.setAttribute('aria-selected',String(tab===button)));refresh();
  }));
  $('#copy-chef-url').addEventListener('click',async()=>{if(!shareUrl||blocked)return;try{await navigator.clipboard.writeText(shareUrl);status('Enlace copiado.');}catch{$('#chef-share-url').focus();$('#chef-share-url').select();status('Seleccioné tu enlace para que puedas copiarlo.');}});
  $('#share-chef-url').addEventListener('click',async()=>{if(!shareUrl||blocked)return;try{await navigator.share({title:'Mi TEC Chef',text:'Te comparto mi Chef para recetas y ayuda con tus productos.',url:shareUrl});}catch(error){if(error.name!=='AbortError')status('No pude abrir Compartir. Puedes copiar tu enlace.',true);}});
  document.addEventListener('visibilitychange',()=>{clearTimeout(timer);if(!document.hidden)refresh();});window.addEventListener('pagehide',()=>clearTimeout(timer));refresh();
})();
