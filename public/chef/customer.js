(() => {
  'use strict';
  const $=selector=>document.querySelector(selector),id=()=>crypto.randomUUID();
  const slug=decodeURIComponent(location.pathname.split('/').filter(Boolean)[1]||'').toLowerCase();
  const suffix=slug?'?chef='+encodeURIComponent(slug):'',key='tec-chef-public:'+slug;
  let sessionId='',busy=false,pendingChat=null,sellerName='',contactAvailable=false,leadBusy=false,pendingLead=null;
  const node=(tag,cls,text)=>{const n=document.createElement(tag);if(cls)n.className=cls;if(text)n.textContent=text;return n;};
  const status=(target,text,error=false)=>{target.textContent=text;target.dataset.error=String(error);};
  const scroll=()=>{const area=$('.phone-conversation');area.scrollTop=area.scrollHeight;};
  async function api(kind,body,extra=''){
    const response=await fetch('/api/chef/public/'+kind+suffix+extra,{method:body?'POST':'GET',credentials:'same-origin',cache:'no-store',headers:body?{'Content-Type':'application/json'}:{},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(60000)});
    const data=await response.json().catch(()=>({}));if(!response.ok)throw Error(data.error||'No pude completar la solicitud. Conserva tus datos y vuelve a intentarlo.');return data;
  }
  function bubble(role,text,sources=[]){
    const card=node('div','chef-message '+(role==='user'?'user':'assistant'));card.append(node('div','message-label',role==='user'?'TÚ':'TEC CHEF'),node('div','',String(text).replace(/\*\*/g,'')));
    if(sources.length){const links=node('div','message-sources');for(const source of sources){try{const url=new URL(source.url);if(url.protocol!=='https:'||!['royalprestige.com','www.royalprestige.com','youtube.com','www.youtube.com'].includes(url.hostname))continue;const a=node('a','',source.title);a.href=url.href;a.target='_blank';a.rel='noopener noreferrer';links.append(a);}catch{}}card.append(links);}
    $('#messages').append(card);scroll();return card;
  }
  function setBusy(value){busy=value;$('#send').disabled=$('#question').disabled=$('#new-chat').disabled=value;document.querySelectorAll('[data-prompt]').forEach(button=>button.disabled=value);}
  function openContact(){if(!contactAvailable||leadBusy)return;$('#contact-dialog').showModal();$('#contact-form').elements.fullName.focus({preventScroll:true});}
  $('#request-information').addEventListener('click',openContact);
  $('#contact-close').addEventListener('click',()=>{if(!leadBusy)$('#contact-dialog').close();});
  $('#contact-dialog').addEventListener('cancel',event=>{if(leadBusy)event.preventDefault();});
  $('#contact-dialog').addEventListener('close',()=>$('#request-information').focus({preventScroll:true}));
  $('#request-demo').addEventListener('change',()=>{const checked=$('#request-demo').checked;$('#preferred-time-label').hidden=!checked;$('#contact-form').elements.appointmentPreferredTime.required=checked;});
  async function history(){
    const query=(suffix?'&':'?')+'sessionId='+encodeURIComponent(sessionId),data=await api('conversation',null,query);$('#messages').replaceChildren();
    if(!data.messages.length)bubble('assistant','¡Hola! Soy TEC Chef. ¿Qué ingredientes tienes o qué producto quieres usar?');
    for(const item of data.messages)bubble(item.role,item.content,item.sources);
  }
  async function send(question){
    question=String(question||'').trim();if(!question||busy)return;
    if(navigator.onLine===false){status($('#chat-status'),'Necesitas conexión. Tu pregunta sigue aquí.',true);return;}
    if(!pendingChat||pendingChat.question!==question)pendingChat={question,requestId:id()};
    setBusy(true);status($('#chat-status'),'El Chef está escribiendo…');
    try{const data=await api('chat',{pregunta:question,sessionId,requestId:pendingChat.requestId});bubble('user',question);const reply=bubble('assistant',data.respuesta,data.sources);if(data.appointmentSuggested&&contactAvailable){const b=node('button','chef-button compact appointment-action','Solicitar información');b.type='button';b.addEventListener('click',openContact);reply.append(b);}pendingChat=null;$('#question').value='';status($('#chat-status'),'');}
    catch(error){status($('#chat-status'),error.message||'No se completó la respuesta. Tu pregunta sigue aquí.',true);}finally{setBusy(false);$('#question').focus({preventScroll:true});}
  }
  $('#chat-form').addEventListener('submit',event=>{event.preventDefault();send($('#question').value);});
  $('#question').addEventListener('keydown',event=>{if(event.key==='Enter'&&!event.shiftKey&&!event.isComposing){event.preventDefault();$('#chat-form').requestSubmit();}});
  document.querySelectorAll('[data-prompt]').forEach(button=>button.addEventListener('click',()=>{$('#question').value=button.dataset.prompt;send(button.dataset.prompt);}));
  $('#new-chat').addEventListener('click',async()=>{if(busy)return;sessionId=id();try{sessionStorage.setItem(key,sessionId);}catch{}pendingChat=null;$('#question').value='';try{await history();status($('#chat-status'),'Nueva conversación.');}catch(error){status($('#chat-status'),error.message,true);}});
  $('#contact-form').addEventListener('submit',async event=>{
    event.preventDefault();if(leadBusy||!contactAvailable)return;const form=event.currentTarget;
    if(!form.reportValidity())return;
    const fields=new FormData(form),contact=String(fields.get('contact')||'').trim(),isEmail=/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(contact),phone=contact.replace(/[^+\d]/g,'');
    if(!isEmail&&!/^\+?\d{10,15}$/.test(phone)){status($('#contact-status'),'Revisa tu teléfono o correo.',true);return;}
    const body={fullName:fields.get('fullName'),phone:isEmail?'':phone,email:isEmail?contact:'',city:fields.get('city'),interest:fields.get('interest'),notes:fields.get('notes'),appointmentRequested:fields.has('appointmentRequested'),appointmentPreferredTime:fields.has('appointmentRequested')?fields.get('appointmentPreferredTime'):'',consentGiven:fields.has('consentGiven')};
    if(!body.consentGiven){status($('#contact-status'),'Confirma la autorización antes de enviar.',true);return;}
    leadBusy=true;$('#contact-submit').disabled=$('#contact-close').disabled=true;form.querySelectorAll('input,textarea').forEach(input=>input.disabled=true);status($('#contact-status'),'Guardando tu solicitud…');
    try{
      const bytes=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify(body))),fingerprint=Array.from(new Uint8Array(bytes),value=>value.toString(16).padStart(2,'0')).join('');
      let pending=pendingLead;try{pending=pending||JSON.parse(sessionStorage.getItem(key+':lead')||'null');}catch{}
      if(pending?.fingerprint!==fingerprint)pending={fingerprint,requestId:id()};
      pendingLead=pending;try{sessionStorage.setItem(key+':lead',JSON.stringify(pending));}catch{}
      const result=await api('leads',{...body,requestId:pending.requestId});
      if(!result.ok)throw Error('No se confirmó el guardado. Conserva tus datos e inténtalo de nuevo.');
      pendingLead=null;try{sessionStorage.removeItem(key+':lead');}catch{}
      const message='Tu solicitud quedó guardada para '+result.sellerName+'. Su equipo te contactará para confirmar la información'+(body.appointmentRequested?' y la disponibilidad.':'.');
      bubble('assistant',message);form.reset();$('#preferred-time-label').hidden=true;form.elements.appointmentPreferredTime.required=false;$('#contact-dialog').close();status($('#chat-status'),'Solicitud enviada.');status($('#contact-status'),'');
    }catch(error){status($('#contact-status'),error.message||'No se confirmó el envío. Tus datos siguen aquí para reintentar.',true);}
    finally{leadBusy=false;$('#contact-submit').disabled=$('#contact-close').disabled=false;form.querySelectorAll('input,textarea').forEach(input=>input.disabled=false);}
  });
  async function init(){
    try{
      const data=await api('bootstrap');sellerName=data.sellerName;contactAvailable=data.contactAvailable;
      $('#representative-note').textContent=contactAvailable?'Este Chef te lo comparte '+sellerName+'.':'Recetas e ideas para tu cocina, sin iniciar sesión.';
      $('#recipient-name').textContent=$('#consent-recipient').textContent=sellerName;
      $('#request-information').hidden=!contactAvailable;
      try{sessionId=sessionStorage.getItem(key)||id();sessionStorage.setItem(key,sessionId);}catch{sessionId=id();}
      $('#customer-content').hidden=false;await history();$('#access-status').hidden=true;
    }catch(error){status($('#access-status'),error.message||'No pude abrir el Chef. Intenta recargar esta página.',true);}
  }
  init();
})();
