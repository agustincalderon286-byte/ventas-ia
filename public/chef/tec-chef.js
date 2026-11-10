(() => {
 'use strict';
 const $=selector=>document.querySelector(selector),messages=$('#messages'),flow=globalThis.TecChefAppointmentFlow;
 let userId='',sessionId='',busy=false,pendingChat=null,appointment=null,emailReady=false;
 const id=()=>crypto.randomUUID();
 const node=(tag,cls,text)=>{const n=document.createElement(tag);if(cls)n.className=cls;if(text)n.textContent=text;return n;};
 async function api(url,body){const response=await fetch(url,{method:body?'POST':'GET',credentials:'same-origin',cache:'no-store',headers:body?{'Content-Type':'application/json'}:{},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(60000)});const data=await response.json().catch(()=>({}));if(!response.ok)throw Error(data.error||'No pude completar la solicitud. Inténtalo de nuevo.');return data;}
 function status(target,value,error=false){target.textContent=value;target.dataset.error=String(error);}
 function scrollChat(){const area=$('.phone-conversation');area.scrollTop=area.scrollHeight;}
 function safeSource(url){try{const u=new URL(url);return u.protocol==='https:'&&['www.royalprestige.com','royalprestige.com','www.youtube.com','youtube.com'].includes(u.hostname)?u.href:'';}catch{return '';}}
 function bubble(role,content,sources=[]){
  const n=node('div','chef-message '+(role==='user'?'user':'assistant'));n.append(node('div','message-label',role==='user'?'TÚ':'TEC CHEF'),node('div','',String(content).replace(/\*\*/g,'')));
  if(sources.length){const links=node('div','message-sources');for(const source of sources){const url=safeSource(source.url);if(!url)continue;const u=new URL(url),videoId=u.hostname.includes('youtube.com')?u.searchParams.get('v'):'';
   if(videoId&&globalThis.TecProductVideoCatalog?.byId(videoId)){const b=node('button','chef-button secondary compact','▶ '+source.title);b.type='button';b.addEventListener('click',()=>window.TecProductVideos.openVideo(videoId,b));links.append(b);}
   else{const a=node('a','',source.title);a.href=url;a.target='_blank';a.rel='noopener noreferrer';links.append(a);}}
   n.append(links);
  }
  messages.append(n);scrollChat();return n;
 }
 function setBusy(value){
  busy=value;$('#new-chat').disabled=value;
  $('#send').disabled=$('#question').disabled=value||Boolean(appointment&&appointment.step>=flow.steps.length);
  document.querySelectorAll('[data-prompt]').forEach(b=>b.disabled=value||Boolean(appointment));
  $('#appointment-start').disabled=value||Boolean(appointment);$('#appointment-cancel').disabled=value;$('#appointment-submit').disabled=value;
 }
 function updateComposer(){
  $('#appointment-review').hidden=!(appointment&&appointment.step>=flow.steps.length);
  $('#appointment-cancel').hidden=!appointment;
  $('#question').placeholder=appointment?(flow.steps[appointment.step]?.placeholder||'Revisa la solicitud para enviarla'):'Escríbele al Chef…';
  $('#question').maxLength=appointment?(flow.steps[appointment.step]?.max||3000):3000;
  setBusy(busy);
 }
 function cancelAppointment(){appointment=null;$('#appointment-consent').checked=false;$('#question').value='';updateComposer();status($('#chat-status'),'Solicitud cancelada. No se enviaron datos.');}
 function startAppointment(){
  if(busy||appointment)return;
  appointment=flow.create(id());$('#appointment-consent').checked=false;$('#question').value='';updateComposer();
  bubble('assistant','Te ayudo a solicitar información. Enviaremos los datos a Agustín Calderón para que confirme contigo la cita. '+flow.steps[0].question);
  status($('#chat-status'),'Solicitud · Paso 1 de '+flow.steps.length);$('#question').focus({preventScroll:true});
 }
 function answerAppointment(value){
  try{
   const next=flow.answer(appointment,value);bubble('user',value.trim());appointment=next;$('#question').value='';updateComposer();
   if(appointment.step<flow.steps.length){bubble('assistant',flow.steps[appointment.step].question);status($('#chat-status'),`Solicitud · Paso ${appointment.step+1} de ${flow.steps.length}`);$('#question').focus({preventScroll:true});}
   else{
    $('#appointment-summary').replaceChildren();for(const [label,value] of flow.summary(appointment))$('#appointment-summary').append(node('dt','',label),node('dd','',value));
    status($('#appointment-status'),emailReady?'Se enviará a agustincalderon286@gmail.com al confirmar.':'El envío por correo todavía no está configurado. La solicitud sí puede guardarse en TEC.',!emailReady);
    status($('#chat-status'),'Revisa los datos y confirma el envío.');scrollChat();
   }
  }catch(e){status($('#chat-status'),e.message,true);}
 }
 async function loadHistory(){messages.replaceChildren();const data=await api('/api/chef/admin/conversation?sessionId='+encodeURIComponent(sessionId));if(!data.messages.length)bubble('assistant','¡Hola! Soy TEC Chef, tu ayudante en la cocina. ¿Qué ingredientes tienes o qué producto quieres usar?');for(const row of data.messages)bubble(row.role,row.content,row.sources);}
 async function sendQuestion(question){
  if(busy||!question.trim())return;
  if(appointment){answerAppointment(question);return;}
  question=question.trim();if(navigator.onLine===false){status($('#chat-status'),'Necesitas internet para consultar al Chef. Tu pregunta sigue aquí.',true);return;}
  if(!pendingChat||pendingChat.question!==question)pendingChat={question,requestId:id()};setBusy(true);status($('#chat-status'),'El Chef está escribiendo…');
  try{const data=await api('/api/chef/admin/chat',{pregunta:question,sessionId,requestId:pendingChat.requestId});bubble('user',question);const reply=bubble('assistant',data.respuesta,data.sources);
   if(data.appointmentSuggested){const button=node('button','chef-button appointment-action','Solicitar cita');button.type='button';button.addEventListener('click',startAppointment);reply.append(button);scrollChat();}
   pendingChat=null;$('#question').value='';status($('#chat-status'),'');
  }catch(e){status($('#chat-status'),e.message,true);}finally{setBusy(false);$('#question').focus({preventScroll:true});}
 }
 async function loadLeads(){
  const data=await api('/api/chef/admin/leads');$('#leads').replaceChildren();
  if(!data.leads.length)$('#leads').append(node('p','small','Aquí aparecerán las solicitudes que confirmes durante la prueba.'));
  for(const row of data.leads){const item=node('article','lead-item');item.append(node('strong','',row.fullName),node('p','',[row.phone,row.email,row.city].filter(Boolean).join(' · ')),node('p','',row.interest||'Interés por confirmar'));if(row.appointmentRequested)item.append(node('p','','Solicitud de cita · '+row.appointmentPreferredTime));$('#leads').append(item);}
 }
 $('#question').addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey&&!e.isComposing){e.preventDefault();$('#chat-form').requestSubmit();}});
 $('#chat-form').addEventListener('submit',e=>{e.preventDefault();sendQuestion($('#question').value);});
 document.querySelectorAll('[data-prompt]').forEach(button=>button.addEventListener('click',()=>{if(appointment)return;$('#question').value=button.dataset.prompt;sendQuestion(button.dataset.prompt);}));
 $('#appointment-start').addEventListener('click',startAppointment);$('#appointment-cancel').addEventListener('click',cancelAppointment);
 $('#appointment-submit').addEventListener('click',async()=>{
  if(busy||!appointment||appointment.step!==flow.steps.length)return;
  if(!$('#appointment-consent').checked){status($('#appointment-status'),'Confirma la autorización de contacto antes de enviar.',true);return;}
  if(navigator.onLine===false){status($('#appointment-status'),'Necesitas conexión para enviar. Los datos siguen aquí.',true);return;}
  setBusy(true);status($('#appointment-status'),'Guardando y enviando tu solicitud…');
  try{
   const result=await api('/api/chef/admin/leads',{...appointment.data,requestId:appointment.requestId,consentGiven:true});
   const sent=result.emailStatus==='sent';
   const message=sent?'Solicitud guardada en TEC y enviada a Agustín Calderón. Él confirmará la disponibilidad contigo.':'Solicitud guardada en TEC. No pudimos confirmar el envío del correo; la cita sigue pendiente de confirmación.';
   appointment=null;$('#appointment-consent').checked=false;updateComposer();bubble('assistant',message);status($('#lead-status'),message,!sent);status($('#chat-status'),'');
   await loadLeads().catch(()=>status($('#lead-status'),message+' Actualiza la lista para verla.',!sent));
  }catch(e){status($('#appointment-status'),e.message,true);}finally{setBusy(false);}
 });
 $('#new-chat').addEventListener('click',async()=>{if(busy)return;appointment=null;updateComposer();sessionId=id();try{sessionStorage.setItem('tec-chef-session:'+userId,sessionId);}catch{}pendingChat=null;$('#question').value='';status($('#chat-status'),'Nueva conversación.');try{await loadHistory();}catch(e){status($('#chat-status'),e.message,true);}});
 $('#refresh-leads').addEventListener('click',()=>loadLeads().catch(e=>status($('#lead-status'),e.message,true)));
 async function init(){
  try{
   const data=await api('/api/chef/admin');userId=data.user.id;emailReady=data.leadEmailReady===true;$('#admin-name').textContent=data.user.name||'administrador';$('#recipe-count').textContent=data.manifest.recipes;$('#product-count').textContent=data.manifest.products;
   try{sessionId=sessionStorage.getItem('tec-chef-session:'+userId)||id();sessionStorage.setItem('tec-chef-session:'+userId,sessionId);}catch{sessionId=id();}
   $('#chef-content').hidden=false;$('#access-status').hidden=true;await Promise.all([loadHistory(),loadLeads()]);
  }catch(e){status($('#access-status'),e.message,true);}
 }
 init();
})();
