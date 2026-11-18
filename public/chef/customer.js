(() => {
  'use strict';
  const $=selector=>document.querySelector(selector),id=()=>crypto.randomUUID();
  const slug=decodeURIComponent(location.pathname.split('/').filter(Boolean)[1]||'').toLowerCase();
  const suffix=slug?'?chef='+encodeURIComponent(slug):'',key='tec-chef-public:'+slug;
  const alexaAppointment=Boolean(slug&&new URLSearchParams(location.search).get('origen')==='alexa');
  let photo='',preparingPhoto=false,voice=null,voiceBusy=false,resetting=false;
  let sessionId='',busy=false,pendingChat=null,sellerName='',contactAvailable=false,leadBusy=false,pendingLead=null;
  const node=(tag,cls,text)=>{const n=document.createElement(tag);if(cls)n.className=cls;if(text)n.textContent=text;return n;};
  const status=(target,text,error=false)=>{target.textContent=text;target.dataset.error=String(error);};
  const scroll=()=>{const area=$('.phone-conversation');area.scrollTop=area.scrollHeight;};
  async function api(kind,body,extra=''){
    const response=await fetch('/api/chef/public/'+kind+suffix+extra,{method:body?'POST':'GET',credentials:'same-origin',cache:'no-store',headers:body?{'Content-Type':'application/json'}:{},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(60000)});
    const data=await response.json().catch(()=>({}));if(!response.ok)throw Error(data.error||'No pude completar la solicitud. Conserva tus datos y vuelve a intentarlo.');return data;
  }
  function bubble(role,text,sources=[],hasPhoto=false){
    const card=node('div','chef-message '+(role==='user'?'user':'assistant'));
    card.append(node('div','message-label',role==='user'?'TÚ':'TEC CHEF'));
    if(hasPhoto&&role==='user')card.append(node('div','message-photo-label','Foto adjunta'));
    card.append(node('div','',String(text).replace(/\*\*/g,'')));
    if(sources.length){const links=node('div','message-sources');for(const source of sources){try{const url=new URL(source.url);if(url.protocol!=='https:'||!['royalprestige.com','www.royalprestige.com','youtube.com','www.youtube.com'].includes(url.hostname))continue;const a=node('a','',source.title);a.href=url.href;a.target='_blank';a.rel='noopener noreferrer';links.append(a);}catch{}}card.append(links);}
    $('#messages').append(card);scroll();return card;
  }
  function syncActions(){
    const locked = busy || preparingPhoto || voiceBusy || resetting;
    $('#new-chat').disabled=$('#attach-photo').disabled=$('#remove-photo').disabled=locked;
    $('.chef-prompts').hidden=Boolean(photo);
    document.querySelectorAll('[data-prompt]').forEach(button=>button.disabled=locked);
  }
  function syncComposer(){
    const locked = busy || preparingPhoto || resetting;
    if (voice) voice.setChatBusy(locked);
    else $('#question').disabled=$('#send').disabled=locked;
    syncActions();
  }
  function initVoice(){
    voice=globalThis.TecCoachVoice?.init({
      forms:[$('#chat-form')], speaker:'Chef', toolbarSelector:'.composer',
      hasExtraInput:()=>Boolean(photo), maxBytes:5*1024*1024,
      voiceApi:{transcription:'/api/chef/public/voice/transcribe'+suffix,speech:'/api/chef/public/voice/speech'+suffix},
      sendMessage:send,
      onBusyChange:active=>{voiceBusy=active;syncActions();}
    });
  }
  function setPhoto(encoded){
    photo=encoded;
    const preview=$('#photo-preview'),thumb=$('#photo-thumb');
    preview.hidden=!encoded;
    if(encoded)thumb.src='data:image/jpeg;base64,'+encoded;
    else thumb.removeAttribute('src');
    $('#question').placeholder=encoded?'Comenta si quieres…':'Escríbele al Chef…';
    syncComposer();
  }
  async function preparePhoto(file){
    if(file.type==='image/svg+xml')throw Error('Usa una foto de la cámara o la galería.');
    if(!file.size||file.size>25*1024*1024)throw Error('Usa una foto de hasta 25 MB.');
    const url=URL.createObjectURL(file);
    try{
      const image=await new Promise((resolve,reject)=>{
        const picture=new Image();
        picture.onload=()=>resolve(picture);
        picture.onerror=()=>reject(Error('No pude abrir esta imagen. Prueba con otra foto o con un archivo JPG.'));
        picture.src=url;
      });
      const {naturalWidth:width,naturalHeight:height}=image;
      if(!width||!height||width*height>60000000)throw Error('La foto es demasiado grande. Prueba otra imagen.');
      const scale=Math.min(1,1440/Math.max(width,height));
      const canvas=node('canvas');canvas.width=Math.max(1,Math.round(width*scale));canvas.height=Math.max(1,Math.round(height*scale));
      const ctx=canvas.getContext('2d',{alpha:false});if(!ctx)throw Error('No pude preparar la foto en este navegador.');
      ctx.fillStyle='#fff';ctx.fillRect(0,0,canvas.width,canvas.height);ctx.drawImage(image,0,0,canvas.width,canvas.height);
      const jpeg=canvas.toDataURL('image/jpeg',.82),prefix='data:image/jpeg;base64,';
      if(!jpeg.startsWith(prefix)||jpeg.length-prefix.length>4*Math.ceil((2*1024*1024)/3))throw Error('No pude preparar la foto. Prueba otra más pequeña.');
      return jpeg.slice(prefix.length);
    }finally{URL.revokeObjectURL(url);}
  }
  $('#attach-photo').addEventListener('click',()=>$('#photo-picker').showModal());
  $('#close-photo').addEventListener('click',()=>$('#photo-picker').close());
  for(const [button,input] of [['#take-photo','#photo-camera'],['#choose-photo','#photo-file']]){
    $(button).addEventListener('click',()=>{$('#photo-picker').close();$(input).click();});
    $(input).addEventListener('change',async event=>{
      const control=event.currentTarget,file=control.files?.[0];control.value='';
      if(!file||busy||preparingPhoto||voiceBusy||resetting)return;
      preparingPhoto=true;syncComposer();status($('#chat-status'),'Preparando la foto…');
      try{setPhoto(await preparePhoto(file));status($('#chat-status'),'');}
      catch(error){status($('#chat-status'),error.message,true);}
      finally{preparingPhoto=false;syncComposer();}
    });
  }
  $('#remove-photo').addEventListener('click',()=>{setPhoto('');status($('#chat-status'),'');$('#attach-photo').focus({preventScroll:true});});
  function openContact(){
    if(!contactAvailable||leadBusy||$('#contact-dialog').open)return;
    if(alexaAppointment){
      $('#contact-heading').textContent='Solicitar demostración';
      $('#request-demo').checked=true;
      updateAppointmentFields();
    }
    $('#contact-dialog').showModal();
    $('#contact-form').elements.fullName.focus({preventScroll:true});
  }
  $('#request-information').addEventListener('click',openContact);
  $('#contact-close').addEventListener('click',()=>{if(!leadBusy)$('#contact-dialog').close();});
  $('#contact-dialog').addEventListener('cancel',event=>{if(leadBusy)event.preventDefault();});
  $('#contact-dialog').addEventListener('close',()=>$('#request-information').focus({preventScroll:true}));
  function updateAppointmentFields(){
    const checked=$('#request-demo').checked;
    $('#preferred-time-label').hidden=!checked;
    $('#contact-form').elements.appointmentPreferredTime.required=checked;
  }
  $('#request-demo').addEventListener('change',updateAppointmentFields);
  async function history(){
    const query=(suffix?'&':'?')+'sessionId='+encodeURIComponent(sessionId),data=await api('conversation',null,query);$('#messages').replaceChildren();
    if(!data.messages.length)bubble('assistant','¡Hola! Soy TEC Chef. Cuéntame qué tienes a mano o envíame una foto para pensar en un platillo.');
    for(const item of data.messages)bubble(item.role,item.content,item.sources,item.hasPhoto);
  }
  async function send(question,audio=null){
    question=String(question||'').trim();
    const uploaded=photo, audioUrl=audio?.audioUrl;
    if(busy||preparingPhoto||resetting||(voiceBusy&&!audioUrl))return{ok:false};
    if(!question&&!uploaded){status($('#chat-status'),'Escribe una pregunta o adjunta una foto.',true);return{ok:false};}
    if(navigator.onLine===false){status($('#chat-status'),'Necesitas conexión. Tu mensaje sigue aquí.',true);return{ok:false};}
    if(!pendingChat||pendingChat.question!==question||pendingChat.photo!==uploaded||pendingChat.audioUrl!==audioUrl){
      pendingChat={question,photo:uploaded,audioUrl,requestId:id()};
    }
    const {requestId}=pendingChat, target=$('.phone-conversation');
    if (!audioUrl) voice?.pausePlayback();
    if(audioUrl&&!pendingChat.voiceCard){
      const card=bubble('user',question,[],Boolean(uploaded));
      pendingChat.voiceCard=card;
      voice.attachInput([{card,target}],audioUrl,audio.audioDuration);
    }
    busy=true;syncComposer();status($('#chat-status'),uploaded?'El Chef está revisando la foto…':'El Chef está preparando tu respuesta…');
    try{
      const data=await api('chat',{pregunta:question,sessionId,requestId,...(audioUrl?{responseMode:'audio'}:{}),...(uploaded?{photo:uploaded}:{})});
      if(!audioUrl)bubble('user',question||(uploaded?'¿Qué platillos puedo preparar con lo que hay en la foto?':''),[],Boolean(uploaded));
      const reply=bubble('assistant',data.respuesta,data.sources);
      if(data.appointmentSuggested&&contactAvailable){const b=node('button','chef-button compact appointment-action','Solicitar información');b.type='button';b.addEventListener('click',openContact);reply.append(b);}
      pendingChat=null;
      if(uploaded)setPhoto('');
      $('#question').value='';status($('#chat-status'),'');
      if(audioUrl)await voice.speak(data.respuesta,[{card:reply,target}],{sessionId,requestId});
      return{ok:true};
    }catch(error){status($('#chat-status'),error.message||'No se completó la respuesta. Tu mensaje sigue aquí.',true);return{ok:false};}
    finally{busy=false;syncComposer();if(!uploaded&&!audioUrl)$('#question').focus({preventScroll:true});}
  }
  $('#chat-form').addEventListener('submit',event=>{event.preventDefault();send($('#question').value);});
  $('#question').addEventListener('keydown',event=>{if(event.key==='Enter'&&!event.shiftKey&&!event.isComposing){event.preventDefault();$('#chat-form').requestSubmit();}});
  document.querySelectorAll('[data-prompt]').forEach(button=>button.addEventListener('click',()=>{$('#question').value=button.dataset.prompt;send(button.dataset.prompt);}));
  $('#new-chat').addEventListener('click',async()=>{
    if(busy||preparingPhoto||voiceBusy||resetting)return;
    resetting=true;syncComposer();
    voice?.reset();
    sessionId=id();try{sessionStorage.setItem(key,sessionId);}catch{}
    pendingChat=null;if(photo)setPhoto('');$('#question').value='';
    try{await history();status($('#chat-status'),'Nueva conversación.');}
    catch(error){status($('#chat-status'),error.message,true);}
    finally{resetting=false;syncComposer();}
  });
  $('#contact-form').addEventListener('submit',async event=>{
    event.preventDefault();if(leadBusy||!contactAvailable)return;const form=event.currentTarget;
    if(!form.reportValidity())return;
    const fields=new FormData(form),contact=String(fields.get('contact')||'').trim(),isEmail=/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(contact),phone=contact.replace(/[^+\d]/g,'');
    if(!isEmail&&!/^\+?\d{10,15}$/.test(phone)){status($('#contact-status'),'Revisa tu teléfono o correo.',true);return;}
    const body={fullName:fields.get('fullName'),phone:isEmail?'':phone,email:isEmail?contact:'',city:fields.get('city'),interest:fields.get('interest'),notes:fields.get('notes'),appointmentRequested:fields.has('appointmentRequested'),appointmentPreferredTime:fields.has('appointmentRequested')?fields.get('appointmentPreferredTime'):'',consentGiven:fields.has('consentGiven')};
    if(alexaAppointment&&body.appointmentRequested)body.origin='alexa';
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
      const data=await api('bootstrap');$('#attach-photo').hidden=data.canAttachPhoto!==true;sellerName=data.sellerName;contactAvailable=data.contactAvailable;
      $('#representative-note').textContent=contactAvailable?'Este Chef te lo comparte '+sellerName+'.':'Recetas e ideas para tu cocina, sin iniciar sesión.';
      $('#recipient-name').textContent=$('#consent-recipient').textContent=sellerName;
      $('#request-information').hidden=!contactAvailable;
      if(alexaAppointment)openContact();
      try{sessionId=sessionStorage.getItem(key)||id();sessionStorage.setItem(key,sessionId);}catch{sessionId=id();}
      $('#customer-content').hidden=false;await history();
      if(data.canSendVoice) initVoice();
      $('#access-status').hidden=true;
    }catch(error){status($('#access-status'),error.message||'No pude abrir el Chef. Intenta recargar esta página.',true);}
  }
  init();
})();
