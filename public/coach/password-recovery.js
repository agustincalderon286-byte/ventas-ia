(() => {
  'use strict';
  const requestPanel=document.querySelector('[data-recovery-request]'),resetPanel=document.querySelector('[data-recovery-reset]'),completePanel=document.querySelector('[data-recovery-complete]'),invalidPanel=document.querySelector('[data-recovery-invalid]');
  if(!requestPanel||!resetPanel)return;
  const requestForm=requestPanel.querySelector('form'),resetForm=resetPanel.querySelector('form');
  let token='',flow=0;
  const pending=new Set();
  const fieldError=(form,name,message)=>{const field=form.elements.namedItem(name),node=form.querySelector(`[data-error-${name}]`);if(field)field.setAttribute('aria-invalid',message?'true':'false');if(node){node.textContent=message;node.hidden=!message;}};
  const clear=(form)=>{for(const field of form.querySelectorAll('input'))fieldError(form,field.name,'');};
  const result=(node,text,state='')=>{node.textContent=text;node.dataset.state=state;};
  function adoptFragment(){
    const raw=window.location.hash;
    token=new URLSearchParams(raw.slice(1)).get('token') || '';
    // Same-document links are handled too; the secret never enters navigation requests or visible DOM.
    if(raw)window.history.replaceState(null,'',window.location.pathname);
    flow++;
    for(const controller of pending)controller.abort();
    for(const form of [requestForm,resetForm]){form.reset();clear(form);form.querySelector('[type=submit]').disabled=false;}
    result(requestForm.querySelector('[data-request-result]'),'');result(resetForm.querySelector('[data-reset-result]'),'');
    requestPanel.hidden=Boolean(raw);resetPanel.hidden=true;completePanel.hidden=true;invalidPanel.hidden=true;
    if(raw){if(/^[A-Za-z0-9_-]{43}$/.test(token))resetPanel.hidden=false;else{token='';invalidPanel.hidden=false;}}
  }
  adoptFragment();window.addEventListener('hashchange',adoptFragment);
  async function send(path,body){const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),20000);pending.add(controller);try{const response=await fetch(path,{method:'POST',credentials:'same-origin',cache:'no-store',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),signal:controller.signal});let data;try{data=await response.json();}catch{throw new Error('No pudimos completar la solicitud. Inténtalo de nuevo.');}if(!response.ok)throw Object.assign(new Error(data.error||'Vuelve a intentarlo.'),{field:data.field});return data;}finally{pending.delete(controller);clearTimeout(timeout);}}
  for(const form of [requestForm,resetForm]){form.noValidate=true;form.addEventListener('input',event=>{if(event.target.name)fieldError(form,event.target.name,'');});}
  requestForm.addEventListener('submit',async event=>{event.preventDefault();const button=requestForm.querySelector('[type=submit]');if(button.disabled)return;const version=flow;clear(requestForm);const email=requestForm.elements.namedItem('email'),status=requestForm.querySelector('[data-request-result]');result(status,'');if(!email.value.trim()||!email.validity.valid){fieldError(requestForm,'email','Escribe el correo de tu cuenta.');email.focus();return;}button.disabled=true;try{const data=await send('/api/coach/password/request',{email:email.value.trim()});if(version!==flow)return;result(status,data.message,'success');}catch(error){if(version!==flow)return;result(status,error.name==='AbortError'?'La conexión tardó demasiado. Revisa tu correo antes de solicitar otro enlace.':error.message,'error');if(error.field)fieldError(requestForm,error.field,error.message);}finally{if(version===flow)button.disabled=false;}});
  resetForm.addEventListener('submit',async event=>{event.preventDefault();const button=resetForm.querySelector('[type=submit]');if(button.disabled)return;const version=flow;clear(resetForm);const password=resetForm.elements.namedItem('password'),confirmation=resetForm.elements.namedItem('passwordConfirmation'),status=resetForm.querySelector('[data-reset-result]');result(status,'');if(password.value.trim().length<8||password.value.length>128){fieldError(resetForm,'password','Usa una contraseña de 8 a 128 caracteres.');password.focus();return;}if(password.value!==confirmation.value){fieldError(resetForm,'passwordConfirmation','Las contraseñas deben ser iguales.');confirmation.focus();return;}button.disabled=true;try{await send('/api/coach/password/reset',{token,password:password.value,passwordConfirmation:confirmation.value});if(version!==flow)return;token='';resetForm.reset();resetPanel.hidden=true;completePanel.hidden=false;completePanel.querySelector('a').focus();}catch(error){if(version!==flow)return;result(status,error.name==='AbortError'?'La conexión tardó demasiado. Intenta entrar con tu nueva contraseña antes de volver a usar este enlace.':error.message,'error');if(error.field)fieldError(resetForm,error.field,error.message);}finally{if(version===flow)button.disabled=false;}});
})();
