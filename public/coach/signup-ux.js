(() => {
  'use strict';
  const controllers=new WeakMap();
  function init(form){
    if(!form)return null;
    if(controllers.has(form))return controllers.get(form);
    const fields=Array.from(form.querySelectorAll('input')).filter(input=>['name','email','password'].includes(input.name));
    const message=form.querySelector('.form-result'),existing=form.querySelector('[data-signup-existing-account]');
    let ready=false;
    form.noValidate=true;
    function fieldError(input){
      const value=String(input.value||'');
      if(input.name==='name')return value.trim()?'':'Escribe tu nombre.';
      if(input.name==='email')return !value.trim()?'Escribe tu correo.':(input.validity?.typeMismatch || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim()))?'Escribe un correo válido, por ejemplo nombre@correo.com.':'';
      return value.length<8?'Crea una contraseña de al menos 8 caracteres.':'';
    }
    function showField(input,text){
      const node=form.querySelector(`[data-signup-error="${input.name}"]`);
      input.setAttribute('aria-invalid',String(Boolean(text)));
      if(node){node.textContent=text;node.hidden=!text;}
    }
    function clearMessage(){if(message){message.textContent='';message.dataset.state='';message.classList.remove('is-visible');}if(existing)existing.hidden=true;}
    function validate(){
      clearMessage();let first=null;
      for(const input of fields){const error=fieldError(input);showField(input,error);if(error&&!first)first=input;}
      if(first){if(message){message.textContent='Revisa los campos marcados para continuar.';message.dataset.state='error';message.classList.add('is-visible');}first.focus();return false;}
      return true;
    }
    function serverError(error){
      let field='',text='',link=false;const status=Number(error?.status||0),detail=String(error?.message||'').toLowerCase();
      if(status===409){field='email';text='Este correo ya tiene una cuenta. Entra para continuar.';link=true;}
      else if(status===400){field=/nombre/.test(detail)?'name':/correo|email/.test(detail)?'email':/contras|password/.test(detail)?'password':'';text=field==='name'?'Escribe tu nombre.':field==='email'?'Revisa el correo que escribiste.':field==='password'?'Crea una contraseña de al menos 8 caracteres.':'Revisa tus datos e intenta de nuevo.';}
      else if(status===403){text='Esta cuenta requiere acceso por administración. Si ya tienes acceso, entra al Coach.';link=true;}
      else if(status===429)text='Has realizado varios intentos. Espera unos minutos antes de volver a intentar.';
      else{text='No pudimos continuar con el registro o el pago. Intenta de nuevo. Si tu cuenta ya se creó, entra para continuar con el pago.';link=true;}
      const input=fields.find(node=>node.name===field);if(input){showField(input,text);input.focus();}
      if(existing)existing.hidden=!link;
      return text;
    }
    fields.forEach(input=>{
      input.addEventListener('blur',()=>showField(input,fieldError(input)));
      input.addEventListener('input',()=>{if(input.getAttribute('aria-invalid')==='true')showField(input,fieldError(input));clearMessage();});
    });
    // Prevent accidental native navigation while the account/plan controls are preparing.
    form.addEventListener('submit',event=>{
      event.preventDefault();
      if(!ready){if(message){message.textContent='Estamos preparando el registro. Espera un momento y vuelve a intentar.';message.dataset.state='info';message.classList.add('is-visible');}event.stopImmediatePropagation();}
      else if(!validate())event.stopImmediatePropagation();
    },true);
    const controller={validate,serverError,setReady:()=>{ready=true;}};
    controllers.set(form,controller);return controller;
  }
  window.TecSignupUx=Object.freeze({init});
  init(document.querySelector('[data-signup-checkout-form]'));
})();
