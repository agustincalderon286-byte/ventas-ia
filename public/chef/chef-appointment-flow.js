(function(root){
 'use strict';
 const steps=[
  {key:'fullName',label:'Nombre',question:'¿Cuál es el nombre de la persona que desea información?',placeholder:'Nombre completo',max:120,min:2},
  {key:'contact',label:'Contacto',question:'¿A qué teléfono o correo puede contactarla Agustín?',placeholder:'Teléfono de 10 dígitos o correo',max:160,min:5},
  {key:'city',label:'Ciudad',question:'¿En qué ciudad se encuentra?',placeholder:'Ciudad y estado',max:120,min:2},
  {key:'interest',label:'Producto',question:'¿Qué producto le interesa conocer?',placeholder:'Por ejemplo: Power Blender Max',max:200,min:2},
  {key:'appointmentPreferredTime',label:'Horario',question:'¿Qué día y horario le vienen bien? Es una preferencia; Agustín confirmará disponibilidad.',placeholder:'Por ejemplo: martes después de las 5 p. m.',max:160,min:3}
 ];
 function answer(state,value){
  const step=steps[state.step];if(!step)throw Error('Revisa y confirma la solicitud.');
  const clean=String(value||'').trim();if(clean.length<step.min||clean.length>step.max)throw Error(`Escribe ${step.label.toLowerCase()} entre ${step.min} y ${step.max} caracteres.`);
  const data={...state.data};
  if(step.key==='contact'){
   const email=clean.toLowerCase(),phone=clean.replace(/[^+\d]/g,'');
   if(/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)){data.email=email;data.phone='';}
   else if(/^\+?\d{10,15}$/.test(phone)){data.phone=phone;data.email='';}
   else throw Error('Escribe un teléfono de 10 a 15 dígitos o un correo válido.');
  }else data[step.key]=clean;
  return {...state,step:state.step+1,data};
 }
 const summary=state=>[['Nombre',state.data.fullName],['Contacto',state.data.phone||state.data.email],['Ciudad',state.data.city],['Producto',state.data.interest],['Horario preferido',state.data.appointmentPreferredTime]];
 root.TecChefAppointmentFlow=Object.freeze({steps,answer,summary,create:requestId=>({requestId,step:0,data:{appointmentRequested:true,notes:''}})});
})(globalThis);
