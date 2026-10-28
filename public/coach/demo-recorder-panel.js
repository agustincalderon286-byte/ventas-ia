(() => {
  'use strict';
  const panel=document.querySelector('[data-demo-recorder]'),toggle=document.querySelector('[data-demo-panel-toggle]');
  if(!panel||!toggle)return;
  const close=()=>{panel.close();toggle.setAttribute('aria-expanded','false');toggle.focus({preventScroll:true});};
  toggle.addEventListener('click',()=>{if(panel.open){close();return;}panel.showModal();toggle.setAttribute('aria-expanded','true');});
  panel.querySelector('[data-demo-panel-close]').addEventListener('click',close);
  panel.addEventListener('cancel',event=>{event.preventDefault();close();});
})();
