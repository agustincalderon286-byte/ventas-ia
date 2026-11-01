// App-only search. The underlying Cook County address/city fields remain unchanged.
export function parseCombinedAddress(value) {
  const text=String(value||'').trim().replace(/\s+/g,' ');
  if(!/^\d[\dA-Z/-]*\s+[\p{L}]/iu.test(text)||text.length>220)return null;
  let street='',city='';
  const comma=text.split(',').map(x=>x.trim()).filter(Boolean);
  if(comma.length>=2){street=comma[0];city=comma[1];}
  else {
    const match=text.match(/^(.*\b(?:STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|BOULEVARD|BLVD|LANE|LN|COURT|CT|PLACE|PL|TERRACE|TER|PARKWAY|PKWY))\s+(.+)$/i);
    if(match){street=match[1];city=match[2];}
  }
  city=city.replace(/\s+(?:IL|ILLINOIS)\b.*$/i,'').replace(/\s+\d{5}(?:-\d{4})?$/,'').trim();
  if(!street||!city||street.length>160||city.length>60||!/^[\p{L} .'-]+$/u.test(city)||/^(IL|ILLINOIS)$/i.test(city))return null;
  return {address:street,city};
}
async function nativeAddressLookup(query,request) {
  const parsed=parseCombinedAddress(query);
  const payload=parsed?await request(`/api/coach/properties/suggest?${new URLSearchParams(parsed)}`):{suggestions:[]};
  const suggestions=(payload?.suggestions||[]).filter(row=>row && typeof row.address==='string' && /^\d/.test(row.address) && row.address.length<=160 && typeof row.city==='string' && row.city.length>0 && row.city.length<=60).slice(0,8).map(row=>({...row,formattedAddress:String(row.formattedAddress||`${row.address}, ${row.city}, IL`).slice(0,300)}));
  return {suggestions,truncated:payload?.truncated===true};
}
function nativeStreetIdentity(value) {
  const aliases={NORTH:'N',SOUTH:'S',EAST:'E',WEST:'W',STREET:'ST',AVENUE:'AVE',ROAD:'RD',DRIVE:'DR',BOULEVARD:'BLVD',LANE:'LN',COURT:'CT',PLACE:'PL',TERRACE:'TER',PARKWAY:'PKWY'};
  return String(value||'').normalize('NFKD').replace(/[\u0300-\u036f]/g,'').toUpperCase().replace(/\b(NORTH|SOUTH|EAST|WEST|STREET|AVENUE|ROAD|DRIVE|BOULEVARD|LANE|COURT|PLACE|TERRACE|PARKWAY)\b/g,word=>aliases[word]).replace(/[^A-Z0-9/#-]/g,'');
}
async function nativeCanonicalAddress(row,request) {
  try {
    const payload=await request(`/api/coach/properties/suggest?${new URLSearchParams({address:row.address,city:row.city})}`);
    const matches=(payload?.suggestions||[]).filter(candidate=>nativeStreetIdentity(candidate.address)===nativeStreetIdentity(row.address) && nativeStreetIdentity(candidate.city)===nativeStreetIdentity(row.city));
    if(matches.length===1)return matches[0];
  }catch{}
  return row;
}
export function initTecAddressSuggestions({form,request,onChoose=()=>{}}) {
  const input=form.elements.address,city=form.elements.city;
  const popup=form.querySelector('[data-address-popup]'),list=form.querySelector('[data-address-options]'),status=form.querySelector('[data-address-status]');
  if(!input||!city||!popup||!list||!status)return {close(){}};
  const address=document.createElement('input');address.type='hidden';address.name='address';address.value=input.value;input.name='nativeAddressText';form.append(address);
  const cityLabel=city.closest('label');if(cityLabel){cityLabel.hidden=true;cityLabel.style.display='none';}city.type='hidden';
  const label=input.closest('label');if(label)for(const child of label.childNodes)if(child.nodeType===3)child.textContent='Dirección y ciudad';
  input.placeholder='Ej. 4415 Harrison St, Hillside';input.maxLength=220;
  input.value=[address.value,city.value].filter(Boolean).join(', ');
  const help=form.parentElement?.querySelector('.tec-property-search-tools span');if(help)help.textContent='Escribe calle y ciudad en un solo campo, por ejemplo «4415 Harrison St, Hillside». Elige una dirección sugerida y pulsa Buscar propiedades. La consulta de propietarios sigue cubriendo Cook County.';
  let generation=0,selectionRevision=0,timer,options=[],active=-1,composing=false,selectedText='',verification=null,queuedSubmit=null;const cache=new Map();
  function close(){clearTimeout(timer);generation++;popup.hidden=true;input.setAttribute('aria-expanded','false');input.removeAttribute('aria-activedescendant');options=[];active=-1;list.replaceChildren();}
  function show(text){status.textContent=text;popup.hidden=false;input.setAttribute('aria-expanded','true');}
  function prepare(){
    if(input.value===selectedText && address.value && city.value){input.setCustomValidity('');return true;}
    const parsed=parseCombinedAddress(input.value);address.value=parsed?.address||'';city.value=parsed?.city||'';
    input.setCustomValidity(parsed?'':'Elige una dirección sugerida o añade la ciudad en este mismo campo.');return !!parsed;
  }
  function apply(row){address.value=row.address;city.value=row.city;input.value=row.formattedAddress||`${row.address}, ${row.city}, IL`;selectedText=input.value;input.setCustomValidity('');}
  function choose(chosen){
    if(!chosen)return;
    // Selecting is synchronous. Keyboard dismissal and popup closure must never undo it.
    close();queuedSubmit=null;const revision=++selectionRevision;apply(chosen);onChoose(chosen);
    const work=nativeCanonicalAddress(chosen,request).then(row=>{
      if(revision===selectionRevision && input.value===selectedText)apply(row);
    }).finally(()=>{if(verification===work)verification=null;});
    verification=work;
  }
  function render(payload){options=payload.suggestions;active=-1;list.replaceChildren();const renderedGeneration=generation;
    options.forEach((row,i)=>{const element=document.createElement('button');element.type='button';element.id=`${list.id}-${i}`;element.className='tec-address-option';element.setAttribute('role','option');element.setAttribute('aria-selected','false');element.textContent=row.formattedAddress;
      Object.assign(element.style,{display:'block',width:'100%',textAlign:'left',font:'inherit',whiteSpace:'normal',touchAction:'pan-y'});
      let pointer=null,handled=false;
      const select=()=>{if(handled||renderedGeneration!==generation)return;handled=true;choose(row);};
      element.addEventListener('pointerdown',e=>{handled=false;pointer={id:e.pointerId,x:e.clientX,y:e.clientY};if(e.pointerType==='mouse')e.preventDefault();});
      element.addEventListener('pointerup',e=>{if(!pointer||pointer.id!==e.pointerId)return;const moved=Math.hypot(e.clientX-pointer.x,e.clientY-pointer.y)>10;pointer=null;if(moved){handled=true;return;}if(e.pointerType!=='mouse'){e.preventDefault();select();}});
      element.addEventListener('pointercancel',()=>{pointer=null;handled=true;});element.addEventListener('click',select);list.append(element);
    });
    show(options.length?'Elige la dirección completa de tu cita.':'Añade la ciudad en este mismo campo para buscar en Cook County, por ejemplo «4415 Harrison St, Hillside».');
  }
  function schedule(){close();selectionRevision++;verification=null;queuedSubmit=null;selectedText='';prepare();if(composing)return;const query=input.value.trim();if(query.length<5||!/^\d[\dA-Z/-]*\s+[\p{L}]/iu.test(query))return;
    if(!navigator.onLine){show('Las sugerencias necesitan internet. Tu recorrido guardado sigue disponible.');return;}
    const token=generation;timer=setTimeout(async()=>{if(token!==generation)return;list.replaceChildren();show('Buscando direcciones…');
      try{let payload=cache.get(query.toUpperCase());if(!payload){payload=await nativeAddressLookup(query,request);if(payload.suggestions.length){if(cache.size>=30)cache.delete(cache.keys().next().value);cache.set(query.toUpperCase(),payload);}}if(token===generation)render(payload);}
      catch(error){if(token===generation)show(error.status===429?'Espera unos momentos antes de buscar otra vez.':'No pude cargar sugerencias. Incluye calle y ciudad, por ejemplo «4415 Harrison St, Hillside», y pulsa Buscar propiedades.');}
    },600);
  }
  input.addEventListener('input',schedule);input.addEventListener('compositionstart',()=>{composing=true;close();});input.addEventListener('compositionend',()=>{composing=false;schedule();});
  input.addEventListener('keydown',event=>{if(event.key==='Escape'){close();return;}if(popup.hidden||!options.length)return;
    if(event.key==='Enter'&&active>=0){event.preventDefault();choose(options[active]);return;}
    if(!['ArrowDown','ArrowUp'].includes(event.key))return;event.preventDefault();active=active<0?(event.key==='ArrowDown'?0:options.length-1):(active+(event.key==='ArrowDown'?1:-1)+options.length)%options.length;
    [...list.children].forEach((row,i)=>row.setAttribute('aria-selected',String(i===active)));input.setAttribute('aria-activedescendant',list.children[active].id);list.children[active].scrollIntoView({block:'nearest'});
  });
  form.addEventListener('submit',event=>{
    close();
    if(verification||queuedSubmit){
      event.preventDefault();event.stopImmediatePropagation();if(queuedSubmit)return;
      const revision=selectionRevision,work=verification,submitter=event.submitter;let timeout;
      const queued=Promise.race([work,new Promise(resolve=>{timeout=setTimeout(resolve,3500);})]).then(()=>{
        clearTimeout(timeout);if(queuedSubmit!==queued)return;queuedSubmit=null;
        if(revision!==selectionRevision)return;
        // Freeze this submitted selection. A late county reply must not rewrite
        // fields that now describe an already-running property search.
        selectionRevision++;verification=null;form.requestSubmit(submitter||undefined);
      });queuedSubmit=queued;return;
    }
    if(!prepare()){event.preventDefault();event.stopImmediatePropagation();input.reportValidity();}
  });
  document.addEventListener('pointerdown',event=>{if(!popup.parentElement.contains(event.target))close();});form.addEventListener('focusout',event=>{if(event.relatedTarget && !popup.parentElement.contains(event.relatedTarget))close();});window.addEventListener('offline',close);
  return {close,refresh(){input.value=[address.value,city.value].filter(Boolean).join(', ');schedule();input.focus({preventScroll:true});}};
}
