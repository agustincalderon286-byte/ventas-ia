const $=s=>document.querySelector(s);
const search=$('[data-reading-search]');
if(search){
  const cards=[...document.querySelectorAll('[data-reading-card]')];
  search.addEventListener('input',()=>{const q=search.value.normalize('NFD').replace(/\p{Diacritic}/gu,'').toLowerCase().trim();let n=0;for(const c of cards){c.hidden=!c.dataset.search.includes(q);if(!c.hidden)n++;}$('[data-reading-count]').textContent=n?`${n} ${n===1?'libro disponible':'libros disponibles'}`:'No encontramos libros para esta búsqueda.';});
}

const id=document.body.dataset.bookId;
if(id){
  const root='/api/audiolibros/lecturas/'+encodeURIComponent(id),pendingKey='tec.reading.return.v1';
  const gate=$('[data-reading-gate]'),status=$('[data-reading-status]'),workspace=$('[data-reading-workspace]'),body=$('[data-reading-body]'),select=$('[data-reading-chapter]'),progress=$('[data-reading-progress]'),resume=$('[data-reading-resume]');
  let book,chapter=1,paragraph=0,saved,accountKey='',sequence=0,observer,lastSave=0,dirty=false,restoring=false,font=21,persistence=Promise.resolve(),blocked=false;
  const readStore=key=>{try{return JSON.parse(localStorage.getItem(key));}catch{return null;}};
  const writeStore=(key,value)=>{try{localStorage.setItem(key,JSON.stringify(value));return true;}catch{return false;}};
  const remember=()=>writeStore(pendingKey,{id,at:Date.now()});
  document.querySelectorAll('[data-reading-login],[data-reading-subscribe]').forEach(a=>a.addEventListener('click',remember));
  function showGate(message){blocked=true;sequence++;observer?.disconnect();body.replaceChildren();workspace.hidden=true;gate.hidden=false;status.textContent=message;}
  async function api(path='',method='GET',data){
    const r=await fetch(root+path,{method,credentials:'same-origin',cache:'no-store',...(data?{headers:{'Content-Type':'application/json'},body:JSON.stringify(data),keepalive:true}:{})});
    const value=await r.json().catch(()=>({}));
    if(r.status===401||r.status===403){showGate(r.status===401?'Ingresa con tu cuenta de Audiolibros TEC para leer este libro.':'La lectura completa requiere una membresía activa de Audiolibros TEC.');throw Object.assign(new Error(value.error||'Ingresa para continuar.'),{access:true});}
    if(!r.ok)throw new Error(value.error||'No se pudo abrir el libro. Inténtalo de nuevo.');return value;
  }
  const prefs=readStore('tec.reading.preferences.v1')||{};
  if(Number.isInteger(prefs.font)&&prefs.font>=17&&prefs.font<=32)font=prefs.font;
  const theme=['paper','sepia','night'].includes(prefs.theme)?prefs.theme:'sepia';
  document.body.dataset.theme=theme;$('[data-reading-theme]').value=theme;
  function preferences(){document.body.style.setProperty('--reading-size',font+'px');writeStore('tec.reading.preferences.v1',{font,theme:document.body.dataset.theme});$('[data-font-small]').disabled=font<=17;$('[data-font-large]').disabled=font>=32;}
  preferences();
  $('[data-font-small]').addEventListener('click',()=>{font=Math.max(17,font-1);preferences();});
  $('[data-font-large]').addEventListener('click',()=>{font=Math.min(32,font+1);preferences();});
  $('[data-reading-theme]').addEventListener('change',e=>{document.body.dataset.theme=e.target.value;preferences();});
  function localProgress(){return {chapter,paragraph,at:Date.now()};}
  function save(){
    if(!book||blocked||!dirty)return persistence;dirty=false;
    const point=localProgress();writeStore('tec.reading.progress.'+accountKey,point);lastSave=Date.now();
    progress.textContent='Guardando tu avance…';
    persistence=persistence.catch(()=>{}).then(()=>api('/progreso','PUT',{chapter:point.chapter,paragraph:point.paragraph})).then(()=>{if(!blocked)progress.textContent='Avance guardado en tu cuenta.';}).catch(e=>{if(e.access)return;dirty=true;progress.textContent='Avance guardado en este dispositivo. Se enviará al recuperar la conexión.';});
    return persistence;
  }
  function watchParagraphs(){
    observer?.disconnect();
    if(!('IntersectionObserver'in window))return;
    observer=new IntersectionObserver(entries=>{
      if(restoring||blocked)return;
      const visible=entries.filter(e=>e.isIntersecting).sort((a,b)=>a.boundingClientRect.top-b.boundingClientRect.top);
      if(!visible.length)return;paragraph=Number(visible[0].target.dataset.paragraph);dirty=true;
      writeStore('tec.reading.progress.'+accountKey,localProgress());
      if(Date.now()-lastSave>10000)save();
    },{rootMargin:'-120px 0px -55% 0px',threshold:0});
    body.querySelectorAll('[data-paragraph]').forEach(p=>observer.observe(p));
  }
  async function openChapter(number,point=0,scroll=true){
    if(!book||number<1||number>book.chapters.length||blocked)return;
    await save();if(blocked)return;
    const turn=++sequence;progress.textContent='Abriendo capítulo…';
    try{
      const data=await api('/capitulos/'+number);if(turn!==sequence||blocked)return;
      observer?.disconnect();chapter=number;paragraph=Math.min(Math.max(0,point),data.paragraphs.length-1);restoring=true;
      const frag=document.createDocumentFragment();
      data.paragraphs.forEach((text,i)=>{const p=document.createElement('p');p.textContent=text;p.dataset.paragraph=String(i);p.id='pasaje-'+i;frag.append(p);});
      body.replaceChildren(frag);$('#reading-chapter-title').textContent=data.title;$('[data-reading-position]').textContent=book.title;
      select.value=String(number);$('[data-chapter-prev]').disabled=number===1;$('[data-chapter-next]').disabled=number===book.chapters.length;$('[data-chapter-position]').textContent=number+' / '+book.chapters.length;
      history.replaceState(null,'',location.pathname+(number>1?'?capitulo='+number:''));
      if(scroll){const target=paragraph?$('#pasaje-'+paragraph):$('#lectura');target?.scrollIntoView({block:'start'});}
      progress.textContent='Tu avance se guarda automáticamente.';resume.hidden=true;dirty=true;lastSave=Date.now();
      requestAnimationFrame(()=>{restoring=false;watchParagraphs();});
    }catch(e){if(e.access)return;progress.textContent=e.message;select.value=String(chapter);}
  }
  select.addEventListener('change',()=>openChapter(Number(select.value)));
  $('[data-chapter-prev]').addEventListener('click',()=>openChapter(chapter-1));
  $('[data-chapter-next]').addEventListener('click',()=>openChapter(chapter+1));
  resume.addEventListener('click',()=>openChapter(saved.chapter,saved.paragraph));
  window.addEventListener('pagehide',()=>save());
  window.addEventListener('pageshow',e=>{if(e.persisted)location.reload();});
  document.addEventListener('visibilitychange',()=>{if(document.hidden)save();});
  window.addEventListener('online',()=>{if(dirty)save();});
  window.addEventListener('offline',()=>{if(!blocked)progress.textContent='Sin conexión. Puedes seguir leyendo el capítulo abierto; tu avance se conserva en este dispositivo.';});
  // Clear a protected reading when another tab signs out of the same account.
  window.addEventListener('storage',e=>{if(e.key==='tec.reading.signed-out'&&e.newValue)showGate('Tu sesión terminó. Ingresa de nuevo para continuar.');});
  try{
    const data=await api();book=data.book;accountKey=data.storageKey;
    const local=readStore('tec.reading.progress.'+accountKey);saved=data.progress;
    if(local&&Number.isInteger(local.chapter)&&local.chapter>=1&&local.chapter<=book.chapters.length&&Number.isInteger(local.paragraph)&&local.paragraph>=0&&local.paragraph<book.chapters[local.chapter-1].paragraphCount){
      // Only prefer a local position which was not yet saved to the server.
      if(!saved||local.at>Number(data.progress?.updatedAt||0))saved=local;
    }
    for(let i=0;i<book.chapters.length;i++){const o=document.createElement('option');o.value=String(i+1);o.textContent=book.chapters[i].title;select.append(o);}
    gate.hidden=true;workspace.hidden=false;
    if(saved&&(!Number.isInteger(saved.chapter)||saved.chapter<1||saved.chapter>book.chapters.length||!Number.isInteger(saved.paragraph)||saved.paragraph<0||saved.paragraph>=book.chapters[saved.chapter-1].paragraphCount))saved=null;
    const requested=Number(new URLSearchParams(location.search).get('capitulo')||saved?.chapter||1);
    const initial=Number.isInteger(requested)&&requested>=1&&requested<=book.chapters.length?requested:1;
    const continuing=saved&&initial===saved.chapter;
    await openChapter(initial,continuing?saved.paragraph:0,Boolean(continuing));
    if(saved&&!continuing){resume.hidden=false;resume.textContent='Retomar: '+book.chapters[saved.chapter-1].title;}
  }catch(e){if(!e.access){gate.hidden=false;status.textContent=e.message;}}
}
