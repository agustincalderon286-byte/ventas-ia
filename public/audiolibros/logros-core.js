// Shared deterministic rules. Position alone never proves that a book was heard.
export const BOOK_LOGROS = Object.freeze([
  ['como-un-hombre-piensa', 'Mente en movimiento', 'Como un hombre piensa', 'JA'],
  ['acres-de-diamantes', 'Descubre tus diamantes', 'Acres de diamantes', 'AD'],
  ['el-arte-de-la-guerra', 'Estratega', 'El arte de la guerra', 'SZ'],
  ['genesis-rv1909', 'En el principio', 'Génesis', 'G'],
  ['exodo-rv1909', 'Un nuevo camino', 'Éxodo', 'É'],
  ['ayudate', 'Espíritu de iniciativa', '¡Ayúdate!', 'SS'],
  ['la-ciencia-de-hacerse-rico', 'Pensamiento y acción', 'La ciencia de hacerse rico', 'WW'],
  ['el-kybalion', 'Buscador de principios', 'El Kybalión', 'K']
]);
const times = [[60,'Primer minuto','1m'],[300,'Cinco para ti','5m'],[900,'Un cuarto de hora','15m'],[1800,'Media hora de historias','30m'],[3600,'Una hora de ideas','1h'],[10800,'Oídos curiosos','3h'],[18000,'Gran escucha','5h'],[36000,'Diez horas contigo','10h']];
export const BADGES = Object.freeze([
  ...times.map(([target,name,stamp])=>({id:'tiempo-'+target,name,stamp,group:'tiempo',target,description:`Escucha ${target < 3600 ? target/60+' minuto'+(target===60?'':'s') : target/3600+' hora'+(target===3600?'':'s')} de contenido distinto.`})),
  ...[[1,'Primera exploración'],[3,'Tres descubrimientos'],[5,'Explorador de muestras']].map(([target,name])=>({id:'muestras-'+target,name,stamp:String(target),group:'muestras',target,description:`Escucha al menos el 80 % de ${target===1?'una muestra':target+' muestras diferentes'}.`})),
  ...[[1,'Primera historia completa'],[3,'Trilogía personal'],[5,'Biblioteca en marcha'],[8,'Coleccionista TEC']].map(([target,name])=>({id:'libros-'+target,name,stamp:String(target),group:'libros',target,description:`Completa ${target===1?'tu primer audiolibro':target+' audiolibros diferentes'}.`})),
  ...BOOK_LOGROS.map(([bookId,name,title,stamp])=>({id:'titulo-'+bookId,bookId,name,stamp,group:'titulo',target:1,description:`Completa ${title}.`})),
  {id:'lectura-300',name:'Lee y escucha',stamp:'Aa',group:'lectura',target:300,description:'Escucha 5 minutos con la lectura sincronizada abierta y la página visible.'},
  {id:'temas-3',name:'Tres mundos',stamp:'3',group:'temas',target:3,description:'Escucha 5 minutos de tres temas: desarrollo personal, estrategia, Biblia o filosofía.'}
]);
export const emptyState = () => ({v:1,books:{}});
export function mergeRanges(...lists) {
  const sorted=lists.flat().filter(r=>Array.isArray(r)&&r.length===2&&r.every(Number.isFinite)&&r[0]>=0&&r[1]>r[0])
    .map(([a,b])=>[Math.round(a*100)/100,Math.round(b*100)/100]).filter(([a,b])=>b>a).sort((a,b)=>a[0]-b[0]||a[1]-b[1]);
  const merged=[];
  for(const range of sorted){const last=merged.at(-1);if(last&&range[0]<=last[1])last[1]=Math.max(last[1],range[1]);else merged.push(range);}
  return merged;
}
export const rangeSeconds = list => mergeRanges(list||[]).reduce((sum,[a,b])=>sum+b-a,0);
const fields=['sample','full','readingSample','readingFull'];
export function mergeStates(...states) {
  const out=emptyState();
  for(const state of states) for(const [id,row] of Object.entries(state?.books||{})) {
    if(!BOOK_LOGROS.some(b=>b[0]===id)||!row)continue;
    const book=out.books[id] ||= {};
    for(const field of fields)book[field]=mergeRanges(book[field]||[],Array.isArray(row[field])?row[field]:[]);
  }
  return out;
}
export function recordRange(state,context,start,end,reading=false) {
  if(!context?.id||!BOOK_LOGROS.some(b=>b[0]===context.id)||!Number.isFinite(context.duration))return state;
  const limit=context.full?context.duration:context.previewDuration;
  const a=Math.max(0,start),b=Math.min(limit,end);
  if(!Number.isFinite(a)||!Number.isFinite(b)||b<=a)return state;
  const row=state.books[context.id] ||= {};
  const field=context.full?'full':'sample';
  row[field]=mergeRanges(row[field]||[],[[a,b]]);
  if(reading){const key=context.full?'readingFull':'readingSample';row[key]=mergeRanges(row[key]||[],[[a,b]]);}
  return state;
}
function intersect(a,b) {
  const out=[];for(const x of mergeRanges(a||[]))for(const y of mergeRanges(b||[])){const start=Math.max(x[0],y[0]),end=Math.min(x[1],y[1]);if(end>start)out.push([start,end]);}return mergeRanges(out);
}
export function evaluateBadges(state,catalog) {
  const completed=new Set(),heardById=new Map();let seconds=0,samples=0,reading=0;
  for(const book of catalog){
    const row=state?.books?.[book.id]||{},heard=mergeRanges(row.sample||[],row.full||[]);
    const duration=book.duration,preview=book.previewDuration??Math.floor(duration*100)/1000;
    heardById.set(book.id,rangeSeconds(heard));seconds+=rangeSeconds(heard);
    if(preview>0&&rangeSeconds(intersect(heard,[[0,preview]]))>=preview*.8)samples++;
    reading+=rangeSeconds(intersect(mergeRanges(row.readingSample||[],row.readingFull||[]),heard));
    const full=mergeRanges(row.full||[]);
    if(rangeSeconds(full)>=duration*.9&&full.some(([a,b])=>a<=duration-5&&b>=duration-1))completed.add(book.id);
  }
  const themes=[['como-un-hombre-piensa','acres-de-diamantes','ayudate','la-ciencia-de-hacerse-rico'],['el-arte-de-la-guerra'],['genesis-rv1909','exodo-rv1909'],['el-kybalion']];
  const topics=themes.filter(ids=>ids.reduce((sum,id)=>sum+(heardById.get(id)||0),0)>=300).length;
  const badges=BADGES.map(b=>{const current=b.group==='tiempo'?seconds:b.group==='muestras'?samples:b.group==='libros'?completed.size:b.group==='titulo'?Number(completed.has(b.bookId)):b.group==='lectura'?reading:topics;return {...b,current:Math.min(current,b.target),earned:current>=b.target};});
  return {badges,earned:badges.filter(b=>b.earned).length,total:BADGES.length,seconds,completed:completed.size,samples,reading,topics};
}

// Uses media movement AND elapsed time. Seeking, waiting and paused time are excluded.
export class ListeningTracker {
  constructor(onRange){this.onRange=onRange;this.anchor=null;}
  reset(){this.anchor=null;}
  tick({position,now,playing,rate=1,reading=false,seeking=false}){
    if(!Number.isFinite(position)||!Number.isFinite(now)||seeking){this.reset();return;}
    const prev=this.anchor;
    if(prev?.playing){const delta=position-prev.position,elapsed=(now-prev.now)/1000;
      if(elapsed>0&&delta>0&&delta<=elapsed*prev.rate+1&&delta<=Math.max(1,elapsed*prev.rate*1.5))this.onRange(prev.position,position,prev.reading&&reading);
    }
    this.anchor={position,now,playing,rate,reading};
  }
}
