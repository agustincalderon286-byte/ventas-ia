/* Shared by the server and the offline calculator/survey. No catalog or prices embedded. */
(function (root) {
  'use strict';
  const VERSION = '20261025-alias1';
  const stop = new Set('el la los las de del un una unos unas en que es se su sus y o a para por con como cuanto cuanta cuantos cuantas cual cuales me mi tiene tienen royal prestige producto productos necesito quiero saber sirve hace hacer puedes dar dime sobre al lo le este esta esto ese esa mas precio cuesta costo vale comprar busco buscar aparato maquina cosa usa usar'.split(' '));
  function normalize(value) {
    return String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
      .replace(/(\d)\s*(cm|qt|litros?|pulgadas?)/g, '$1 $2')
      .replace(/licuad\s+ora/g, 'licuadora').replace(/cosina/g,'cocina')
      .replace(/\bfp\s*(\d{4})\b/g,'frescapure $1').replace(/\bpb\s*(max|go)\b/g,'power blender $1')
      .replace(/perfetc pop/g,'perfect pop').replace(/utelsilios|utenzilios|utelzilios/g,'utensilios')
      .replace(/sartenes/g,'sarten').replace(/ollas/g,'olla').replace(/cuchillos/g,'cuchillo')
      .replace(/valvulas/g,'valvula').replace(/aros/g,'aro').replace(/empaques/g,'empaque')
      .replace(/jugos/g,'jugo').replace(/silicon(a|e)?\b/g,'silicona')
      .replace(/licuadoras/g,'licuadora').replace(/batidoras/g,'batidora')
      .replace(/piezas/g,'pieza').replace(/litros/g,'litro').replace(/cuartos/g,'cuarto')
      .replace(/pulgadas/g,'pulgada').replace(/[^a-z0-9.]+/g,' ').trim();
  }
  const terms = text => normalize(text).split(/\s+/).filter(t=>t && !stop.has(t));
  const phrase = text => terms(text).join(' ');
  // Aliases describe identities/functions, never promises of performance or compatibility.
  const rules = [
    [/power blender (max|go)/, ['licuadora','blender','batidora','licuar']],
    [/power blender max/, ['licuadora de vaso','licuadora de mesa','licuadora grande','moler cafe','triturar semillas','hacer harina','hacer licuados','hacer batidos']],
    [/power blender go/, ['batidora de mano','licuadora de mano','batidora de inmersion','licuadora inalambrica','licuar en la olla','hacer pure en la olla']],
    [/extractor.*(jugo|juego)|juicer|maxtractor/, ['extractor de jugos','sacar jugos','sacar jugo de frutas','maquina de jugos','juicer']],
    [/exprimidor/, ['exprimir naranjas','exprimir limones','exprimidor de citricos']],
    [/easy release|\ber\b/, ['easy release','antiadherente','no se pega','que no pega','efecto piedra']],
    [/olla.*presion/, ['olla expres','olla express','olla rapida','olla de presion','cocinar a presion','pressure cooker']],
    [/maquina.*ensalada/, ['ralladora de verduras','cortar verduras','corta verduras','rebanar verduras','rallar verduras','maquina de ensaladas']],
    [/perfect?c? pop/, ['palomera','palomitas','hacer popcorn','hacer palomitas']],
    [/chocolatera/, ['hacer chocolate','chocolate caliente','chocolatera']],
    [/barista|espresso/, ['cafetera','hacer cafe','preparar espresso']],
    [/expertea/, ['tetera','hacer te','preparar te']],
    [/hervidor/, ['hervir agua','calentar agua','hervidor']],
    [/plancha|comal/, ['comal','plancha','calentar tortillas','hacer tortillas']],
    [/pavera/, ['hornear pavo','pavera','rosticera']],
    [/wok/, ['wok','saltear verduras']],
    [/paellera/, ['paellera','hacer paella']],
    [/warmer/, ['mantener comida caliente','calentador de comida']],
    [/precision cook/, ['quemador portatil','parrilla de induccion','estufa portatil']],
    [/base magnetica/, ['base para ollas calientes','salvamanteles','protector de mesa']],
    [/frescapure|frescaflow/, ['filtro de agua','purificador de agua','filtrar agua']],
    [/frescaflow/, ['osmosis inversa','frescaflow']],
    [/filtro.*ducha/, ['filtro de regadera','filtro de ducha']],
    [/filtro de aire|pure ambience/, ['purificador de aire','filtro de aire']],
    [/cartucho|prefiltro|mineralizador/, ['cartucho','repuesto de filtro','filtro de reemplazo']],
    [/tazon/, ['bowl','recipiente para mezclar','tazon']],
    [/recipientes.*almacenar/, ['guardar comida','recipientes para guardar']],
    [/tabla.*bambu/, ['tabla para picar','tabla para cortar']],
    [/afilador/, ['sacar filo','afilar cuchillos','afilador']],
    [/bloque.*bambu/, ['porta cuchillos','bloque de cuchillos']],
    [/cuchillo|santoku|damascus|hacha/, ['cuchillo','knife']],
    [/cuchillo.*carne|churrasco/, ['cuchillos para carne','cuchillos para steak']],
    [/pelador/, ['pelar papas','pelar verduras','pelador']],
    [/destapador/, ['abrir botellas','destapador']],
    [/corta.*pizza/, ['cortador de pizza','cortar pizza']],
    [/machacador/, ['machacar papas','hacer pure de papas']],
    [/rallador/, ['rallar queso','rallador']],
    [/espatula.*helado/, ['sacar bolas de helado','cuchara de helado']],
    [/batidor.*globo/, ['batidor manual','batir huevos']],
    [/colador/, ['colador','escurridor','escurrir pasta']],
    [/vaso.*tequilero/, ['caballitos','vasos para tequila']],
    [/utensilio/, ['utensilios de cocina','herramientas de cocina']],
    [/recipiente.*utensilio/, ['porta utensilios','guardar utensilios']],
    [/parrilla/, ['rejilla para olla','parrilla interior']],
    [/cacerola/, ['cacerola','ollita','olla pequena']],
    [/multipan/, ['sarten multifuncion','sarten de 11 funciones','multipan']],
    [/taza.*pared doble/, ['tazas de doble pared','tazas para bebidas']],
    [/copa.*helado/, ['copas para postre','servir helado']],
    [/azucarera/, ['azucarera','lechera','jarrito para leche']],
    [/espatula/, ['espatula','volteador','voltear comida']],
    [/copa.*vino/, ['copas de vino','copas gala']],
    [/vaso.*agua/, ['vasos de agua','vasos london']],
    [/vaso.*corto/, ['vasos cortos','vasos bajos']],
    [/jarra.*tritan/, ['jarra tritan','vaso de tritan']],
    [/maxcup/, ['max cup','maxcup']],
    [/fresh max/, ['licuado al vacio','extraer aire de la licuadora','freshmax']],
    [/batidor y cepillo/, ['batidor con cepillo','accesorio batidor']],
    [/taza y picadora/, ['picadora con taza','accesorio picadora']],
    [/pieza.*rebanar/, ['juego para rebanar','juego para cortar']],
    [/luz ultra violeta/, ['luz ultravioleta','lampara uv']],
    [/kit debajo.*mostrador/, ['kit bajo mostrador','kit bajo fregadero']],
    [/tapa/, ['tapadera','tapa','lid']],
    [/agarradera|mango|asa\b/, ['agarradera','mango','asa','manija']],
    [/\baro\b|empaque|junta/, ['aro','empaque','junta','goma','anillo de silicona','sello de silicona']],
    [/valvula/, ['valvula','pito de la tapa','silbato de la tapa','valvula que silba']],
    [/olla/, ['olla','cacerola','pot']],
    [/sarten|easy release [0-9]/, ['sarten','frying pan']],
    [/sistema|\bset\b/, ['juego de ollas','bateria de cocina','sistema de cocina']],
  ];
  const partPattern = /\b(aro|empaque|junta|goma|sello|valvula|repuesto|reemplazo|refaccion|agarradera|mango|asa|manija|tapon|tornillo)\b/;
  function isPartQuery(query) { return partPattern.test(normalize(query)); }
  function info(item) {
    const code = String(item.codigo_producto ?? item.code ?? item['Codigo Del Producto'] ?? '').trim().toUpperCase();
    const name = String(item.nombre_producto ?? item.name ?? item.description ?? item['Nombre Del Producto NOVEL'] ?? '').trim();
    return {code,name};
  }
  function createIndex(products) {
    const seen = new Set();
    return products.flatMap(item => {
      const {code,name}=info(item); if(!name) return [];
      const key=code+'|'+normalize(name)+'|'+(item.precio_base_catalogo ?? item.Precio ?? '');
      if(seen.has(key)) return []; seen.add(key);
      const title=normalize(name), aliases=[name,...(item.search_terms || [])];
      for(const [pattern, names] of rules) if(pattern.test(title)) aliases.push(...names);
      const aliasPhrases=[...new Set(aliases.map(phrase).filter(Boolean))];
      const titleTerms=terms(name), aliasTerms=[...new Set(aliasPhrases.flatMap(a=>a.split(' ')))];
      return [{item,code,name,title,titleTerms,aliases:aliasPhrases,
        titleSet:new Set(titleTerms), allTerms:[...new Set([...titleTerms,...aliasTerms])],
        descriptionTerms:terms(item.search_description || ''),part:isPartQuery(name)}];
    });
  }
  function oneEdit(a,b) {
    if(a===b) return true;
    if(Math.abs(a.length-b.length)>1) return false;
    let i=0,j=0,edits=0;
    while(i<a.length && j<b.length) {
      if(a[i]===b[j]) {i++;j++;continue;}
      if(++edits>1) return false;
      if(a.length===b.length) {
        if(a[i]===b[j+1] && a[i+1]===b[j]) {i+=2;j+=2;} else {i++;j++;}
      } else if(a.length>b.length) i++; else j++;
    }
    return edits+(i<a.length || j<b.length ? 1:0)<=1;
  }
  function search(index, rawQuery, {limit=5}={}) {
    const query=normalize(rawQuery), qt=terms(query), qp=qt.join(' ');
    if(query.length<3 || !qt.length) return [];
    const codes=query.match(/\b[a-z]{2}\d{3,5}\b/g)||[];
    if(codes.length) return index.filter(r=>codes.includes(r.code.toLowerCase())).slice(0,limit).map(r=>({...r,score:1000,matchType:'code'}));
    const partQuery=isPartQuery(query);
    const ranked=index.map(r=>{
      if(partQuery && !r.part) return null;
      if(partQuery && /\b(aro|goma|empaque|junta|sello)\b/.test(query) && !/\b(aro|empaque|junta|sello)\b/.test(r.title)) return null;
      if(partQuery && /\bvalvula\b/.test(query) && !/valvula/.test(r.title)) return null;
      if(partQuery && /\b(agarradera|mango|asa|manija)\b/.test(query) && !/agarradera|mango|asa\b/.test(r.title)) return null;
      if(partQuery && /\binnove\b/.test(query) && !/innove/.test(r.title)) return null;
      if(/\b(olla express?|olla rapida|pressure cooker)\b/.test(query) && !/presion/.test(r.title)) return null;
      let score=0, hits=0, fuzzy=0, aliasSize=0;
      const namePhrase=phrase(r.name);
      const nameExact=qp===namePhrase || (r.titleTerms.length>1 && (' '+qp+' ').includes(' '+namePhrase+' '));
      const titleSet=r.titleSet, all=new Set(r.allTerms);
      for(const t of qt) {
        if(titleSet.has(t)) {score+=12;hits++;}
        else if(all.has(t)) {score+=6;hits++;}
        else if(t.length>=3 && !/\d/.test(t) && [...titleSet].some(w=>w.startsWith(t))) {score+=4;hits++;}
        else if(t.length>=5 && !/\d/.test(t) && [...all].some(w=>w.length>=5 && oneEdit(t,w))) {score+=4;hits++;fuzzy++;}
        else if(r.descriptionTerms.includes(t)) {score+=2;hits++;}
      }
      for(const a of r.aliases) if((' '+qp+' ').includes(' '+a+' ')) aliasSize=Math.max(aliasSize,a.split(' ').length);
      if(fuzzy && qt.length===1 && r.aliases.some(a=>!a.includes(' ') && oneEdit(qt[0],a))) aliasSize=Math.max(aliasSize,1);
      if(!hits) return null;
      if(nameExact) score+=500;
      else if(aliasSize) score+=50+aliasSize*35;
      // Explicit sizes and piece counts must not silently become another variant.
      const nums=qt.filter(t=>/^\d+(\.\d+)?$/.test(t));
      if(nums.some(n=>!titleSet.has(n))) return null;
      const category = ['juego','set','sistema'].includes(qt[0]) ? qt[1] : qt[0];
      if(category==='cuchillo' && !/cuchillo|santoku|hacha|afilador/.test(r.title)) return null;
      if(category==='sarten' && !/sarten|^easy release/.test(r.title)) return null;
      if(category==='olla' && !/olla|sistema|set|presion/.test(r.title) && !partQuery) return null;
      if(!partQuery && r.part) score-=60;
      return {...r,score,aliasSize,matchType:nameExact?'name':fuzzy?'fuzzy':aliasSize?'alias':'description'};
    }).filter(r=>r && r.score>0).sort((a,b)=>b.score-a.score || a.name.localeCompare(b.name));
    if(!ranked.length) return [];
    const strongestAlias=Math.max(...ranked.map(r=>r.aliasSize));
    const threshold=ranked[0].score>=500?ranked[0].score-20:Math.max(3,ranked[0].score*0.60);
    return ranked.filter(r=>r.score>=threshold && (!strongestAlias || r.aliasSize>=strongestAlias || r.matchType==='name')).slice(0,limit);
  }
  root.TecProductSearch=Object.freeze({VERSION,normalize,createIndex,search,isPartQuery});
})(globalThis);
