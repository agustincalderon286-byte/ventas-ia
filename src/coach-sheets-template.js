// Hoja TEC: la hoja de Google que el Coach crea para cada vendedor desde Mis conexiones.
// 4 en 14 y Boletas conservan el formato que ya usan los telemarketers (bloques por anfitrión, sus columnas
// y sus colores) y agregan al final las columnas de seguimiento. El Resumen cuenta los resultados con fórmulas.
// La hoja nace vacía: ningún dato de clientes sale de aquí.

export const TEC_SHEET_IDS = Object.freeze({guide:0, summary:4, agenda:3, referrals:1, tickets:2, legend:6, lists:5});
export const TEC_SHEET_TITLE = 'Hoja TEC';
const ROWS = 2000;

const REFERRAL_HEADERS = ['NRO','NOMBRES','TELEFONOS','CIUDAD O ZONA','FECHA DEL 4/14','CONOCE RP','RELACION CON EL ANFITRION',
  'OBSERVACIONES DEL VENDEDOR (pais, estado civil, etc)','VENDEDOR QUE PROSPECTÓ','COMENTARIOS DEL TELEMARKETING','RESULTADO DE LA CITA',
  'TELEMARKETER','ESTADO','ÚLTIMO CONTACTO','PRÓXIMA LLAMADA','HUBO CONTACTO (automático)'];
const TICKET_HEADERS = ['NOMBRE DEL PROSPECTO','Email','TELEFONOS','Toma agua','Mejor hora para llamar','Agenda hora','¿A cuál producto le daría más uso?',
  'Conoce Royal Prestige','Tiene Royal Prestige','¿Qué productos tiene?','Nombre del representante','LUGAR O TIENDA DONDE SE PROSPECTÓ',
  'CIUDAD O ZONA DE RESIDENCIA / ZIP CODE','OBSERVACIONES DEL PROSPECTADOR (PAÍS DE ORIGEN, CONOCE RP, ETC)','¿Qué se ganó?','Casado',
  'Número de ganador','PROSPECTADOR','BOLETAS / RASPADITO','COMENTARIOS DEL TELEMARKETING','RESULTADO DE LA CITA',
  'FECHA DE ENTRADA','TELEMARKETER','ESTADO','ÚLTIMO CONTACTO','PRÓXIMA LLAMADA','HUBO CONTACTO (automático)'];
export const AGENDA_HEADERS = ['Fecha','Hora','Vendedor','Telemarketer','Cliente','Teléfono','Dirección','Origen','Notas del telemarketer',
  'Resultado','Venta ($)','Referidos obtenidos','Notas del vendedor'];
export const LEAD_STATES = Object.freeze([['Nuevo','No'],['No contesta','No'],['Número equivocado','No'],['Colgó','Sí'],['No califica','Sí'],
  ['No interesado','Sí'],['Cita','Sí'],['Pendiente','Sí'],['Llamar en las tardes','Sí'],['Solo mañanas','Sí'],['Solo fines de semana','Sí']]);
export const RESULTS = [['Pendiente','No'],['Venta','Sí'],['Demo sin venta','Sí'],['No estaba','No'],['Canceló','No'],['Reagendar','No']];
// Los mismos colores de la leyenda que ya usan los telemarketers.
const COLORS = [
  [['No interesado','Colgó','No califica','Número equivocado'],'FF0000','000000','NO INTERESADO, COLGÓ, NO CALIFICA, NÚMEROS EQUIVOCADOS O DESCONECTADOS'],
  [['Cita'],'00FF00','000000','CITAS'],
  [['Solo fines de semana'],'FFFF00','000000','SOLO PUEDEN LOS FINES DE SEMANA'],
  [['Llamar en las tardes'],'4A86E8','000000','LLAMAR EN LAS TARDES, DESPUÉS DEL TRABAJO'],
  [['Pendiente'],'FF9900','000000','PENDIENTES, SE TUVO CONTACTO PERO NO SE HA CONCRETADO CITA'],
  [['Solo mañanas'],'9900FF','FFFFFF','SOLO PUEDE EN LAS MAÑANAS']
];

const rgb = hex => ({red:parseInt(hex.slice(0,2),16)/255, green:parseInt(hex.slice(2,4),16)/255, blue:parseInt(hex.slice(4,6),16)/255});
const grid = (sheetId,r1,r2,c1,c2) => ({sheetId,startRowIndex:r1,endRowIndex:r2,startColumnIndex:c1,endColumnIndex:c2});
const cell = value => value == null ? {} : {userEnteredValue:typeof value==='number' ? {numberValue:value}
  : String(value).startsWith('=') ? {formulaValue:value} : {stringValue:String(value)}};
const values = (sheetId,row,col,rows) => ({updateCells:{start:{sheetId,rowIndex:row,columnIndex:col},fields:'userEnteredValue',
  rows:rows.map(r=>({values:r.map(cell)}))}});
const format = (range,f,fields) => ({repeatCell:{range,cell:{userEnteredFormat:f},fields:fields.map(x=>'userEnteredFormat.'+x).join(',')}});
const width = (sheetId,col,px,count=1) => ({updateDimensionProperties:{range:{sheetId,dimension:'COLUMNS',startIndex:col,endIndex:col+count},
  properties:{pixelSize:px},fields:'pixelSize'}});
const listRule = range => ({condition:{type:'ONE_OF_RANGE',values:[{userEnteredValue:range}]},strict:true,showCustomUi:true});
const choice = (...items) => ({condition:{type:'ONE_OF_LIST',values:items.map(v=>({userEnteredValue:v}))},strict:true,showCustomUi:true});
const validate = (range,rule) => ({setDataValidation:{range,rule}});
const merge = range => ({mergeCells:{range,mergeType:'MERGE_ALL'}});
const warn = (range,description) => ({addProtectedRange:{protectedRange:{range,description,warningOnly:true}}});
const DATE = {numberFormat:{type:'DATE',pattern:'dd/mm/yyyy'}};
const head = (bg='FFFFFF',fg='000000') => ({textFormat:{bold:true,fontFamily:'Arial',foregroundColor:rgb(fg)},backgroundColor:rgb(bg),
  wrapStrategy:'WRAP',verticalAlignment:'MIDDLE',horizontalAlignment:'CENTER'});
const HEAD_FIELDS = ['textFormat','backgroundColor','wrapStrategy','verticalAlignment','horizontalAlignment'];

// Las columnas que cuenta el Resumen en cada pestaña de leads.
const leadTabs = {
  referrals:{tab:"'4 en 14'",first:4,date:'F',telemarketer:'M',state:'N',contacted:'O',contact:'Q'},
  tickets:{tab:'Boletas',first:2,date:'V',telemarketer:'W',state:'X',contacted:'Y',contact:'AA'}
};
const col = (kind,key) => { const t=leadTabs[kind]; return `${t.tab}!$${t[key]}$${t.first}:$${t[key]}`; };

function summaryRows() {
  const each = f => ['B','C','D'].map(c=>f.replaceAll('#',c));
  const since = kind => `${col(kind,'date')},">="&#$5,${col(kind,'date')},"<"&#$6`;
  const agenda = 'Agenda!$A$2:$A,">="&#$5,Agenda!$A$2:$A,"<"&#$6', result = 'Agenda!$J$2:$J';
  const row = (label,f) => [label,...each(f)];
  const rows = [
    ['Resumen de resultados'],['Se calcula solo con las pestañas Agenda, 4 en 14 y Boletas. No escribas en esta pestaña.'],[],
    ['Periodo','Esta semana','Este mes','Mes pasado'],
    ['Desde','=TODAY()-WEEKDAY(TODAY(),2)+1','=EOMONTH(TODAY(),-1)+1','=EOMONTH(TODAY(),-2)+1'],
    ['Hasta (sin incluir)','=B5+7','=EOMONTH(TODAY(),0)+1','=EOMONTH(TODAY(),-1)+1'],[],
    ['Leads'],
    row('Leads nuevos del 4 en 14',`=COUNTIFS(${since('referrals')})`),
    row('Leads nuevos de boletas',`=COUNTIFS(${since('tickets')})`),
    row('Leads nuevos en total','=#9+#10'),[],
    ['Citas y ventas'],
    row('Citas en la agenda',`=COUNTIFS(${agenda})`),
    row('Demos hechas',`=COUNTIFS(${agenda},${result},"Venta")+COUNTIFS(${agenda},${result},"Demo sin venta")`),
    row('Citas que se cayeron (no estaba o canceló)',`=COUNTIFS(${agenda},${result},"No estaba")+COUNTIFS(${agenda},${result},"Canceló")`),
    row('Citas que se hicieron, de las que ya tienen resultado','=IF(#15+#16=0,0,#15/(#15+#16))'),
    row('Ventas',`=COUNTIFS(${agenda},${result},"Venta")`),
    row('Porcentaje de cierre (ventas entre demos)','=IF(#15=0,0,#18/#15)'),
    row('Total vendido ($)',`=SUMIFS(Agenda!$K$2:$K,${agenda})`),
    row('Venta promedio ($)','=IF(#18=0,0,#20/#18)'),
    row('Referidos obtenidos',`=SUMIFS(Agenda!$L$2:$L,${agenda})`),
    row('Referidos por demo','=IF(#15=0,0,#22/#15)'),
    row('Ventas que vinieron del 4 en 14',`=COUNTIFS(${agenda},${result},"Venta",Agenda!$H$2:$H,"4 en 14")`),
    row('Ventas que vinieron de boletas',`=COUNTIFS(${agenda},${result},"Venta",Agenda!$H$2:$H,"Boletas")`),[],
    ['Al día de hoy'],
    ['Leads nuevos sin llamar después de 2 días',`=COUNTIFS(${col('referrals','state')},"Nuevo",${col('referrals','date')},"<="&(TODAY()-2))+COUNTIFS(${col('tickets','state')},"Nuevo",${col('tickets','date')},"<="&(TODAY()-2))`],
    ['Citas pasadas sin resultado anotado','=COUNTIFS(Agenda!$J$2:$J,"Pendiente",Agenda!$A$2:$A,"<"&TODAY())+COUNTIFS(Agenda!$E$2:$E,"<>",Agenda!$J$2:$J,"",Agenda!$A$2:$A,"<"&TODAY())'],[],
    ['Telemarketers · este mes'],
    ['Telemarketer','Leads trabajados','Contactos','Citas agendadas','Citas entre contactos','Citas que se hicieron','Hechas entre agendadas','Ventas','Leads sin llamar (más de 2 días)']
  ];
  for (let i=0;i<6;i++) {
    const r=33+i, name='$A'+r, guard=f=>`=IF(${name}="","",${f})`;
    const worked=(kind,extra='')=>`COUNTIFS(${col(kind,'telemarketer')},${name},${col(kind,'contacted')},">="&$C$5,${col(kind,'contacted')},"<"&$C$6${extra})`;
    const booked=`Agenda!$D$2:$D,${name},Agenda!$A$2:$A,">="&$C$5,Agenda!$A$2:$A,"<"&$C$6`;
    const waiting=kind=>`COUNTIFS(${col(kind,'telemarketer')},${name},${col(kind,'state')},"Nuevo",${col(kind,'date')},"<="&(TODAY()-2))`;
    rows.push([`=IF(Listas!G${2+i}="","",Listas!G${2+i})`,
      guard(worked('referrals')+'+'+worked('tickets')),
      guard(worked('referrals',`,${col('referrals','contact')},"Sí"`)+'+'+worked('tickets',`,${col('tickets','contact')},"Sí"`)),
      guard(`COUNTIFS(${booked})`),guard(`IF(C${r}=0,0,D${r}/C${r})`),
      guard(`COUNTIFS(${booked},Agenda!$J$2:$J,"Venta")+COUNTIFS(${booked},Agenda!$J$2:$J,"Demo sin venta")`),
      guard(`IF(D${r}=0,0,F${r}/D${r})`),guard(`COUNTIFS(${booked},Agenda!$J$2:$J,"Venta")`),
      guard(waiting('referrals')+'+'+waiting('tickets'))]);
  }
  return rows;
}

const GUIDE = [
  ['Hoja TEC'],
  ['La creó el Coach TEC para este vendedor. Los telemarketers trabajan aquí como siempre y el Resumen cuenta los resultados solo.'],[],
  ['Pestañas'],
  ['Resumen: los números de la semana, del mes y del mes pasado, y una tabla por telemarketer. Se calcula solo.'],
  ['Agenda: cada cita en una fila. El telemarketer llena fecha, hora, cliente y sus notas. El vendedor llena el resultado, la venta y los referidos.'],
  ['4 en 14: igual que la hoja que ya usan los telemarketers. El anfitrión va a la izquierda y sus referidos numerados al lado. Al final están las columnas de Seguimiento TEC (en azul).'],
  ['Boletas: las mismas columnas de siempre y, al final, las de Seguimiento TEC (en azul): fecha de entrada, telemarketer, estado, último contacto y próxima llamada.'],
  ['Listas y Leyenda de colores: los estados, los resultados y los nombres de las listas desplegables, y qué significa cada color. Para agregar un telemarketer, escríbelo en Listas, columna Telemarketers.'],[],
  ['Reglas para que los números salgan bien'],
  ['1. Elige el ESTADO de la lista: la fila se pinta sola con el color de siempre. Ya no hace falta pintar a mano.'],
  ['2. Anota la fecha del 4/14 (o la fecha de entrada de la boleta) y, cada vez que llames, el Último contacto. Escribe la fecha en su casilla, no dentro de los comentarios.'],
  ['3. Los comentarios del telemarketing se siguen escribiendo como siempre, en su columna.'],
  ['4. Cuando agendes una cita, ponla también en la pestaña Agenda con su fecha y su hora. El vendedor anota ahí el resultado, la venta y los referidos.'],
  ['5. Usa las listas desplegables de Estado, Resultado, Telemarketer y Origen. Si escribes otra palabra, el Resumen no la cuenta.'],
  ['6. Las casillas que se calculan solas (Hubo contacto y el Resumen) avisan si alguien intenta escribir en ellas.']
];

// Cuerpo de spreadsheets.create: solo las pestañas. El contenido va en tecSheetRequests, en una sola actualización.
export function tecSheetCreateBody({title=TEC_SHEET_TITLE,timeZone='America/Chicago'}={}) {
  const sheet = (sheetId,name,index,rowCount,columnCount,frozenRowCount=0,frozenColumnCount=0) =>
    ({properties:{sheetId,title:name,index,gridProperties:{rowCount,columnCount,frozenRowCount,frozenColumnCount}}});
  const id = TEC_SHEET_IDS;
  return {properties:{title,locale:'en_US',timeZone},sheets:[
    sheet(id.guide,'Cómo usar',0,40,1),sheet(id.summary,'Resumen',1,60,9,4),sheet(id.agenda,'Agenda',2,ROWS,13,1),
    sheet(id.referrals,'4 en 14',3,ROWS,17,3),sheet(id.tickets,'Boletas',4,ROWS,27,1,1),sheet(id.legend,'Leyenda de colores',5,20,2),
    sheet(id.lists,'Listas',6,50,8,1)]};
}

export function tecSheetRequests({sellerName=''}={}) {
  const id = TEC_SHEET_IDS, req = [];
  const font = (s,cols,rows=ROWS) => req.push(format(grid(s,0,rows,0,cols),{textFormat:{fontFamily:'Arial'},verticalAlignment:'TOP'},['textFormat.fontFamily','verticalAlignment']));
  const colorRules = (s,first,stateCol,c1,c2) => COLORS.forEach(([states,bg,fg],index)=>req.push({addConditionalFormatRule:{index,rule:{
    ranges:[grid(s,first-1,ROWS,c1,c2)],booleanRule:{condition:{type:'CUSTOM_FORMULA',
      values:[{userEnteredValue:'=OR('+states.map(x=>`$${stateCol}${first}="${x}"`).join(',')+')'}]},
    format:{backgroundColor:rgb(bg),textFormat:{foregroundColor:rgb(fg)}}}}}}));
  const contactFormula = (stateCol,first) => `=ARRAYFORMULA(IF(${stateCol}${first}:${stateCol}="","",IFERROR(VLOOKUP(${stateCol}${first}:${stateCol},Listas!$A$2:$B,2,FALSE),"")))`;

  // Cómo usar
  font(id.guide,1,40);
  req.push(values(id.guide,0,0,GUIDE), width(id.guide,0,820), format(grid(id.guide,0,40,0,1),{wrapStrategy:'WRAP'},['wrapStrategy']));
  req.push(format(grid(id.guide,0,1,0,1),{textFormat:{bold:true,fontSize:14,fontFamily:'Arial'}},['textFormat']));
  for (const r of [3,10]) req.push(format(grid(id.guide,r,r+1,0,1),{textFormat:{bold:true,fontFamily:'Arial'}},['textFormat']));

  // Listas
  font(id.lists,8,50);
  req.push(values(id.lists,0,0,[['Estado del lead','¿Hubo contacto?',null,'Resultado de la cita','¿Se hizo la demo?',null,'Telemarketers','Vendedores'],
    ...Array.from({length:LEAD_STATES.length},(_,i)=>[...(LEAD_STATES[i]||[null,null]),null,...(RESULTS[i]||[null,null]),null,null,i===0 && sellerName ? sellerName : null])]));
  req.push(format(grid(id.lists,0,1,0,8),head('1F3864','FFFFFF'),HEAD_FIELDS));
  for (const [c1,c2] of [[0,2],[3,5],[6,8]]) req.push(format(grid(id.lists,1,50,c1,c2),{backgroundColor:rgb('FFF2CC')},['backgroundColor']));
  [160,110,20,140,130,20,140,140].forEach((px,i)=>req.push(width(id.lists,i,px)));

  // Leyenda de colores
  font(id.legend,2,20);
  req.push(values(id.legend,0,0,[['Leyenda de colores'],['El color de la fila se pone solo cuando el telemarketer elige el ESTADO.'],[],
    ...COLORS.map(c=>[null,c[3]]),[null,'NUEVO O NO CONTESTA (sin color)']]));
  req.push(format(grid(id.legend,0,1,0,1),{textFormat:{bold:true,fontSize:14,fontFamily:'Arial'}},['textFormat']));
  [...COLORS,[null,'FFFFFF','000000']].forEach(([,bg,fg],i)=>req.push(format(grid(id.legend,3+i,4+i,1,2),
    {backgroundColor:rgb(bg),textFormat:{bold:true,fontFamily:'Arial',foregroundColor:rgb(fg)},verticalAlignment:'MIDDLE'},['backgroundColor','textFormat','verticalAlignment'])));
  req.push(width(id.legend,0,30), width(id.legend,1,620));

  // 4 en 14, con los encabezados del formato de Chicago: REFERENCIA y REFERIDOS arriba, columnas en la fila 2.
  const R = id.referrals;
  font(R,17);
  req.push(values(R,0,0,[['REFERENCIA','REFERIDOS',...Array(10).fill(null),'SEGUIMIENTO TEC'],[null,...REFERRAL_HEADERS]]));
  req.push(values(R,3,16,[[contactFormula('N',4)]]));
  req.push(merge(grid(R,0,2,0,1)), merge(grid(R,0,1,1,12)), merge(grid(R,0,1,12,17)), merge(grid(R,2,3,0,17)));
  req.push(format(grid(R,0,2,0,12),head(),HEAD_FIELDS), format(grid(R,1,2,12,17),head('CFE2F3'),HEAD_FIELDS),
    format(grid(R,0,1,12,17),head('1F3864','FFFFFF'),HEAD_FIELDS));
  req.push(format(grid(R,3,ROWS,0,1),{...head('CCCCCC'),textFormat:{bold:true,fontFamily:'Arial'}},HEAD_FIELDS));
  req.push(format(grid(R,3,ROWS,1,2),{horizontalAlignment:'CENTER'},['horizontalAlignment']));
  for (const c of [5,14,15]) req.push(format(grid(R,3,ROWS,c,c+1),DATE,['numberFormat']));
  for (const c of [8,10]) req.push(format(grid(R,3,ROWS,c,c+1),{wrapStrategy:'WRAP'},['wrapStrategy']));
  req.push({updateBorders:{range:grid(R,0,ROWS,11,12),right:{style:'SOLID_MEDIUM',colorStyle:{rgbColor:rgb('000000')}}}});
  colorRules(R,4,'N',2,17);
  req.push(validate(grid(R,3,ROWS,13,14),listRule('=Listas!$A$2:$A$30')), validate(grid(R,3,ROWS,12,13),listRule('=Listas!$G$2:$G$30')),
    validate(grid(R,3,ROWS,6,7),choice('Sí','No')));
  [140,50,150,115,115,100,80,150,200,140,300,150,125,160,110,110,110].forEach((px,i)=>req.push(width(R,i,px)));
  req.push(warn(grid(R,0,ROWS,16,17),'Esta columna se calcula sola.'));

  // Boletas: sus columnas de siempre y las de seguimiento al final.
  const T = id.tickets;
  font(T,27);
  req.push(values(T,0,0,[TICKET_HEADERS]), values(T,1,26,[[contactFormula('X',2)]]));
  req.push(format(grid(T,0,1,0,21),head(),HEAD_FIELDS), format(grid(T,0,1,21,27),head('CFE2F3'),HEAD_FIELDS));
  for (const c of [21,24,25]) req.push(format(grid(T,1,ROWS,c,c+1),DATE,['numberFormat']));
  for (const c of [13,19]) req.push(format(grid(T,1,ROWS,c,c+1),{wrapStrategy:'WRAP'},['wrapStrategy']));
  req.push({updateBorders:{range:grid(T,0,ROWS,20,21),right:{style:'SOLID_MEDIUM',colorStyle:{rgbColor:rgb('000000')}}}});
  colorRules(T,2,'X',0,27);
  req.push(validate(grid(T,1,ROWS,23,24),listRule('=Listas!$A$2:$A$30')), validate(grid(T,1,ROWS,22,23),listRule('=Listas!$G$2:$G$30')),
    validate(grid(T,1,ROWS,7,9),choice('Sí','No')), validate(grid(T,1,ROWS,15,16),choice('Sí','No')));
  req.push(width(T,0,145,2), width(T,2,120,11), width(T,13,220), width(T,14,120,5), width(T,19,300), width(T,20,150), width(T,21,120,2), width(T,23,160), width(T,24,110,3));
  req.push(warn(grid(T,0,ROWS,26,27),'Esta columna se calcula sola.'));

  // Agenda
  const A = id.agenda;
  font(A,13);
  req.push(values(A,0,0,[AGENDA_HEADERS]), format(grid(A,0,1,0,13),head('1F3864','FFFFFF'),HEAD_FIELDS));
  req.push(format(grid(A,1,ROWS,0,13),{backgroundColor:rgb('FFF2CC')},['backgroundColor']), format(grid(A,1,ROWS,0,1),DATE,['numberFormat']),
    format(grid(A,1,ROWS,1,2),{numberFormat:{type:'TIME',pattern:'h:mm am/pm'}},['numberFormat']),
    format(grid(A,1,ROWS,10,11),{numberFormat:{type:'CURRENCY',pattern:'$#,##0'}},['numberFormat']),
    format(grid(A,1,ROWS,11,12),{numberFormat:{type:'NUMBER',pattern:'0'}},['numberFormat']));
  for (const c of [6,8,12]) req.push(format(grid(A,1,ROWS,c,c+1),{wrapStrategy:'WRAP'},['wrapStrategy']));
  req.push(validate(grid(A,1,ROWS,9,10),listRule('=Listas!$D$2:$D$30')), validate(grid(A,1,ROWS,3,4),listRule('=Listas!$G$2:$G$30')),
    validate(grid(A,1,ROWS,2,3),listRule('=Listas!$H$2:$H$30')), validate(grid(A,1,ROWS,7,8),choice('4 en 14','Boletas')));
  [95,80,140,125,140,110,230,85,230,120,90,90,200].forEach((px,i)=>req.push(width(A,i,px)));

  // Resumen
  const S = id.summary, gray = {backgroundColor:rgb('EEEEEE')};
  font(S,9,60);
  req.push(values(S,0,0,summaryRows()));
  req.push(format(grid(S,3,4,0,4),head('1F3864','FFFFFF'),HEAD_FIELDS), format(grid(S,31,32,0,9),head('1F3864','FFFFFF'),HEAD_FIELDS));
  for (const [r1,r2,c1,c2] of [[4,6,1,4],[8,11,1,4],[13,25,1,4],[27,29,1,2],[32,38,0,9]]) req.push(format(grid(S,r1,r2,c1,c2),gray,['backgroundColor']));
  req.push(format(grid(S,4,6,1,4),DATE,['numberFormat']));
  for (const r of [16,18]) req.push(format(grid(S,r,r+1,1,4),{numberFormat:{type:'PERCENT',pattern:'0%'}},['numberFormat']));
  req.push(format(grid(S,19,21,1,4),{numberFormat:{type:'CURRENCY',pattern:'$#,##0'}},['numberFormat']),
    format(grid(S,22,23,1,4),{numberFormat:{type:'NUMBER',pattern:'0.0'}},['numberFormat']));
  for (const c of [4,6]) req.push(format(grid(S,32,38,c,c+1),{numberFormat:{type:'PERCENT',pattern:'0%'}},['numberFormat']));
  req.push(format(grid(S,0,1,0,1),{textFormat:{bold:true,fontSize:14,fontFamily:'Arial'}},['textFormat']));
  for (const r of [7,12,26,30]) req.push(format(grid(S,r,r+1,0,1),{textFormat:{bold:true,fontFamily:'Arial'}},['textFormat']));
  req.push(width(S,0,330), width(S,1,120,4), width(S,5,130,2), width(S,7,90), width(S,8,150));
  req.push(warn({sheetId:S},'El Resumen se calcula solo.'));
  return req;
}
