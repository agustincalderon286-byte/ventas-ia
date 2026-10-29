// Shared, deterministic navigation calculations. GPS never completes a visit.
export function meters(a,b) {
  const rad=Math.PI/180, x=(b.lon-a.lon)*rad*Math.cos((a.lat+b.lat)*rad/2), y=(b.lat-a.lat)*rad;
  return Math.hypot(x,y)*6371000;
}
export function projectRoute(point, coordinates) {
  let best={distance:Infinity,along:0}, length=0;
  const sx=111320*Math.cos(point.lat*Math.PI/180),sy=111320;
  for(let i=1;i<coordinates.length;i++) {
    const a=coordinates[i-1],b=coordinates[i], ax=(a[0]-point.lon)*sx,ay=(a[1]-point.lat)*sy,bx=(b[0]-point.lon)*sx,by=(b[1]-point.lat)*sy;
    const dx=bx-ax,dy=by-ay, size=Math.hypot(dx,dy), t=size?Math.max(0,Math.min(1,-(ax*dx+ay*dy)/(size*size))):0;
    const distance=Math.hypot(ax+t*dx,ay+t*dy);
    if(distance<best.distance) best={distance,along:length+t*size};
    length+=size;
  }
  return {...best,length};
}
export function navigationFix(point, plan, destination) {
  if(!point || !Number.isFinite(point.lat) || !Number.isFinite(point.lon) || point.accuracy < 0 || point.accuracy>40) return {state:'imprecise',message:'Esperando una ubicación más precisa…'};
  const projection=projectRoute(point,plan.coordinates);
  if(projection.distance>60) return {state:'offroute',message:'Te alejaste del trayecto. Recalculando…'};
  if(meters(point,destination)<25 && point.accuracy<=25) return {state:'arrived',message:'Estás cerca de la casa. Confirma el número y registra el resultado.'};
  const index=plan.steps.findIndex(step=>step.along>projection.along+10);
  const step=plan.steps[index<0?plan.steps.length-1:index];
  return {state:'guiding',instructionId:index,message:step?`${Math.max(0,Math.round((step.along-projection.along)/10)*10)} m · ${step.text}`:'Continúa hacia la casa.',remaining:Math.max(0,projection.length-projection.along)};
}
export function spanishManeuver(step) {
 const type=step.maneuver?.type, modifier=step.maneuver?.modifier;
 const directions={left:'a la izquierda',right:'a la derecha','slight left':'ligeramente a la izquierda','slight right':'ligeramente a la derecha','sharp left':'cerrado a la izquierda','sharp right':'cerrado a la derecha',straight:'recto',uturn:'y da la vuelta'};
 if(type==='arrive') return 'Llegas al destino; confirma el número de la casa.';
 const verb=type==='depart'?'Comienza el recorrido':type==='roundabout'||type==='rotary'?'Entra a la glorieta':type==='merge'?'Incorpórate':type==='fork'?'Toma la bifurcación':type==='continue'||type==='new name'?'Continúa':'Gira';
 return `${verb}${type==='depart'?'':directions[modifier]?' '+directions[modifier]:''}${step.name?' por '+step.name:''}${step.maneuver?.exit?'; toma la salida '+step.maneuver.exit:''}.`;
}
