export function createCaseControls(THREE,system,onChange=()=>{}) {
  const lid=system.root.getObjectByName('Tapa_articulada');
  if(!lid)throw new Error('Falta la tapa articulada.');
  const axis=new THREE.Vector3(1,0,0),closed=lid.quaternion.clone().multiply(new THREE.Quaternion().setFromAxisAngle(axis,108*Math.PI/180));
  const ledMaterials=new Set(),lights=[];let angle=108,target=108,enabled=true;
  system.root.traverse(n=>{if(!n.isMesh)return;const original=Array.isArray(n.material)?n.material:[n.material];const replacement=original.map(m=>{if(!m.name.includes('LED'))return m;const copy=m.clone();ledMaterials.add(copy);return copy;});n.material=Array.isArray(n.material)?replacement:replacement[0];});
  for(const x of [-.42,.42]){const light=new THREE.PointLight(0xeaf4ff,.5,.9,2);light.position.set(x,-.025,.38);lid.add(light);lights.push(light);}
  function notify(message){onChange({angle,target,lights:enabled,message});}
  function setOpen(open){if(system.holders.size){notify('Suelta la pieza antes de mover la tapa.');return false;}target=open?108:0;notify(open?'Abriendo maleta…':'Cerrando maleta…');return true;}
  function setLights(on){enabled=!!on;for(const m of ledMaterials){m.emissiveIntensity=enabled?3:0;m.color.setHex(enabled?0xffffff:0x575d62);}for(const l of lights)l.visible=enabled;notify(enabled?'LED encendidos':'LED apagados');}
  function update(dt){if(angle===target)return;const step=Math.min(Math.max(dt,0),.05)*95;angle=THREE.MathUtils.clamp(angle+Math.sign(target-angle)*step,Math.min(angle,target),Math.max(angle,target));lid.quaternion.copy(closed).multiply(new THREE.Quaternion().setFromAxisAngle(axis,-angle*Math.PI/180));notify();}
  setLights(true);
  return {setOpen,toggle:()=>setOpen(target<54),setLights,toggleLights:()=>setLights(!enabled),update,get angle(){return angle;},get target(){return target;},get lights(){return enabled;},canAccess:()=>angle>=75&&target>=75};
}
