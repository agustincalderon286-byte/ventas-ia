import * as THREE from 'three';
export const STATIONS={island:{x:0,z:.35,label:'Isla'},stove:{x:0,z:-2.68,label:'Estufa'},sink:{x:1.65,z:-2.68,label:'Fregadero'},fridge:{x:2.85,z:-2.30,label:'Refrigerador'},drawers:{x:-1.4,z:-2.68,label:'Cajones'}};
const obstacles=[{minX:-2.10,maxX:2.10,minZ:-2.18,maxZ:-.32},{minX:-3.7,maxX:3.7,minZ:-4.3,maxZ:-3.08}];
export function canOccupy(x,z){return x>=-3.65&&x<=3.65&&z>=-3.65&&z<=3.55&&!obstacles.some(b=>x>b.minX&&x<b.maxX&&z>b.minZ&&z<b.maxZ);}
export function createLocomotion({rig,camera,renderer,onMove=()=>{},beforeJump=()=>{}}){
  let turnReady=true,lastStation='island';const head=new THREE.Vector3(),forward=new THREE.Vector3(),right=new THREE.Vector3(),up=new THREE.Vector3(0,1,0);
  function headPosition(){renderer.xr.getCamera(camera).getWorldPosition(head);return head;}
  function translate(dx,dz){headPosition();let moved=false;if(canOccupy(head.x+dx,head.z)){rig.position.x+=dx;head.x+=dx;moved=true;}if(canOccupy(head.x,head.z+dz)){rig.position.z+=dz;head.z+=dz;moved=true;}if(moved)onMove({x:head.x,z:head.z,station:lastStation});}
  function teleportTo(place,station='floor'){if(!place||!Number.isFinite(place.x)||!Number.isFinite(place.z)||!canOccupy(place.x,place.z))return false;beforeJump();headPosition();rig.position.x+=place.x-head.x;rig.position.z+=place.z-head.z;lastStation=station;rig.updateMatrixWorld(true);onMove({...place,station:lastStation});return true;}
  function teleport(id){return teleportTo(STATIONS[id],id);}
  function turn(angle){beforeJump();headPosition();const pivot=head.clone(),offset=rig.position.clone().sub(pivot).applyAxisAngle(up,angle);rig.rotation.y+=angle;rig.position.copy(pivot).add(offset);rig.updateMatrixWorld(true);}
  function update(dt){if(!renderer.xr.isPresenting)return;const sources=renderer.xr.getSession()?.inputSources||[];let x=0,y=0,turnX=0;
    for(const source of sources){const axes=source.gamepad?.axes;if(!axes?.length)continue;const a=axes.length>=4?axes.slice(2,4):axes.slice(0,2);if(source.handedness==='left'){x=a[0];y=a[1];}if(source.handedness==='right')turnX=a[0];}
    if(Math.hypot(x,y)>.18){renderer.xr.getCamera(camera).getWorldDirection(forward);forward.y=0;forward.normalize();right.crossVectors(forward,up).normalize();const speed=Math.min(dt,.05)*.9;const delta=right.multiplyScalar(x).add(forward.multiplyScalar(-y));if(delta.length()>1)delta.normalize();translate(delta.x*speed,delta.z*speed);rig.updateMatrixWorld(true);}
    if(Math.abs(turnX)<.25)turnReady=true;else if(Math.abs(turnX)>.7&&turnReady){turnReady=false;turn(-Math.sign(turnX)*Math.PI/6);}
  }
  function reset(){rig.position.set(0,0,0);rig.rotation.set(0,0,0);rig.updateMatrixWorld(true);turnReady=true;}
  return {update,teleport,teleportTo,turn,reset};
}
