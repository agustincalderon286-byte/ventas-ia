import * as THREE from 'three';

// Closed means no world UI at all. Palm gesture explicitly summons a stationary panel.
export function createHandMenu({panel,head,isBusy=()=>false}) {
  const position=new THREE.Vector3(),q=new THREE.Quaternion(),forward=new THREE.Vector3(0,0,-1),direction=new THREE.Vector3();
  let active=false,pending=false;
  function show(){if(!active)return;panel.visible=true;pending=true;}
  function hide(){panel.visible=false;pending=false;}
  function update(){
    if(!active||!panel.visible||!pending||isBusy())return;
    head.getWorldPosition(position);head.getWorldQuaternion(q);
    direction.set(0,0,-1).applyQuaternion(q);
    const gazeSlope=direction.y/Math.max(.25,Math.hypot(direction.x,direction.z));direction.y=0;
    if(direction.lengthSq()>.01)forward.copy(direction).normalize();
    panel.position.copy(position).addScaledVector(forward,1.45);
    panel.position.y+=.16+THREE.MathUtils.clamp(gazeSlope*1.45,-.65,.25);
    panel.quaternion.setFromAxisAngle(new THREE.Vector3(0,1,0),Math.atan2(-forward.x,-forward.z));
    panel.updateWorldMatrix(true,true);pending=false;
  }
  return {show,hide,toggle:()=>panel.visible?hide():show(),update,
    start(){active=true;hide();},stop(){active=false;hide();},
    recenter(){if(active&&panel.visible)pending=true;},get visible(){return panel.visible;}};
}
