import * as THREE from 'three';

// Both the panel and its recall button are world-anchored. Only a deliberate
// recall, travel, or a sustained look away relocates them; never a head tilt.
export function createHandMenu({panel,button,head,isBusy=()=>false,now=()=>performance.now()}) {
  const position=new THREE.Vector3(),q=new THREE.Quaternion(),forward=new THREE.Vector3(0,0,-1);
  const direction=new THREE.Vector3(),yaw=new THREE.Quaternion(),up=new THREE.Vector3(0,1,0);
  let active=false,pending=false,lostSince=null;
  function show(){if(!active)return;panel.visible=true;button.visible=false;pending=true;lostSince=null;}
  function hide(){panel.visible=false;button.visible=active;pending=true;lostSince=null;}
  function update(){
    if(!active)return;
    head.getWorldPosition(position);head.getWorldQuaternion(q);
    direction.set(0,0,-1).applyQuaternion(q);
    const gazeSlope=direction.y/Math.max(.25,Math.hypot(direction.x,direction.z));
    direction.y=0;
    // Preserve the previous horizontal heading when looking straight up/down.
    if(direction.lengthSq()>.01)forward.copy(direction).normalize();
    yaw.setFromAxisAngle(up,Math.atan2(-forward.x,-forward.z));
    const target=panel.visible?panel:button;
    direction.copy(target.position).sub(position);direction.y=0;
    const distance=direction.length();
    const lost=distance>2||distance<.45||direction.normalize().dot(forward)<.8;
    if(lost&&!isBusy()){lostSince??=now();}else lostSince=null;
    if(!isBusy()&&(pending||(lostSince!==null&&now()-lostSince>=650))){
      target.position.copy(position).addScaledVector(forward,panel.visible?1.45:1.15);
      target.position.y+=panel.visible?.22+THREE.MathUtils.clamp(gazeSlope*1.45,-.65,.35):-.15+THREE.MathUtils.clamp(gazeSlope*1.15,-.55,.25);
      target.quaternion.copy(yaw);pending=false;lostSince=null;
    }
    // Ray tests run before rendering: don't leave their transforms one frame behind.
    panel.updateWorldMatrix(true,true);button.updateWorldMatrix(true,true);
  }
  return {show,hide,toggle:()=>panel.visible?hide():show(),update,
    start(){active=true;show();},
    stop(){active=false;button.visible=false;panel.visible=false;pending=false;lostSince=null;},
    recenter(){if(active){pending=true;lostSince=null;}},get visible(){return panel.visible;}};
}
