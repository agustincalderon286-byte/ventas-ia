import * as THREE from 'three';
// The summon button follows the view. The open panel stays still while the user points at it.
export function createHandMenu({panel,button,head,isBusy=()=>false}) {
  const position=new THREE.Vector3(),q=new THREE.Quaternion(),forward=new THREE.Vector3(),up=new THREE.Vector3(0,1,0);
  let active=false,pending=false;
  function pose(){head.getWorldPosition(position);head.getWorldQuaternion(q);forward.set(0,0,-1).applyQuaternion(q);forward.y=0;if(forward.lengthSq()<.01)forward.set(0,0,-1);forward.normalize();return new THREE.Quaternion().setFromUnitVectors(new THREE.Vector3(0,0,-1),forward);}
  function show(){if(!active)return;pending=true;panel.visible=true;}
  function update(){if(!active)return;const yaw=pose();
    button.position.copy(position).add(new THREE.Vector3(-.4,-.24,-.8).applyQuaternion(q));button.quaternion.copy(q);
    // Recover automatically after a teleport or when the old menu is behind the user.
    const toPanel=panel.position.clone().sub(position);const lost=toPanel.length()>2||toPanel.clone().normalize().dot(forward)<.05;
    if(panel.visible&&(pending||lost)&&!isBusy()){
      panel.position.copy(position).add(new THREE.Vector3(0,.18,-1.6).applyQuaternion(q));panel.quaternion.copy(q);pending=false;
    }
  }
  return {show,hide:()=>{panel.visible=false;},toggle:()=>panel.visible?(panel.visible=false):show(),update,
    start(){active=true;button.visible=true;show();update();},stop(){active=false;button.visible=false;panel.visible=false;},recenter:()=>{pending=true;},get visible(){return panel.visible;}};
}
