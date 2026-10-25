import {createActionPress,isVisible} from './intentional-action.js';
export function installXRHands(THREE,{renderer,scene,rig=scene,system,handModelFactory,buttons=[],onMessage=()=>{},now=()=>performance.now()}) {
  const ray=new THREE.Raycaster(),rotation=new THREE.Matrix4();const states=[];
  function intersect(source,uiOnly=false){source.updateWorldMatrix(true,false);rotation.extractRotation(source.matrixWorld);ray.ray.origin.setFromMatrixPosition(source.matrixWorld);ray.ray.direction.set(0,0,-1).applyMatrix4(rotation);const hudHit=ray.intersectObjects(buttons.filter(b=>b.userData.hud&&isVisible(b)),false)[0];if(hudHit)return hudHit;return ray.intersectObjects([...buttons,...(uiOnly?[]:system.meshes())].filter(isVisible),false)[0];}
  function nearButton(point){let best=null,distance=Infinity;for(const button of buttons){if(!isVisible(button)||button.userData.rayOnly)continue;const d=new THREE.Box3().setFromObject(button).distanceToPoint(point);if(d<(button.userData.nearRadius||.055)&&d<distance){best=button;distance=d;}}return best;}
  for(let index=0;index<2;index++){
    const controller=renderer.xr.getController(index),grip=renderer.xr.getControllerGrip(index),hand=renderer.xr.getHand(index),pinchHolder=new THREE.Group();rig.add(controller,grip,hand);scene.add(pinchHolder);
    hand.add(handModelFactory.createHandModel(hand,'spheres'));
    const line=new THREE.Line(new THREE.BufferGeometry().setFromPoints([new THREE.Vector3(),new THREE.Vector3(0,0,-1)]),new THREE.LineBasicMaterial({color:0x86f6d3,transparent:true,opacity:.65}));line.scale.z=3;controller.add(line);
    const cursor=new THREE.Mesh(new THREE.SphereGeometry(.008,8,6),new THREE.MeshBasicMaterial({color:0x86f6d3}));scene.add(cursor);cursor.visible=false;
    const state={controller,grip,hand,pinchHolder,line,cursor,input:null,pinching:false,press:createActionPress(),nearPress:createActionPress()};states.push(state);
    controller.addEventListener('connected',event=>{state.input=event.data;});
    controller.addEventListener('disconnected',()=>{system.release(controller,false);system.release(hand,false);state.press.cancel();state.nearPress.cancel();state.input=null;state.pinching=false;cursor.visible=false;});
    controller.addEventListener('selectstart',()=>{
      const hit=intersect(controller,!!state.input?.hand);if(!hit)return;
      // A nearby pot takes priority over a distant knob behind it when pinching with bare hands.
      if(state.input?.hand&&hand.visible&&!hit.object.userData.hud){const thumb=hand.joints?.['thumb-tip'],finger=hand.joints?.['index-finger-tip'];if(thumb?.visible&&finger?.visible){const near=thumb.getWorldPosition(new THREE.Vector3()).add(finger.getWorldPosition(new THREE.Vector3())).multiplyScalar(.5);if(!nearButton(near)&&system.closest(near,.065))return;}}
      if(hit.object.userData.action){if(!state.nearPress.active&&!system.holders.has(hand)&&!system.holders.has(controller))state.press.begin(hit.object,now());return;}
      if(state.input?.hand)return;
      const id=system.resolve(hit.object);if(id)system.beginGrab(id,controller,grip);
    });
    controller.addEventListener('selectend',()=>{if(state.press.active)state.press.release(intersect(controller,!!state.input?.hand)?.object,now(),state.input?.hand?250:0);system.release(controller);});
  }
  function update(){if(!renderer.xr.isPresenting)return;
    const hovered=new Set();
    for(const state of states){
      const hit=state.input?intersect(state.controller,!!state.input.hand):null;state.line.visible=!!state.input&&(!state.input.hand||!!hit);state.cursor.visible=state.line.visible&&!!hit;
      if(hit){hovered.add(hit.object);state.cursor.position.copy(hit.point);state.line.scale.z=hit.distance;}else state.line.scale.z=3;
      const {hand,pinchHolder}=state,thumb=hand.joints?.['thumb-tip'],index=hand.joints?.['index-finger-tip'];
      if(!hand.visible||!thumb?.visible||!index?.visible){if(state.pinching)system.release(hand,false);state.nearPress.cancel();if(state.input?.hand)state.press.cancel();state.pinching=false;continue;}
      const a=thumb.getWorldPosition(new THREE.Vector3()),b=index.getWorldPosition(new THREE.Vector3());pinchHolder.position.copy(a).add(b).multiplyScalar(.5);const wrist=hand.joints?.wrist;pinchHolder.quaternion.copy((wrist||index).getWorldQuaternion(new THREE.Quaternion()));pinchHolder.updateMatrixWorld(true);
      const pinching=a.distanceTo(b)<(state.pinching ? .040 : .026);
      if(pinching&&!state.pinching&&!state.press.active){const button=nearButton(pinchHolder.position);if(button)state.nearPress.begin(button,now());else{const id=system.closest(pinchHolder.position,.065);if(id)system.beginGrab(id,hand,pinchHolder);}}
      if(!pinching&&state.pinching){state.nearPress.release(nearButton(pinchHolder.position),now(),350);system.release(hand);}state.pinching=pinching;
    }
    for(const button of buttons)button.userData.setHover?.(hovered.has(button));
  }
  function resetInput(){for(const button of buttons)button.userData.setHover?.(false);system.releaseAll();for(const s of states){s.pinching=false;s.cursor.visible=false;s.press.cancel();s.nearPress.cancel();}}
  renderer.xr.addEventListener('sessionend',resetInput);return {update,resetInput,isPressing:()=>states.some(s=>s.press.active||s.nearPress.active)};
}
