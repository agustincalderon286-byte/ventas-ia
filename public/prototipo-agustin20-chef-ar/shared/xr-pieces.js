import {createActionPress,isVisible} from './intentional-action.js';
import {createPalmMenuGesture} from './palm-menu-gesture.js?v=6';

export function installXRHands(THREE,{renderer,scene,rig=scene,system,handModelFactory,buttons=[],head=null,floorTravel=null,onMenu=()=>{},onMessage=()=>{},now=()=>performance.now()}) {
  const ray=new THREE.Raycaster(),rotation=new THREE.Matrix4(),states=[];
  const palm=createPalmMenuGesture(THREE,{head,now,onOpen:()=>{cancelActions();onMenu();}});
  function intersect(source,uiOnly=false){
    source.updateWorldMatrix(true,false);rotation.extractRotation(source.matrixWorld);ray.ray.origin.setFromMatrixPosition(source.matrixWorld);ray.ray.direction.set(0,0,-1).applyMatrix4(rotation);
    const hudHit=ray.intersectObjects(buttons.filter(b=>b.userData.hud&&isVisible(b)),false)[0];if(hudHit)return hudHit;
    return ray.intersectObjects([...buttons,...(uiOnly?[]:system.meshes())].filter(isVisible),false)[0];
  }
  function nearButton(point){let best=null,distance=Infinity;for(const button of buttons){if(!isVisible(button)||button.userData.rayOnly)continue;const d=new THREE.Box3().setFromObject(button).distanceToPoint(point);if(d<(button.userData.nearRadius||.055)&&d<distance){best=button;distance=d;}}return best;}
  function cancelActions(){floorTravel?.cancel();for(const s of states){s.press.cancel();s.nearPress.cancel();s.palmRing.visible=false;}for(const b of buttons)b.userData.setHover?.(false);}
  for(let index=0;index<2;index++){
    const controller=renderer.xr.getController(index),grip=renderer.xr.getControllerGrip(index),hand=renderer.xr.getHand(index),pinchHolder=new THREE.Group();
    // A held object now moves with the player immediately, even on a missing tracking frame.
    rig.add(controller,grip,hand,pinchHolder);hand.add(handModelFactory.createHandModel(hand,'spheres'));
    const line=new THREE.Line(new THREE.BufferGeometry().setFromPoints([new THREE.Vector3(),new THREE.Vector3(0,0,-1)]),new THREE.LineBasicMaterial({color:0x86f6d3,transparent:true,opacity:.65}));line.scale.z=3;controller.add(line);
    const cursor=new THREE.Mesh(new THREE.SphereGeometry(.008,8,6),new THREE.MeshBasicMaterial({color:0x86f6d3}));scene.add(cursor);cursor.visible=false;
    const palmRing=new THREE.Mesh(new THREE.RingGeometry(.035,.043,48),new THREE.MeshBasicMaterial({color:0x86f6d3,side:THREE.DoubleSide,depthTest:false}));palmRing.rotation.x=-Math.PI/2;palmRing.renderOrder=1001;palmRing.visible=false;scene.add(palmRing);
    const state={controller,grip,hand,pinchHolder,line,cursor,palmRing,input:null,disconnected:false,pinching:false,blockUntilOpen:false,lostSince:null,openSince:null,press:createActionPress(),nearPress:createActionPress()};states.push(state);
    controller.addEventListener('connected',event=>{state.input=event.data;state.disconnected=false;});
    controller.addEventListener('disconnected',()=>{system.release(controller,false);state.press.cancel();state.nearPress.cancel();floorTravel?.cancel(hand);state.input=null;state.disconnected=true;state.lostSince??=now();cursor.visible=false;});
    controller.addEventListener('selectstart',()=>{
      if(state.blockUntilOpen||floorTravel?.isActive(hand)||system.holders.has(hand))return;
      const hit=intersect(controller,!!state.input?.hand);if(!hit)return;
      if(state.input?.hand&&hand.visible){const thumb=hand.joints?.['thumb-tip'],finger=hand.joints?.['index-finger-tip'];if(thumb?.visible&&finger?.visible){const near=thumb.getWorldPosition(new THREE.Vector3()).add(finger.getWorldPosition(new THREE.Vector3())).multiplyScalar(.5);if(!nearButton(near)&&system.closest(near,.065))return;}}
      if(hit.object.userData.action){if(!state.nearPress.active&&!system.holders.has(controller))state.press.begin(hit.object,now());return;}
      if(state.input?.hand)return;
      const id=system.resolve(hit.object);if(id)system.beginGrab(id,controller,grip);
    });
    controller.addEventListener('selectend',()=>{if(state.press.active)state.press.release(intersect(controller,!!state.input?.hand)?.object,now(),state.input?.hand?250:0);system.release(controller);});
  }
  function update(){if(!renderer.xr.isPresenting)return;
    const hovered=new Set();
    for(const state of states){
      const {hand,pinchHolder,controller}=state,thumb=hand.joints?.['thumb-tip'],index=hand.joints?.['index-finger-tip'];
      const tracked=!state.disconnected&&hand.visible&&thumb?.visible&&index?.visible;
      const hit=state.input&&(!state.input.hand||tracked)?intersect(controller,!!state.input.hand):null;
      state.line.visible=!!state.input&&(!state.input.hand||!!hit);state.cursor.visible=state.line.visible&&!!hit;state.palmRing.visible=false;
      if(hit){hovered.add(hit.object);state.cursor.position.copy(hit.point);state.line.scale.z=hit.distance;}else state.line.scale.z=3;
      if(!tracked){
        state.lostSince??=now();state.openSince=null;state.nearPress.cancel();if(state.input?.hand||state.disconnected)state.press.cancel();floorTravel?.cancel(hand);palm.update(hand,state.input?.handedness,true);
        if(system.holders.has(hand)){
          if(now()-state.lostSince>=1200){system.release(hand,false);state.pinching=false;state.blockUntilOpen=true;onMessage('Seguimiento perdido: la pieza quedó donde estaba. Abre la mano y vuelve a tomarla.');}
        }else{state.pinching=false;state.blockUntilOpen=true;}
        continue;
      }
      state.lostSince=null;
      const a=thumb.getWorldPosition(new THREE.Vector3()),b=index.getWorldPosition(new THREE.Vector3()),center=a.clone().add(b).multiplyScalar(.5);
      const wrist=hand.joints?.wrist,worldQ=(wrist||index).getWorldQuaternion(new THREE.Quaternion()),rigQ=rig.getWorldQuaternion(new THREE.Quaternion());
      pinchHolder.position.copy(rig.worldToLocal(center.clone()));pinchHolder.quaternion.copy(rigQ.invert().multiply(worldQ));pinchHolder.updateMatrixWorld(true);
      const distance=a.distanceTo(b),pinching=distance<((state.pinching||system.holders.has(hand))?.045:.026);
      if(state.blockUntilOpen){if(distance>.05)state.blockUntilOpen=false;else continue;}
      const held=system.holders.has(hand),progress=palm.update(hand,state.input?.handedness,held||pinching||!!floorTravel?.active);
      if(progress>0){state.palmRing.visible=true;state.palmRing.position.copy(wrist.getWorldPosition(new THREE.Vector3()));state.palmRing.position.y+=.045;state.palmRing.geometry.setDrawRange(0,Math.ceil(progress*48)*6);}
      if(pinching&&!state.pinching&&!state.press.active&&!held){
        const button=nearButton(center),id=button?null:system.closest(center,.065);
        if(button)state.nearPress.begin(button,now());
        else if(id)system.beginGrab(id,hand,pinchHolder);
        else if(!hit?.object.userData.action)floorTravel?.begin(hand,controller);
      }
      if(pinching){state.openSince=null;if(floorTravel?.isActive(hand))floorTravel.aim(hand,controller);}
      if(!pinching&&state.pinching){
        state.nearPress.release(nearButton(center),now(),350);
        if(floorTravel?.isActive(hand))floorTravel.finish(hand);
      }
      if(!pinching&&system.holders.has(hand)){
        // A brief finger-distance spike is not an intentional release.
        state.openSince??=now();if(now()-state.openSince>=140){system.release(hand);state.openSince=null;}
      }
      state.pinching=pinching;
    }
    for(const button of buttons)button.userData.setHover?.(hovered.has(button));
  }
  function resetInput(){cancelActions();palm.reset();system.releaseAll();for(const s of states){s.pinching=false;s.blockUntilOpen=true;s.lostSince=null;s.openSince=null;s.cursor.visible=false;s.line.visible=false;}}
  renderer.xr.addEventListener('sessionend',resetInput);
  return {update,resetInput,cancelActions,isPressing:()=>states.some(s=>s.press.active||s.nearPress.active)||!!floorTravel?.active};
}
