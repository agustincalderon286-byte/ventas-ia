export function installXRHands(THREE,{renderer,scene,system,handModelFactory,buttons=[],onMessage=()=>{}}) {
  const ray = new THREE.Raycaster(), rotation=new THREE.Matrix4(), point=new THREE.Vector3();
  const hands=[],controllers=[];
  function intersect(source) {
    source.updateWorldMatrix(true,false);rotation.extractRotation(source.matrixWorld);
    ray.ray.origin.setFromMatrixPosition(source.matrixWorld);ray.ray.direction.set(0,0,-1).applyMatrix4(rotation);
    return ray.intersectObjects([...buttons,...system.meshes()],false)[0];
  }
  for(let index=0;index<2;index++) {
    const controller=renderer.xr.getController(index),grip=renderer.xr.getControllerGrip(index),hand=renderer.xr.getHand(index);
    const pinchHolder=new THREE.Group();scene.add(controller,grip,hand,pinchHolder);
    hand.add(handModelFactory.createHandModel(hand,'spheres'));
    const line=new THREE.Line(new THREE.BufferGeometry().setFromPoints([new THREE.Vector3(),new THREE.Vector3(0,0,-1)]),new THREE.LineBasicMaterial({color:0x86f6d3,transparent:true,opacity:.7}));
    line.scale.z=3;controller.add(line);
    const cursor=new THREE.Mesh(new THREE.SphereGeometry(.008,8,6),new THREE.MeshBasicMaterial({color:0x86f6d3}));scene.add(cursor);cursor.visible=false;
    const state={controller,grip,hand,pinchHolder,line,cursor,input:null,pinching:false};controllers.push(state);hands.push(state);
    controller.addEventListener('connected',event=>{state.input=event.data;});
    controller.addEventListener('disconnected',()=>{system.release(controller,false);system.release(hand,false);state.input=null;state.pinching=false;cursor.visible=false;});
    controller.addEventListener('selectstart',()=>{
      if(state.input?.hand)return;
      const hit=intersect(controller);if(!hit)return;
      if(hit.object.userData.action){hit.object.userData.action();return;}
      const id=system.resolve(hit.object);if(!id)return;
      // Attach to grip space while retaining the current offset: no jump at pickup.
      system.beginGrab(id,controller,grip);
    });
    controller.addEventListener('selectend',()=>system.release(controller));
  }
  function update() {
    if(!renderer.xr.isPresenting)return;
    for(const state of controllers) {
      state.line.visible=!!state.input && !state.input.hand;
      const hit=state.line.visible ? intersect(state.controller) : null;
      state.cursor.visible=!!hit;if(hit){state.cursor.position.copy(hit.point);state.line.scale.z=hit.distance;}
    }
    for(const state of hands) {
      const {hand,pinchHolder}=state, thumb=hand.joints?.['thumb-tip'],index=hand.joints?.['index-finger-tip'];
      if(!hand.visible||!thumb?.visible||!index?.visible) { if(state.pinching)system.release(hand,false);state.pinching=false;continue; }
      const a=thumb.getWorldPosition(new THREE.Vector3()), b=index.getWorldPosition(new THREE.Vector3());
      pinchHolder.position.copy(a).add(b).multiplyScalar(.5);
      const wrist=hand.joints?.wrist;pinchHolder.quaternion.copy((wrist||index).getWorldQuaternion(new THREE.Quaternion()));pinchHolder.updateMatrixWorld(true);
      const distance=a.distanceTo(b), pinching=distance<(state.pinching ? .040 : .026);
      if(pinching&&!state.pinching) {
        const button=buttons.find(mesh=>new THREE.Box3().setFromObject(mesh).distanceToPoint(pinchHolder.position)<.065);
        if(button)button.userData.action();
        else { const id=system.closest(pinchHolder.position,.065);if(id)system.beginGrab(id,hand,pinchHolder); }
      }
      if(!pinching&&state.pinching)system.release(hand);
      state.pinching=pinching;
    }
  }
  function resetInput() { system.releaseAll();for(const s of hands){s.pinching=false;s.cursor.visible=false;} }
  renderer.xr.addEventListener('sessionend',resetInput);
  return {update,resetInput};
}
