// Touch/mouse gestures shared by camera AR and the desktop Quest preview.
export function installPiecePointer(THREE, {canvas,camera,system,world,controls,canInteract=()=>true,mode=()=> 'move'}) {
  const ray = new THREE.Raycaster(), pointer=new THREE.Vector2(), plane=new THREE.Plane();
  const holder = new THREE.Group(); world.add(holder);
  const owner = {}, point = new THREE.Vector3(); let drag=null;
  function cast(e) { const b=canvas.getBoundingClientRect(); pointer.set((e.clientX-b.left)/b.width*2-1,-(e.clientY-b.top)/b.height*2+1);ray.setFromCamera(pointer,camera); }
  function end(e, snap=true) {
    if(!drag || (e && e.pointerId!==drag.pointer))return;
    const id=drag.pointer, previousControls=drag.previousControls; drag=null; system.release(owner,snap); if(controls)controls.enabled=previousControls;
    if(canvas.hasPointerCapture(id))canvas.releasePointerCapture(id);
  }
  canvas.addEventListener('pointerdown', e=>{
    if(e.button!==0 || !e.isPrimary || !canInteract())return;
    cast(e); const hit=ray.intersectObjects(system.meshes(),false)[0]; const id=hit&&system.resolve(hit.object);
    if(!id)return;
    e.stopImmediatePropagation(); system.select(id); const previousControls=controls?.enabled; if(controls)controls.enabled=false;
    canvas.setPointerCapture(e.pointerId);
    drag={previousControls,pointer:e.pointerId,id,x:e.clientX,y:e.clientY,lastX:e.clientX,lastY:e.clientY,moving:false};
  },true);
  canvas.addEventListener('pointermove', e=>{
    if(!drag||drag.pointer!==e.pointerId)return;
    e.stopImmediatePropagation();
    if(!canInteract()){end(e,false);return;}
    const d=drag;
    if(!d.moving && Math.hypot(e.clientX-d.x,e.clientY-d.y)<7)return;
    if(!d.moving) {
      holder.position.set(0,0,0); holder.quaternion.identity(); holder.scale.setScalar(1);holder.updateMatrixWorld(true);
      if(!system.beginGrab(d.id,owner,holder)){end(e,false);return;}
      d.moving=true;
      const node=system.item(d.id).node; const position=node.getWorldPosition(new THREE.Vector3());
      // Screen-facing plane permits a natural lift away from a case viewed at an angle.
      plane.setFromNormalAndCoplanarPoint(camera.getWorldDirection(new THREE.Vector3()),position);
      cast({clientX:d.x,clientY:d.y}); d.start=ray.ray.intersectPlane(plane,new THREE.Vector3())?.clone();
      d.nodeStart=node.position.clone();
    }
    const node=system.item(d.id).node;
    if(mode()==='rotate') {
      const q=new THREE.Quaternion();const parentQ=node.parent.getWorldQuaternion(new THREE.Quaternion()).invert();
      const up=new THREE.Vector3(0,1,0).applyQuaternion(camera.quaternion).applyQuaternion(parentQ);
      const right=new THREE.Vector3(1,0,0).applyQuaternion(camera.quaternion).applyQuaternion(parentQ);
      node.quaternion.premultiply(q.setFromAxisAngle(up,(e.clientX-d.lastX)*.009));
      node.quaternion.premultiply(q.setFromAxisAngle(right,(e.clientY-d.lastY)*.009));
    } else {
      cast(e); if(d.start && ray.ray.intersectPlane(plane,point)) {
        const localA=holder.worldToLocal(d.start.clone()), localB=holder.worldToLocal(point.clone());
        node.position.copy(d.nodeStart).add(localB.sub(localA));
      }
    }
    d.lastX=e.clientX; d.lastY=e.clientY;
  },true);
  canvas.addEventListener('pointerup',e=>end(e,mode()!=='rotate'),true);
  canvas.addEventListener('pointercancel',e=>end(e,false),true);
  canvas.addEventListener('lostpointercapture',()=>end(null,false),true);
  return {cancel:()=>end(null,false)};
}
