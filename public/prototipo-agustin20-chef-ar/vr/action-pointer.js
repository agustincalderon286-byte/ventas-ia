import * as THREE from 'three';
import {isVisible} from '../shared/intentional-action.js';
// The same world controls are usable in the desktop preview with a normal click.
export function installActionPointer({canvas,camera,buttons,controls,system,enabled=()=>true}){
 const ray=new THREE.Raycaster(),point=new THREE.Vector2();let pressed=null;
 function hit(e){const b=canvas.getBoundingClientRect();point.set((e.clientX-b.left)/b.width*2-1,1-(e.clientY-b.top)/b.height*2);ray.setFromCamera(point,camera);const visible=buttons.filter(isVisible);const hud=ray.intersectObjects(visible.filter(b=>b.userData.hud),false)[0];if(hud)return hud.object;const target=ray.intersectObjects(visible,false)[0],object=ray.intersectObjects(system?.meshes()||[],false)[0];return target&&(!object||target.distance<=object.distance+.001)?target.object:null;}
 function end(e,activate){if(!pressed)return;const old=pressed;pressed=null;controls.enabled=old.controls;if(canvas.hasPointerCapture(e.pointerId))canvas.releasePointerCapture(e.pointerId);if(activate&&enabled()&&hit(e)===old.target&&Math.hypot(e.clientX-old.x,e.clientY-old.y)<12)old.target.userData.action?.();}
 canvas.addEventListener('pointerdown',e=>{if(e.button!==0||!e.isPrimary||!enabled())return;const target=hit(e);if(!target)return;e.stopImmediatePropagation();pressed={target,x:e.clientX,y:e.clientY,controls:controls.enabled};controls.enabled=false;canvas.setPointerCapture(e.pointerId);},true);
 canvas.addEventListener('pointermove',e=>{if(pressed)e.stopImmediatePropagation();},true);
 canvas.addEventListener('pointerup',e=>{if(pressed)e.stopImmediatePropagation();end(e,true);},true);
 canvas.addEventListener('pointercancel',e=>end(e,false),true);
}
