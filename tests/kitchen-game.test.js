import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import * as THREE from 'three';
import {GLTFLoader} from 'three/addons/loaders/GLTFLoader.js';
import {createCaseSystem} from '../public/prototipo-agustin20-chef-ar/shared/case-system.js';
import {createCooking} from '../public/prototipo-agustin20-chef-ar/vr/cooking.js';
import {createFurniture} from '../public/prototipo-agustin20-chef-ar/vr/furniture.js';
import {createHandMenu} from '../public/prototipo-agustin20-chef-ar/vr/hand-menu.js';
const bytes=fs.readFileSync(new URL('../public/prototipo-agustin20-chef-ar/assets/maleta-royal-prestige-ar-v3.glb',import.meta.url));
globalThis.self=globalThis;globalThis.createImageBitmap=async()=>({width:1,height:1,close(){}});
const gltf=await new GLTFLoader().parseAsync(bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength),'');
function setup(){const scene=new THREE.Scene(),root=gltf.scene.clone(true);scene.add(root);let cooking;const system=createCaseSystem(THREE,root,scene,()=>{},{onRelease:p=>cooking.release(p)});const kitchen={state:{water:false,burners:[false,false,false,false]}};cooking=createCooking({system,scene,kitchen});return {scene,system,kitchen,cooking,step:(seconds)=>{for(let i=0;i<seconds*60;i++)cooking.update(1/60);}};}
test('a real pot fills only under running tap, keeps water while carried and steams only with heat',()=>{
 const {system,kitchen,cooking,step}=setup();system.extract('tapa_6');assert.ok(cooking.place('olla_6','sink'));step(2);assert.equal(cooking.pots.get('olla_6').fill,0);
 kitchen.state.water=true;step(8);const pot=cooking.pots.get('olla_6');assert.equal(pot.fill,1);assert.equal(pot.steam.visible,false);
 cooking.place('olla_6','stove',1);step(1);assert.equal(pot.fill,1);assert.equal(pot.steam.visible,false);
 kitchen.state.burners[1]=true;step(3);assert.ok(pot.steam.visible);assert.ok(pot.heat>.5);
 cooking.place('olla_6','island');step(4);assert.equal(pot.steam.visible,false);assert.ok(pot.fill>.9);
});
test('covered or sideways pot does not fill; tilt empties and dry pot never steams',()=>{
 const {system,kitchen,cooking,step}=setup();system.extract('tapa_3');cooking.place('olla_3','sink');kitchen.state.water=true;
 const p=cooking.pots.get('olla_3'),lid=system.pieces.get('tapa_3');lid.node.position.set(1.65,.89+p.height,-3.64);step(2);assert.equal(p.fill,0);
 lid.node.position.x+=1;step(2);assert.ok(p.fill>.2);p.item.node.rotation.z=Math.PI/2;step(4);assert.equal(p.fill,0);
 cooking.place('olla_3','stove',1);kitchen.state.burners[1]=true;step(3);assert.equal(p.steam.visible,false);
});
test('release snaps to a free burner, refuses occupied burner and water state resets with exhibit',()=>{
 const {system,cooking,scene,kitchen,step}=setup();system.extract('tapa_6');system.extract('tapa_3');cooking.place('olla_6','stove',1);
 system.extract('olla_3');const p=system.pieces.get('olla_3'),holder=new THREE.Group();scene.add(holder);system.beginGrab('olla_3','hand',holder);p.node.position.set(-.23,1.01,-3.45);system.release('hand');assert.notEqual(p.node.position.y,.982);
 p.node.position.set(.23,1.01,-3.45);assert.ok(cooking.release(p));assert.equal(p.node.position.y,.982);
 cooking.place('olla_3','sink');kitchen.state.water=true;step(3);assert.ok(cooking.pots.get('olla_3').fill>0);system.reset();cooking.reset();assert.equal(cooking.pots.get('olla_3').fill,0);assert.equal(cooking.pots.get('olla_3').water.visible,false);
});
test('food can be released into a pot, travels with it, reheats visually and can be taken out',()=>{
 const {system,cooking,scene,kitchen,step}=setup();const tomato=new THREE.Group(),mesh=new THREE.Mesh(new THREE.SphereGeometry(.04),new THREE.MeshStandardMaterial({color:0xdd3322}));tomato.add(mesh);scene.add(tomato);system.registerExtra('tomato',tomato,'Tomate');cooking.registerFood('tomato',tomato);
 system.extract('tapa_6');cooking.place('olla_6','sink');kitchen.state.water=true;step(3);tomato.position.set(1.65,1.05,-3.64);assert.ok(cooking.release(system.item('tomato')));assert.equal(tomato.parent,system.item('olla_6').node);
 cooking.place('olla_6','stove',1);kitchen.state.burners[1]=true;step(2);assert.ok(cooking.snapshot()[0].ingredients.includes('tomato'));
 const hand=new THREE.Group();scene.add(hand);assert.ok(system.beginGrab('tomato','left',hand));step(.1);assert.equal(cooking.snapshot()[0].ingredients.length,0);
 system.reset();cooking.reset();assert.equal(tomato.parent,scene);
});
test('fridge doors and drawers open independently and food is inaccessible until the fridge opens',()=>{
 const group=new THREE.Group(),m=Object.fromEntries(['wood','lacquer','brass','steel'].map(k=>[k,new THREE.MeshStandardMaterial()])),actions=[];
 const f=createFurniture({group,materials:m,actions});assert.equal(f.food.length,9);assert.equal(f.food[0].canTake(),false);
 f.setFridge(true);for(let i=0;i<100;i++)f.update(.016);assert.equal(f.food[0].canTake(),true);assert.equal(f.state.fridge,true);
 f.setDrawer(2,true);for(let i=0;i<100;i++)f.update(.016);assert.deepEqual(f.state.drawers,[false,false,true,false]);
 f.setFridge(false);for(let i=0;i<100;i++)f.update(.016);assert.equal(f.food[0].canTake(),false);assert.equal(f.state.drawers[2],true);assert.ok(actions.length>=12);
});
function menuSetup(){
 const scene=new THREE.Scene(),rig=new THREE.Group(),head=new THREE.PerspectiveCamera(),panel=new THREE.Group(),button=new THREE.Object3D();scene.add(rig,panel,button);rig.add(head);head.position.set(0,1.6,0);let busy=false,time=0;
 const menu=createHandMenu({panel,button,head,isBusy:()=>busy,now:()=>time});menu.start();menu.update();
 return {scene,rig,head,panel,button,menu,busy:value=>busy=value,tick:ms=>{time+=ms;menu.update();}};
}
function upright(node){assert.ok(new THREE.Vector3(0,1,0).applyQuaternion(node.quaternion).distanceTo(new THREE.Vector3(0,1,0))<1e-8);}
test('menu and recall button stay upright with head tilt, roll and a rotated travel rig',()=>{
 const {rig,head,panel,button,menu}=menuSetup();rig.position.set(1.65,0,-2.68);rig.rotation.y=1.2;head.rotation.set(-.6,.4,.45);menu.recenter();menu.update();upright(panel);
 const position=head.getWorldPosition(new THREE.Vector3());assert.ok(panel.position.distanceTo(position)<1.7);
 menu.hide();menu.update();upright(button);assert.ok(button.position.distanceTo(position)<1.4);assert.equal(panel.visible,false);assert.equal(button.visible,true);
 const facing=new THREE.Vector3(0,0,-1).applyQuaternion(head.getWorldQuaternion(new THREE.Quaternion()));facing.y=0;facing.normalize();const offset=button.position.clone().sub(position);offset.y=0;assert.ok(offset.normalize().dot(facing)>.999);
});
test('recall button remains stationary while aiming and small head movements never chase the pointer',()=>{
 const {head,button,menu,tick,busy}=menuSetup();menu.hide();menu.update();const original=button.position.clone(),q=button.quaternion.clone();
 head.position.x+=.08;head.rotation.set(.3,.25,.2);tick(1000);assert.ok(button.position.equals(original));assert.ok(button.quaternion.equals(q));
 head.rotation.y=Math.PI;busy(true);tick(1000);tick(1000);assert.ok(button.position.equals(original));busy(false);tick(0);tick(649);assert.ok(button.position.equals(original));tick(1);assert.ok(button.position.z>head.position.z);upright(button);
 menu.show();menu.update();assert.equal(button.visible,false);assert.equal(menu.visible,true);
});
test('travel keeps menu open and readable; hidden menu recovers after turning and repeated visits',()=>{
 const {head,panel,button,menu,tick}=menuSetup();
 for(const [x,z] of [[1.65,-2.68],[0,.35],[2.85,-2.3],[-1.4,-2.68],[0,.35]]){
  head.position.set(x,1.6,z);menu.show();menu.update();assert.equal(panel.visible,true);assert.equal(button.visible,false);assert.ok(panel.position.distanceTo(head.position)<1.7);
  // Its raycast matrix is current even before the renderer has drawn a frame.
  assert.ok(new THREE.Vector3().setFromMatrixPosition(panel.matrixWorld).equals(panel.position));
 }
 menu.hide();menu.update();head.rotation.y=Math.PI;tick(0);tick(650);assert.ok(button.position.z>head.position.z);
 menu.show();menu.update();assert.ok(panel.position.z>head.position.z);menu.stop();tick(1000);assert.equal(button.visible,false);assert.equal(panel.visible,false);
});
test('recall waits for release, never moves a menu during selection, and new session uses its first pose',()=>{
 const {head,panel,menu,busy}=menuSetup();const original=panel.position.clone();busy(true);head.position.x+=3;menu.recenter();menu.update();assert.ok(panel.position.equals(original));busy(false);menu.update();assert.ok(panel.position.distanceTo(original)>2);
 menu.stop();head.position.set(9,9,9);menu.start();head.position.set(0,1.6,0);menu.update();assert.ok(panel.position.distanceTo(head.position)<1.7);
 head.rotation.x=Math.PI/2;menu.recenter();menu.update();upright(panel);assert.ok(panel.position.toArray().every(Number.isFinite));
});
