import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import * as THREE from 'three';
import {GLTFLoader} from 'three/addons/loaders/GLTFLoader.js';
import {createCaseSystem,PIECE_ORDER} from '../public/prototipo-agustin20-chef-ar/shared/case-system.js';
// Geometry/transform tests do not upload textures to a GPU; browser QA checks rendering.
const bytes=fs.readFileSync(new URL('../public/prototipo-agustin20-chef-ar/assets/maleta-royal-prestige-ar-v3.glb',import.meta.url));
globalThis.self=globalThis;globalThis.createImageBitmap=async()=>({width:1,height:1,close(){}});
const gltf=await new GLTFLoader().parseAsync(bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength),'');
function setup() {
  const scene=new THREE.Scene(),world=new THREE.Group();scene.add(world);
  const root=gltf.scene.clone(true);world.add(root);root.position.set(-.35,1.045,-1.25);world.updateMatrixWorld(true);
  const messages=[];const system=createCaseSystem(THREE,root,world,s=>messages.push(s.message));
  return {scene,world,root,system,messages};
}
function close(a,b,eps=1e-6){assert.ok(a.distanceTo(b)<eps,`${a.toArray()} != ${b.toArray()}`);}
function home(p){close(p.node.position,p.home);assert.ok(p.node.quaternion.angleTo(p.homeQ)<1e-6);assert.equal(p.node.parent,p.parent);assert.equal(p.out,false);}

test('all original parts can leave and return in physically valid order',()=>{
  const {system}=setup();assert.equal(system.pieces.size,7);
  assert.equal(system.extract('olla_6'),false);
  for(const id of PIECE_ORDER)assert.equal(system.extract(id),true,id);
  assert.equal(system.store('tapa_6'),false);assert.equal(system.store('multipan_tapa'),false);
  for(const id of ['olla_6','olla_3','multipan','vaporera','tapa_6','tapa_3','multipan_tapa'])assert.equal(system.store(id),true,id);
  for(const p of system.pieces.values())home(p);
});

test('moving and rotating the case carries stored pots but leaves extracted parts in the world',()=>{
  const {scene,world,root,system}=setup();system.extract('tapa_6');
  const p=system.pieces.get('tapa_6'),original=p.node.getWorldPosition(new THREE.Vector3());
  const body=system.pieces.get('olla_6'),before=body.node.getWorldPosition(new THREE.Vector3());
  const grip=new THREE.Group();scene.add(grip);grip.position.set(.3,1.2,.1);grip.rotation.y=.3;scene.updateMatrixWorld(true);
  const rootBefore=root.getWorldPosition(new THREE.Vector3());assert.equal(system.beginGrab('case','right',grip),true);close(root.getWorldPosition(new THREE.Vector3()),rootBefore);
  grip.position.x+=.5;grip.rotation.y+=.8;scene.updateMatrixWorld(true);
  close(p.node.getWorldPosition(new THREE.Vector3()),original);assert.ok(body.node.getWorldPosition(new THREE.Vector3()).distanceTo(before)>.2);
  system.release('right',false);assert.equal(root.parent,world);assert.equal(system.store('tapa_6'),true);home(p);
});

test('two hands cannot own the same object and case grabs cannot conflict with a held piece',()=>{
  const {scene,system}=setup();const a=new THREE.Group(),b=new THREE.Group();scene.add(a,b);
  assert.equal(system.beginGrab('tapa_6','left',a),true);
  assert.equal(system.beginGrab('tapa_6','right',b),false);
  assert.equal(system.beginGrab('case','right',b),false);
  assert.equal(system.beginGrab('tapa_3','right',b),true);
  system.releaseAll();assert.equal(system.holders.size,0);
  assert.equal(system.beginGrab('case','left',a),true);
  assert.equal(system.beginGrab('tapa_6','right',b),false);
  system.releaseAll();assert.equal(system.holders.size,0);
});

test('snap uses the new compartment location after translating, rotating and scaling the case',()=>{
  const {scene,world,root,system}=setup();system.extract('tapa_6');root.position.set(2,.4,-2);root.rotation.set(.2,.7,0);root.scale.setScalar(.8);
  world.rotation.x=Math.PI/2;world.scale.setScalar(.6);world.position.set(.1,.2,.3);scene.updateMatrixWorld(true);
  const hand=new THREE.Group();scene.add(hand);const p=system.pieces.get('tapa_6');
  assert.equal(system.beginGrab(p.id,'left',hand),true);
  p.node.position.copy(hand.worldToLocal(system.homeWorld(p).add(new THREE.Vector3(.02,0,.01))));
  scene.updateMatrixWorld(true);system.release('left');home(p);
});

test('releasing far from the compartment retains pose; reset recovers objects and Novel',()=>{
  const {scene,root,system}=setup(),grip=new THREE.Group();scene.add(grip);
  const novel=new THREE.Group();novel.position.set(1,2,3);scene.add(novel);system.registerExtra('novel',novel,'Novel original');
  system.beginGrab('tapa_6','left',grip);grip.position.set(3,4,5);scene.updateMatrixWorld(true);
  const p=system.pieces.get('tapa_6'),before=p.node.getWorldPosition(new THREE.Vector3());system.release('left');close(before,p.node.getWorldPosition(new THREE.Vector3()));assert.equal(p.out,true);
  system.beginGrab('novel','left',grip);grip.position.set(6,4,5);scene.updateMatrixWorld(true);system.reset();
  for(const p of system.pieces.values())home(p);close(novel.position,new THREE.Vector3(1,2,3));close(root.position,new THREE.Vector3(-.35,1.045,-1.25));assert.equal(system.holders.size,0);
});

const {installXRHands}=await import('../public/prototipo-agustin20-chef-ar/shared/xr-pieces.js');
function xrHarness(scene,system,options={}) {
  const controllers=[new THREE.Group(),new THREE.Group()],grips=[new THREE.Group(),new THREE.Group()],hands=[new THREE.Group(),new THREE.Group()];
  for(const hand of hands){hand.visible=false;hand.joints={};for(const name of ['wrist','thumb-tip','index-finger-tip']){const joint=new THREE.Group();hand.add(joint);hand.joints[name]=joint;}}
  const xr=new THREE.EventDispatcher();Object.assign(xr,{isPresenting:true,getController:i=>controllers[i],getControllerGrip:i=>grips[i],getHand:i=>hands[i]});
  const input=installXRHands(THREE,{renderer:{xr},scene,system,handModelFactory:{createHandModel:()=>new THREE.Group()},...options});
  return {controllers,grips,hands,xr,input};
}

test('Quest controller trigger grabs the ray-selected lid and session exit releases it safely',()=>{
  const {scene,system}=setup(),{controllers,grips,xr,input}=xrHarness(scene,system);
  const p=system.pieces.get('tapa_6'),center=new THREE.Box3().setFromObject(p.node).getCenter(new THREE.Vector3());
  controllers[0].position.copy(center).add(new THREE.Vector3(0,.7,0));controllers[0].lookAt(center);controllers[0].rotateY(Math.PI);grips[0].position.copy(controllers[0].position);scene.updateMatrixWorld(true);
  controllers[0].dispatchEvent({type:'connected',data:{targetRayMode:'tracked-pointer'}});
  controllers[0].dispatchEvent({type:'selectstart'});assert.equal(system.holders.get(controllers[0])?.id,'tapa_6');
  grips[0].position.x+=.5;grips[0].rotation.z=.4;scene.updateMatrixWorld(true);const pose=p.node.getWorldPosition(new THREE.Vector3());
  xr.dispatchEvent({type:'sessionend'});assert.equal(system.holders.size,0);close(p.node.getWorldPosition(new THREE.Vector3()),pose);assert.equal(p.out,true);
});

test('Quest hand pinch picks an individual lid, follows wrist rotation, and tracking loss releases it',()=>{
  const {scene,system}=setup(),{hands,input}=xrHarness(scene,system),hand=hands[0];hand.visible=true;
  const p=system.pieces.get('tapa_6'),center=new THREE.Box3().setFromObject(p.node).getCenter(new THREE.Vector3());
  hand.joints['thumb-tip'].position.copy(center).add(new THREE.Vector3(-.008,0,0));hand.joints['index-finger-tip'].position.copy(center).add(new THREE.Vector3(.008,0,0));
  scene.updateMatrixWorld(true);input.update();assert.equal(system.holders.get(hand)?.id,'tapa_6');
  const before=p.node.getWorldQuaternion(new THREE.Quaternion());hand.joints.wrist.rotation.y=.8;
  hand.joints['thumb-tip'].position.x+=.4;hand.joints['index-finger-tip'].position.x+=.4;scene.updateMatrixWorld(true);input.update();scene.updateMatrixWorld(true);
  assert.ok(before.angleTo(p.node.getWorldQuaternion(new THREE.Quaternion()))>.7);
  hand.visible=false;input.update();assert.equal(system.holders.size,0);assert.equal(p.out,true);
});

const {installPiecePointer}=await import('../public/prototipo-agustin20-chef-ar/shared/touch-pieces.js');
test('touch rotation changes only the selected lid and never enables orbit controls during camera AR',()=>{
  const {scene,world,system}=setup(),p=system.pieces.get('tapa_6');
  const center=new THREE.Box3().setFromObject(p.node).getCenter(new THREE.Vector3());
  const camera=new THREE.PerspectiveCamera(45,1000/800,.01,100);camera.position.copy(center).add(new THREE.Vector3(0,.8,.4));camera.lookAt(center);camera.updateMatrixWorld(true);
  const handlers={},captures=new Set(),canvas={getBoundingClientRect:()=>({left:0,top:0,width:1000,height:800}),addEventListener:(name,fn)=>{handlers[name]=fn;},setPointerCapture:id=>captures.add(id),hasPointerCapture:id=>captures.has(id),releasePointerCapture:id=>captures.delete(id)};
  const controls={enabled:false};const others=system.snapshot().filter(x=>x.id!==p.id);
  installPiecePointer(THREE,{canvas,camera,system,world,controls,canInteract:()=>true,mode:()=> 'rotate'});
  const event=(x,y)=>({clientX:x,clientY:y,pointerId:1,isPrimary:true,button:0,stopImmediatePropagation(){}});
  handlers.pointerdown(event(500,400));handlers.pointermove(event(580,410));scene.updateMatrixWorld(true);handlers.pointerup(event(580,410));
  assert.equal(system.selected,p.id);assert.equal(p.out,true);assert.ok(p.node.quaternion.angleTo(p.homeQ)>.5);assert.equal(controls.enabled,false);assert.deepEqual(system.snapshot().filter(x=>x.id!==p.id),others);assert.equal(captures.size,0);
});

const {createCaseControls}=await import('../public/prototipo-agustin20-chef-ar/shared/case-controls.js');
test('real GLB lid closes and reopens exactly, LEDs switch and held objects prevent hinge motion',()=>{
  const {scene,root,system}=setup(),lid=root.getObjectByName('Tapa_articulada'),opened=lid.quaternion.clone(),mechanics=createCaseControls(THREE,system);
  assert.equal(mechanics.setOpen(false),true);for(let i=0;i<100;i++)mechanics.update(.016);
  assert.equal(mechanics.angle,0);assert.equal(mechanics.canAccess(),false);assert.ok(lid.quaternion.angleTo(new THREE.Quaternion())<1e-5);
  mechanics.setLights(false);root.traverse(n=>{if(n.isMesh&&n.material.name.includes('LED'))assert.equal(n.material.emissiveIntensity,0);});
  mechanics.setOpen(true);for(let i=0;i<100;i++)mechanics.update(.016);assert.ok(lid.quaternion.angleTo(opened)<1e-6);
  const grip=new THREE.Group();scene.add(grip);system.beginGrab('tapa_6','left',grip);assert.equal(mechanics.setOpen(false),false);assert.equal(mechanics.target,108);
});
test('closed case prevents extraction and storage without losing the extracted piece',()=>{
  const scene=new THREE.Scene(),root=gltf.scene.clone(true);scene.add(root);let access=true;
  const system=createCaseSystem(THREE,root,scene,()=>{},{canAccess:()=>access});
  system.extract('tapa_6');access=false;assert.equal(system.store('tapa_6'),false);assert.equal(system.pieces.get('tapa_6').out,true);assert.equal(system.extract('tapa_3'),false);
  access=true;assert.equal(system.store('tapa_6'),true);
});


test('Quest panel activation happens on deliberate release; hidden UI and passing hands cannot reset',()=>{
 const {scene,system}=setup();let time=100,calls=0;
 const button=new THREE.Mesh(new THREE.PlaneGeometry(.4,.2),new THREE.MeshBasicMaterial({side:THREE.DoubleSide}));button.position.set(2,1.5,-1);button.userData.action=()=>calls++;button.userData.rayOnly=true;scene.add(button);
 const {controllers,hands,input}=xrHarness(scene,system,{buttons:[button],now:()=>time});
 controllers[0].position.set(2,1.5,0);scene.updateMatrixWorld(true);controllers[0].dispatchEvent({type:'connected',data:{hand:{},targetRayMode:'tracked-pointer'}});
 controllers[0].dispatchEvent({type:'selectstart'});assert.equal(calls,0);time+=100;controllers[0].dispatchEvent({type:'selectend'});assert.equal(calls,0);
 controllers[0].dispatchEvent({type:'selectstart'});time+=350;controllers[0].dispatchEvent({type:'selectend'});assert.equal(calls,1);
 button.visible=false;controllers[0].dispatchEvent({type:'selectstart'});time+=350;controllers[0].dispatchEvent({type:'selectend'});assert.equal(calls,1);
 button.visible=true;const hand=hands[0];hand.visible=true;hand.joints['thumb-tip'].position.set(1.995,1.5,-1);hand.joints['index-finger-tip'].position.set(2.005,1.5,-1);scene.updateMatrixWorld(true);input.update();time+=400;hand.joints['index-finger-tip'].position.x+=.1;scene.updateMatrixWorld(true);input.update();assert.equal(calls,1);
});
