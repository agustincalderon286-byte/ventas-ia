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
test('hands-only menu is recalled after teleport and turning, without shifting during a press',()=>{
 const scene=new THREE.Scene(),head=new THREE.PerspectiveCamera(),panel=new THREE.Group(),button=new THREE.Object3D();scene.add(head,panel,button);head.position.set(0,1.6,0);let busy=false;
 const menu=createHandMenu({panel,button,head,isBusy:()=>busy});menu.start();scene.updateMatrixWorld(true);const initial=panel.position.clone();
 head.position.set(1.65,1.6,-2.68);menu.recenter();menu.update();assert.ok(panel.position.distanceTo(head.position)<1.8);assert.ok(panel.position.distanceTo(initial)>2);
 menu.hide();head.rotation.y=Math.PI;menu.update();assert.ok(button.position.z>head.position.z);menu.show();menu.update();assert.ok(panel.position.z>head.position.z);
 busy=true;const frozen=panel.position.clone();head.position.x+=3;menu.recenter();menu.update();assert.ok(panel.position.equals(frozen));busy=false;menu.update();assert.ok(panel.position.distanceTo(frozen)>2);menu.stop();assert.equal(button.visible,false);assert.equal(panel.visible,false);
});
