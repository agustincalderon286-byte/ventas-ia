import test from 'node:test';
import assert from 'node:assert/strict';
import * as THREE from 'three';
import {createActionPress} from '../public/prototipo-agustin20-chef-ar/shared/intentional-action.js';
import {createConfirmation} from '../public/prototipo-agustin20-chef-ar/shared/confirm-action.js';
import {createLocomotion,canOccupy,STATIONS} from '../public/prototipo-agustin20-chef-ar/vr/locomotion.js';
test('actions require press and release on the same visible target, including hold time',()=>{
 let count=0;const parent=new THREE.Group(),a=new THREE.Object3D(),b=new THREE.Object3D();parent.add(a,b);a.userData.action=()=>count++;a.userData.holdMs=350;const press=createActionPress();
 press.begin(a,10);assert.equal(count,0);press.release(a,200);assert.equal(count,0);
 press.begin(a,300);press.release(b,900);assert.equal(count,0);
 press.begin(a,1000);parent.visible=false;press.release(a,1500);assert.equal(count,0);
 parent.visible=true;press.begin(a,2000);press.release(a,2400);assert.equal(count,1);press.release(a,2900);assert.equal(count,1);
 press.begin(a,3000);press.cancel();press.release(a,3500);assert.equal(count,1);
});
test('reset needs two completed actions and expiration cancels confirmation',()=>{
 let time=100,count=0;const guard=createConfirmation(()=>count++,()=>{},()=>time);
 assert.equal(guard.press(),false);assert.equal(count,0);time=500;assert.equal(guard.press(),true);assert.equal(count,1);
 guard.press();time=8000;guard.update();assert.equal(guard.pending,false);assert.equal(guard.press(),false);assert.equal(count,1);guard.cancel();assert.equal(guard.pending,false);
});
function harness(){const scene=new THREE.Scene(),rig=new THREE.Group(),camera=new THREE.PerspectiveCamera();scene.add(rig);rig.add(camera);camera.position.set(.3,1.65,.2);let sources=[];const renderer={xr:{isPresenting:true,getCamera:()=>camera,getSession:()=>({inputSources:sources})}};scene.updateMatrixWorld(true);return {rig,camera,scene,move:createLocomotion({rig,camera,renderer}),sources};}
test('station teleport positions actual headset, and snap turn keeps head position fixed',()=>{
 const {rig,camera,scene,move}=harness();
 for(const id of Object.keys(STATIONS)){assert.ok(move.teleport(id));const p=camera.getWorldPosition(new THREE.Vector3());assert.ok(Math.abs(p.x-STATIONS[id].x)<1e-6);assert.ok(Math.abs(p.z-STATIONS[id].z)<1e-6);assert.equal(p.y,1.65);}
 const before=camera.getWorldPosition(new THREE.Vector3());move.turn(Math.PI/6);assert.ok(camera.getWorldPosition(new THREE.Vector3()).distanceTo(before)<1e-6);move.reset();assert.equal(rig.position.length(),0);
});
test('joystick movement stops before the island and cabinets; right stick snaps once until released',()=>{
 const {camera,move,sources,rig}=harness();sources.push({handedness:'left',gamepad:{axes:[0,0,0,-1]}},{handedness:'right',gamepad:{axes:[0,0,0,0]}});
 for(let i=0;i<300;i++)move.update(.016);const p=camera.getWorldPosition(new THREE.Vector3());assert.ok(p.z>=-.32);assert.ok(canOccupy(p.x,p.z));assert.equal(canOccupy(0,-1.25),false);assert.equal(canOccupy(0,-3.6),false);
 sources[0].gamepad.axes[3]=0;sources[1].gamepad.axes[2]=1;move.update(.016);const yaw=rig.rotation.y;move.update(.016);assert.equal(rig.rotation.y,yaw);sources[1].gamepad.axes[2]=0;move.update(.016);sources[1].gamepad.axes[2]=1;move.update(.016);assert.ok(Math.abs(rig.rotation.y-2*yaw)<1e-6);
});
