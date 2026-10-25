import test from 'node:test';
import assert from 'node:assert/strict';
import * as THREE from 'three';
import {createPalmMenuGesture} from '../public/prototipo-agustin20-chef-ar/shared/palm-menu-gesture.js';
import {traceFloorArc,createFloorTravel} from '../public/prototipo-agustin20-chef-ar/vr/floor-travel.js';
import {createLocomotion,canOccupy} from '../public/prototipo-agustin20-chef-ar/vr/locomotion.js';
import {reloadVRPage,recoverVRControls} from '../public/prototipo-agustin20-chef-ar/vr/session-recovery.js';
import {createConfirmation} from '../public/prototipo-agustin20-chef-ar/shared/confirm-action.js';

function palmHand(side){
 const hand=new THREE.Group();hand.joints={};hand.position.set(side==='right'?.3:-.3,1.15,-.4);
 function joint(name,x,y,z){const node=new THREE.Object3D();node.position.set(x,y,z);hand.add(node);hand.joints[name]=node;}
 const sign=side==='right'?1:-1;joint('wrist',0,0,0);joint('thumb-tip',-.09*sign,0,-.08);
 for(const [i,finger]of ['index','middle','ring','pinky'].entries()){const x=(-.03+i*.02)*sign;joint(`${finger}-finger-phalanx-proximal`,x,0,-.07);joint(`${finger}-finger-tip`,x,0,-.16);}
 return hand;
}
for(const side of ['left','right'])test(`open upward ${side} palm invokes once, re-arms, rejects pinch/downward/held hands`,()=>{
 const scene=new THREE.Scene(),head=new THREE.PerspectiveCamera(),hand=palmHand(side);head.position.y=1.65;scene.add(head,hand);let time=0,calls=0;
 const gesture=createPalmMenuGesture(THREE,{head,now:()=>time,onOpen:()=>calls++});const tick=ms=>{time+=ms;scene.updateMatrixWorld(true);return gesture.update(hand,side);};
 tick(0);tick(699);assert.equal(calls,0);tick(1);assert.equal(calls,1);tick(1000);assert.equal(calls,1);
 hand.rotation.z=Math.PI;tick(0);tick(400);hand.rotation.z=0;tick(0);tick(700);assert.equal(calls,2);
 gesture.reset();hand.joints['thumb-tip'].position.copy(hand.joints['index-finger-tip'].position);tick(0);tick(1000);assert.equal(calls,2);
 hand.joints['thumb-tip'].position.x=side==='right'?-.09:.09;gesture.reset();gesture.update(hand,side,true);time+=1000;gesture.update(hand,side,true);assert.equal(calls,2);
 hand.visible=false;tick(1000);assert.equal(calls,2);
});
test('palm gesture resets on incomplete joints and ignores ordinary forward pointing',()=>{
 const head=new THREE.PerspectiveCamera();head.position.y=1.65;const hand=palmHand('right');let time=0,calls=0;const g=createPalmMenuGesture(THREE,{head,now:()=>time,onOpen:()=>calls++});
 g.update(hand,'right');time=500;hand.joints['ring-finger-tip'].visible=false;g.update(hand,'right');time=1000;hand.joints['ring-finger-tip'].visible=true;g.update(hand,'right');time=1699;g.update(hand,'right');assert.equal(calls,0);time=1700;g.update(hand,'right');assert.equal(calls,1);
 g.reset();hand.rotation.x=Math.PI/2;time=2000;g.update(hand,'right');time=3000;g.update(hand,'right');assert.equal(calls,1);
});
function travelSetup(){const scene=new THREE.Scene(),rig=new THREE.Group(),head=new THREE.PerspectiveCamera(),source=new THREE.Group();scene.add(rig,source);rig.add(head);head.position.y=1.65;source.position.set(0,1.1,2.8);source.rotation.x=-.7;let time=0,travels=0,enabled=true;const renderer={xr:{getCamera:()=>head}};const locomotion=createLocomotion({rig,camera:head,renderer});const travel=createFloorTravel({scene,locomotion,enabled:()=>enabled,now:()=>time,onTravel:()=>travels++});return {scene,rig,head,source,travel,locomotion,time:t=>time=t,count:()=>travels,enable:v=>enabled=v};}
test('floor targeting rejects horizontal rays, blocked floor, room exterior and invalid coordinates',()=>{
 const {source,locomotion}=travelSetup();assert.equal(traceFloorArc(source).valid,true);source.rotation.x=0;assert.equal(traceFloorArc(source),null);
 source.position.set(0,1.1,.1);source.rotation.x=-.7;assert.equal(traceFloorArc(source).valid,false);source.position.x=10;assert.equal(traceFloorArc(source).valid,false);
 assert.equal(locomotion.teleportTo({x:NaN,z:0}),false);assert.equal(locomotion.teleportTo({x:0,z:-1}),false);
});
test('floor travel requires a deliberate hold and release; cancellation and tracking loss cannot teleport',()=>{
 const {travel,source,time,count,enable,head}=travelSetup();const hand={};assert.ok(travel.begin(hand,source));time(100);assert.equal(travel.finish(hand),false);assert.equal(count(),0);
 time(200);assert.ok(travel.begin(hand,source));time(600);travel.aim(hand,source);assert.ok(travel.finish(hand));assert.equal(count(),1);const p=head.getWorldPosition(new THREE.Vector3());assert.ok(canOccupy(p.x,p.z));assert.equal(p.y,1.65);
 time(700);travel.begin(hand,source);time(1100);travel.aim(hand,source);travel.cancel(hand);assert.equal(travel.finish(hand),false);assert.equal(count(),1);
 travel.begin(hand,source);enable(false);time(1600);assert.equal(travel.finish(hand),false);assert.equal(count(),1);
});
test('only one hand can own travel and an invalid last target never reuses a valid destination',()=>{
 const {travel,source,time,count}=travelSetup(),a={},b={};travel.begin(a,source);assert.equal(travel.begin(b,source),false);travel.cancel(b);assert.equal(travel.isActive(a),true);
 source.position.x=10;time(500);travel.aim(a,source);assert.equal(travel.finish(a),false);assert.equal(count(),0);
});
test('refresh requires confirmation, exits XR before reload, and reports exit failure',async()=>{
 let time=0,events=[],resolveEnd;const ended=new Promise(resolve=>resolveEnd=resolve);
 const guard=createConfirmation(()=>reloadVRPage({getSession:()=>({end:()=>{events.push('end');return ended;}}),reload:()=>events.push('reload')}),()=>{},()=>time);
 guard.press();assert.deepEqual(events,[]);time=400;guard.press();assert.deepEqual(events,['end']);resolveEnd();await ended;await Promise.resolve();assert.deepEqual(events,['end','reload']);
 let error=false;await reloadVRPage({getSession:()=>({end:async()=>{throw Error('exit');}}),reload:()=>assert.fail('must not reload'),onError:()=>error=true});assert.ok(error);
});
test('recover controls cancels pending actions and reopens UI without resetting gameplay',()=>{
 const events=[];recoverVRControls({input:{resetInput:()=>events.push('input')},pointer:{cancel:()=>events.push('pointer')},menu:{show:()=>events.push('menu')},confirmations:[{cancel:()=>events.push('confirmation')}]});assert.deepEqual(events,['pointer','input','confirmation','menu']);
});
test('floor arc snaps to the sink and the visible arc ends exactly at its destination',()=>{
 const source=new THREE.Group();source.position.set(1.7,1.1,-2.55);source.rotation.x=-Math.PI/2;const arc=traceFloorArc(source);assert.equal(arc.station,'sink');assert.ok(arc.points.at(-1).distanceTo(arc.destination)<1e-8);assert.ok(canOccupy(arc.destination.x,arc.destination.z));
});
