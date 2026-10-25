import * as THREE from 'three';
import {RoundedBoxGeometry} from 'three/addons/geometries/RoundedBoxGeometry.js';
export function createFurniture({group,materials:m,actions,onChange=()=>{}}){
  const moving=[],food=[];let fridgeOpen=false,drawersOpen=false;
  function box(parent,w,h,d,mat,x,y,z){const n=new THREE.Mesh(new RoundedBoxGeometry(w,h,d,1,.003),mat);n.position.set(x,y,z);n.receiveShadow=true;parent.add(n);return n;}
  function action(mesh,fn){mesh.userData.action=fn;mesh.userData.holdMs=250;mesh.userData.nearRadius=.065;actions.push(mesh);}
  const inner=new THREE.MeshStandardMaterial({color:0xd7e4e4,roughness:.35});
  const fridge=new THREE.Group();group.add(fridge);
  box(fridge,1.15,2.65,.035,m.lacquer,2.84,1.325,-3.98);
  for(const x of [2.28,3.4])box(fridge,.035,2.65,.66,m.lacquer,x,1.325,-3.67);
  for(const y of [.1,2.65])box(fridge,1.15,.035,.66,m.lacquer,2.84,y,-3.67);
  box(fridge,1.06,2.48,.015,inner,2.84,1.36,-3.95);
  for(const y of [.45,.95,1.45,1.95])box(fridge,1.06,.024,.55,inner,2.84,y,-3.65);
  const lamp=new THREE.PointLight(0xd8edff,.55,1.7,2);lamp.position.set(2.84,2.36,-3.44);lamp.intensity=0;fridge.add(lamp);
  const doors=[];
  for(const side of [-1,1]){
    const pivot=new THREE.Group();pivot.position.set(2.84+side*.573,1.41,-3.32);fridge.add(pivot);
    const face=box(pivot,.559,2.48,.035,m.lacquer,-side*.286,0,0);
    const grip=box(pivot,.02,.6,.05,m.brass,-side*.49,-.11,.05);
    const joint={node:pivot,value:0,target:0,axis:'y',kind:'door',sign:side};moving.push(joint);doors.push(joint);
    action(face,()=>setFridge(!fridgeOpen));action(grip,()=>setFridge(!fridgeOpen));
  }
  const drawers=[];
  for(const [i,x]of [-1.8,-.9,.9,1.8].entries()){
    const drawer=new THREE.Group();drawer.position.set(x,.778,-3.337);group.add(drawer);
    const face=box(drawer,.878,.17,.025,m.wood,0,0,0),grip=box(drawer,.3,.018,.035,m.brass,0,.025,.04);
    box(drawer,.82,.015,.53,m.wood,0,-.072,-.26);
    for(const dx of [-.405,.405])box(drawer,.014,.13,.53,m.wood,dx,-.01,-.26);
    box(drawer,.82,.13,.014,m.wood,0,-.01,-.52);
    // Trays and cutlery stay with the drawer as it slides.
    box(drawer,.7,.015,.38,inner,0,-.052,-.25);
    for(let j=0;j<4;j++){box(drawer,.016,.012,.24,m.steel,-.21+j*.14,-.036,-.25);const bowl=new THREE.Mesh(new THREE.SphereGeometry(.028,12,8),m.steel);bowl.scale.set(.7,.13,1);bowl.position.set(-.21+j*.14,-.032,-.11);drawer.add(bowl);}
    const joint={node:drawer,value:0,target:0,axis:'z',origin:-3.337,kind:'drawer',index:i};moving.push(joint);drawers.push(joint);
    action(face,()=>setDrawer(i,joint.target===0));action(grip,()=>setDrawer(i,joint.target===0));
  }
  function notify(){onChange({fridge:fridgeOpen,drawers:drawers.map(d=>d.target>0)});}
  function setFridge(on){fridgeOpen=!!on;for(const d of doors)d.target=on?d.sign*1.85:0;lamp.intensity=fridgeOpen?.55:0;notify();}
  function setDrawer(i,on){if(!drawers[i])return;drawers[i].target=on?.42:0;drawersOpen=drawers.some(d=>d.target>0);notify();}
  const fruitMats={tomato:new THREE.MeshStandardMaterial({color:0xc93020,roughness:.4}),apple:new THREE.MeshStandardMaterial({color:0xa4bd38,roughness:.4}),carrot:new THREE.MeshStandardMaterial({color:0xe88923,roughness:.68}),leaf:new THREE.MeshStandardMaterial({color:0x3c6b28,roughness:.8})};
  function ingredient(type,index,x,y,z){const item=new THREE.Group();item.position.set(x,y,z);item.name=type+' '+index;fridge.add(item);
    if(type==='carrot'){const body=new THREE.Mesh(new THREE.ConeGeometry(.023,.14,12),fruitMats.carrot);body.rotation.z=-Math.PI/2;body.position.y=.022;item.add(body);}else{const body=new THREE.Mesh(new THREE.SphereGeometry(.04,16,12),fruitMats[type]);body.scale.set(1,type==='tomato'?.8:1,1);body.position.y=.04;item.add(body);}
    for(let k=0;k<3;k++){const leaf=new THREE.Mesh(new THREE.SphereGeometry(.018,8,6),fruitMats.leaf);leaf.scale.set(.35,.12,1);leaf.position.set(type==='carrot'?-.065:0,type==='carrot'?.04:.075,0);leaf.rotation.y=k*2;item.add(leaf);}
    item.userData.food=true;food.push({id:'food_'+type+'_'+index,node:item,label:({tomato:'Tomate',apple:'Manzana',carrot:'Zanahoria'})[type]+' '+(index+1),canTake:()=>Math.abs(doors[0].value)>.5||item.parent!==fridge});
  }
  for(let i=0;i<3;i++){ingredient('tomato',i,2.53+i*.27,1.454,-3.47);ingredient('apple',i,2.53+i*.27,.962,-3.47);ingredient('carrot',i,2.53+i*.27,.463,-3.47);}
  return {food,setFridge,setDrawer,toggleFridge:()=>setFridge(!fridgeOpen),toggleDrawers:()=>{const target=!drawersOpen;for(let i=0;i<4;i++)setDrawer(i,target);},
    update(dt){for(const joint of moving){const step=Math.min(dt,.05)*(joint.kind==='door'?1.8:.5);joint.value=THREE.MathUtils.clamp(joint.value+Math.sign(joint.target-joint.value)*step,Math.min(joint.value,joint.target),Math.max(joint.value,joint.target));if(joint.kind==='door')joint.node.rotation.y=joint.value;else joint.node.position.z=joint.origin+joint.value;}},
    get state(){return {fridge:fridgeOpen,drawers:drawers.map(d=>d.target>0)};}};
}
