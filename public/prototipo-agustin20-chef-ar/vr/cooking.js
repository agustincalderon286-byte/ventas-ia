import * as THREE from 'three';
export const BURNERS=[[-.235,-3.79],[-.235,-3.46],[.235,-3.79],[.235,-3.46]];
export const POT_SPECS={olla_6:{radius:.119,height:.105},olla_3:{radius:.09,height:.084},multipan:{radius:.137,height:.126}};
const UP=new THREE.Vector3(0,1,0);
export function createCooking({system,scene,kitchen,onChange=()=>{}}){
  const pots=new Map(),food=new Map(),worldPos=new THREE.Vector3(),worldQ=new THREE.Quaternion();let elapsed=0,lastReport='';
  const pixels=new Uint8Array(32*32*4);for(let y=0;y<32;y++)for(let x=0;x<32;x++){const i=(y*32+x)*4,d=Math.hypot((x-15.5)/15.5,(y-15.5)/15.5);pixels.set([240,244,247,Math.round(Math.max(0,1-d)**1.5*220)],i);}
  const mistMap=new THREE.DataTexture(pixels,32,32);mistMap.needsUpdate=true;
  for(const [id,spec]of Object.entries(POT_SPECS)){
    const item=system.pieces.get(id);if(!item)continue;
    const water=new THREE.Mesh(new THREE.CircleGeometry(spec.radius,40),new THREE.MeshPhysicalMaterial({color:0x7ab6c3,transparent:true,opacity:.78,metalness:.18,roughness:.13,clearcoat:1,side:THREE.DoubleSide}));
    water.rotation.x=-Math.PI/2;water.visible=false;water.userData.ignorePick=true;item.node.add(water);
    const steam=new THREE.Group();scene.add(steam);steam.visible=false;
    for(let i=0;i<14;i++){const puff=new THREE.Sprite(new THREE.SpriteMaterial({map:mistMap,color:0xbdc9d1,transparent:true,opacity:0,depthWrite:false}));puff.userData.ignorePick=true;steam.add(puff);}
    pots.set(id,{id,item,...spec,water,steam,fill:0,heat:0,burner:-1,ingredients:new Set()});
  }
  function potPose(p){p.item.node.getWorldPosition(worldPos);p.item.node.getWorldQuaternion(worldQ);const scale=p.item.node.getWorldScale(new THREE.Vector3()).x;return {position:worldPos.clone(),up:UP.clone().applyQuaternion(worldQ),scale:Math.abs(scale)};}
  function place(id,destination,index=1){const p=pots.get(id);if(!p)return false;if(p.item.owner!==null)system.release(p.item.owner,false);if(!p.item.out&&!system.extract(id))return false;
    if(destination==='stove'&&[...pots.values()].some(other=>other!==p&&other.burner===index)){onChange(snapshot(),'Ese quemador ya tiene una olla.');return false;}
    scene.attach(p.item.node);p.item.node.quaternion.identity();p.item.node.scale.setScalar(1);
    if(destination==='sink')p.item.node.position.set(1.65,.89,-3.64);
    else if(destination==='stove'){if(!BURNERS[index])return false;p.item.node.position.set(BURNERS[index][0],.982,BURNERS[index][1]);}
    else p.item.node.position.set(.8,1.045,-1.25);
    p.burner=destination==='stove'?index:-1;system.select(id);system.update();report(true);return true;
  }
  function release(item){const p=pots.get(item.id);
    if(p){const {position,up}=potPose(p);if(up.y<.65)return false;
      let nearest=-1,distance=.23;BURNERS.forEach(([x,z],i)=>{const d=Math.hypot(position.x-x,position.z-z);if(d<distance&&Math.abs(position.y-.982)<.24){nearest=i;distance=d;}});
      if(nearest>=0)return place(item.id,'stove',nearest);
      if(Math.hypot(position.x-1.65,position.z+3.64)<.21&&Math.abs(position.y-.89)<.23)return place(item.id,'sink');
      if(position.x>-.1&&position.x<1.1&&position.z>-1.9&&position.z<-.6&&Math.abs(position.y-1.04)<.18){position.y=1.045;scene.attach(item.node);item.node.position.copy(position);item.node.quaternion.identity();return true;}return false;
    }
    if(food.has(item.id)){
      const position=item.node.getWorldPosition(new THREE.Vector3());
      for(const pot of pots.values()){const {position:center}=potPose(pot);if(pot.item.out&&Math.hypot(position.x-center.x,position.z-center.z)<pot.radius+.055&&position.y>center.y-.02&&position.y<center.y+pot.height+.26){
        pot.item.node.attach(item.node);item.node.position.set((pot.ingredients.size%2?1:-1)*pot.radius*.3,.025+pot.fill*(pot.height-.035),0);pot.ingredients.add(item.id);report(true);return true;}}
      // Food rests on counters when released close to them, rather than disappearing through a surface.
      const y=position.z<-3.25?.945:position.z>-2&&position.z<-.5?1.065:null;
      if(y!==null&&Math.abs(position.y-y)<.3){scene.attach(item.node);item.node.position.y=y;return true;}
    }
    return false;
  }
  function registerFood(id,node){food.set(id,{node,original:[]});node.traverse(n=>{if(n.isMesh){n.material=n.material.clone();food.get(id).original.push([n.material,n.material.color.clone()]);}});}
  function snapshot(){return [...pots.values()].map(p=>({id:p.id,water:Math.round(p.fill*100),heat:Math.round(p.heat*100),steaming:p.steam.visible,burner:p.burner,ingredients:[...p.ingredients]}));}
  function report(force=false){const value=snapshot(),key=JSON.stringify(value);if(force||key!==lastReport){lastReport=key;onChange(value);}}
  function reset(){for(const p of pots.values()){p.fill=0;p.heat=0;p.burner=-1;p.ingredients.clear();p.water.visible=p.steam.visible=false;}for(const f of food.values())for(const [m,color]of f.original)m.color.copy(color);report(true);}
  function update(dt){dt=Math.min(.1,Math.max(0,dt));elapsed+=dt;let streamBottom=.889;const state=kitchen.state;
    for(const p of pots.values()){
      if(!p.item.out){p.fill=0;p.heat=0;p.burner=-1;p.water.visible=p.steam.visible=false;continue;}
      const {position,up,scale}=potPose(p),upright=up.y>.8,rim=position.y+p.height*scale;
      // Water only enters an upright, uncovered vessel actually intersecting the running stream.
      let covered=false;const lid=system.pieces.get(p.id==='olla_6'?'tapa_6':p.id==='olla_3'?'tapa_3':'multipan_tapa');
      if(lid?.out){const lp=lid.node.getWorldPosition(new THREE.Vector3());covered=Math.hypot(lp.x-position.x,lp.z-position.z)<p.radius*.8&&Math.abs(lp.y-rim)<.06;}
      if(state.water&&upright&&!covered&&Math.hypot(position.x-1.65,position.z+3.64)<p.radius*scale*.8&&rim<1.195&&rim>.88){p.fill=Math.min(1,p.fill+dt*.14);streamBottom=position.y+.018+p.fill*(p.height-.035)*scale;}
      if(up.y<.5)p.fill=Math.max(0,p.fill-dt*.45);
      p.burner=-1;BURNERS.forEach(([x,z],i)=>{if(upright&&Math.hypot(position.x-x,position.z-z)<.12&&Math.abs(position.y-.982)<.055)p.burner=i;});
      const heating=p.burner>=0&&state.burners[p.burner]&&p.fill>.01;
      p.heat=THREE.MathUtils.clamp(p.heat+dt*(heating?.22:-.3),0,1);
      p.water.visible=p.fill>.005;p.water.position.y=.018+p.fill*(p.height-.035)+Math.sin(elapsed*5)*.0008;
      p.steam.visible=p.heat>.22&&p.fill>.005&&upright;
      if(p.steam.visible){p.steam.position.set(position.x,rim,position.z);for(const [i,puff]of p.steam.children.entries()){const phase=(elapsed*.45+i/14)%1,a=i*2.4;puff.position.set(Math.cos(a+elapsed*.3)*p.radius*.6,phase*.55,Math.sin(a)*p.radius*.6);puff.scale.setScalar(.065+phase*.20);puff.material.opacity=Math.sin(phase*Math.PI)*p.heat*.8;}if(heating)p.fill=Math.max(0,p.fill-dt*.002);}
      for(const id of [...p.ingredients]){const f=food.get(id);if(!f||f.node.parent!==p.item.node){p.ingredients.delete(id);continue;}f.node.position.y=.025+p.fill*(p.height-.035);if(heating)for(const [m,color]of f.original)m.color.lerp(color.clone().multiplyScalar(.63),dt*.12);}
    }
    kitchen.setStreamBottom?.(streamBottom);report();
  }
  return {update,release,place,registerFood,snapshot,reset,pots};
}
