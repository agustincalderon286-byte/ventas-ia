// Joint geometry, not the system-reserved palm-and-pinch gesture.
export function createPalmMenuGesture(THREE,{head,onOpen=()=>{},now=()=>performance.now()}) {
  const slots=new Map(),w=new THREE.Vector3(),a=new THREE.Vector3(),b=new THREE.Vector3(),p=new THREE.Vector3(),tip=new THREE.Vector3(),eye=new THREE.Vector3(),forward=new THREE.Vector3(),normal=new THREE.Vector3();
  function eligible(hand,handedness){
    if(!head||!hand.visible||!['left','right'].includes(handedness))return false;
    const joints=hand.joints||{},joint=name=>joints[name]?.visible?joints[name]:null;
    if(!joint('wrist')||!joint('thumb-tip'))return false;
    joint('wrist').getWorldPosition(w);head.getWorldPosition(eye);head.getWorldDirection(forward);
    p.copy(w).sub(eye);
    if(p.length()<.2||p.length()>1||p.y>-.1||p.y<-.85||p.dot(forward)<.12)return false;
    for(const finger of ['index','middle','ring','pinky']){
      const base=joint(`${finger}-finger-phalanx-proximal`),end=joint(`${finger}-finger-tip`);
      if(!base||!end)return false;
      base.getWorldPosition(a);end.getWorldPosition(tip);
      if(tip.distanceTo(w)<a.distanceTo(w)*1.35||tip.distanceTo(a)<.035)return false;
      if(tip.clone().sub(a).normalize().dot(a.clone().sub(w).normalize())<.3)return false;
    }
    joint('index-finger-phalanx-proximal').getWorldPosition(a).sub(w);
    joint('pinky-finger-phalanx-proximal').getWorldPosition(b).sub(w);
    normal.crossVectors(a,b).normalize().multiplyScalar(handedness==='right'?-1:1);
    joint('thumb-tip').getWorldPosition(a);joint('index-finger-tip').getWorldPosition(b);
    return normal.y>.72&&a.distanceTo(b)>.045;
  }
  function update(hand,handedness,blocked=false){
    let state=slots.get(hand);if(!state){state={since:null,neutral:null,latched:false,anchor:new THREE.Vector3()};slots.set(hand,state);}
    const time=now(),valid=!blocked&&eligible(hand,handedness);
    if(!valid){state.since=null;state.neutral??=time;if(time-state.neutral>300)state.latched=false;return 0;}
    state.neutral=null;if(state.latched)return 0;
    if(state.since===null||state.anchor.distanceTo(w)>.1){state.since=time;state.anchor.copy(w);}
    const progress=Math.min(1,(time-state.since)/700);
    if(progress===1){state.latched=true;state.since=null;onOpen(hand);return 0;}
    return progress;
  }
  return {update,reset:()=>slots.clear()};
}
