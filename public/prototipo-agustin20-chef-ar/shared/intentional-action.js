export function isVisible(object){for(let n=object;n;n=n.parent)if(!n.visible)return false;return true;}
export function createActionPress(){let press=null;
  return {begin(target,now){press=target?{target,started:now}:null;},cancel(){press=null;},get active(){return !!press;},release(target,now,minDuration=0){const pending=press;press=null;if(!pending||pending.target!==target||!isVisible(target)||now-pending.started<Math.max(minDuration,target.userData.holdMs||0))return false;target.userData.action?.();return true;}};
}
