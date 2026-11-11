// Network ancestry is parentUserId. Billing ownership and commission referrals
// do not confer visibility into peers, an upline or another branch.
export const canReadDownline=user=>Boolean(user?._id&&(
 ['owner','leader'].includes(user.accountType||'owner')||user.teamSupervisionEnabled===true
));
export const canManageTeamSeats=user=>Boolean(user?._id&&['owner','leader'].includes(user.accountType||'owner'));

export async function downlineMembers(user,users,{maxMembers=10000,maxDepth=64}={}){
 if(!canReadDownline(user))return [];
 const seen=new Set([String(user._id)]),members=[];
 let frontier=[user._id],depth=0;
 while(frontier.length){
  if(depth++>=maxDepth)throw Error('network_scope_limit');
  const children=await users.find({parentUserId:{$in:frontier}}).select('-passwordHash -passwordSalt').lean();
  const parents=new Set(frontier.map(String));
  const next=[];
  for(const child of children){
   if(!child?._id||!parents.has(String(child.parentUserId)))throw Error('network_scope_invalid');
   const id=String(child._id);
   if(seen.has(id))throw Error('network_scope_cycle');
   if(members.length>=maxMembers)throw Error('network_scope_limit');
   seen.add(id);members.push(child);next.push(child._id);
  }
  frontier=next;
 }
 return members;
}

export async function downlineScope(user,users){
 const members=await downlineMembers(user,users);
 return {members,query:{_id:{$in:members.map(m=>m._id)}},canManageSeats:canManageTeamSeats(user)};
}

export async function analysisViewerIds(user,users){
 if(!user?._id)return [];
 return [user._id,...(await downlineMembers(user,users)).map(m=>m._id)];
}
