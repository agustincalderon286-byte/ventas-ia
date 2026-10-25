// Reload leaves immersive mode first. Recovery intentionally does not reset gameplay.
export async function reloadVRPage({getSession,reload,onError=()=>{}}){
  try{const session=getSession();if(session)await session.end();reload();return true;}
  catch(error){onError(error);return false;}
}
export function recoverVRControls({input,pointer,menu,confirmations=[]}){
  pointer?.cancel();input?.resetInput();for(const guard of confirmations)guard.cancel();menu.show();
}
