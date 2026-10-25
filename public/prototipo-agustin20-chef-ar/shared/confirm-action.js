// Two distinct completed presses are required; passing through a control cannot reset the exhibit.
export function createConfirmation(action,onChange=()=>{},now=()=>performance.now()) {
  let until=0;
  function cancel(){until=0;onChange(false);}
  return {press(){if(until&&now()<until){cancel();action();return true;}until=now()+7000;onChange(true);return false;},cancel,update(){if(until&&now()>=until)cancel();},get pending(){return until>now();}};
}
