// POSIT looks along +Z; Three looks along -Z. Reflect both coordinate bases
// to preserve a proper rotation while keeping the printed marker facing the viewer.
export function writeMarkerMatrix(matrix,pose) {
  const r=pose.bestRotation,t=pose.bestTranslation;
  if(![...r.flat(),...t].every(Number.isFinite)||t[2]<=0)return false;
  matrix.set(r[0][0],r[0][1],-r[0][2],t[0],r[1][0],r[1][1],-r[1][2],t[1],-r[2][0],-r[2][1],r[2][2],-t[2],0,0,0,1);return true;
}
export function videoProjection(width,height,viewWidth,viewHeight) {
  const focal=.5*Math.max(width,height)/Math.tan(Math.PI/6),cover=Math.max(viewWidth/width,viewHeight/height);
  return {focal,fov:2*Math.atan(viewHeight/(2*focal*cover))*180/Math.PI,aspect:viewWidth/viewHeight};
}
