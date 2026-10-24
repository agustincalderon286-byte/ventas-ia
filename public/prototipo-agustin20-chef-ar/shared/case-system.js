// Shared scene logic: all home transforms remain relative to the moving case.
export const PIECE_ORDER = ['tapa_6','olla_6','tapa_3','olla_3','multipan_tapa','vaporera','multipan'];
const SUPPORTS = {tapa_6:'olla_6',tapa_3:'olla_3',vaporera:'multipan',multipan_tapa:'vaporera'};
const PARKED = [[-.72,.65,.8],[-.78,.5,1.25],[-.25,.65,.8],[-.25,.5,1.25],[.24,.65,.8],[.7,.55,.8],[.5,.5,1.3]];

export function createCaseSystem(THREE, root, world, onChange = () => {}) {
  const pieces = new Map(), holders = new Map(), extras = new Map();
  const rootHome = {parent:root.parent, position:root.position.clone(), quaternion:root.quaternion.clone(), scale:root.scale.clone()};
  root.traverse(node => {
    const id = node.userData.pieceId;
    if (id) pieces.set(id, {id, node, parent:node.parent, home:node.position.clone(), homeQ:node.quaternion.clone(), homeS:node.scale.clone(), label:node.userData.label, blockers:node.userData.blockers || [], out:false, owner:null});
    if (node.isMesh) {
      node.castShadow = true; node.receiveShadow = true;
      for (const m of Array.isArray(node.material) ? node.material : [node.material]) {
        if (m.name.includes('RP Acero')) { m.roughness=.26; m.metalness=1; }
        if (m.name.includes('Foam')) m.roughness=.96;
      }
    }
  });
  if (pieces.size !== 7) throw new Error('No se encontraron las siete piezas de la maleta.');
  const caseItem = {id:'case', node:root, label:'Maleta completa', owner:null};
  let selected = 'tapa_6';
  const ring = new THREE.Mesh(new THREE.RingGeometry(.13,.145,48),new THREE.MeshBasicMaterial({color:0x6cf6c0,side:THREE.DoubleSide,transparent:true,opacity:.8,depthWrite:false}));
  ring.rotation.x=-Math.PI/2; ring.visible=false; ring.userData.ignorePick=true;
  const item = id => id === 'case' ? caseItem : pieces.get(id) || extras.get(id);
  function update(message) {
    const p = pieces.get(selected);
    ring.visible = !!p?.out;
    if (p) { p.parent.add(ring); ring.position.copy(p.home); ring.position.y+=.012; }
    onChange({selected, message:message || `${item(selected).label} · ${p ? (p.out ? 'Fuera de la maleta' : 'En su lugar') : (selected==='case' ? 'Mueve la carcasa para llevar las piezas guardadas' : 'Producto independiente')}`, pieces:[...pieces.values()].map(p=>({id:p.id,label:p.label,out:p.out,held:p.owner!==null})), held:holders.size});
  }
  function select(id) { if (!item(id)) return; selected=id; update(); }
  function canTake(id) {
    const p=item(id); if (!p || p.owner!==null) return false;
    if (caseItem.owner!==null || (id==='case' && holders.size)) { update('Suelta primero lo que tienes en la mano.'); return false; }
    if (pieces.has(id) && !p.out) {
      const blocked=p.blockers.map(id=>pieces.get(id)).find(q=>q&&!q.out);
      if (blocked) { update('Retira primero: '+blocked.label+'.'); return false; }
    }
    return true;
  }
  function detach(p) { if (!pieces.has(p.id) || p.out) return; world.updateMatrixWorld(true); world.attach(p.node); p.out=true; }
  function beginGrab(id, owner, holder) {
    if (holders.has(owner) || !canTake(id)) return false;
    const p=item(id); select(id); detach(p); holder.updateWorldMatrix(true,false); holder.attach(p.node);
    p.owner=owner; holders.set(owner,p); update('Suelta para dejar la pieza. Acércala a su espacio para guardarla.'); return true;
  }
  function homeWorld(p) { p.parent.updateWorldMatrix(true,false); return p.parent.localToWorld(p.home.clone()); }
  function store(id) {
    const p=pieces.get(id); if (!p || !p.out || p.owner!==null) return false;
    const support=pieces.get(SUPPORTS[id]);
    if (support?.out) { update('Guarda primero: '+support.label+'.'); return false; }
    p.parent.add(p.node); p.node.position.copy(p.home); p.node.quaternion.copy(p.homeQ); p.node.scale.copy(p.homeS); p.out=false; update(); return true;
  }
  function trySnap(p) {
    const distance=p.node.getWorldPosition(new THREE.Vector3()).distanceTo(homeWorld(p));
    const scale=p.parent.getWorldScale(new THREE.Vector3()).x;
    if (distance < .18 * Math.abs(scale)) return store(p.id);
    return false;
  }
  function release(owner, snap=true) {
    const p=holders.get(owner); if (!p) return;
    world.updateWorldMatrix(true,false); world.attach(p.node); holders.delete(owner); p.owner=null;
    if (pieces.has(p.id) && snap && trySnap(p)) return;
    update();
  }
  function extract(id) {
    const p=pieces.get(id); if (!p || p.out || !canTake(id)) return false;
    detach(p); p.parent.updateWorldMatrix(true,false);
    const position=p.parent.localToWorld(new THREE.Vector3(...PARKED[PIECE_ORDER.indexOf(id)]));
    p.node.position.copy(world.worldToLocal(position)); select(id); update(); return true;
  }
  function reset() {
    for (const owner of [...holders.keys()]) release(owner,false);
    rootHome.parent.add(root); root.position.copy(rootHome.position); root.quaternion.copy(rootHome.quaternion); root.scale.copy(rootHome.scale);
    for (const p of [...pieces.values(),...extras.values()]) { p.parent.add(p.node); p.node.position.copy(p.home); p.node.quaternion.copy(p.homeQ); p.node.scale.copy(p.homeS); p.out=false; }
    update('Maleta y siete piezas en su posición inicial.');
  }
  function resolve(object) {
    while(object) { if (object.userData.ignorePick) return null; if (object.userData.pieceId) return object.userData.pieceId; if(object.userData.extraId)return object.userData.extraId; if(object===root)return 'case'; object=object.parent; }
    return null;
  }
  function meshes() { const result=[]; for(const p of [caseItem,...extras.values(),...pieces.values()].filter(p=>p.id==='case'||p.out||p.extra))p.node.traverse(n=>{if(n.isMesh&&!n.userData.ignorePick)result.push(n);}); return result; }
  function closest(point, maxDistance=.065) {
    const box=new THREE.Box3(), nearest=new THREE.Vector3(),center=new THREE.Vector3();
    let best=null, score=Infinity;
    for (const p of [...pieces.values(),...extras.values()]) {
      if(p.owner!==null)continue;
      box.setFromObject(p.node); box.clampPoint(point,nearest);
      const distance=point.distanceTo(nearest); const rank=distance*10+point.distanceTo(box.getCenter(center));
      if(distance<=maxDistance && rank<score){best=p.id;score=rank;}
    }
    if(best)return best;
    // Check the actual shell meshes, not the case bounding box that encloses empty air.
    for(const mesh of meshes()) if(resolve(mesh)==='case') {
      box.setFromObject(mesh); box.clampPoint(point,nearest);
      if(point.distanceTo(nearest)<=maxDistance)return 'case';
    }
    return null;
  }
  function registerExtra(id,node,label) { node.userData.extraId=id; extras.set(id,{id,node,label,extra:true,owner:null,out:true,parent:node.parent,home:node.position.clone(),homeQ:node.quaternion.clone(),homeS:node.scale.clone()}); }
  function releaseAll() { for (const owner of [...holders.keys()]) release(owner,false); }
  return {root,pieces,holders,registerExtra,item,select,get selected(){return selected;},update,beginGrab,release,releaseAll,extract,store,reset,resolve,meshes,closest,homeWorld,
    snapshot:()=>[...pieces.values()].map(p=>({id:p.id,out:p.out,position:p.node.position.toArray(),quaternion:p.node.quaternion.toArray()}))};
}
