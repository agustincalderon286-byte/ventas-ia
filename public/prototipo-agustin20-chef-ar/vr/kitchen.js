import * as THREE from 'three';
import {RoundedBoxGeometry} from 'three/addons/geometries/RoundedBoxGeometry.js';
import {RGBELoader} from 'three/addons/loaders/RGBELoader.js';

// All dimensions in metres. Shared materials and modest bevels keep the room practical for standalone VR.
export async function buildKitchen(scene,renderer) {
  const group=new THREE.Group();group.name='Cocina arquitectónica';scene.add(group);
  const loader=new THREE.TextureLoader(),base='../assets/kitchen/';
  async function maps(name,tile=1) {
    const [map,normalMap,roughnessMap]=await Promise.all(['Diffuse','nor_gl','Rough'].map(k=>loader.loadAsync(base+name+'_'+k+'.jpg')));
    for(const t of [map,normalMap,roughnessMap]){t.wrapS=t.wrapT=THREE.RepeatWrapping;t.repeat.setScalar(1/tile);t.anisotropy=Math.min(4,renderer.capabilities.getMaxAnisotropy());}
    map.colorSpace=THREE.SRGBColorSpace;return {map,normalMap,roughnessMap};
  }
  const [woodMaps,tileMaps]=await Promise.all([maps('american_walnut_veneer',1.3),maps('marble_01',3.2)]);
  const m={
    wood:new THREE.MeshStandardMaterial({...woodMaps,color:0xddd1c1,roughness:.85,normalScale:new THREE.Vector2(.16,.16)}),
    stone:new THREE.MeshPhysicalMaterial({color:0xd4d3ca,roughness:.25,clearcoat:.3,clearcoatRoughness:.35}),
    floor:new THREE.MeshStandardMaterial({...tileMaps,color:0xdfdcd6,roughness:.85,normalScale:new THREE.Vector2(.3,.3)}),
    wall:new THREE.MeshStandardMaterial({color:0xc9c3b7,roughness:.95}),
    ceiling:new THREE.MeshStandardMaterial({color:0xe8e4dc,roughness:1}),
    lacquer:new THREE.MeshStandardMaterial({color:0xb4aea0,roughness:.43}),
    black:new THREE.MeshStandardMaterial({color:0x16191a,roughness:.36}),
    shadow:new THREE.MeshStandardMaterial({color:0x171613,roughness:.95}),
    steel:new THREE.MeshStandardMaterial({color:0xbfc6c9,metalness:1,roughness:.29}),
    brass:new THREE.MeshStandardMaterial({color:0x938065,metalness:.85,roughness:.34}),
    glass:new THREE.MeshPhysicalMaterial({color:0x121c20,metalness:.16,roughness:.13,clearcoat:1}),
    porcelain:new THREE.MeshStandardMaterial({color:0xddd8c9,roughness:.24}),
    led:new THREE.MeshBasicMaterial({color:new THREE.Color(3.4,2.9,2.2),toneMapped:false}),
    daylight:new THREE.MeshBasicMaterial({color:new THREE.Color(2.2,2.5,2.7),toneMapped:false})
  };
  function uvMetres(g){const p=g.attributes.position,n=g.attributes.normal,uv=g.attributes.uv;for(let i=0;i<p.count;i++){const nx=Math.abs(n.getX(i)),ny=Math.abs(n.getY(i)),nz=Math.abs(n.getZ(i));uv.setXY(i,ny>nx&&ny>nz?p.getX(i):nx>nz?p.getZ(i):p.getX(i),ny>nx&&ny>nz?p.getZ(i):p.getY(i));}return g;}
  function box(w,h,d,mat,x,y,z,r=.004){const g=uvMetres(new RoundedBoxGeometry(w,h,d,1,Math.min(r,w/5,h/5,d/5)));const mesh=new THREE.Mesh(g,mat);mesh.position.set(x,y,z);mesh.castShadow=false;mesh.receiveShadow=true;group.add(mesh);return mesh;}
  function cyl(rt,rb,h,mat,x,y,z,segments=24){const mesh=new THREE.Mesh(new THREE.CylinderGeometry(rt,rb,h,segments),mat);mesh.position.set(x,y,z);mesh.castShadow=true;mesh.receiveShadow=true;group.add(mesh);return mesh;}
  function tube(points,r,mat){const curve=new THREE.CatmullRomCurve3(points.map(p=>new THREE.Vector3(...p)));const mesh=new THREE.Mesh(new THREE.TubeGeometry(curve,32,r,8,false),mat);mesh.castShadow=true;group.add(mesh);return mesh;}
  function handle(x,y,z,w=.28){box(w,.014,.025,m.brass,x,y,z,.006);box(.015,.015,.025,m.brass,x-w*.38,y,z-.018);box(.015,.015,.025,m.brass,x+w*.38,y,z-.018);}
  // Bounded room, skirting, ceiling and a broad daylight window.
  box(8,.08,9,m.floor,0,-.04,-.5);
  box(8,3.1,.15,m.wall,0,1.55,-4.1);box(.15,3.1,9,m.wall,4,1.55,-.5);box(8,.12,9,m.ceiling,0,3.16,-.5);
  box(.15,1,9,m.wall,-4,.5,-.5);box(.15,.38,9,m.wall,-4,2.91,-.5);
  box(.15,1.72,2.1,m.wall,-4,1.86,-3.95);box(.15,1.72,3.3,m.wall,-4,1.86,2.35);
  box(.06,1.65,3.6,m.daylight,-4.015,1.86,-1.1);
  box(.13,.07,3.72,m.black,-3.94,1.01,-1.1);box(.13,.07,3.72,m.black,-3.94,2.71,-1.1);
  for(const z of [-2.96,-1.1,.76])box(.13,1.72,.045,m.black,-3.94,1.86,z);
  box(.24,.04,3.9,m.stone,-3.86,1.0,-1.1);
  box(7.9,.095,.025,m.lacquer,0,.047,-4.0);box(.025,.095,8.9,m.lacquer,3.91,.047,-.5);
  // Tall pantry at left, refrigerator at right, paired inset doors with true shadow gaps.
  for(const x of [-2.84,2.84]){
    box(1.15,2.65,.66,m.shadow,x,1.325,-3.67);
    for(const dx of [-.286,.286]){box(.559,2.48,.03,x<0?m.wood:m.lacquer,x+dx,1.41,-3.32);box(.012,.6,.04,m.brass,x+dx+(dx<0?.21:-.21),1.3,-3.278);}
    box(1.13,.1,.56,m.black,x,.05,-3.66);
  }
  // Back counter at a realistic 92 cm, drawers and doors individually modelled.
  box(4.45,.78,.63,m.shadow,0,.48,-3.67);box(4.5,.035,.73,m.stone,0,.9075,-3.64);
  box(4.45,.84,.025,m.stone,0,1.35,-4.0);
  for(const x of [-1.8,-.9,.9,1.8]){
    box(.878,.46,.025,m.wood,x,.45,-3.337);box(.878,.17,.025,m.wood,x,.778,-3.337);handle(x,.8,-3.3,.3);handle(x,.63,-3.3,.3);
  }
  box(4.3,.1,.52,m.black,0,.05,-3.68);
  // Stainless oven, dark glazed door, rails, knobs and a cast-iron gas hob.
  box(.875,.79,.62,m.steel,0,.48,-3.64);box(.81,.54,.022,m.glass,0,.425,-3.313);
  box(.7,.35,.005,m.black,0,.43,-3.297);handle(0,.685,-3.26,.62);
  for(const x of [-.3,-.1,.1,.3]){const k=cyl(.027,.027,.03,m.steel,x,.8,-3.287);k.rotation.x=Math.PI/2;box(.003,.013,.003,m.black,x,.811,-3.268);}
  box(.84,.018,.59,m.steel,0,.934,-3.63);
  for(const x of [-.235,.235])for(const z of [-3.79,-3.46]){
    cyl(.065,.065,.018,m.black,x,.95,z);const ring=new THREE.Mesh(new THREE.TorusGeometry(.105,.008,6,24),m.black);ring.rotation.x=Math.PI/2;ring.position.set(x,.96,z);group.add(ring);
    box(.27,.018,.018,m.black,x,.969,z);box(.018,.018,.26,m.black,x,.969,z);
  }
  box(.92,.14,.53,m.steel,0,1.87,-3.73);box(.37,.85,.26,m.steel,0,2.355,-3.92);box(.83,.012,.4,m.black,0,1.79,-3.69);
  // Upper doors, subtle warm task strips and switch plates.
  for(const x of [-1.79,-.91,.91,1.79]){box(.86,.83,.34,m.lacquer,x,2.18,-3.85);box(.82,.79,.018,m.lacquer,x,2.18,-3.67);box(.6,.008,.012,m.led,x,1.758,-3.62);}
  for(const x of [-1.55,1.55]){box(.13,.085,.012,m.lacquer,x,1.2,-3.974);for(const dx of [-.029,.029]){box(.015,.025,.004,m.black,x+dx,1.2,-3.965);}}
  // Waterfall island, real countertop thickness and recessed plinth.
  const exhibitZ=-1.25;
  box(3.48,.82,1.14,m.shadow,0,.55,exhibitZ);
  box(3.76,.04,1.42,m.stone,0,1.02,exhibitZ);
  for(const x of [-1.855,1.855])box(.05,1.0,1.42,m.stone,x,.5,exhibitZ);
  for(const x of [-1.38,-.46,.46,1.38]){box(.9,.77,.025,m.wood,x,.575,-.667);box(.9,.77,.025,m.wood,x,.575,-1.835);}
  box(3.35,.15,1.0,m.black,0,.075,exhibitZ);box(3.4,.018,.025,m.black,0,.975,-.65);
  // Novel sits on a quiet stone trivet instead of the glowing game pedestal.
  cyl(.31,.31,.035,m.stone,1.32,1.0625,exhibitZ,48);
  // Pendant shades with inner diffusers, not gold balls.
  for(const x of [-1.15,1.15]){cyl(.006,.006,.78,m.black,x,2.73,exhibitZ,8);cyl(.045,.045,.025,m.black,x,3.087,exhibitZ);cyl(.07,.23,.19,m.black,x,2.28,exhibitZ,32);cyl(.207,.207,.008,m.led,x,2.182,exhibitZ,32);}
  // A sink and curved mixer tap on the right return; dark inset bowl avoids a solid fake basin.
  box(.75,.02,.42,m.steel,1.65,.932,-3.61);box(.65,.012,.32,m.shadow,1.65,.944,-3.61);box(.59,.006,.265,m.steel,1.65,.95,-3.61);
  tube([[1.65,.94,-3.87],[1.65,1.25,-3.87],[1.65,1.34,-3.78],[1.65,1.29,-3.64],[1.65,1.2,-3.64]],.016,m.steel);cyl(.026,.026,.06,m.steel,1.84,.965,-3.86);
  // Everyday details give the room scale: cutting boards, linen, stacked dishes and a ceramic vase.
  const board=box(.3,.42,.022,m.wood,-1.55,1.15,-3.94,.015);board.rotation.x=-.13;board.rotation.z=.13;
  const small=box(.23,.32,.019,m.wood,-1.38,1.10,-3.91,.012);small.rotation.z=-.08;
  for(let i=0;i<4;i++)cyl(.105,.09,.012,m.porcelain,-.98,.936+i*.014,-3.56,32);
  const vasePoints=[[0,0],[.09,0],[.10,.04],[.09,.18],[.05,.29],[.045,.31]].map(p=>new THREE.Vector2(...p));const vase=new THREE.Mesh(new THREE.LatheGeometry(vasePoints,24),m.porcelain);vase.position.set(-2,.925,-3.7);vase.castShadow=true;group.add(vase);
  const leafMat=new THREE.MeshStandardMaterial({color:0x3b5034,roughness:.8,side:THREE.DoubleSide});
  for(let i=0;i<7;i++){const a=i*2.4,dx=Math.cos(a)*.15,dz=Math.sin(a)*.12; tube([[-2,1.20,-3.7],[-2+dx*.4,1.48,-3.7+dz*.5],[-2+dx,1.65+i*.018,-3.7+dz]],.0025,leafMat);for(let k=0;k<3;k++){const leaf=new THREE.Mesh(new THREE.SphereGeometry(1,8,6),leafMat);leaf.scale.set(.022,.003,.07);leaf.position.set(-2+dx*(.5+k*.2),1.43+k*.08,-3.7+dz*(.5+k*.2));leaf.rotation.set(.5,a+k,.3);group.add(leaf);}}
  // Soft contact shading is static and cheap in both eyes; only the sun casts a live shadow.
  const c=document.createElement('canvas');c.width=c.height=128;const ctx=c.getContext('2d'),grad=ctx.createRadialGradient(64,64,12,64,64,64);grad.addColorStop(0,'rgba(0,0,0,.38)');grad.addColorStop(.6,'rgba(0,0,0,.18)');grad.addColorStop(1,'rgba(0,0,0,0)');ctx.fillStyle=grad;ctx.fillRect(0,0,128,128);
  const shadowTex=new THREE.CanvasTexture(c);for(const [x,z,w,d]of [[0,-1.25,4.5,2.2],[0,-3.6,7.4,1.6]]){const mesh=new THREE.Mesh(new THREE.PlaneGeometry(w,d),new THREE.MeshBasicMaterial({map:shadowTex,transparent:true,depthWrite:false}));mesh.rotation.x=-Math.PI/2;mesh.position.set(x,.002,z);group.add(mesh);}
  scene.add(new THREE.HemisphereLight(0xdfe8ee,0x65594c,.65));
  const sun=new THREE.DirectionalLight(0xfff4df,2.3);sun.position.set(-3.7,2.65,-.2);sun.target.position.set(0,0,-2);sun.castShadow=true;sun.shadow.mapSize.set(2048,2048);Object.assign(sun.shadow.camera,{left:-5,right:5,top:4,bottom:-4,near:.1,far:12});sun.shadow.bias=-.00002;sun.shadow.normalBias=.003;sun.shadow.radius=4;sun.shadow.blurSamples=8;scene.add(sun,sun.target);
  const fill=new THREE.PointLight(0xfff2e0,9,7,2);fill.position.set(0,2.65,-1.25);scene.add(fill);
  const task=new THREE.PointLight(0xffebcf,3,3,2);task.position.set(0,1.75,-3.35);scene.add(task);
  const hdr=await new RGBELoader().loadAsync(base+'kiara_interior_1k.hdr');const pmrem=new THREE.PMREMGenerator(renderer);scene.environment=pmrem.fromEquirectangular(hdr).texture;hdr.dispose();
  // One local probe gives polished cookware reflections of THIS kitchen, including its window and lamps.
  const target=new THREE.WebGLCubeRenderTarget(256,{type:THREE.HalfFloatType});const probe=new THREE.CubeCamera(.1,20,target);probe.position.set(0,1.6,-1.25);probe.update(renderer,scene);const previous=scene.environment;scene.environment=pmrem.fromCubemap(target.texture).texture;previous.dispose();target.dispose();pmrem.dispose();
  document.body.dataset.kitchen='pbr-v2';return {exhibitZ,group};
}
