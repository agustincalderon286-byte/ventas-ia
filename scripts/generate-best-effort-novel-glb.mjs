import fs from "fs";
import path from "path";

const ROOT = "/Users/monse/Documents/New project/public/prototipo-catalog-novel-ar";
const ASSETS = path.join(ROOT, "assets");
const GLB_PATH = path.join(ASSETS, "novel-best-effort.glb");
const REFERENCE_PATH = path.join(ASSETS, "catalog-reference.png");
const COVER_PATH = path.join(ASSETS, "catalog-product-cover.webp");
const PREVIEW_SVG_PATH = path.join(ASSETS, "novel-best-effort-preview.svg");

const CATALOG_PREVIEW_SOURCE = "/Users/monse/Documents/New project/tmp_catalog_preview/catalog-preview.png";
const COVER_SOURCE =
  "/Users/monse/Documents/New project/public/prototipo-agustin20-chef-ar/assets/cover_novel.webp";

fs.mkdirSync(ASSETS, { recursive: true });

function vec3(x = 0, y = 0, z = 0) {
  return [x, y, z];
}

function add(a, b) {
  return [a[0] + b[0], a[1] + b[1], a[2] + b[2]];
}

function sub(a, b) {
  return [a[0] - b[0], a[1] - b[1], a[2] - b[2]];
}

function scale(v, s) {
  return [v[0] * s, v[1] * s, v[2] * s];
}

function cross(a, b) {
  return [
    a[1] * b[2] - a[2] * b[1],
    a[2] * b[0] - a[0] * b[2],
    a[0] * b[1] - a[1] * b[0],
  ];
}

function dot(a, b) {
  return a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
}

function length(v) {
  return Math.hypot(v[0], v[1], v[2]);
}

function normalize(v) {
  const len = length(v);
  if (!len) return [0, 1, 0];
  return [v[0] / len, v[1] / len, v[2] / len];
}

function average3(a, b, c) {
  return [(a[0] + b[0] + c[0]) / 3, (a[1] + b[1] + c[1]) / 3, (a[2] + b[2] + c[2]) / 3];
}

function createMesh(name, material) {
  return {
    name,
    material,
    positions: [],
    normals: [],
  };
}

function pushTriangle(mesh, a, b, c, outwardHint = null) {
  let normal = normalize(cross(sub(b, a), sub(c, a)));
  if (outwardHint && dot(normal, outwardHint) < 0) {
    normal = scale(normal, -1);
    const tmp = b;
    b = c;
    c = tmp;
  }

  mesh.positions.push(...a, ...b, ...c);
  mesh.normals.push(...normal, ...normal, ...normal);
}

function pushLatheTriangle(mesh, a, b, c) {
  const center = average3(a, b, c);
  const radial = vec3(center[0], 0, center[2]);
  const outward = length(radial) > 1e-5 ? normalize(radial) : vec3(0, Math.sign(center[1] || 1), 0);
  pushTriangle(mesh, a, b, c, outward);
}

function pushCylinderXTriangle(mesh, a, b, c) {
  const center = average3(a, b, c);
  const radial = vec3(0, center[1], center[2]);
  const outward = length(radial) > 1e-5 ? normalize(radial) : vec3(Math.sign(center[0] || 1), 0, 0);
  pushTriangle(mesh, a, b, c, outward);
}

function rotateY(point, theta) {
  const cosT = Math.cos(theta);
  const sinT = Math.sin(theta);
  return [point[0] * cosT + point[2] * sinT, point[1], -point[0] * sinT + point[2] * cosT];
}

function latheMesh(name, material, profile, segments = 56) {
  const mesh = createMesh(name, material);

  for (let i = 0; i < segments; i += 1) {
    const theta0 = (i / segments) * Math.PI * 2;
    const theta1 = ((i + 1) / segments) * Math.PI * 2;

    for (let j = 0; j < profile.length - 1; j += 1) {
      const p0 = profile[j];
      const p1 = profile[j + 1];
      const a = rotateY([p0[0], p0[1], 0], theta0);
      const b = rotateY([p0[0], p0[1], 0], theta1);
      const c = rotateY([p1[0], p1[1], 0], theta1);
      const d = rotateY([p1[0], p1[1], 0], theta0);

      pushLatheTriangle(mesh, a, b, c);
      pushLatheTriangle(mesh, a, c, d);
    }
  }

  return mesh;
}

function translateMesh(mesh, dx, dy, dz) {
  for (let i = 0; i < mesh.positions.length; i += 3) {
    mesh.positions[i] += dx;
    mesh.positions[i + 1] += dy;
    mesh.positions[i + 2] += dz;
  }
  return mesh;
}

function cylinderXMesh(name, material, { center, length: bodyLength, radius, segments = 32 }) {
  const mesh = createMesh(name, material);
  const x0 = center[0] - bodyLength / 2;
  const x1 = center[0] + bodyLength / 2;
  const leftCenter = [x0, center[1], center[2]];
  const rightCenter = [x1, center[1], center[2]];

  for (let i = 0; i < segments; i += 1) {
    const a0 = (i / segments) * Math.PI * 2;
    const a1 = ((i + 1) / segments) * Math.PI * 2;

    const p00 = [x0, center[1] + Math.cos(a0) * radius, center[2] + Math.sin(a0) * radius];
    const p01 = [x0, center[1] + Math.cos(a1) * radius, center[2] + Math.sin(a1) * radius];
    const p10 = [x1, center[1] + Math.cos(a0) * radius, center[2] + Math.sin(a0) * radius];
    const p11 = [x1, center[1] + Math.cos(a1) * radius, center[2] + Math.sin(a1) * radius];

    pushCylinderXTriangle(mesh, p00, p10, p11);
    pushCylinderXTriangle(mesh, p00, p11, p01);
    pushCylinderXTriangle(mesh, leftCenter, p01, p00);
    pushCylinderXTriangle(mesh, rightCenter, p10, p11);
  }

  return mesh;
}

function boxMesh(name, material, { center, size }) {
  const mesh = createMesh(name, material);
  const [cx, cy, cz] = center;
  const [sx, sy, sz] = size;
  const x0 = cx - sx / 2;
  const x1 = cx + sx / 2;
  const y0 = cy - sy / 2;
  const y1 = cy + sy / 2;
  const z0 = cz - sz / 2;
  const z1 = cz + sz / 2;

  const v = {
    lbf: [x0, y0, z1],
    rbf: [x1, y0, z1],
    rtf: [x1, y1, z1],
    ltf: [x0, y1, z1],
    lbb: [x0, y0, z0],
    rbb: [x1, y0, z0],
    rtb: [x1, y1, z0],
    ltb: [x0, y1, z0],
  };

  const faces = [
    [v.lbf, v.rbf, v.rtf, v.ltf, [0, 0, 1]],
    [v.rbb, v.lbb, v.ltb, v.rtb, [0, 0, -1]],
    [v.lbb, v.lbf, v.ltf, v.ltb, [-1, 0, 0]],
    [v.rbf, v.rbb, v.rtb, v.rtf, [1, 0, 0]],
    [v.ltf, v.rtf, v.rtb, v.ltb, [0, 1, 0]],
    [v.lbb, v.rbb, v.rbf, v.lbf, [0, -1, 0]],
  ];

  for (const [a, b, c, d, outward] of faces) {
    pushTriangle(mesh, a, b, c, outward);
    pushTriangle(mesh, a, c, d, outward);
  }

  return mesh;
}

function buildNovelMeshes() {
  const materials = {
    stainless: 0,
    black: 1,
    red: 2,
  };

  const bodyProfile = [
    [0.0, -0.74],
    [0.96, -0.74],
    [1.12, -0.66],
    [1.19, -0.08],
    [1.19, 0.38],
    [1.13, 0.64],
    [0.92, 0.72],
    [0.0, 0.72],
  ];

  const lidProfile = [
    [0.0, 0.72],
    [0.42, 0.74],
    [0.78, 0.79],
    [0.95, 0.86],
    [0.88, 0.95],
    [0.48, 1.03],
    [0.0, 1.05],
  ];

  const ringProfile = [
    [1.15, 0.11],
    [1.19, 0.11],
    [1.19, 0.17],
    [1.15, 0.17],
  ];

  const knobProfile = [
    [0.0, 1.01],
    [0.18, 1.02],
    [0.23, 1.09],
    [0.22, 1.17],
    [0.12, 1.22],
    [0.0, 1.22],
  ];

  const collarProfile = [
    [0.0, 0.98],
    [0.18, 0.98],
    [0.2, 1.02],
    [0.0, 1.02],
  ];

  const valveProfile = [
    [0.0, -0.06],
    [0.05, -0.05],
    [0.08, 0.0],
    [0.05, 0.05],
    [0.0, 0.06],
  ];

  const meshes = [
    latheMesh("NovelBody", materials.stainless, bodyProfile, 72),
    latheMesh("NovelLid", materials.stainless, lidProfile, 72),
    latheMesh("NovelRedRing", materials.red, ringProfile, 72),
    latheMesh("NovelKnob", materials.black, knobProfile, 48),
    latheMesh("NovelKnobCollar", materials.stainless, collarProfile, 48),
    translateMesh(latheMesh("NovelValveCap", materials.black, valveProfile, 32), 0.16, 0.9, 0.52),
    cylinderXMesh("NovelMainHandle", materials.black, {
      center: [-1.88, 0.03, 0.0],
      length: 1.85,
      radius: 0.16,
      segments: 28,
    }),
    boxMesh("NovelMainBracket", materials.stainless, {
      center: [-0.92, 0.02, 0.0],
      size: [0.28, 0.18, 0.22],
    }),
    cylinderXMesh("NovelSideHandle", materials.black, {
      center: [1.53, 0.0, 0.0],
      length: 0.7,
      radius: 0.13,
      segments: 24,
    }),
    boxMesh("NovelSideBracket", materials.stainless, {
      center: [1.16, 0.0, 0.0],
      size: [0.2, 0.16, 0.18],
    }),
  ];

  return {
    meshes,
    materials: [
      {
        name: "Stainless",
        pbrMetallicRoughness: {
          baseColorFactor: [0.8, 0.81, 0.84, 1],
          metallicFactor: 1,
          roughnessFactor: 0.22,
        },
        doubleSided: true,
      },
      {
        name: "BlackHandle",
        pbrMetallicRoughness: {
          baseColorFactor: [0.05, 0.05, 0.06, 1],
          metallicFactor: 0.08,
          roughnessFactor: 0.52,
        },
        doubleSided: true,
      },
      {
        name: "RedRing",
        pbrMetallicRoughness: {
          baseColorFactor: [0.8, 0.12, 0.12, 1],
          metallicFactor: 0,
          roughnessFactor: 0.36,
        },
        emissiveFactor: [0.06, 0.0, 0.0],
        doubleSided: true,
      },
    ],
  };
}

function floatArrayBuffer(values) {
  return Buffer.from(new Float32Array(values).buffer);
}

function alignBuffer(buffer, alignment = 4, fill = 0) {
  const padding = (alignment - (buffer.length % alignment)) % alignment;
  return padding ? Buffer.concat([buffer, Buffer.alloc(padding, fill)]) : buffer;
}

function minMaxFromPositions(values) {
  const min = [Infinity, Infinity, Infinity];
  const max = [-Infinity, -Infinity, -Infinity];

  for (let i = 0; i < values.length; i += 3) {
    min[0] = Math.min(min[0], values[i]);
    min[1] = Math.min(min[1], values[i + 1]);
    min[2] = Math.min(min[2], values[i + 2]);
    max[0] = Math.max(max[0], values[i]);
    max[1] = Math.max(max[1], values[i + 1]);
    max[2] = Math.max(max[2], values[i + 2]);
  }

  return { min, max };
}

function buildGlb({ meshes, materials }) {
  const json = {
    asset: { version: "2.0", generator: "Codex best-effort Novel AR generator" },
    scene: 0,
    scenes: [{ nodes: [] }],
    nodes: [],
    meshes: [],
    materials,
    accessors: [],
    bufferViews: [],
    buffers: [{ byteLength: 0 }],
  };

  let bin = Buffer.alloc(0);

  function appendBuffer(buffer) {
    bin = alignBuffer(bin, 4, 0);
    const byteOffset = bin.length;
    bin = Buffer.concat([bin, buffer]);
    return byteOffset;
  }

  function addAccessor(values, type, includeMinMax = false) {
    const byteOffset = appendBuffer(floatArrayBuffer(values));
    const bufferViewIndex = json.bufferViews.length;
    json.bufferViews.push({
      buffer: 0,
      byteOffset,
      byteLength: values.length * 4,
      target: 34962,
    });

    const accessor = {
      bufferView: bufferViewIndex,
      componentType: 5126,
      count: values.length / 3,
      type,
    };

    if (includeMinMax) {
      const { min, max } = minMaxFromPositions(values);
      accessor.min = min;
      accessor.max = max;
    }

    json.accessors.push(accessor);
    return json.accessors.length - 1;
  }

  meshes.forEach((mesh) => {
    const positionAccessor = addAccessor(mesh.positions, "VEC3", true);
    const normalAccessor = addAccessor(mesh.normals, "VEC3", false);
    const meshIndex = json.meshes.length;

    json.meshes.push({
      name: mesh.name,
      primitives: [
        {
          attributes: {
            POSITION: positionAccessor,
            NORMAL: normalAccessor,
          },
          material: mesh.material,
          mode: 4,
        },
      ],
    });

    json.nodes.push({
      name: mesh.name,
      mesh: meshIndex,
    });
    json.scenes[0].nodes.push(json.nodes.length - 1);
  });

  bin = alignBuffer(bin, 4, 0);
  json.buffers[0].byteLength = bin.length;

  const jsonBuffer = alignBuffer(Buffer.from(JSON.stringify(json), "utf8"), 4, 0x20);

  const header = Buffer.alloc(12);
  header.writeUInt32LE(0x46546c67, 0);
  header.writeUInt32LE(2, 4);
  header.writeUInt32LE(12 + 8 + jsonBuffer.length + 8 + bin.length, 8);

  const jsonChunkHeader = Buffer.alloc(8);
  jsonChunkHeader.writeUInt32LE(jsonBuffer.length, 0);
  jsonChunkHeader.writeUInt32LE(0x4e4f534a, 4);

  const binChunkHeader = Buffer.alloc(8);
  binChunkHeader.writeUInt32LE(bin.length, 0);
  binChunkHeader.writeUInt32LE(0x004e4942, 4);

  return Buffer.concat([header, jsonChunkHeader, jsonBuffer, binChunkHeader, bin]);
}

function copyReferenceAssets() {
  if (fs.existsSync(CATALOG_PREVIEW_SOURCE)) {
    fs.copyFileSync(CATALOG_PREVIEW_SOURCE, REFERENCE_PATH);
  }
  if (fs.existsSync(COVER_SOURCE)) {
    fs.copyFileSync(COVER_SOURCE, COVER_PATH);
  }
}

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

function rgbFromFactor(color, intensity = 1) {
  const r = Math.round(clamp(color[0] * intensity) * 255);
  const g = Math.round(clamp(color[1] * intensity) * 255);
  const b = Math.round(clamp(color[2] * intensity) * 255);
  return `rgb(${r},${g},${b})`;
}

function normalizeSafe(v) {
  return normalize(v);
}

function renderSvgPreview({ meshes, materials }) {
  const width = 1400;
  const height = 1024;
  const camera = [4.4, 2.2, -4.8];
  const target = [0, 0.32, 0];
  const upHint = [0, 1, 0];
  const forward = normalizeSafe(sub(target, camera));
  const right = normalizeSafe(cross(forward, upHint));
  const up = normalizeSafe(cross(right, forward));
  const lightDir = normalizeSafe([0.65, 1, 0.42]);
  const focal = 1080;
  const background = "#0a1322";
  const triangles = [];

  function project(point) {
    const relative = sub(point, camera);
    const x = dot(relative, right);
    const y = dot(relative, up);
    const z = dot(relative, forward);
    const safeZ = Math.max(z, 0.1);
    return [
      width / 2 + (x / safeZ) * focal,
      height / 2 - (y / safeZ) * focal,
      safeZ,
    ];
  }

  meshes.forEach((mesh) => {
    const material = materials[mesh.material];
    const baseColor = material.pbrMetallicRoughness.baseColorFactor;
    for (let i = 0; i < mesh.positions.length; i += 9) {
      const a = [mesh.positions[i], mesh.positions[i + 1], mesh.positions[i + 2]];
      const b = [mesh.positions[i + 3], mesh.positions[i + 4], mesh.positions[i + 5]];
      const c = [mesh.positions[i + 6], mesh.positions[i + 7], mesh.positions[i + 8]];
      const normal = normalizeSafe([
        mesh.normals[i],
        mesh.normals[i + 1],
        mesh.normals[i + 2],
      ]);
      const shade = 0.38 + Math.max(0, dot(normal, lightDir)) * 0.62;
      const pa = project(a);
      const pb = project(b);
      const pc = project(c);
      const avgDepth = (pa[2] + pb[2] + pc[2]) / 3;

      triangles.push({
        depth: avgDepth,
        points: `${pa[0].toFixed(2)},${pa[1].toFixed(2)} ${pb[0].toFixed(2)},${pb[1].toFixed(2)} ${pc[0].toFixed(2)},${pc[1].toFixed(2)}`,
        fill: rgbFromFactor(baseColor, shade),
      });
    }
  });

  triangles.sort((a, b) => b.depth - a.depth);

  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <defs>
    <radialGradient id="bgGlow" cx="50%" cy="30%" r="70%">
      <stop offset="0%" stop-color="#233c62"/>
      <stop offset="100%" stop-color="${background}"/>
    </radialGradient>
    <filter id="shadow" x="-30%" y="-30%" width="160%" height="160%">
      <feDropShadow dx="0" dy="24" stdDeviation="24" flood-color="#03070e" flood-opacity="0.55"/>
    </filter>
  </defs>
  <rect width="${width}" height="${height}" fill="url(#bgGlow)"/>
  <ellipse cx="${width / 2}" cy="${height - 128}" rx="328" ry="60" fill="rgba(0,0,0,0.22)"/>
  <g filter="url(#shadow)">
    ${triangles.map((triangle) => `<polygon points="${triangle.points}" fill="${triangle.fill}" stroke="rgba(255,255,255,0.05)" stroke-width="0.25"/>`).join("\n    ")}
  </g>
</svg>`;

  fs.writeFileSync(PREVIEW_SVG_PATH, svg, "utf8");
}

function main() {
  copyReferenceAssets();
  const model = buildNovelMeshes();
  renderSvgPreview(model);
  const glb = buildGlb(model);
  fs.writeFileSync(GLB_PATH, glb);
  console.log("Wrote", GLB_PATH, glb.length, "bytes");
  console.log("Reference copied:", fs.existsSync(REFERENCE_PATH));
  console.log("Cover copied:", fs.existsSync(COVER_PATH));
  console.log("Preview copied:", fs.existsSync(PREVIEW_SVG_PATH));
}

main();
