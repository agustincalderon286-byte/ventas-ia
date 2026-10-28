/* Official, manually reviewed links. No YouTube API key, cookies or videos stored by TEC.
   Source: @RoyalPrestigeOfficial, reviewed 2026-10-28. Keep code mappings explicit;
   never infer a model from a loose term such as "licuadora", "olla" or a spare part. */
(function (root) {
  'use strict';
  const videos = [
  {
    "product": "Power Blender Max",
    "title": "¿Cómo usar tu Royal Prestige® Power Blender Max?",
    "id": "CWVjdd84sZA",
    "duration": "0:50",
    "type": "Uso y cuidados",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=CWVjdd84sZA",
    "family": "blender-max"
  },
  {
    "product": "Power Blender Go",
    "title": "Cómo usar tu Royal Prestige® Power Blender Go",
    "id": "mN2JvoB9aeo",
    "duration": "1:42",
    "type": "Uso y cuidados",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=mN2JvoB9aeo",
    "family": "blender-go"
  },
  {
    "product": "Power Blender Go Plus",
    "title": "Cómo usar y cuidar tu Royal Prestige® Power Blender Go Plus",
    "id": "Naoe05OwjW8",
    "duration": "3:42",
    "type": "Uso y cuidados",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=Naoe05OwjW8",
    "family": "blender-go-plus"
  },
  {
    "product": "Extractor de jugos",
    "title": "Cómo Usar tu Royal Prestige® Juicer",
    "id": "bPa7N0R-FGg",
    "duration": "",
    "type": "Uso y cuidados",
    "category": "Electrodomésticos",
    "url": "https://www.youtube.com/watch?v=bPa7N0R-FGg",
    "family": "juicer"
  },
  {
    "product": "NOVEL",
    "title": "Sistemas de Cocina Royal Prestige® NOVEL™ Uso y Cuidados",
    "id": "4zKG8m3jhkA",
    "duration": "1:22",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=4zKG8m3jhkA",
    "family": "novel"
  },
  {
    "product": "INNOVE",
    "title": "Antes de Usar tu Royal Prestige® INNOVE™ Por Primera Vez",
    "id": "5G86YsDe6-4",
    "duration": "1:22",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=5G86YsDe6-4",
    "family": "innove"
  },
  {
    "product": "Sistema de 5 capas",
    "title": "5 Capas Uso y Cuidado",
    "id": "aa3c1fX83N8",
    "duration": "1:21",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=aa3c1fX83N8",
    "family": "5-ply"
  },
  {
    "product": "Olla de presión",
    "title": "Royal Prestige® Pressure Cooker Uso y Cuidados",
    "id": "t6LCgx98lVU",
    "duration": "3:00",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=t6LCgx98lVU",
    "family": "pressure"
  },
  {
    "product": "Olla de presión · presentación",
    "title": "Royal Prestige® Pressure Cooker evoluciona",
    "id": "g0VqtkEeIcM",
    "duration": "1:02",
    "type": "Presentación",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=g0VqtkEeIcM",
    "family": "pressure"
  },
  {
    "product": "Deluxe Easy Release",
    "title": "Como Cuidar tu Royal Prestige® Deluxe Easy Release",
    "id": "gIRieCFeMQc",
    "duration": "0:54",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=gIRieCFeMQc",
    "family": "easy-release"
  },
  {
    "product": "Deluxe Easy Release · presentación",
    "title": "Royal Prestige® Deluxe Easy Release Cookware",
    "id": "m7tI3WypyVQ",
    "duration": "1:14",
    "type": "Presentación",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=m7tI3WypyVQ",
    "family": "easy-release"
  },
  {
    "product": "MultiPan",
    "title": "Conoce el Royal Prestige® MultiPan",
    "id": "i4FhdX12EpI",
    "duration": "0:57",
    "type": "Presentación",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=i4FhdX12EpI",
    "family": "multipan"
  },
  {
    "product": "Chocolatera",
    "title": "Cómo Usar tu Royal Prestige® Chocolatera",
    "id": "9Qp3YyHIL4U",
    "duration": "0:51",
    "type": "Uso y cuidados",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=9Qp3YyHIL4U",
    "family": "chocolatera"
  },
  {
    "product": "Paellera",
    "title": "Cómo Usar tu Royal Prestige® Paella Pan",
    "id": "1Y8tRXCYYeE",
    "duration": "0:50",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=1Y8tRXCYYeE",
    "family": "paella"
  },
  {
    "product": "Pavera ovalada",
    "title": "Pavera Ovalada Uso y Cuidados U&C",
    "id": "oagV3XP4ANA",
    "duration": "1:13",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=oagV3XP4ANA",
    "family": "roaster"
  },
  {
    "product": "Warmer Pro",
    "title": "Cómo usar y cuidar tu Royal Prestige® Warmer Pro",
    "id": "7mWnwsxppwc",
    "duration": "1:22",
    "type": "Uso y cuidados",
    "category": "Electrodomésticos",
    "url": "https://www.youtube.com/watch?v=7mWnwsxppwc",
    "family": "warmer"
  },
  {
    "product": "Filtro de aire",
    "title": "Royal Prestige® Air Filtration System Uso y Cuidados",
    "id": "G39m2Xuekgc",
    "duration": "2:37",
    "type": "Uso y cuidados",
    "category": "Filtración",
    "url": "https://www.youtube.com/watch?v=G39m2Xuekgc",
    "family": "air"
  },
  {
    "product": "FrescaFlow · beneficios",
    "title": "Conoce los Beneficios de tu Royal Prestige® FrescaFlow",
    "id": "N5UzL3Lgj38",
    "duration": "1:43",
    "type": "Presentación",
    "category": "Filtración",
    "url": "https://www.youtube.com/watch?v=N5UzL3Lgj38",
    "family": "frescaflow"
  },
  {
    "product": "FrescaFlow · presentación",
    "title": "¡Conoce el Royal Prestige® FrescaFlow!",
    "id": "psqkTxfZ3Pw",
    "duration": "1:20",
    "type": "Presentación",
    "category": "Filtración",
    "url": "https://www.youtube.com/watch?v=psqkTxfZ3Pw",
    "family": "frescaflow"
  },
  {
    "product": "FrescaFlow · cartuchos",
    "title": "Cómo y Cuándo Reemplazar los Cartuchos de Royal Prestige® FrescaFlow",
    "id": "yCRIsyi62GU",
    "duration": "2:49",
    "type": "Uso y cuidados",
    "category": "Filtración",
    "url": "https://www.youtube.com/watch?v=yCRIsyi62GU",
    "family": "frescaflow"
  },
  {
    "product": "ExperTea",
    "title": "Cómo Usar y Cuidar tu Royal Prestige® ExperTea",
    "id": "Pfl3ZcOPajE",
    "duration": "1:04",
    "type": "Uso y cuidados",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=Pfl3ZcOPajE",
    "family": "expertea"
  },
  {
    "product": "Cuchillos",
    "title": "Cómo usar y cuidar tu Royal Prestige® Cutlery",
    "id": "VFrJLn1MeD4",
    "duration": "0:40",
    "type": "Uso y cuidados",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=VFrJLn1MeD4",
    "family": "cutlery"
  },
  {
    "product": "Tazones para mezclar",
    "title": "Chef Omar te enseña sobre los nuevos Royal Prestige® Mixing Bowls",
    "id": "LggddajOKlA",
    "duration": "1:19",
    "type": "Presentación",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=LggddajOKlA",
    "family": "bowls"
  }
];
  const families = [
  {
    "id": "blender-max",
    "label": "Power Blender Max",
    "codes": [
      "ES0082"
    ],
    "videoIds": [
      "CWVjdd84sZA"
    ]
  },
  {
    "id": "blender-go",
    "label": "Power Blender Go",
    "codes": [
      "ES4200"
    ],
    "videoIds": [
      "mN2JvoB9aeo"
    ]
  },
  {
    "id": "blender-go-plus",
    "label": "Power Blender Go Plus",
    "codes": [],
    "videoIds": [
      "Naoe05OwjW8"
    ]
  },
  {
    "id": "juicer",
    "label": "Extractor de jugos",
    "codes": [
      "JU0038"
    ],
    "videoIds": [
      "bPa7N0R-FGg"
    ]
  },
  {
    "id": "novel",
    "label": "NOVEL",
    "codes": [
      "CO9893",
      "CO9563",
      "CO9573",
      "CO9883",
      "CO9554",
      "CO9550",
      "CO9555",
      "CO9560",
      "CO9565",
      "CO9570",
      "CO9575",
      "CO9580",
      "CO9590",
      "CO9598",
      "CO9660",
      "CO9675",
      "CO9144"
    ],
    "videoIds": [
      "4zKG8m3jhkA"
    ]
  },
  {
    "id": "innove",
    "label": "INNOVE",
    "codes": [
      "CO9846",
      "CO9251",
      "CO9265",
      "CO9232",
      "CO9234",
      "CO9256",
      "CO9261",
      "CO9267",
      "CO9268",
      "CO9269",
      "CO9315",
      "CO9317",
      "CO9220",
      "CO9219",
      "CO9218"
    ],
    "videoIds": [
      "5G86YsDe6-4"
    ]
  },
  {
    "id": "5-ply",
    "label": "Sistema de 5 capas",
    "codes": [
      "CO7014",
      "CO7024",
      "CO7034",
      "CO7044",
      "CO8072",
      "CO8070",
      "CO9687",
      "CO9686",
      "CO9685"
    ],
    "videoIds": [
      "aa3c1fX83N8"
    ]
  },
  {
    "id": "pressure",
    "label": "Olla de presión",
    "codes": [
      "CO1253",
      "CO1258"
    ],
    "videoIds": [
      "t6LCgx98lVU",
      "g0VqtkEeIcM"
    ]
  },
  {
    "id": "easy-release",
    "label": "Deluxe Easy Release",
    "codes": [
      "CO9740",
      "CO9736",
      "CO9738",
      "CO9739",
      "CO9780",
      "CO9777",
      "CO9778",
      "CO9779"
    ],
    "videoIds": [
      "gIRieCFeMQc",
      "m7tI3WypyVQ"
    ]
  },
  {
    "id": "multipan",
    "label": "MultiPan",
    "codes": [
      "CO4911"
    ],
    "videoIds": [
      "i4FhdX12EpI"
    ]
  },
  {
    "id": "chocolatera",
    "label": "Chocolatera",
    "codes": [
      "CO0101"
    ],
    "videoIds": [
      "9Qp3YyHIL4U"
    ]
  },
  {
    "id": "paella",
    "label": "Paellera",
    "codes": [
      "CO9598",
      "CO9315",
      "CO9317",
      "CO8072",
      "CO8070"
    ],
    "videoIds": [
      "1Y8tRXCYYeE"
    ]
  },
  {
    "id": "roaster",
    "label": "Pavera ovalada",
    "codes": [
      "CO6589"
    ],
    "videoIds": [
      "oagV3XP4ANA"
    ]
  },
  {
    "id": "warmer",
    "label": "Warmer Pro",
    "codes": [
      "PE4000"
    ],
    "videoIds": [
      "7mWnwsxppwc"
    ]
  },
  {
    "id": "air",
    "label": "Filtro de aire",
    "codes": [
      "AP2801"
    ],
    "videoIds": [
      "G39m2Xuekgc"
    ]
  },
  {
    "id": "frescaflow",
    "label": "FrescaFlow",
    "codes": [
      "WF1200"
    ],
    "videoIds": [
      "N5UzL3Lgj38",
      "psqkTxfZ3Pw",
      "yCRIsyi62GU"
    ]
  },
  {
    "id": "expertea",
    "label": "ExperTea",
    "codes": [
      "CO1001"
    ],
    "videoIds": [
      "Pfl3ZcOPajE"
    ]
  },
  {
    "id": "cutlery",
    "label": "Cuchillos",
    "codes": [
      "CU0931",
      "CU0800",
      "CU0810",
      "CU814",
      "CU0825"
    ],
    "videoIds": [
      "VFrJLn1MeD4"
    ]
  },
  {
    "id": "bowls",
    "label": "Tazones para mezclar",
    "codes": [
      "CO4900"
    ],
    "videoIds": [
      "LggddajOKlA"
    ]
  }
];
  const normalize = value => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  const byId = new Map(videos.map(video => [video.id, Object.freeze(video)]));
  function forProduct(product) {
    if (!product) return [];
    const code = String(product.codigo_producto || product.code || '').trim().toUpperCase();
    const name = normalize(product.nombre_producto || product.name);
    // CO9630 is repeated in the price sheet under two descriptions; only the
    // explicitly named INNOVE entry can be safely associated with that line.
    const selected = code === 'CO9630' ? (name.includes('innove') ? ['innove'] : [])
      : code ? families.filter(family => family.codes.includes(code)).map(family => family.id)
      : families.filter(family => normalize(family.label) === name || normalize('Royal Prestige ' + family.label) === name).map(family => family.id);
    return videos.filter(video => selected.includes(video.family));
  }
  root.TecProductVideoCatalog = Object.freeze({
    version: '20261028-videos1', videos: Object.freeze(videos), families: Object.freeze(families),
    forProduct, byId: id => byId.get(id) || null, normalize
  });
})(globalThis);
