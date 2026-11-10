/* Explicit SKU mappings; do not infer a model from a loose product name. */
(function(root){
  'use strict';
  const models = [
  {
    "id": "olla-1-5-cuartos",
    "title": "Olla NOVEL 1½ cuartos",
    "family": "NOVEL",
    "codes": [
      "CO9550"
    ],
    "names": [
      "OLLA DE 1 1/2 CUARTOS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-2-cuartos",
    "title": "Olla NOVEL 2 cuartos",
    "family": "NOVEL",
    "codes": [
      "CO9555"
    ],
    "names": [
      "OLLA DE 2 CUARTOS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-3-cuartos",
    "title": "Olla NOVEL 3 cuartos",
    "family": "NOVEL",
    "codes": [
      "CO9560"
    ],
    "names": [
      "OLLA DE 3 CUARTOS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-4-cuartos",
    "title": "Olla NOVEL 4 cuartos",
    "family": "NOVEL",
    "codes": [
      "CO9565"
    ],
    "names": [
      "OLLA DE 4 CUARTOS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-6-cuartos",
    "title": "Olla NOVEL 6 cuartos",
    "family": "NOVEL",
    "codes": [
      "CO9570"
    ],
    "names": [
      "OLLA DE 6 CUARTOS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-8-cuartos",
    "title": "Olla NOVEL 8 cuartos",
    "family": "NOVEL",
    "codes": [
      "CO9575"
    ],
    "names": [
      "OLLA DE 8 CUARTOS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-12-cuartos",
    "title": "Olla grande 12 cuartos",
    "family": "Ollas grandes",
    "codes": [
      "CO9660"
    ],
    "names": [
      "OLLA DE 12 CUARTOS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-20-cuartos",
    "title": "Olla grande 20 cuartos",
    "family": "Ollas grandes",
    "codes": [
      "CO9675"
    ],
    "names": [
      "OLLA DE 20 CUARTOS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-60-cuartos",
    "title": "Olla grande 60 cuartos",
    "family": "Ollas grandes",
    "codes": [
      "CO9144"
    ],
    "names": [
      "OLLA DE 60 CUARTOS Y PARRILLA"
    ],
    "orbit": "25deg 65deg auto",
    "note": "Esta vista representa la olla; la parrilla no está incluida en el modelo."
  },
  {
    "id": "olla-er-2-cuartos",
    "title": "Easy Release 2 cuartos",
    "family": "Easy Release",
    "codes": [
      "CO9777"
    ],
    "names": [
      "OLLA DE 2 CUARTOS ER"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-er-3-cuartos",
    "title": "Easy Release 3 cuartos",
    "family": "Easy Release",
    "codes": [
      "CO9778"
    ],
    "names": [
      "OLLA DE 3 CUARTOS ER"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-er-3-5-cuartos",
    "title": "Easy Release 3½ cuartos",
    "family": "Easy Release",
    "codes": [
      "CO9779"
    ],
    "names": [
      "OLLA DE 3.5 CUARTOS ER"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-presion-6-litros",
    "title": "Olla de presión 6 litros",
    "family": "Presion",
    "codes": [
      "CO1253"
    ],
    "names": [
      "OLLA DE PRESION DE 6 LITROS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-presion-10-litros",
    "title": "Olla de presión 10 litros",
    "family": "Presion",
    "codes": [
      "CO1258"
    ],
    "names": [
      "OLLA DE PRESION DE 10 LITROS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-paellera-14-pulgadas",
    "title": "Paellera NOVEL 14 pulgadas",
    "family": "Paelleras",
    "codes": [
      "CO9598"
    ],
    "names": [
      "PAELLERA DE 14 PULGADAS"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "olla-pavera-ovalada",
    "title": "Royal Prestige Pavera ovalada",
    "family": "Especiales",
    "codes": [
      "CO6589"
    ],
    "names": [
      "PAVERA OVALADA"
    ],
    "orbit": "25deg 65deg auto"
  },
  {
    "id": "power-blender-max",
    "title": "Power Blender Max",
    "family": "Royal Prestige",
    "codes": [
      "ES0082"
    ],
    "names": [
      "POWER BLENDER MAX"
    ],
    "orbit": "15deg 75deg auto"
  },
  {
    "id": "chocolatera",
    "title": "Chocolatera Royal Prestige",
    "family": "Royal Prestige",
    "codes": [
      "CO0101"
    ],
    "names": [
      "CHOCOLATERA"
    ],
    "orbit": "-35deg 65deg auto"
  }
];
  const normal = value => String(value || '').normalize('NFD').replace(/\p{Diacritic}/gu,'').toUpperCase().replace(/[^A-Z0-9]+/g,' ').trim();
  const codeKey = value => String(value || '').trim().toUpperCase().replace(/^C0/, 'CO');
  const byCode = new Map(models.flatMap(m => m.codes.map(code => [code,m])));
  function forProduct(product){
    if(!product || typeof product !== 'object') return null;
    const model=byCode.get(codeKey(product.codigo_producto));
    if(!model) return null;
    const name=normal(product.nombre_producto);
    if(name && !model.names.some(n => normal(n) === name)) return null;
    return model;
  }
  root.TecProductModelCatalog=Object.freeze({models,forProduct,byId:id=>models.find(m=>m.id===id)||null});
})(globalThis);
