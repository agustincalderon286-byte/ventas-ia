import fs from "fs";

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function normalize(value = "") {
  return String(value || "").trim();
}

const internal = readJson("./src/data/Lista_Precios_2026");
const catalog = readJson("./src/data/lista_de_precios.json");
const catalogCodes = new Set(catalog.map(row => normalize(row["Codigo Del Producto"])).filter(Boolean));
const partKeywords =
  /(tapa|agarradera|folleto|kit|cuchara|tenedor|servidor|base|parrilla|jarro|azucarera|taza|tazas|volcan|cono|cartucho|filtro|pieza|complement|repuesto|reemplazo|handle|lid|cover|grater|bowl|utensil|utensilio|utensilios|mango|asa|aro|anillo|resorte|valvula|manija|gasket|adapter|adaptador|motor|jarra|tapon|cepillo|tolva|presionador|mecanis|mecanismo|cuchilla|infusora)/i;

const dedupe = new Map();

for (const row of internal) {
  const code = normalize(row["Codigo del producto"]);

  if (!code || dedupe.has(code)) {
    continue;
  }

  const description = normalize(row["Descripcion"]);
  const category = normalize(row["Categoria"]);

  dedupe.set(code, {
    category,
    code,
    description,
    inPublicCatalog: catalogCodes.has(code),
    likelyPartOrInternal: category === "MISC" || partKeywords.test(description)
  });
}

const uniqueCodes = Array.from(dedupe.values());
const productLikeCodes = uniqueCodes.filter(row => !row.likelyPartOrInternal);
const partOrInternalCodes = uniqueCodes.filter(row => row.likelyPartOrInternal);

const payload = {
  generatedAt: new Date().toISOString(),
  note: "Derived from Lista_Precios_2026. Prices intentionally removed. Classification is heuristic and should be reviewed before production use.",
  summary: {
    uniqueCodes: uniqueCodes.length,
    productLikeCodes: productLikeCodes.length,
    partOrInternalCodes: partOrInternalCodes.length,
    alsoInPublicCatalog: uniqueCodes.filter(row => row.inPublicCatalog).length,
    internalOnly: uniqueCodes.filter(row => !row.inPublicCatalog).length
  },
  productLikeCodes,
  partOrInternalCodes
};

const markdown = [
  "# Auditoria de Codigos Internos 2026",
  "",
  "- Fuente interna: `src/data/Lista_Precios_2026`",
  "- Fuente publica de catalogo: `src/data/lista_de_precios.json`",
  "- Objetivo: conservar codigos utiles sin usar precios internos para cotizacion.",
  "",
  "## Resumen",
  "",
  `- Codigos unicos en lista interna: ${payload.summary.uniqueCodes}`,
  `- Codigos tipo producto: ${payload.summary.productLikeCodes}`,
  `- Codigos tipo parte o interno: ${payload.summary.partOrInternalCodes}`,
  `- Codigos que tambien existen en el catalogo publico: ${payload.summary.alsoInPublicCatalog}`,
  `- Codigos solo internos: ${payload.summary.internalOnly}`,
  "",
  "## Hallazgo clave",
  "",
  "- `CO0101` (Chocolatera) aparece con dos valores muy distintos:",
  "  - Catalogo curado: `599`",
  "  - Lista interna 2026: `76.28`",
  "- Conclusion: la lista interna no debe usarse para cotizar al cliente final.",
  "",
  "## Archivo recomendado",
  "",
  "- `src/data/codigos_productos_internos_2026.json`",
  "- Ese archivo ya no trae precios; solo codigos, descripcion, categoria y una bandera heuristica para separar producto vs parte/interno.",
  "",
  "## Regla recomendada",
  "",
  "- Para precios del Coach: solo `lista_de_precios.json`.",
  "- Para codigos y piezas internas: `codigos_productos_internos_2026.json`.",
  ""
].join("\n");

fs.writeFileSync("./src/data/codigos_productos_internos_2026.json", `${JSON.stringify(payload, null, 2)}\n`);
fs.writeFileSync("./docs/auditoria-codigos-internos-2026.md", `${markdown}\n`);

console.log(JSON.stringify(payload.summary, null, 2));
