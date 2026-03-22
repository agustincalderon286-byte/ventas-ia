import crypto from "crypto";
import fs from "fs/promises";
import path from "path";

export const KNOWLEDGE_SOURCE_COLLECTION = "knowledge_sources";
export const KNOWLEDGE_CHUNK_COLLECTION = "knowledge_chunks";
const DEFAULT_CHUNK_SIZE = 1400;
const DEFAULT_CHUNK_OVERLAP_PARAGRAPHS = 1;
const DEFAULT_EMBEDDING_MODEL = process.env.KNOWLEDGE_EMBEDDING_MODEL || "text-embedding-3-small";
const DEFAULT_VECTOR_INDEX = process.env.KNOWLEDGE_VECTOR_INDEX || "knowledge_embedding_index";

function limpiarTexto(value) {
  return String(value ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\u0000/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function capitalizarEtiqueta(value) {
  return String(value)
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, char => char.toUpperCase());
}

function esPrimitivo(value) {
  return value === null || ["string", "number", "boolean"].includes(typeof value);
}

function obtenerTituloRegistro(node, fallback = "Registro") {
  if (!node || typeof node !== "object" || Array.isArray(node)) {
    return fallback;
  }

  const camposCandidatos = [
    "nombre",
    "titulo",
    "pregunta",
    "producto",
    "caracteristica",
    "Codigo Del Producto",
    "Nombre Del Producto NOVEL",
    "id"
  ];

  for (const campo of camposCandidatos) {
    if (node[campo]) {
      return String(node[campo]).trim();
    }
  }

  return fallback;
}

function serializarNodo(node, label = "Documento", depth = 0) {
  const titulo = capitalizarEtiqueta(label);

  if (esPrimitivo(node)) {
    return `${titulo}: ${limpiarTexto(node)}`;
  }

  if (Array.isArray(node)) {
    if (!node.length) {
      return `${titulo}: vacio`;
    }

    if (node.every(esPrimitivo)) {
      return `${titulo}:\n${node.map(item => `- ${limpiarTexto(item)}`).join("\n")}`;
    }

    return node
      .map((item, index) =>
        serializarNodo(item, `${titulo} / ${obtenerTituloRegistro(item, `Elemento ${index + 1}`)}`, depth + 1)
      )
      .join("\n\n");
  }

  const entradas = Object.entries(node);
  const simples = entradas.filter(([, value]) => esPrimitivo(value));
  const complejas = entradas.filter(([, value]) => !esPrimitivo(value));
  const bloques = [];

  if (depth === 0 || simples.length) {
    const header = [titulo];
    for (const [key, value] of simples) {
      header.push(`${capitalizarEtiqueta(key)}: ${limpiarTexto(value)}`);
    }
    bloques.push(header.join("\n"));
  }

  for (const [key, value] of complejas) {
    bloques.push(serializarNodo(value, key, depth + 1));
  }

  return bloques.filter(Boolean).join("\n\n");
}

function dividirEnChunks(text, maxChars = DEFAULT_CHUNK_SIZE, overlapParagraphs = DEFAULT_CHUNK_OVERLAP_PARAGRAPHS) {
  const limpio = limpiarTexto(text);

  if (!limpio) {
    return [];
  }

  const parrafos = limpio
    .split(/\n\s*\n/)
    .map(paragraph => paragraph.trim())
    .filter(Boolean);

  if (!parrafos.length) {
    return [limpio];
  }

  const chunks = [];
  let index = 0;

  while (index < parrafos.length) {
    let actual = "";
    let end = index;

    while (end < parrafos.length) {
      const candidato = actual ? `${actual}\n\n${parrafos[end]}` : parrafos[end];

      if (actual && candidato.length > maxChars) {
        break;
      }

      actual = candidato;
      end += 1;
    }

    if (!actual) {
      actual = parrafos[index].slice(0, maxChars);
      end = index + 1;
    }

    chunks.push(actual);

    if (end >= parrafos.length) {
      break;
    }

    index = Math.max(end - overlapParagraphs, index + 1);
  }

  return chunks;
}

function inferirSourceType(relativePath) {
  const nombre = relativePath.toLowerCase();

  if (nombre.includes("receta") || nombre.includes("cocina") || nombre.includes("clases")) {
    return "recipes";
  }

  if (nombre.includes("precio") || nombre.includes("pago") || nombre.includes("payment") || nombre.includes("financ")) {
    return "pricing";
  }

  if (nombre.includes("producto") || nombre.includes("garantia") || nombre.includes("especific") || nombre.includes("caracter") || nombre.includes("consejo") || nombre.includes("frescaflow")) {
    return "product_knowledge";
  }

  if (nombre.includes("encuesta")) {
    return "survey";
  }

  if (nombre.includes("demo") || nombre.includes("cierres") || nombre.includes("mentalidad") || nombre.includes("reclutamiento") || nombre.includes("sistema") || nombre.includes("material")) {
    return "sales_training";
  }

  if (nombre.includes("redfin")) {
    return "real_estate";
  }

  return "general";
}

function construirChunksEspeciales(parsedContent, sourceType, fileName) {
  if (
    sourceType === "recipes" &&
    parsedContent &&
    typeof parsedContent === "object" &&
    !Array.isArray(parsedContent) &&
    Array.isArray(parsedContent.recetas)
  ) {
    return parsedContent.recetas
      .map((receta, index) => {
        const titulo = receta?.nombre_receta || receta?.nombre || receta?.id || `Receta ${index + 1}`;

        return {
          text: serializarNodo(receta, `Receta / ${titulo}`),
          itemId: receta?.id || null,
          itemTitle: titulo
        };
      })
      .filter(chunk => limpiarTexto(chunk.text));
  }

  if (
    sourceType === "product_knowledge" &&
    parsedContent &&
    typeof parsedContent === "object" &&
    !Array.isArray(parsedContent) &&
    Array.isArray(parsedContent.productos)
  ) {
    return parsedContent.productos
      .map((producto, index) => {
        const titulo = producto?.titulo || producto?.id || `Producto ${index + 1}`;

        return {
          text: serializarNodo(producto, `Producto / ${titulo}`),
          itemId: producto?.id || null,
          itemTitle: titulo
        };
      })
      .filter(chunk => limpiarTexto(chunk.text));
  }

  if (
    sourceType === "product_knowledge" &&
    parsedContent &&
    typeof parsedContent === "object" &&
    !Array.isArray(parsedContent) &&
    Array.isArray(parsedContent.secciones)
  ) {
    return parsedContent.secciones
      .map((seccion, index) => {
        const titulo = seccion?.titulo || seccion?.id || `Sección ${index + 1}`;

        return {
          text: serializarNodo(seccion, `Garantía / ${titulo}`),
          itemId: seccion?.id || null,
          itemTitle: titulo
        };
      })
      .filter(chunk => limpiarTexto(chunk.text));
  }

  if (
    sourceType === "product_knowledge" &&
    parsedContent &&
    typeof parsedContent === "object" &&
    !Array.isArray(parsedContent) &&
    Array.isArray(parsedContent.consejos)
  ) {
    return parsedContent.consejos
      .map((consejo, index) => {
        const titulo = consejo?.pregunta || consejo?.id || `Consejo ${index + 1}`;

        return {
          text: serializarNodo(consejo, `Consejo útil / ${titulo}`),
          itemId: consejo?.id || null,
          itemTitle: titulo
        };
      })
      .filter(chunk => limpiarTexto(chunk.text));
  }

  if (
    sourceType === "pricing" &&
    parsedContent &&
    typeof parsedContent === "object" &&
    !Array.isArray(parsedContent) &&
    Array.isArray(parsedContent.metodos_pago)
  ) {
    return parsedContent.metodos_pago
      .map((metodo, index) => {
        const titulo = metodo?.nombre || metodo?.id || `Método de pago ${index + 1}`;

        return {
          text: serializarNodo(metodo, `Método de pago / ${titulo}`),
          itemId: metodo?.id || null,
          itemTitle: titulo
        };
      })
      .filter(chunk => limpiarTexto(chunk.text));
  }

  return [];
}

function inferirTags(relativePath) {
  return Array.from(
    new Set(
      relativePath
        .toLowerCase()
        .replace(/\.[a-z0-9]+$/i, "")
        .split(/[^a-z0-9áéíóúñ]+/i)
        .map(tag => tag.trim())
        .filter(tag => tag.length > 2)
    )
  );
}

function construirSourceKey(relativePath) {
  return relativePath
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function obtenerHash(rawContent) {
  return crypto.createHash("sha256").update(rawContent).digest("hex");
}

function intentarParsear(rawContent) {
  try {
    return JSON.parse(rawContent);
  } catch (error) {
    return rawContent;
  }
}

async function listarArchivos(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = await Promise.all(
    entries.map(async entry => {
      const fullPath = path.join(dir, entry.name);

      if (entry.isDirectory()) {
        return listarArchivos(fullPath);
      }

      return [fullPath];
    })
  );

  return files.flat();
}

async function resolverArchivos(dataDir, requestedPaths = []) {
  if (requestedPaths.length) {
    return requestedPaths.map(filePath =>
      path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath)
    );
  }

  return await listarArchivos(dataDir);
}

function obtenerColecciones(mongoose) {
  const db = mongoose.connection.db;

  return {
    sources: db.collection(KNOWLEDGE_SOURCE_COLLECTION),
    chunks: db.collection(KNOWLEDGE_CHUNK_COLLECTION)
  };
}

async function asegurarIndicesBasicos(mongoose) {
  const { sources, chunks } = obtenerColecciones(mongoose);

  await Promise.all([
    sources.createIndex({ sourceKey: 1 }, { unique: true }),
    sources.createIndex({ sourceType: 1 }),
    chunks.createIndex({ sourceKey: 1, chunkIndex: 1 }, { unique: true }),
    chunks.createIndex({ sourceType: 1 }),
    chunks.createIndex({ tags: 1 })
  ]);
}

export async function crearEmbeddings(texts, options = {}) {
  const apiKey = options.apiKey || process.env.OPENAI_API_KEY;
  const model = options.model || DEFAULT_EMBEDDING_MODEL;

  if (!texts.length) {
    return [];
  }

  if (!apiKey) {
    throw new Error("OPENAI_API_KEY no configurada para generar embeddings");
  }

  const response = await fetch("https://api.openai.com/v1/embeddings", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model,
      input: texts
    })
  });

  const payload = await response.json().catch(() => null);

  if (!response.ok || !payload?.data) {
    throw new Error(
      `Error creando embeddings: ${response.status} ${payload?.error?.message || response.statusText}`
    );
  }

  return payload.data
    .sort((a, b) => a.index - b.index)
    .map(item => item.embedding);
}

export async function importarKnowledge({
  mongoose,
  dataDir = path.join(process.cwd(), "src/data"),
  requestedPaths = [],
  skipEmbeddings = false,
  logger = console
}) {
  await asegurarIndicesBasicos(mongoose);

  const { sources, chunks } = obtenerColecciones(mongoose);
  const filePaths = await resolverArchivos(dataDir, requestedPaths);
  const summary = [];

  for (const filePath of filePaths) {
    const rawContent = await fs.readFile(filePath, "utf8");
    const parsedContent = intentarParsear(rawContent);
    const relativePath = path.relative(process.cwd(), filePath).replace(/\\/g, "/");
    const sourceKey = construirSourceKey(relativePath);
    const sourceType = inferirSourceType(relativePath);
    const tags = inferirTags(relativePath);
    const fileName = path.basename(filePath);
    const normalizedText = serializarNodo(parsedContent, fileName);
    const semanticChunks = construirChunksEspeciales(parsedContent, sourceType, fileName);
    const chunkDocs = semanticChunks.length
      ? semanticChunks
      : dividirEnChunks(normalizedText).map(text => ({
          text,
          itemId: null,
          itemTitle: null
        }));
    const textChunks = chunkDocs.map(chunk => chunk.text);
    const contentHash = obtenerHash(rawContent);
    const existingSource = await sources.findOne({ sourceKey });

    if (existingSource?.contentHash === contentHash) {
      logger.log(`Sin cambios: ${relativePath}`);
      summary.push({
        sourceKey,
        relativePath,
        sourceType,
        chunkCount: existingSource.chunkCount || 0,
        skipped: true
      });
      continue;
    }

    const embeddings = skipEmbeddings
      ? textChunks.map(() => [])
      : await crearEmbeddings(textChunks, {
          model: process.env.KNOWLEDGE_EMBEDDING_MODEL || DEFAULT_EMBEDDING_MODEL
        });

    const now = new Date();
    const embeddingDimensions = embeddings[0]?.length || 0;

    await chunks.deleteMany({ sourceKey });

    if (textChunks.length) {
      await chunks.insertMany(
        chunkDocs.map((chunk, chunkIndex) => ({
          sourceKey,
          relativePath,
          fileName,
          sourceType,
          tags,
          chunkIndex,
          itemId: chunk.itemId || null,
          itemTitle: chunk.itemTitle || null,
          text: chunk.text,
          embedding: embeddings[chunkIndex] || [],
          embeddingModel: skipEmbeddings ? null : process.env.KNOWLEDGE_EMBEDDING_MODEL || DEFAULT_EMBEDDING_MODEL,
          embeddingDimensions,
          charCount: chunk.text.length,
          wordCount: chunk.text.split(/\s+/).filter(Boolean).length,
          createdAt: existingSource?.createdAt || now,
          updatedAt: now
        }))
      );
    }

    await sources.updateOne(
      { sourceKey },
      {
        $set: {
          sourceKey,
          relativePath,
          fileName: path.basename(filePath),
          sourceType,
          tags,
          contentHash,
          charCount: normalizedText.length,
          chunkCount: textChunks.length,
          paragraphCount: normalizedText.split(/\n\s*\n/).filter(Boolean).length,
          embeddingModel: skipEmbeddings ? null : process.env.KNOWLEDGE_EMBEDDING_MODEL || DEFAULT_EMBEDDING_MODEL,
          embeddingDimensions,
          text: normalizedText,
          updatedAt: now
        },
        $setOnInsert: {
          createdAt: now
        }
      },
      { upsert: true }
    );

    logger.log(`Importado: ${relativePath} -> ${textChunks.length} chunks`);
    summary.push({
      sourceKey,
      relativePath,
      sourceType,
      chunkCount: textChunks.length,
      skipped: false
    });
  }

  return summary;
}

export function inferirTiposFuentePorPregunta(question) {
  const pregunta = question.toLowerCase();
  const tipos = new Set();

  if (/receta|cocinar|pollo|carne|res|pescado|salmon|huevo|pancake|hotcake|panqueque|sopa|arroz|pasta|verdura|ensalada|desayuno|comida|cena/i.test(pregunta)) {
    tipos.add("recipes");
  }

  if (/precio|precios|cu[aá]nto|cuesta|pago|pagos|diario|mensual|financiamiento|plan/i.test(pregunta)) {
    tipos.add("pricing");
  }

  if (/garantia|material|olla|sarten|cuchillo|santoku|easy release|paellera|vaporera|producto|extractor|exprimidor|juicer|licuadora|blender|filtro|fresca(flow|pure)|cafetera|barista|expertea|fresh max|precision cook|smart temp|warmer pro|mixing bowl|recipiente|utensilio/i.test(pregunta)) {
    tipos.add("product_knowledge");
  }

  if (/venta|cerrar|objeci[oó]n|cita|representante|agendar|demo|demostraci[oó]n|equipo|reclutar/i.test(pregunta)) {
    tipos.add("sales_training");
  }

  if (/casa|inversion|propiedad|roi|renta/i.test(pregunta)) {
    tipos.add("real_estate");
  }

  if (!tipos.size) {
    tipos.add("general");
  }

  return Array.from(tipos);
}

export async function buscarKnowledgeVectorial({
  mongoose,
  question,
  limit = 6,
  numCandidates = 40,
  sourceTypes = [],
  logger = console
}) {
  const apiKey = process.env.OPENAI_API_KEY;

  if (!question || !apiKey) {
    return [];
  }

  const { chunks } = obtenerColecciones(mongoose);

  try {
    const [queryEmbedding] = await crearEmbeddings([question], {
      model: process.env.KNOWLEDGE_EMBEDDING_MODEL || DEFAULT_EMBEDDING_MODEL
    });

    const filter = sourceTypes.length ? { sourceType: { $in: sourceTypes } } : undefined;
    const pipeline = [
      {
        $vectorSearch: {
          index: process.env.KNOWLEDGE_VECTOR_INDEX || DEFAULT_VECTOR_INDEX,
          path: "embedding",
          queryVector: queryEmbedding,
          numCandidates,
          limit,
          ...(filter ? { filter } : {})
        }
      },
      {
        $project: {
          _id: 0,
          sourceKey: 1,
          relativePath: 1,
          sourceType: 1,
          chunkIndex: 1,
          text: 1,
          score: { $meta: "vectorSearchScore" }
        }
      }
    ];

    return await chunks.aggregate(pipeline).toArray();
  } catch (error) {
    logger.log("Vector search no disponible, usando fallback:", error.message);
    return [];
  }
}

export function construirContextoVectorial(matches = []) {
  if (!matches.length) {
    return "";
  }

  const bloques = matches.map((match, index) => {
    return [
      `FRAGMENTO ${index + 1}`,
      `source_type: ${match.sourceType || "general"}`,
      `source_key: ${match.sourceKey || "desconocido"}`,
      `score: ${typeof match.score === "number" ? match.score.toFixed(4) : "n/a"}`,
      match.text
    ].join("\n");
  });

  return `
CONTEXTO VECTORIAL RELEVANTE:
Usa primero estos fragmentos recuperados por similitud semantica. Si aqui esta la respuesta, priorizala sobre el contexto estatico amplio.

${bloques.join("\n\n")}
`;
}
