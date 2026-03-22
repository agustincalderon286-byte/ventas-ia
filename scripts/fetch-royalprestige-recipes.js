import fs from "fs/promises";
import path from "path";

const DEFAULT_OUTPUT = path.join(process.cwd(), "src/data/royalprestige_recetas_site.json");
const API_URL = "https://www.royalprestige.com/api/rp/recipes";
const DEFAULT_SITE_ID = "77368a0b-d1cc-4d0e-983a-6b5231c7ff85";

function decodeHtmlEntities(text) {
  return String(text || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&frac12;/gi, "1/2")
    .replace(/&frac14;/gi, "1/4")
    .replace(/&frac34;/gi, "3/4")
    .replace(/&deg;/gi, "°")
    .replace(/&aacute;/gi, "á")
    .replace(/&eacute;/gi, "é")
    .replace(/&iacute;/gi, "í")
    .replace(/&oacute;/gi, "ó")
    .replace(/&uacute;/gi, "ú")
    .replace(/&ntilde;/gi, "ñ")
    .replace(/&uuml;/gi, "ü")
    .replace(/&Aacute;/gi, "Á")
    .replace(/&Eacute;/gi, "É")
    .replace(/&Iacute;/gi, "Í")
    .replace(/&Oacute;/gi, "Ó")
    .replace(/&Uacute;/gi, "Ú")
    .replace(/&Ntilde;/gi, "Ñ")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCharCode(parseInt(code, 16)));
}

function stripHtml(html) {
  return decodeHtmlEntities(
    String(html || "")
      .replace(/<sup[^>]*>.*?<\/sup>/gi, "")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<[^>]+>/g, "")
  )
    .replace(/\u00a0/g, " ")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function extractListItems(html) {
  const matches = Array.from(String(html || "").matchAll(/<li[^>]*>([\s\S]*?)<\/li>/gi));

  return matches
    .map(match => stripHtml(match[1]))
    .map(item => item.replace(/\s+/g, " ").trim())
    .filter(item => item && item !== " ");
}

function normalizarTitle(text) {
  return stripHtml(text).replace(/\s+/g, " ").trim();
}

function parseIngredientesYUtensilios(html) {
  const items = extractListItems(html);
  const ingredientes = [];
  const utensilios = [];
  let target = "ingredientes";

  for (const item of items) {
    const limpio = item.replace(/\s+/g, " ").trim();
    const lower = limpio.toLowerCase();

    if (!limpio) {
      continue;
    }

    if (lower.includes("utensilios royal prestige")) {
      target = "utensilios";
      continue;
    }

    if (lower === "adorno" || lower === "guarnición" || lower === "guarnicion" || lower === "vinagreta" || lower === "relleno" || lower === "masa" || lower === "salsa") {
      ingredientes.push(limpio);
      continue;
    }

    if (target === "utensilios") {
      utensilios.push(limpio);
    } else {
      ingredientes.push(limpio);
    }
  }

  return {
    ingredientes,
    utensilios_recomendados: utensilios
  };
}

function parsePasos(html) {
  return extractListItems(html);
}

function inferirObjetivo(recipe) {
  const partes = [
    recipe.text2,
    recipe.categories?.[0]?.text,
    recipe.foodTypes?.[0]?.text
  ]
    .map(value => normalizarTitle(value))
    .filter(Boolean);

  return partes.join(" — ");
}

function inferirProductoRecomendado(utensilios = []) {
  if (!utensilios.length) {
    return "";
  }

  return utensilios.slice(0, 3).join(" + ");
}

function construirRegistroReceta(recipe, index) {
  const { ingredientes, utensilios_recomendados } = parseIngredientesYUtensilios(recipe.ingredients);
  const pasos_clave = parsePasos(recipe.instructions);

  return {
    id: recipe.urlName || recipe.id || `rp_recipe_${index + 1}`,
    titulo: normalizarTitle(recipe.title || recipe.text1),
    descripcion: normalizarTitle(recipe.text2),
    objetivo: inferirObjetivo(recipe),
    difficulty: recipe.difficulty?.text || "",
    difficulty_value: recipe.difficulty?.persistedValue || "",
    porciones: recipe.actualFoodPortions || "",
    tiempo_minutos: recipe.actualCookingTime || "",
    categoria: recipe.categories?.map(item => normalizarTitle(item.text)).filter(Boolean) || [],
    tipo_comida: recipe.foodTypes?.map(item => normalizarTitle(item.text)).filter(Boolean) || [],
    ingredientes,
    utensilios_recomendados,
    pasos_clave,
    producto_recomendado: inferirProductoRecomendado(utensilios_recomendados),
    por_que_royal_prestige: utensilios_recomendados.length
      ? `La receta utiliza ${inferirProductoRecomendado(utensilios_recomendados)} para cocinar con mejor control, practicidad y presentacion.`
      : "",
    url_detalle: recipe.recipeDetailUrl
      ? new URL(recipe.recipeDetailUrl, "https://www.royalprestige.com").toString()
      : "",
    image_url: recipe.imageURL || "",
    publication_date: recipe.publicationDate || "",
    original_content_id: recipe.originalContentId || "",
    source_id: recipe.id || ""
  };
}

async function fetchRecipesPage(pageNumber) {
  const response = await fetch(API_URL, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      language: "es",
      siteId: DEFAULT_SITE_ID,
      filterOptions: { pageNumber },
      filters: [{ name: "ingredients", values: [], displayExpanded: true }],
      isDesktop: true
    })
  });

  const payload = await response.json();

  if (!response.ok || payload?.errorDetail?.isError) {
    throw new Error(`No se pudo obtener recetas, pagina ${pageNumber}`);
  }

  return payload.data;
}

async function main() {
  const outputPath = process.argv[2]
    ? path.isAbsolute(process.argv[2])
      ? process.argv[2]
      : path.join(process.cwd(), process.argv[2])
    : DEFAULT_OUTPUT;

  const firstPage = await fetchRecipesPage(1);
  const totalPages = firstPage.paging?.totalPages || 1;
  const collected = [...(firstPage.recipes || [])];

  for (let page = 2; page <= totalPages; page += 1) {
    const pageData = await fetchRecipesPage(page);
    collected.push(...(pageData.recipes || []));
  }

  const recetas = collected.map((recipe, index) => construirRegistroReceta(recipe, index));
  const documento = {
    metadata: {
      coleccion: "royalprestige_recetas_site",
      descripcion: "Recetas estructuradas descargadas desde el API oficial de Royal Prestige Mexico.",
      total_recetas: recetas.length,
      fetched_at: new Date().toISOString(),
      source: API_URL,
      site_id: DEFAULT_SITE_ID,
      campos_por_chunk: [
        "titulo",
        "descripcion",
        "objetivo",
        "ingredientes",
        "utensilios_recomendados",
        "pasos_clave",
        "producto_recomendado",
        "por_que_royal_prestige"
      ]
    },
    recetas
  };

  await fs.writeFile(outputPath, `${JSON.stringify(documento, null, 2)}\n`, "utf8");

  console.log(
    JSON.stringify(
      {
        outputPath,
        totalRecetas: recetas.length,
        firstRecipe: recetas[0]?.titulo || null
      },
      null,
      2
    )
  );
}

main().catch(error => {
  console.error("Error descargando recetas Royal Prestige:", error.message);
  process.exit(1);
});
