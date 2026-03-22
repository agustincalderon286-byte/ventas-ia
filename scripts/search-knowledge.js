import "dotenv/config";
import mongoose from "mongoose";
import { buscarKnowledgeVectorial, construirContextoVectorial, inferirTiposFuentePorPregunta } from "../src/knowledge/vector-store.js";

async function main() {
  const question = process.argv.slice(2).join(" ").trim();

  if (!question) {
    throw new Error('Usa: node scripts/search-knowledge.js "tu pregunta"');
  }

  if (!process.env.MONGODB_URI) {
    throw new Error("MONGODB_URI no configurada");
  }

  if (!process.env.OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY no configurada");
  }

  await mongoose.connect(process.env.MONGODB_URI);

  try {
    const sourceTypes = inferirTiposFuentePorPregunta(question);
    const matches = await buscarKnowledgeVectorial({
      mongoose,
      question,
      sourceTypes,
      logger: console
    });

    console.log(
      JSON.stringify(
        {
          question,
          sourceTypes,
          results: matches
        },
        null,
        2
      )
    );

    if (matches.length) {
      console.log("\n--- CONTEXTO LISTO PARA PROMPT ---\n");
      console.log(construirContextoVectorial(matches));
    }
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(error => {
  console.error("Error buscando knowledge:", error.message);
  process.exit(1);
});
