import "dotenv/config";
import mongoose from "mongoose";
import path from "path";
import { importarKnowledge } from "../src/knowledge/vector-store.js";

function parseArgs(argv) {
  const requestedPaths = [];
  let skipEmbeddings = false;

  for (const arg of argv) {
    if (arg === "--skip-embeddings") {
      skipEmbeddings = true;
      continue;
    }

    requestedPaths.push(arg);
  }

  return {
    requestedPaths,
    skipEmbeddings
  };
}

async function main() {
  if (!process.env.MONGODB_URI) {
    throw new Error("MONGODB_URI no configurada");
  }

  const { requestedPaths, skipEmbeddings } = parseArgs(process.argv.slice(2));

  if (!skipEmbeddings && !process.env.OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY no configurada para importar embeddings");
  }

  await mongoose.connect(process.env.MONGODB_URI);

  try {
    const summary = await importarKnowledge({
      mongoose,
      dataDir: path.join(process.cwd(), "src/data"),
      requestedPaths,
      skipEmbeddings,
      logger: console
    });

    const totalChunks = summary.reduce((sum, item) => sum + (item.chunkCount || 0), 0);
    const actualizados = summary.filter(item => !item.skipped).length;

    console.log(
      JSON.stringify(
        {
          sources: summary.length,
          updatedSources: actualizados,
          totalChunks
        },
        null,
        2
      )
    );
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(error => {
  console.error("Error importando knowledge:", error.message);
  process.exit(1);
});
