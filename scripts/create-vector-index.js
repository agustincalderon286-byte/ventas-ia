import "dotenv/config";
import mongoose from "mongoose";

async function main() {
  if (!process.env.MONGODB_URI) {
    throw new Error("MONGODB_URI no configurada");
  }

  await mongoose.connect(process.env.MONGODB_URI);

  try {
    const collection = mongoose.connection.db.collection("knowledge_chunks");
    const indexName = process.env.KNOWLEDGE_VECTOR_INDEX || "knowledge_embedding_index";

    const existing = await collection
      .listSearchIndexes(indexName)
      .toArray()
      .catch(() => []);

    if (existing.length) {
      console.log(`Indice ya existe: ${indexName}`);
      return;
    }

    const createdName = await collection.createSearchIndex({
      name: indexName,
      type: "vectorSearch",
      definition: {
        fields: [
          {
            type: "vector",
            path: "embedding",
            numDimensions: 1536,
            similarity: "cosine"
          },
          {
            type: "filter",
            path: "sourceType"
          },
          {
            type: "filter",
            path: "sourceKey"
          },
          {
            type: "filter",
            path: "tags"
          }
        ]
      }
    });

    console.log(`Indice creado: ${createdName}`);
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(error => {
  console.error("Error creando indice vectorial:", error.message);
  process.exit(1);
});
