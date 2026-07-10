import "dotenv/config";
import fs from "node:fs/promises";
import path from "node:path";
import mongoose from "mongoose";
import {
  buscarKnowledgeVectorial,
  construirContextoVectorial,
} from "../src/knowledge/vector-store.js";

const PROJECT_ROOT = process.cwd();
const KNOWLEDGE_SOURCE_COLLECTION = "knowledge_sources";
const LOCAL_FALLBACK_FILES = [
  "src/data/entrenamiento/agustin20-thumbtack-chicago-metal-works-2022-2024.md",
  "src/data/entrenamiento/agustin20-thumbtack-message-patterns-sample.md",
];
const SOURCE_PATTERNS = [
  /agustin20-thumbtack-chicago-metal-works/i,
  /agustin20-thumbtack-message-patterns-sample/i,
  /chicago-metal-works/i,
  /thumbtack/i,
];

async function main() {
  const parsed = parseArgs(process.argv.slice(2));

  if (parsed.help || !parsed.question) {
    console.log(HELP_TEXT);
    return;
  }

  const result = await runKnowledgeSearch(parsed.question, {
    limit: parsed.limit,
  });

  if (parsed.json) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  console.log(formatOutput(result));
}

const HELP_TEXT = `
Chicago Metal Works Knowledge CLI

Usage:
  node scripts/metalworks-knowledge-cli.js "question here"
  node scripts/metalworks-knowledge-cli.js --limit 4 --json "what should we ask first on a new railing lead?"

Purpose:
  Searches only Chicago Metal Works and Agustin 2.0 business knowledge, not the whole mixed knowledge store.
`.trim();

function parseArgs(argv = []) {
  const options = {
    help: false,
    json: false,
    limit: 5,
    questionParts: [],
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = String(argv[index] || "").trim();

    if (!token) continue;
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--json") {
      options.json = true;
      continue;
    }
    if (token === "--limit") {
      options.limit = Number.parseInt(String(argv[index + 1] || ""), 10) || 5;
      index += 1;
      continue;
    }
    if (token.startsWith("--limit=")) {
      options.limit = Number.parseInt(token.split("=")[1] || "", 10) || 5;
      continue;
    }

    options.questionParts.push(token);
  }

  return {
    help: options.help,
    json: options.json,
    limit: Math.min(Math.max(options.limit || 5, 1), 8),
    question: options.questionParts.join(" ").trim(),
  };
}

async function runKnowledgeSearch(question, { limit = 5 } = {}) {
  const mongoAvailable = Boolean(process.env.MONGODB_URI);
  const openAiAvailable = Boolean(process.env.OPENAI_API_KEY);
  const sourceDocs = [];
  let sourceKeys = [];
  let matches = [];
  let mode = "local_fallback";

  if (mongoAvailable) {
    await mongoose.connect(process.env.MONGODB_URI);

    try {
      sourceDocs.push(...(await findRelevantSources()));
      sourceKeys = sourceDocs.map((doc) => doc.sourceKey).filter(Boolean);

      if (openAiAvailable && sourceKeys.length) {
        matches = await buscarKnowledgeVectorial({
          mongoose,
          question,
          limit,
          sourceKeys,
          logger: console,
        });
        if (matches.length) {
          mode = "mongo_vector";
        }
      }
    } finally {
      await mongoose.disconnect();
    }
  }

  if (!matches.length) {
    matches = await searchLocalFallback(question, { limit });
    mode = "local_fallback";
  }

  return {
    ok: true,
    mode,
    question,
    sourceCount: sourceDocs.length,
    matchedCount: matches.length,
    sources:
      sourceDocs.map((doc) => ({
        sourceKey: doc.sourceKey || "",
        relativePath: doc.relativePath || "",
        sourceType: doc.sourceType || "",
      })) || [],
    results: matches,
    context: construirContextoVectorial(matches),
  };
}

async function findRelevantSources() {
  const collection = mongoose.connection.db.collection(KNOWLEDGE_SOURCE_COLLECTION);
  const query = {
    $or: SOURCE_PATTERNS.flatMap((pattern) => [
      { relativePath: pattern },
      { sourceKey: pattern },
    ]),
  };

  return collection
    .find(query, {
      projection: {
        _id: 0,
        sourceKey: 1,
        relativePath: 1,
        sourceType: 1,
      },
    })
    .limit(50)
    .toArray();
}

async function searchLocalFallback(question, { limit = 5 } = {}) {
  const normalizedQuestion = normalizeText(question);
  const keywords = Array.from(
    new Set(
      normalizedQuestion
        .split(/\s+/)
        .map((word) => word.trim())
        .filter((word) => word.length >= 4),
    ),
  );
  const results = [];

  for (const relativeFile of LOCAL_FALLBACK_FILES) {
    const absoluteFile = path.join(PROJECT_ROOT, relativeFile);
    const raw = await fs.readFile(absoluteFile, "utf8");
    const sections = splitMarkdownSections(raw);

    for (const section of sections) {
      const haystack = normalizeText(section.text);
      const score = keywords.reduce((sum, keyword) => {
        return sum + (haystack.includes(keyword) ? 1 : 0);
      }, 0);

      if (!score) continue;

      results.push({
        sourceKey: relativeFile.replace(/[/.]+/g, "-"),
        relativePath: relativeFile,
        sourceType: "sales_training",
        chunkIndex: section.index,
        text: section.text.trim(),
        score,
      });
    }
  }

  return results
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      return left.chunkIndex - right.chunkIndex;
    })
    .slice(0, limit);
}

function splitMarkdownSections(markdown = "") {
  const lines = String(markdown || "").replace(/\r\n/g, "\n").split("\n");
  const sections = [];
  let current = [];
  let index = 0;

  const flush = () => {
    const text = current.join("\n").trim();
    if (!text) return;
    sections.push({ index: index += 1, text });
    current = [];
  };

  for (const line of lines) {
    if (/^#{1,3}\s+/.test(line) && current.length) {
      flush();
    }
    current.push(line);
  }

  flush();
  return sections;
}

function normalizeText(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{Diacritic}+/gu, "")
    .toLowerCase();
}

function formatOutput(result = {}) {
  const lines = [
    `Mode: ${result.mode || "unknown"}`,
    `Question: ${result.question || ""}`,
    `Relevant sources: ${Number(result.sourceCount || 0)}`,
    `Matches: ${Number(result.matchedCount || 0)}`,
  ];

  if (result.context) {
    lines.push("");
    lines.push(result.context.trim());
  } else {
    lines.push("");
    lines.push("No business knowledge context was found.");
  }

  return lines.join("\n");
}

await main().catch((error) => {
  console.error(error instanceof Error ? error.message : "Knowledge CLI error");
  process.exit(1);
});
