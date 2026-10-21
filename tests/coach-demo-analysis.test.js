import test from "node:test";
import assert from "node:assert/strict";

process.env.NODE_ENV = "test";

const {
  analizarCoachDemoTranscript,
  extraerCoachDemoResponseText,
  formatearCoachDemoKnownOutcome,
  normalizarCoachDemoKnownOutcome
} = await import("../server.js");

test("normaliza el resultado conocido de una demostracion", () => {
  assert.equal(normalizarCoachDemoKnownOutcome("sale"), "sale");
  assert.equal(normalizarCoachDemoKnownOutcome("NO_SALE"), "no_sale");
  assert.equal(normalizarCoachDemoKnownOutcome("otro"), "unknown");
  assert.equal(formatearCoachDemoKnownOutcome("follow_up"), "Seguimiento");
});

test("extrae el JSON de texto de una respuesta de OpenAI", () => {
  const report = { title: "Demo de prueba", overallScore: 81 };
  const text = extraerCoachDemoResponseText({
    output: [
      {
        type: "message",
        content: [{ type: "output_text", text: JSON.stringify(report) }]
      }
    ]
  });

  assert.deepEqual(JSON.parse(text), report);
});

test("prefiere output_text directo cuando esta disponible", () => {
  assert.equal(
    extraerCoachDemoResponseText({ output_text: '{"ok":true}', output: [] }),
    '{"ok":true}'
  );
});

test("solicita a OpenAI un reporte estricto y estructurado", async () => {
  const originalFetch = globalThis.fetch;
  let requestBody = null;
  globalThis.fetch = async (url, options) => {
    assert.equal(url, "https://api.openai.com/v1/responses");
    requestBody = JSON.parse(options.body);
    return new Response(
      JSON.stringify({
        output: [
          {
            type: "message",
            content: [
              {
                type: "output_text",
                text: JSON.stringify({ title: "Reporte estructurado", overallScore: 90 })
              }
            ]
          }
        ]
      }),
      { status: 200, headers: { "content-type": "application/json" } }
    );
  };
  process.env.OPENAI_API_KEY = "test-key";

  try {
    const report = await analizarCoachDemoTranscript({
      transcript: "[Minutos aproximados 0-1] El representante hizo preguntas y presento el producto.",
      knownOutcome: "sale"
    });
    assert.equal(report.title, "Reporte estructurado");
    assert.equal(requestBody.text.format.type, "json_schema");
    assert.equal(requestBody.text.format.strict, true);
    assert.match(requestBody.input[1].content, /Resultado indicado por el representante: Venta/);
  } finally {
    globalThis.fetch = originalFetch;
    delete process.env.OPENAI_API_KEY;
  }
});
