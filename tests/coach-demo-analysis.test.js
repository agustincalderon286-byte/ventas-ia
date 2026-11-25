import test from "node:test";
import assert from "node:assert/strict";

process.env.NODE_ENV = "test";

const {
  analizarCoachDemoTranscript,
  extraerCoachDemoResponseText,
  limpiarCoachDemoAnalysis,
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

test('el análisis del piloto valida los candidatos al guardado y mantiene privado su recibo', async () => {
  const originalFetch = globalThis.fetch;
  const originalKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = 'test-key';
  const quote = 'Como referencia anota a Julia Ríos con el número 312-555-0147';
  globalThis.fetch = async (_url, request) => {
    const body = JSON.parse(request.body);
    assert.equal(body.store, false);
    assert.ok(body.text.format.schema.required.includes('autoClose'));
    return new Response(JSON.stringify({ output_text:JSON.stringify({ title:'Cierre ficticio', closeFacts:[],
      autoClose:{ referrals:[{fullName:'Julia Ríos',phone:'3125550147',segment:1,quote}], incompleteReferrals:[], followUp:null } }) }));
  };
  try {
    const report = await analizarCoachDemoTranscript({ transcript:quote, closeSegments:[{ index:0, text:quote }], autoClose:true });
    assert.equal(report.autoClose.referrals.length, 1);
    const row = { _id:'demo', ownerUserId:'vendedor', status:'completed', report, autoCloseReceipt:{ referrals:{ saved:1 } } };
    const colleague = limpiarCoachDemoAnalysis(row,{ includeDetail:true, viewerUserId:'otro' });
    assert.equal(colleague.autoCloseReceipt, undefined);
    assert.equal(colleague.report.autoClose, undefined);
    assert.equal(limpiarCoachDemoAnalysis(row,{ includeDetail:true, viewerUserId:'vendedor' }).autoCloseReceipt.referrals.saved, 1);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = originalKey;
  }
});

const recordedDemo = {
  _id: "demo-prueba",
  ownerUserId: "vendedor",
  status: "completed",
  knownOutcome: "follow_up",
  transcript: "TRANSCRIPCIÓN PRIVADA DE PRUEBA",
  chunks: [{ status: "transcribed", text: "Fragmento privado" }],
  report: {
    title: "Seguimiento de la mensualidad",
    summary: "El cliente preguntó por la mensualidad. Acordaron revisar las opciones el viernes.",
    strengths: [{ title: "Escucha", evidence: "Preguntaste qué mensualidad le quedaba cómoda." }],
    nextSteps: ["Llama el viernes con las dos opciones acordadas."],
    improvements: [{ recommendation: "Practica cómo explicar las dos opciones." }]
  }
};

test("el historial y el detalle dan el mismo resumen y solo el detalle lleva la transcripción", () => {
  const history = limpiarCoachDemoAnalysis(recordedDemo);
  const detail = limpiarCoachDemoAnalysis(recordedDemo, { includeDetail: true });
  assert.deepEqual(history.brief, detail.brief);
  assert.deepEqual(history.brief, { summary: recordedDemo.report.summary, strength: recordedDemo.report.strengths[0].evidence,
    nextStep: recordedDemo.report.nextSteps[0], partial: false });
  assert.equal(history.knownOutcomeLabel, "Seguimiento");
  assert.equal(history.transcript, "");
  assert.deepEqual(history.chunks, []);
  assert.equal(detail.transcript, recordedDemo.transcript);
  assert.deepEqual(detail.report, recordedDemo.report);
});

test("un reporte anterior con pausas mantiene el aviso separado del resumen y recupera una recomendación", () => {
  const report = { title: "[Parcial] Batería", summary: "Pausa sin audio: desde el minuto 3.1, durante 40 segundos.\n\nREPORTE PARCIAL: hubo una interrupcion del audio. Solo se evaluan los segmentos disponibles.\n\n" + recordedDemo.report.summary,
    nextSteps: ["  "], strengths: [{ title: "Escucha" }], improvements: recordedDemo.report.improvements };
  const detail = limpiarCoachDemoAnalysis({ ...recordedDemo, knownOutcome: "unknown", report }, { includeDetail: true });
  assert.equal(detail.brief.summary, recordedDemo.report.summary);
  assert.equal(detail.brief.partial, true);
  assert.equal(detail.brief.strength, "Escucha");
  assert.equal(detail.brief.nextStep, recordedDemo.report.improvements[0].recommendation);
  assert.equal(detail.knownOutcome, "unknown");
  assert.equal(detail.report.summary, report.summary);
});

test("acota los informes largos y evita recomendaciones o resultados ficticios cuando falta información", () => {
  const summary = "El cliente pidió comparar dos opciones. " + "Palabra ".repeat(80);
  const saved = limpiarCoachDemoAnalysis({ ...recordedDemo, report: { summary, recordingGaps: [{}] } });
  assert.deepEqual(saved.brief, { summary: "El cliente pidió comparar dos opciones.", strength: "", nextStep: "", partial: true });
  for (const status of ["processing", "needs_audio", "error"]) {
    assert.equal(limpiarCoachDemoAnalysis({ ...recordedDemo, status }).brief, null);
  }
});

test('los resúmenes históricos recuperan la duración de los segmentos silenciosos y respetan la selección parcial', () => {
  const old = { ...recordedDemo, durationSeconds: 20, chunks: [
    { index: 0, status: 'transcribed', text: '', durationSeconds: 40 },
    { index: 1, status: 'transcribed', text: 'Hubo una conversación.', durationSeconds: 20 },
    { index: 2, status: 'transcribed', text: '', durationSeconds: 10 }
  ] };
  assert.equal(limpiarCoachDemoAnalysis(old).durationSeconds, 70);
  assert.equal(limpiarCoachDemoAnalysis(old, { includeDetail: true }).durationSeconds, 70);
  assert.equal(limpiarCoachDemoAnalysis({ ...old, report: { partial: true, includedChunkIndexes: [0, 1] } }).durationSeconds, 60);
  assert.equal(limpiarCoachDemoAnalysis({ ...old, report: { partial: true } }).durationSeconds, 20);
  assert.equal(limpiarCoachDemoAnalysis({ ...old, chunks: [] }).durationSeconds, 20);
  assert.equal(limpiarCoachDemoAnalysis({ ...old, chunks: [{status: 'transcribed'}] }).durationSeconds, 20);
});
