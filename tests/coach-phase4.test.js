import test from "node:test";
import assert from "node:assert/strict";
import mongoose from "mongoose";

process.env.NODE_ENV = "test";

const {
  construirCoachProgramSheetWorkspaceQuery,
  construirCoachCrmLeadSeedOperation,
  construirCoachCrmSummary,
  construirPayloadCoachLeadDesdeChef,
  detectarEnvioDatosContacto,
  extraerLeadInfo,
  limpiarCoachAgendaRecord,
  resolverCoachCrmStatusDesdeDemoOutcome
} = await import("../server.js");

test("el tablero 4 en 14 consulta por workspace del dueno y no por creador individual", () => {
  const ownerUserId = new mongoose.Types.ObjectId();
  const seatUserId = new mongoose.Types.ObjectId();
  const query = construirCoachProgramSheetWorkspaceQuery(
    {
      _id: seatUserId,
      accountType: "seat",
      teamOwnerUserId: ownerUserId
    },
    {
      programType: "4_en_14"
    }
  );

  assert.equal(String(query.ownerUserId), String(ownerUserId));
  assert.equal(query.programType, "4_en_14");
  assert.equal("generatedByUserId" in query, false);
});

test("detecta datos de contacto compartidos desde Chef", () => {
  const texto =
    "Hola, mi nombre es Debbie Calderon y mi numero es 708 541 0789. Vivo en 2059 Desplaines St Blue Island IL 60406.";
  const leadInfo = extraerLeadInfo(texto);

  assert.ok(leadInfo);
  assert.equal(leadInfo.email, "");
  assert.equal(leadInfo.phone, "7085410789");
  assert.equal(detectarEnvioDatosContacto(texto), true);
});

test("construye payload operativo del Chef con cita y compartido por", () => {
  const payload = construirPayloadCoachLeadDesdeChef(
    {
      name: "Debbie Calderon",
      phone: "7085410789",
      bestCallDay: "domingo",
      bestCallTime: "9am",
      direccion: "2059 Desplaines St Blue Island IL 60406",
      conversationHistory: [
        { role: "user", content: "Una receta de costillas con barbecue." },
        { role: "assistant", content: "Claro, y tambien te puedo ayudar a agendar una llamada." }
      ]
    },
    {
      productosInteres: ["olla de presion"],
      temasInteres: ["recetas saludables"],
      profileSummary: "Prefiere hablar en ingles."
    },
    {
      slug: "esmeralda-chef",
      userDoc: {
        name: "Esmeralda Flores",
        email: "esme@example.com"
      },
      ownership: {
        generatedByName: "Esmeralda Flores"
      }
    }
  );

  assert.ok(payload);
  assert.equal(payload.fullName, "Debbie Calderon");
  assert.equal(payload.phone, "7085410789");
  assert.equal(payload.source, "chef_personal");
  assert.equal(payload.status, "agendado");
  assert.equal(payload.nextAction, "cita");
  assert.ok(payload.nextActionAt);
  assert.match(payload.notes, /Compartido por: Esmeralda Flores\./);
  assert.match(payload.notes, /Cita confirmada:/);
});

test("mapea un lead agendado al seed del CRM con cita y telemarketing por defecto", () => {
  const ownerUserId = new mongoose.Types.ObjectId();
  const generatedByUserId = new mongoose.Types.ObjectId();
  const leadId = new mongoose.Types.ObjectId();
  const telemarketerUserId = new mongoose.Types.ObjectId();
  const nextActionAt = new Date("2026-03-30T14:00:00.000Z");
  const operation = construirCoachCrmLeadSeedOperation(
    {
      _id: leadId,
      ownerUserId,
      ownerEmail: "owner@example.com",
      ownerName: "Agustin Calderon",
      generatedByUserId,
      generatedByName: "Juan Novato",
      generatedByAccountType: "seat",
      fullName: "Debbie Calderon",
      phone: "7085410789",
      email: "debbie@example.com",
      city: "Blue Island",
      zipCode: "60406",
      interest: "olla de presion",
      status: "agendado",
      nextAction: "cita",
      nextActionAt,
      notes: "Cliente quiere llamada en ingles",
      summary: "Interes en olla de presion y cita agendada.",
      updatedAt: nextActionAt,
      createdAt: new Date("2026-03-29T18:00:00.000Z")
    },
    {
      userId: telemarketerUserId,
      name: "Mesa Norte"
    }
  );

  assert.ok(operation?.updateOne);
  assert.equal(String(operation.updateOne.filter.ownerUserId), String(ownerUserId));
  assert.equal(operation.updateOne.filter.sourceType, "lead");
  assert.equal(operation.updateOne.update.$set.status, "cita_agendada");
  assert.equal(operation.updateOne.update.$set.nextAction, "cita");
  assert.equal(
    new Date(operation.updateOne.update.$set.appointmentAt).toISOString(),
    nextActionAt.toISOString()
  );
  assert.equal(operation.updateOne.update.$set.generatedByName, "Juan Novato");
  assert.equal(operation.updateOne.update.$setOnInsert.assignedTelemarketerName, "Mesa Norte");
});

test("convierte un registro CRM en cita operativa de Agenda con telefono y mapa", () => {
  const appointmentAt = new Date("2026-03-31T16:30:00.000Z");
  const record = limpiarCoachAgendaRecord({
    _id: new mongoose.Types.ObjectId(),
    crmRecordId: "crm_lead_demo",
    ownerUserId: new mongoose.Types.ObjectId(),
    generatedByUserId: new mongoose.Types.ObjectId(),
    generatedByName: "Juan Novato",
    sourceType: "lead",
    sourceRecordId: "lead123",
    leadName: "Debbie Calderon",
    phone: "7085410789",
    address: "2059 Desplaines St",
    city: "Blue Island",
    zipCode: "60406",
    status: "cita_agendada",
    nextActionAt: appointmentAt,
    appointmentAt: null,
    updatedAt: appointmentAt,
    createdAt: appointmentAt
  });

  assert.ok(record);
  assert.equal(record.appointmentAt?.toISOString(), appointmentAt.toISOString());
  assert.equal(record.phoneHref, "+17085410789");
  assert.match(record.mapsUrl, /2059%20Desplaines%20St/);
  assert.match(record.mapsUrl, /Blue%20Island/);
});

test("resume ventas y resultados operativos del CRM", () => {
  const now = new Date();
  const summary = construirCoachCrmSummary([
    {
      status: resolverCoachCrmStatusDesdeDemoOutcome("venta", "demo_hecha"),
      sourceType: "lead",
      saleAmount: 3295,
      territoryId: "Chicago Norte",
      assignedTelemarketerUserId: "tele-1",
      assignedTelemarketerName: "Fabiana",
      appointmentRepUserId: "rep-1",
      appointmentRepName: "Esmeralda",
      nextActionAt: now,
      appointmentAt: now
    },
    {
      status: resolverCoachCrmStatusDesdeDemoOutcome("no_atendio", "cita_agendada"),
      sourceType: "programa_4_en_14",
      territoryId: "Chicago Norte",
      assignedTelemarketerUserId: "tele-1",
      assignedTelemarketerName: "Fabiana",
      appointmentRepUserId: "rep-1",
      appointmentRepName: "Esmeralda"
    },
    {
      status: resolverCoachCrmStatusDesdeDemoOutcome("follow_up", "demo_hecha"),
      sourceType: "reclutamiento",
      territoryId: "Chicago Norte",
      assignedTelemarketerUserId: "tele-2",
      assignedTelemarketerName: "Isabel",
      appointmentRepUserId: "rep-2",
      appointmentRepName: "Edgar",
      nextActionAt: now
    }
  ]);

  assert.equal(summary.total, 3);
  assert.equal(summary.sales, 1);
  assert.equal(summary.soldAmount, 3295);
  assert.equal(summary.noAnswerCount, 1);
  assert.equal(summary.followUpCount, 1);
  assert.equal(summary.territoryCount, 1);
  assert.equal(summary.bySource.lead, 1);
  assert.equal(summary.bySource.programa_4_en_14, 1);
  assert.equal(summary.bySource.reclutamiento, 1);
});
