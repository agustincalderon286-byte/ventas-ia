import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import mongoose from "mongoose";

process.env.NODE_ENV = "test";

const {
  construirImagenProductoCatalogoCoach,
  construirCoachProgram414StartWindow,
  construirCoachProgramSheetWorkspaceQuery,
  construirCoachCrmLeadSeedOperation,
  construirCoachCrmSummary,
  construirPayloadCoachLeadDesdeChef,
  detectarEnvioDatosContacto,
  extraerLeadInfo,
  limpiarCoachAgendaRecord,
  parseCoachProgram414ImportedSheets,
  resolverCoachOwnerUserId,
  resolverCoachCrmStatusDesdeDemoOutcome
} = await import("../server.js");

test("asocia productos de la calculadora con su imagen del catalogo INNOVE", () => {
  const sistema = construirImagenProductoCatalogoCoach("CO9261", "SET DE 10 PIEZAS FAMILIAR INNOVE");
  const licuadora = construirImagenProductoCatalogoCoach("ES0082", "POWER BLENDER MAX");
  const filtro = construirImagenProductoCatalogoCoach("WF0419", "FRESCAPURE 3500");

  assert.equal(sistema.catalogo_pagina, 6);
  assert.match(sistema.imagen_producto, /pagina-06\.webp$/);
  assert.equal(licuadora.catalogo_pagina, 24);
  assert.match(licuadora.imagen_producto, /products\/power-blender-max\.webp$/);
  assert.equal(filtro.catalogo_pagina, 48);
  assert.match(filtro.imagen_producto, /products\/frescapure-3500\.webp$/);
});

test("distingue el juego de cuchillos de 5 piezas del sistema de cocina", () => {
  const cuchillos = construirImagenProductoCatalogoCoach("CU0800", "JUEGO DE CUCHILLOS DE 5 PIEZAS");
  const sistemaCinco = construirImagenProductoCatalogoCoach(
    "CO9893",
    "5 PIEZAS SISTEMA DE COSINA COMPLEMENTARIO"
  );
  const sistemaQuince = construirImagenProductoCatalogoCoach(
    "CO9554",
    "15 PIEZAS SISTEMA DE COSINA VERSATIL"
  );

  assert.equal(cuchillos.catalogo_pagina, 30);
  assert.match(cuchillos.imagen_producto, /products\/cuchillos-cocina-5\.webp$/);
  assert.equal(sistemaCinco.catalogo_pagina, 9);
  assert.match(sistemaCinco.imagen_producto, /pagina-09\.webp$/);
  assert.notEqual(sistemaQuince.catalogo_pagina, 9);
});

test("muestra una imagen especifica y distinta para cada tamano de olla", () => {
  const ollas = [
    ["CO9550", "OLLA DE 1 1/2 CUARTOS"],
    ["CO9555", "OLLA DE 2 CUARTOS"],
    ["CO9560", "OLLA DE 3 CUARTOS"],
    ["CO9565", "OLLA DE 4 CUARTOS"],
    ["CO9570", "OLLA DE 6 CUARTOS"],
    ["CO9575", "OLLA DE 8 CUARTOS"]
  ].map(([codigo, nombre]) => construirImagenProductoCatalogoCoach(codigo, nombre));

  assert.equal(new Set(ollas.map(item => item.imagen_producto)).size, ollas.length);
  assert.ok(ollas.every(item => item.imagen_producto.includes("/products/olla-")));
});

test("separa las imagenes del filtro completo, el 5500 y el cartucho de repuesto", () => {
  const unit = construirImagenProductoCatalogoCoach("WF0419", "FRESCAPURE 3500");
  const larger = construirImagenProductoCatalogoCoach("WF0482", "FRESCAPURE 5500");
  const cartridge = construirImagenProductoCatalogoCoach("WF0451", "CARTUCHO DE REPUESTO FRESCAPURE 3500");
  assert.match(unit.imagen_producto, /products\/frescapure-3500\.webp$/);
  assert.match(larger.imagen_producto, /products\/frescapure-5500\.webp$/);
  assert.match(cartridge.imagen_producto, /pagina-48\.webp$/);
  assert.notEqual(cartridge.imagen_producto, unit.imagen_producto);
});

test("mantiene distintas las espatulas y no inventa una foto para la luz ultravioleta", () => {
  const espatula = construirImagenProductoCatalogoCoach("SP1852", "ESPATULA");
  const espatulaHelados = construirImagenProductoCatalogoCoach("SP1859", "ESPATULA PARA HELADOS");
  const luzUltravioleta = construirImagenProductoCatalogoCoach("WF0654", "LUZ ULTRA VIOLETA");

  assert.notEqual(espatula.imagen_producto, espatulaHelados.imagen_producto);
  assert.match(luzUltravioleta.imagen_producto, /pagina-48\.webp$/);
});

test("todos los productos con precio reciben una imagen publica disponible", () => {
  const priceList = JSON.parse(
    readFileSync(new URL("../src/data/lista_de_precios.json", import.meta.url), "utf8")
  );

  assert.ok(priceList.length > 100);

  for (const item of priceList) {
    const image = construirImagenProductoCatalogoCoach(
      item["Codigo Del Producto"],
      item["Nombre Del Producto NOVEL"]
    );
    const imageFile = new URL(`../public${image.imagen_producto}`, import.meta.url);

    assert.ok(existsSync(imageFile), `${item["Nombre Del Producto NOVEL"]} no tiene imagen`);
  }
});

test("calcula inicio y vencimiento automatico de 4 en 14 desde la fecha de inicio", () => {
  const startWindow = construirCoachProgram414StartWindow({
    startDate: "2026-03-30"
  });

  assert.equal(startWindow, "03/30/26 - 04/13/26");
});

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

test("un administrador lider comparte el workspace de la cuenta duena", () => {
  const ownerUserId = new mongoose.Types.ObjectId();
  const leaderUserId = new mongoose.Types.ObjectId();

  assert.equal(
    String(
      resolverCoachOwnerUserId({
        _id: leaderUserId,
        accountType: "leader",
        teamOwnerUserId: ownerUserId
      })
    ),
    String(ownerUserId)
  );
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

test("agrupa un csv de 4 en 14 por anfitrion para importacion compartida", () => {
  const csv = [
    "Nombre,Telefono,Nombre de Anfitrion,Relacion con el cliente,Direccion,Comentario de Telemarketing",
    "Maria Lopez,7735551111,Ana Ruiz,Prima,2059 Desplaines St,Quiere que la llamen el jueves",
    "Jose Perez,3125552222,Ana Ruiz,Vecino,,No contesto",
    "Lupita Gomez,8475553333,Referidos de Carmen Flores,Comadre,123 Main St,Lista para visita"
  ].join("\n");

  const sheets = parseCoachProgram414ImportedSheets("csv", csv);

  assert.equal(sheets.length, 2);
  assert.equal(sheets[0].hostName, "Ana Ruiz");
  assert.equal(sheets[0].referrals.length, 2);
  assert.equal(sheets[0].referrals[0].address, "2059 Desplaines St");
  assert.match(sheets[0].referrals[1].notes, /Relacion: Vecino/);
  assert.equal(sheets[1].hostName, "Carmen Flores");
  assert.equal(sheets[1].referrals.length, 1);
});
