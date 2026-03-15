import express from "express";
import cors from "cors";
import fs from "fs";

const app = express();

app.use(cors());
app.use(express.json());

// MEMORIA DE CONVERSACIONES
const conversaciones = {};

// Cargar lista de precios del catálogo
const preciosCatalogo = JSON.parse(
  fs.readFileSync("./src/data/lista_de_precios.json", "utf8")
);

// Cargar precios del distribuidor
const preciosDistribuidor = JSON.parse(
  fs.readFileSync("./src/data/precios_al_distribuidor.json", "utf8")
);

// Cargar base de datos de telemarketing
const leadsData = JSON.parse(
  fs.readFileSync("./src/data/Eric_Material_viejo", "utf8")
);


// DETECTAR SI HABLA CLIENTE O VENDEDOR
function detectarRol(texto){

  const t = texto.toLowerCase();

  if(
    t.includes("soy vendedor") ||
    t.includes("soy distribuidor") ||
    t.includes("soy representante") ||
    t.includes("ayudame a cerrar") ||
    t.includes("como cierro") ||
    t.includes("tengo un cliente")
  ){
    return "vendedor";
  }

  return "cliente";
}


// BUSCAR PRODUCTO
function buscarProducto(codigo){

  const prodCatalogo = preciosCatalogo.find(p => p.codigo === codigo);

  if(prodCatalogo){
    return {
      tipo: "catalogo",
      data: prodCatalogo
    };
  }

  const piezaInterna = preciosDistribuidor.find(p => p.codigo === codigo);

  if(piezaInterna){
    return {
      tipo: "pieza_interna",
      data: piezaInterna
    };
  }

  return null;

}


// Detectar objeciones
function detectarObjecion(texto) {

  if (!texto) return "general";

  const t = texto.toLowerCase();

  if (t.includes("caro") || t.includes("precio")) return "precio";
  if (t.includes("pensar") || t.includes("luego")) return "indecision";
  if (t.includes("esposo") || t.includes("esposa")) return "decision_familiar";
  if (t.includes("ocupado") || t.includes("tiempo")) return "tiempo";

  return "general";
}


// Sugerencias de cierre
function sugerirCierre(objecion) {

  if (objecion === "precio") {
    return "Muchos clientes pensaban lo mismo al principio, por eso ofrecemos pagos pequeños que se ajustan al presupuesto.";
  }

  if (objecion === "indecision") {
    return "A muchos clientes les ayuda ver una demostración rápida antes de decidir.";
  }

  if (objecion === "decision_familiar") {
    return "Podemos programar una demostración cuando esté su pareja también.";
  }

  if (objecion === "tiempo") {
    return "La demostración dura pocos minutos y aclara todas las dudas.";
  }

  return "";
}


// Analizar dataset
function generarSugerenciaVentas() {

  for (const lead of leadsData) {

    if (!lead.notas) continue;

    const obj = detectarObjecion(lead.notas);

    if (obj !== "general") {
      return sugerirCierre(obj);
    }

  }

  return "";

}



app.post("/chat", async (req, res) => {

  const { pregunta, sessionId } = req.body;

  if (!sessionId) {
    return res.status(400).json({ error: "sessionId requerido" });
  }

  if (!conversaciones[sessionId]) {
    conversaciones[sessionId] = [];
  }

  conversaciones[sessionId].push({
    role: "user",
    content: pregunta
  });


// DETECTAR QUIEN HABLA
  const rol = detectarRol(pregunta);


// SUGERENCIA BASADA EN DATOS REALES
  const sugerenciaVentas = generarSugerenciaVentas();


// PROMPT SEGÚN EL ROL

let instruccionesRol = "";

if(rol === "cliente"){

instruccionesRol = `
Estás hablando con un CLIENTE potencial.

Tu objetivo es:
- responder preguntas
- generar confianza
- explicar beneficios
- guiar naturalmente hacia comprar

No parezcas vendedor agresivo.
Habla como asesor experto.
No menciones que eres IA.

Máximo 2 oraciones.
`;

}

if(rol === "vendedor"){

instruccionesRol = `
Estás hablando con un REPRESENTANTE de ventas.

Tu función es ayudarle a cerrar la venta.
Responde como si fueras su asesor privado.

Puedes sugerir:
- que decir
- como responder objeciones
- que estrategia usar

No expliques demasiado.
Responde como mensaje corto que el vendedor pueda copiar o usar.
`;

}


  try {

    const response = await fetch("https://api.openai.com/v1/chat/completions", {

      method: "POST",

      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
      },

      body: JSON.stringify({

        model: "gpt-5-mini",

        messages: [

          {
            role: "system",
            content: `Eres Agustin 2.0 asistente inteligente de ventas.

${instruccionesRol}

Reglas de precios:

Usa SOLO la lista de catálogo para mostrar precios al cliente.

Si el código pertenece a lista de distribuidor:
- NO mostrar precio
- NO ofrecer venta
- solo explicar que es una pieza interna.

Cálculo de precios:

Tax = 10%
Envio = 5%

Precio final = precio + tax + envio
Pago mensual = precio final * 0.05
Pago semanal = pago mensual / 4
Pago diario = pago mensual / 30

Mostrar siempre:
codigo
nombre
precio
tax
envio
pago mensual
pago semanal
pago diario

No mostrar cálculos.

Sugerencia basada en telemarketing:
${sugerenciaVentas}

Catalogo:
${JSON.stringify(preciosCatalogo)}

Distribuidor:
${JSON.stringify(preciosDistribuidor)}

`
          },

          ...conversaciones[sessionId]

        ]

      })

    });

    const data = await response.json();

    const respuestaIA = data.choices[0].message;

    conversaciones[sessionId].push(respuestaIA);

    res.json({
      respuesta: respuestaIA.content
    });

  } catch (error) {

    console.error(error);

    res.status(500).json({
      error: "Error al procesar la solicitud"
    });

  }

});


const PORT = process.env.PORT || 3000;

app.listen(PORT, () =>
  console.log(`Servidor corriendo en puerto ${PORT}`)
);
