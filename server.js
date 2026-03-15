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
  fs.readFileSync("./src/data/Eric_Material_viejo.json", "utf8")
);

// Detectar objeciones comunes en notas
function detectarObjecion(texto) {

  if (!texto) return "general";

  const t = texto.toLowerCase();

  if (t.includes("caro") || t.includes("precio")) return "precio";
  if (t.includes("pensar") || t.includes("luego")) return "indecision";
  if (t.includes("esposo") || t.includes("esposa") || t.includes("pareja")) return "decision_familiar";
  if (t.includes("ocupado") || t.includes("tiempo")) return "tiempo";

  return "general";
}

// Sugerir cierre basado en objeción
function sugerirCierre(objecion) {

  if (objecion === "precio") {
    return "Muchos clientes pensaban lo mismo al principio, por eso ofrecemos pagos pequeños que se ajustan al presupuesto.";
  }

  if (objecion === "indecision") {
    return "A muchos clientes les ayuda ver una demostración rápida antes de decidir.";
  }

  if (objecion === "decision_familiar") {
    return "Podemos programar una demostración cuando esté su pareja también para que ambos lo vean.";
  }

  if (objecion === "tiempo") {
    return "La demostración normalmente dura pocos minutos y ayuda a entender todo rápidamente.";
  }

  return "";
}

// Analizar dataset para generar sugerencia
function generarSugerenciaVentas() {

  let sugerencia = "";

  for (const lead of leadsData) {

    if (!lead.notas) continue;

    const obj = detectarObjecion(lead.notas);

    if (obj !== "general") {
      sugerencia = sugerirCierre(obj);
      break;
    }

  }

  return sugerencia;

}

app.post("/chat", async (req, res) => {

  const { pregunta, sessionId } = req.body;

  if (!sessionId) {
    return res.status(400).json({ error: "sessionId requerido" });
  }

  // Crear memoria si no existe
  if (!conversaciones[sessionId]) {
    conversaciones[sessionId] = [];
  }

  // Guardar mensaje del usuario
  conversaciones[sessionId].push({
    role: "user",
    content: pregunta
  });

  // Generar sugerencia basada en experiencia real
  const sugerenciaVentas = generarSugerenciaVentas();

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
            content: `Eres Agustin 2.0, asistente virtual coach de ventas de productos de cocina.

Reglas:
1. Responde máximo 2 oraciones.
2. Si el usuario pregunta precio, busca el producto por su codigo en las listas.
3. Usa la lista de catálogo para precio público.
4. Usa la lista de distribuidor para calcular estrategias de venta.

Cálculo de precios:
- Tax = 10% del precio
- Envío = 5% del precio
- Precio final = precio + tax + envío
- Pago mensual = precio final * 0.05
- Pago semanal = pago mensual / 4
- Pago diario = pago mensual / 30
- Siempre mostrar: codigo, nombre producto, precio, tax, envio, pago mensual, semanal y diario.
- No mostrar cálculos.

Si no encuentras el producto responde:
"No tengo el precio exacto, pero puedo ayudar con otros productos".

Sugerencia de estrategia basada en experiencia real de telemarketing:
${sugerenciaVentas}

Lista de precios catálogo:
${JSON.stringify(preciosCatalogo)}

Lista de precios distribuidor:
${JSON.stringify(preciosDistribuidor)}
`
          },

          // HISTORIAL DE CONVERSACIÓN
          ...conversaciones[sessionId]

        ]

      })

    });

    const data = await response.json();

    const respuestaIA = data.choices[0].message;

    // Guardar respuesta en memoria
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
