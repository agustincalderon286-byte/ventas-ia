import express from "express";
import cors from "cors";
import fs from "fs";

const app = express();

app.use(cors());
app.use(express.json());

// MEMORIA DE CONVERSACIONES
const conversaciones = {};


// =============================
// CARGAR BASES DE DATOS
// =============================

// catálogo de productos
const preciosCatalogo = JSON.parse(
  fs.readFileSync("./src/data/Lista_Precios_2026", "utf8")
);

// beneficios y características
const beneficiosProductos = JSON.parse(
  fs.readFileSync("./src/data/Caracteristicas_Ventajas_Beneficios", "utf8")
);

// encuesta inteligente
const encuestaVentas = JSON.parse(
  fs.readFileSync("./src/data/Encuesta_intelijente", "utf8")
);

// experiencia de telemarketing
const inteligenciaVentas = JSON.parse(
  fs.readFileSync("./src/data/Eric_Material_viejo", "utf8")
);



app.post("/chat", async (req, res) => {

  const { pregunta, sessionId } = req.body;

  if (!sessionId) {
    return res.status(400).json({ error: "sessionId requerido" });
  }

  // crear memoria si no existe
  if (!conversaciones[sessionId]) {
    conversaciones[sessionId] = [];
  }

  // guardar mensaje usuario
  conversaciones[sessionId].push({
    role: "user",
    content: pregunta
  });

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
            content: `Eres Agustin 2.0, asistente experto en ventas de productos de cocina premium.

OBJETIVO
Ayudar a clientes y vendedores a entender los productos, responder preguntas y facilitar decisiones de compra.

REGLAS
- Responde máximo en 2 oraciones.
- Usa lenguaje claro y natural.
- No menciones que eres inteligencia artificial.

PRECIOS
Si el usuario pide precio de un producto:

Cálculo:

Tax = 10%
Envio = 5%

Precio final = precio + tax + envio
Pago mensual = precio final * 0.05
Pago semanal = pago mensual / 4
Pago diario = pago mensual / 30

Mostrar siempre:

codigo
nombre producto
precio
tax
envio
pago mensual
pago semanal
pago diario

No mostrar cálculos internos.

VENTAS

Si el cliente tiene dudas:

- usa características
- usa beneficios
- responde objeciones comunes

Objeciones comunes detectadas en telemarketing:

precio
pensarlo
hablar con pareja
tiempo

Responde de forma natural ayudando a avanzar la conversación.

DATOS DISPONIBLES

CATALOGO DE PRODUCTOS:
${JSON.stringify(preciosCatalogo)}

CARACTERISTICAS Y BENEFICIOS:
${JSON.stringify(beneficiosProductos)}

ENCUESTA INTELIGENTE:
${JSON.stringify(encuestaVentas)}

EXPERIENCIA REAL DE TELEMARKETING:
${JSON.stringify(inteligenciaVentas)}

`
          },

          // historial de conversación
          ...conversaciones[sessionId]

        ]

      })

    });

    const data = await response.json();

    const respuestaIA = data.choices[0].message;

    // guardar respuesta IA
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
