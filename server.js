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

// catálogo público
const preciosCatalogo = JSON.parse(
  fs.readFileSync("./src/data/Lista_Precios_2026", "utf8")
);

// beneficios de productos
const beneficiosProductos = JSON.parse(
  fs.readFileSync("./src/data/Caracteristicas_Ventajas_Beneficios", "utf8")
);

// encuesta de ventas
const encuestaVentas = JSON.parse(
  fs.readFileSync("./src/data/Encuesta_intelijente", "utf8")
);

// base de datos de telemarketing
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
            content: `Eres Agustin 2.0, un asistente experto en ventas de productos de cocina premium.

OBJETIVO
Ayudar a vendedores y clientes a entender los productos, responder preguntas y facilitar ventas.

REGLAS
- Responde máximo en 2 oraciones.
- Sé claro y natural.
- No menciones que eres inteligencia artificial.

PRECIOS
- Usa SOLO el catálogo para precios públicos.
- La lista de distribuidor es solo referencia interna.

Si el código pertenece a distribuidor:
- No mostrar precio.
- No ofrecer venta.
- Explicar que es una pieza interna.

CALCULO DE PAGOS
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

Cuando un cliente tenga dudas:

Usa beneficios del producto.
Usa estrategias basadas en experiencia de telemarketing.

Si detectas objeciones comunes:

precio
pensarlo
hablar con pareja
tiempo

responde de forma natural ayudando a avanzar la conversación.

DATOS DISPONIBLES

CATALOGO DE PRODUCTOS:
${JSON.stringify(preciosCatalogo)}

PIEZAS INTERNAS DISTRIBUIDOR:
${JSON.stringify(preciosDistribuidor)}

CARACTERISTICAS Y BENEFICIOS:
${JSON.stringify(beneficiosProductos)}

ENCUESTA INTELIGENTE DE VENTAS:
${JSON.stringify(encuestaVentas)}

BASE DE DATOS TELEMARKETING:
${JSON.stringify(inteligenciaVentas)}

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
