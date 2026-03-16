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

// catálogo de productos con precios
const preciosCatalogo = JSON.parse(
  fs.readFileSync("./src/data/lista_de_precios.json", "utf8")
);

// características y beneficios
const beneficiosProductos = JSON.parse(
  fs.readFileSync("./src/data/Caracteristicas_Ventajas_Beneficios", "utf8")
);

// encuesta inteligente de ventas
const encuestaVentas = JSON.parse(
  fs.readFileSync("./src/data/Encuesta_intelijente", "utf8")
);

// experiencia real de telemarketing
const inteligenciaVentas = JSON.parse(
  fs.readFileSync("./src/data/Eric_Material_viejo", "utf8")
);

// recetas de cocina
const recetasRoyalPrestige = JSON.parse(
  fs.readFileSync("./src/data/recetas_royal_prestige", "utf8")
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

  // guardar mensaje del usuario
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
            content: `Eres Agustin 2.0, asistente experto en cocina y ventas de utensilios de cocina premium.

OBJETIVO
Ayudar a clientes y vendedores a cocinar mejor, aprender recetas, responder preguntas y facilitar decisiones de compra.

COMPORTAMIENTO

Cuando un cliente pida una receta:
- guíalo paso a paso como un chef
- menciona utensilios cuando sea útil
- explica técnicas de cocina
- da consejos prácticos

Cuando un cliente tenga dudas:
- usa características
- usa beneficios
- usa experiencia de ventas reales

Cuando hables de cocina menciona ventajas como:
- mejor sabor
- cocción uniforme
- menos aceite
- fácil limpieza
- durabilidad

ESTILO

- Responde máximo en 3 oraciones.
- Lenguaje claro y natural.
- Explica como un chef que enseña.
- No menciones que eres inteligencia artificial.

PRECIOS

Cuando el usuario pida precio de un producto:

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
- usa experiencia de ventas reales

Objeciones comunes:

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

EXPERIENCIA REAL TELEMARKETING:
${JSON.stringify(inteligenciaVentas)}

RECETAS Y GUIAS DE COCINA:
${JSON.stringify(recetasRoyalPrestige)}

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
