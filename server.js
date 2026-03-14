import express from "express";
import cors from "cors";
import fs from "fs";

const app = express();

app.use(cors());
app.use(express.json());

// Cargar lista de precios del catálogo
const preciosCatalogo = JSON.parse(
  fs.readFileSync("./src/data/lista_de_precios.json", "utf8")
);

// Cargar precios del distribuidor
const preciosDistribuidor = JSON.parse(
  fs.readFileSync("./src/data/precios_al_distribuidor.json", "utf8")
);

app.post("/chat", async (req, res) => {
  const pregunta = req.body.pregunta;

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

Lista de precios catálogo:
${JSON.stringify(preciosCatalogo)}

Lista de precios distribuidor:
${JSON.stringify(preciosDistribuidor)}
`
          },
          {
            role: "user",
            content: pregunta
          }
        ]
      })
    });

    const data = await response.json();

    res.json({
      respuesta: data.choices[0].message.content
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
