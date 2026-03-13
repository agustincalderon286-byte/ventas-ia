import express from "express";
import cors from "cors";
import fs from "fs";

const app = express();

app.use(cors());
app.use(express.json());

// Cargar lista de precios
const precios = JSON.parse(fs.readFileSync("./src/data/lista_de_precios.json", "utf8"));

app.post("/chat", async (req, res) => {
  const pregunta = req.body.pregunta;

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        messages: [
          {
            role: "system",
            content: `Eres Agustin 2.0, asistente virtual coach de ventas de productos de cosina.

Reglas:
1. Responde máximo 2 oraciones.
2. Si el usuario pregunta precio, busca el producto en la lista de precios.
3. Cálculo de precios:
   - Precio regular 
   - Tax = 10% del precio
   - Envío = 5% del precio
   - Precio final = precio + tax + envío
   - Siempre mostrar precio del producto, el tax y el envio
   - Pago mensual = Precio final * 0.05
   - US dollar
   - siempre dar codigo del producto, precio mensual, precio semanal, precio por dia, no muestres calculos solo da respuesta
   - cuando te den un precio Formula matematica precio*5%=pagos mensual dar pago mensual, semanal, diario
   - Dar precio mensul semanal y por dia dividiendo el precio mesnual por 4 para pago semanal y dividiendo por 30 para el pago por dia
4. Si no encuentras el producto, responde: "No tengo el precio exacto, pero puedo ayudar con otros productos".

Lista de precios (JSON):
${JSON.stringify(precios)}`
          },
          {
            role: "user",
            content: pregunta
          }
        ]
      })
    });

    const data = await response.json();
    res.json({ respuesta: data.choices[0].message.content });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Error al procesar la solicitud" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor corriendo en puerto ${PORT}`));
