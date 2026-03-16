import express from "express";
import cors from "cors";
import fs from "fs";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";
import fetch from "node-fetch";

dotenv.config(); // Cargar variables de .env

const app = express();

app.use(cors());
app.use(express.json());

// =============================
// MONGODB
// =============================
const uri = process.env.MONGODB_URI; // Tu URI seguro en .env
const client = new MongoClient(uri);
let db;

async function conectarDB() {
  try {
    await client.connect();
    db = client.db("ventasIA");
    console.log("MongoDB conectado");
  } catch (error) {
    console.error("Error conectando MongoDB", error);
  }
}

conectarDB();

// =============================
// MEMORIA DE CONVERSACIONES
// =============================
const conversaciones = {};

// =============================
// CARGAR BASES DE DATOS
// =============================
const preciosCatalogo = JSON.parse(
  fs.readFileSync("./src/data/lista_de_precios.json", "utf8")
);

const beneficiosProductos = JSON.parse(
  fs.readFileSync("./src/data/Caracteristicas_Ventajas_Beneficios.json", "utf8")
);

const encuestaVentas = JSON.parse(
  fs.readFileSync("./src/data/Encuesta_intelijente.json", "utf8")
);

const inteligenciaVentas = JSON.parse(
  fs.readFileSync("./src/data/Eric_Material_viejo.json", "utf8")
);

// =============================
// ENDPOINT /chat
// =============================
app.post("/chat", async (req, res) => {
  const { pregunta, sessionId } = req.body;

  if (!sessionId) return res.status(400).json({ error: "sessionId requerido" });

  if (!conversaciones[sessionId]) conversaciones[sessionId] = [];

  conversaciones[sessionId].push({ role: "user", content: pregunta });

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        messages: [
          {
            role: "system",
            content: `Eres Agustin 2.0, asistente experto en ventas de productos de cocina premium.
OBJETIVO: Ayudar a clientes y vendedores a entender los productos, responder preguntas y facilitar decisiones de compra.
REGLAS:
- Responde máximo en 2 oraciones.
- Usa lenguaje claro y natural.
- No menciones que eres inteligencia artificial.
PRECIOS:
Tax = 10%
Envio = 5%
Mostrar siempre:
codigo
nombre producto
precio
tax
envio
pago mensual
pago semanal
pago diario
VENTAS:
- usa características
- usa beneficios
- usa experiencia de ventas reales
Objeciones comunes: precio, pensarlo, hablar con pareja, tiempo
DATOS DISPONIBLES:
CATALOGO: ${JSON.stringify(preciosCatalogo)}
CARACTERISTICAS: ${JSON.stringify(beneficiosProductos)}
ENCUESTA: ${JSON.stringify(encuestaVentas)}
EXPERIENCIA REAL: ${JSON.stringify(inteligenciaVentas)}
`
          },
          ...conversaciones[sessionId]
        ],
      }),
    });

    const data = await response.json();
    const respuestaIA = data.choices[0].message;
    conversaciones[sessionId].push(respuestaIA);

    // Guardar en MongoDB
    if (db) {
      await db.collection("conversaciones").insertOne({
        sessionId,
        pregunta,
        respuesta: respuestaIA.content,
        fecha: new Date(),
      });
    }

    res.json({ respuesta: respuestaIA.content });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Error al procesar la solicitud" });
  }
});

// =============================
// INICIAR SERVIDOR
// =============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor corriendo en puerto ${PORT}`));
