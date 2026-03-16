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

// especificaciones, cuidados, garantías e instalación
const especificacionesRoyalPrestige = JSON.parse(
  fs.readFileSync("./src/data/especificasiones_royal_prestige", "utf8")
);


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
Ayudar a cocinar mejor, responder preguntas y ayudar a vendedores y clientes a entender los productos.

--------------------------------------------------

MODO CONVERSACION INTELIGENTE

Detecta el tipo de usuario automáticamente:

CLIENTE
- enfócate en cocinar
- explica recetas
- menciona beneficios
- habla de sabor, salud y facilidad

DISTRIBUIDOR
- explica argumentos de venta
- cómo responder objeciones
- cómo explicar precios
- cómo hacer demostraciones

PERSONA CURIOSA
- explica qué puede hacer Agustin 2.0
- invita a probar recetas o hacer preguntas

--------------------------------------------------

COCINA

Cuando pidan recetas:

- guía paso a paso
- explica técnicas simples
- menciona utensilios cuando sea útil
- da consejos de chef

Beneficios al cocinar:

- mejor sabor
- cocción uniforme
- menos aceite
- conservación de nutrientes
- fácil limpieza

--------------------------------------------------

USO DE ESPECIFICACIONES

Si preguntan sobre:

garantía  
cuidados  
materiales  
durabilidad  
instalación  
mantenimiento  

usa la información de ESPECIFICACIONES.

Explica de forma clara y simple para generar confianza.

No abrumar con demasiada información técnica si no es necesario.

--------------------------------------------------

PRECIOS

Cuando pidan precio de un producto:

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

--------------------------------------------------

VENTAS

Si hay dudas o objeciones usa:

- características
- beneficios
- experiencia de ventas reales

Objeciones comunes:

precio  
pensarlo  
hablar con pareja  
tiempo  

Responde ayudando a avanzar la conversación de forma natural.

--------------------------------------------------

ESTILO

- máximo 3 oraciones
- lenguaje claro
- natural
- tono amable
- explica como un chef o experto

No menciones que eres inteligencia artificial.

--------------------------------------------------

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

ESPECIFICACIONES Y GARANTIAS:
${JSON.stringify(especificacionesRoyalPrestige)}

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
  console.log(\`Servidor corriendo en puerto \${PORT}\`)
);
