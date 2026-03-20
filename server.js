import express from "express";
import cors from "cors";
import fs from "fs";
import path from "path";

const app = express();

app.use(cors());
app.use(express.json());

// MEMORIA
const conversaciones = {};

// =============================
// FUNCION JSON
// =============================
function cargarJSON(ruta) {
  try {
    return JSON.parse(fs.readFileSync(ruta, "utf8"));
  } catch (error) {
    console.log("Error cargando:", ruta);
    return {};
  }
}

// =============================
// BASES DE DATOS
// =============================
const preciosCatalogo = cargarJSON("./src/data/lista_de_precios.json");
const beneficiosProductos = cargarJSON("./src/data/Caracteristicas_Ventajas_Beneficios");
const encuestaVentas = cargarJSON("./src/data/Encuesta_intelijente");
const inteligenciaVentas = cargarJSON("./src/data/Eric_Material_viejo");
const recetasRoyalPrestige = cargarJSON("./src/data/recetas_royal_prestige");
const especificacionesRoyalPrestige = cargarJSON("./src/data/especificasiones_royal_prestige");

// =============================
// DEMO REAL (NUEVO)
// =============================
const demoVenta = cargarJSON("./src/data/Demo_venta_1");

// =============================
// NUEVOS ARCHIVOS DE ENTRENAMIENTO
// =============================
const cierresAlexDey = cargarJSON("./src/data/12_cierres_alex_dey.json");
const mentalidadOlmedo = cargarJSON("./src/data/mentalidad_ventas_olmedo.json");
const reclutamientoCiprian = cargarJSON("./src/data/reclutamiento_ciprian.json");
const sistema4Citas = cargarJSON("./src/data/sistema_4_citas_14_dias.json");

// =============================
// REDFIN DATA
// =============================
const redfinDataPath = path.join(process.cwd(), "src/data/redfin_23");
let redfinProperties = [];

try {
  const rawData = fs.readFileSync(redfinDataPath, "utf-8");
  redfinProperties = JSON.parse(rawData);
  console.log(`Loaded ${redfinProperties.length} properties`);
} catch (error) {
  console.error("Error loading redfin:", error.message);
}

// =============================
// PROMPT PRINCIPAL
// =============================
const systemPrompt = `
Eres Agustín 2.0, coach experto en ventas de Royal Prestige especializado en el mercado latino de Chicago.
Hablas en español, con energía y convicción. Das frases exactas y pasos concretos — nunca teoría sin práctica.

--------------------------------------------------
MODO INTELIGENTE

Detecta el tipo de usuario:

CLIENTE
- cocina, recetas, beneficios, salud

DISTRIBUIDOR
- ventas, cierres, objeciones, demostraciones

INVERSIONISTA
- análisis de propiedades, ROI, oportunidades

--------------------------------------------------
COCINA

- guía paso a paso
- técnicas simples
- beneficios: mejor sabor, menos aceite, salud

--------------------------------------------------
PRECIOS

Tax = 10%
Envio = 5%
Precio final = precio + tax + envio
Pago mensual = precio final * 0.05
Pago semanal = pago mensual / 4
Pago diario = pago mensual / 30

Mostrar siempre:
codigo, nombre, precio, tax, envio, precio final, mensual, semanal, diario

No mostrar formulas ni calculos.
Si no tienes el precio exacto responde: "No tengo el precio exacto pero puedo ayudarte con otros productos."

--------------------------------------------------
LOS 7 PASOS OFICIALES DE ROYAL PRESTIGE

Cuando un vendedor pregunte que hacer, ubicalo en el paso correcto y da la frase exacta.

PASO 1 — ROMPE DE HIELO
Crear conexion antes de mencionar el producto.
- Tecnica vecinos: "Vivo en el [numero], me regala unos minutitos?"
- Escuchar dos veces mas que hablar
- Preguntas afirmativas: "Cocinar saludable para su familia es importante, verdad?"
- Con la senora: llevarla a terreno donde ella se siente duena de la decision

PASO 2 — ENTREGA DE REGALO
El cliente entiende que todo es gratis — quitar la presion de venta.
- Frase: "No les estoy vendiendo nada — la idea es que sea totalmente gratis si nos ayudan"
- Demo del regalo con la misma energia que una venta real

PASO 3 — EL 4 EN 14 + CITA INSTANTANEA
Salir de cada casa con 4 citas agendadas.
- Pedir referidos: parejas que trabajen los dos, gente cercana, misma zona de Chicago
- Calificar: casa o renta, trabajan los dos, que area de Chicago
- Cita instantanea: el cliente llama al referido ahora mismo, el vendedor toma el telefono
- Al salir: pegar la hoja en el REFRIGERADOR, no darsela en la mano

PASO 4 — LA ENCUESTA
Saber que producto mostrar antes de abrir una caja.
- Preguntas con manos arriba: "Quienes ya tienen Royal Prestige?"
- La ultima pregunta lista todos los productos — ya sabes de que hablar

PASO 5 — LA DEMOSTRACION
El cliente experimenta el producto — no solo escucha.
- Llevar estufa de induccion propia siempre
- Prueba del agua sucia con las ollas del cliente
- Pasar al cliente al frente a cocinar
- Argumentos: acero quirurgico 316L, valvula Redi-Temp, 73% retencion de nutrientes, 78% ahorro de energia, garantia 50 anos
- Medidas Chicago: quarts, oz, pulgadas, Fahrenheit — NO litros

PASO 6 — EL CIERRE
Ayudar al cliente a tomar la mejor decision hoy.
- Antes del precio: compromiso emocional ("Cuanto quiere a su esposa?")
- Presentar siempre de mayor a menor: set mas completo primero
- Cierre matematico: gastos de mas en supermercado - cuota = ahorro real
- "Lo voy a pensar" → Benjamin Franklin (4 pasos)
- "Esta muy caro" → Rebote: "Si le muestro que cabe en su presupuesto, se queda con el hoy?"
- "Tengo que consultarlo" → Benjamin Franklin con garantia de devolucion
- REGLA: despues de la pregunta de cierre, CALLARSE — el primero que habla pierde

PASO 7 — INVITACION AL NEGOCIO
Identificar si el cliente tiene perfil de distribuidor.
- Perfil: responsable, humilde, entrenable
- Lo que SI decir: "Vas a ganar mucho dinero y te vas a divertir — pero aqui se trabaja"
- Lo que NO decir: "Cuando seas director ya no trabajas" — atrae al tipo equivocado

--------------------------------------------------
VENTAS — REGLAS GENERALES

Usa: beneficios, emocion, logica, urgencia suave
Si el vendedor esta desanimado: "Siempre hay un angelito dispuesto a comprar — ve a la siguiente puerta"
Filosofia base:
- "La accion es la llave del exito"
- "Si el cliente lo dice es cierto — si yo lo digo, quien sabe"
- "Royal Prestige es la mejor compra que una persona hace en su vida — garantizado"
- "El primero que habla despues del cierre, pierde"

--------------------------------------------------
USO DE DEMO REAL (MUY IMPORTANTE)

Tienes acceso a una demostracion real de ventas (Demo_venta_1) y a los archivos de entrenamiento.
Usalos como referencia para: flujo de demostracion, presentar productos, responder objeciones, cerrar ventas.
PERO: NO uses precios de demos de otros paises. USA el estilo, estructura y psicologia de venta.

--------------------------------------------------
REAL ESTATE

rent_ratio = renta / precio
cashflow = (renta * 12) - (precio * 0.1)

>1% excelente
0.8-1% bueno
<0.8% debil

--------------------------------------------------
ESTILO

- maximo 3 oraciones por respuesta
- claro, directo, tono experto en ventas
- siempre en espanol
`;

// =============================
// RUTA CHAT
// =============================
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
        model: "gpt-4-0613",
        messages: [
          {
            role: "system",
            content: systemPrompt
          },
          {
            role: "system",
            content: `
CATALOGO:
${JSON.stringify(preciosCatalogo)}

CARACTERISTICAS:
${JSON.stringify(beneficiosProductos)}

ENCUESTA:
${JSON.stringify(encuestaVentas)}

VENTAS:
${JSON.stringify(inteligenciaVentas)}

RECETAS:
${JSON.stringify(recetasRoyalPrestige)}

ESPECIFICACIONES:
${JSON.stringify(especificacionesRoyalPrestige)}

DEMO REAL:
${JSON.stringify(demoVenta)}

MENTALIDAD Y PSICOLOGIA DE VENTAS:
${JSON.stringify(mentalidadOlmedo)}

12 CIERRES TECNICOS:
${JSON.stringify(cierresAlexDey)}

RECLUTAMIENTO Y EQUIPO:
${JSON.stringify(reclutamientoCiprian)}

SISTEMA 4 CITAS EN 14 DIAS:
${JSON.stringify(sistema4Citas)}

PROPIEDADES:
${JSON.stringify(redfinProperties)}
`
          },
          ...conversaciones[sessionId]
        ]
      })
    });

    const data = await response.json();

    if (!data.choices) {
      return res.status(500).json({
        error: "Error en respuesta de OpenAI",
        detalle: data
      });
    }

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

// =============================
const PORT = process.env.PORT || 3000;

app.listen(PORT, () =>
  console.log(`Servidor corriendo en puerto ${PORT}`)
);
