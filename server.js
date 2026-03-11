import express from "express";
import cors from "cors";
import fs from "fs";

const app = express();
app.use(cors());
app.use(express.json());

// Cargar lista de precios
const precios = JSON.parse(fs.readFileSync("./src/data/lista_de_precios.json", "utf8"));

// Historial de conversación en memoria
let historial = [];

// Función para buscar producto y calcular precios
const calcularPrecio = (nombreProducto) => {
  const producto = precios.find(
    p => p.Descripcion.toLowerCase() === nombreProducto.toLowerCase()
  );
  if (!producto) return null;

  const precioBase = producto.Precio + producto["Recargo Arancelario"];
  const precioRegular = precioBase * 5;
  const precioDescuento = precioBase * 4.5;
  const tax = precioRegular * 0.10;
  const envio = precioRegular * 0.05;
  const precioFinal = precioRegular + tax + envio;
  const pagoMensual = precioFinal * 0.05;
  const pagoSemanal = pagoMensual / 4;

  return {
    precioRegular,
    precioDescuento,
    tax,
    envio,
    precioFinal,
    pagoMensual,
    pagoSemanal
  };
};

// Función para sugerir cierres de venta
const sugerirCierre = (respuestaCliente) => {
  const texto = respuestaCliente.toLowerCase();
  if (texto.includes("está bien") || texto.includes("sí") || texto.includes("me interesa")) {
    return "Cierre de presunción: Perfecto, lo ponemos en su cocina para que empiece a disfrutarlo mañana.";
  }
  if (texto.includes("no sé") || texto.includes("pensarlo")) {
    return "Cierre de prueba: Si pudiera pagarlo esta semana sin afectar su bolsillo, ¿lo tomaría?";
  }
  if (texto.includes("muy caro")) {
    return "Cierre de alternativa: ¿Prefiere pagarlo en efectivo o plan de pagos?";
  }
  if (texto.includes("lo pensaré")) {
    return "Cierre de resumen: Recuerde que este producto le ahorra tiempo, cocina saludable y protege a su familia… ¿lo llevamos hoy?";
  }
  if (texto.includes("tal vez") || texto.includes("próxima semana")) {
    return "Cierre por urgencia: Solo tenemos unas unidades disponibles esta semana, ¿lo asegura ahora?";
  }
  return "";
};

// Endpoint para chat
app.post("/chat", async (req, res) => {
  const pregunta = req.body.pregunta;

  // Detectar si la pregunta menciona un producto
  const nombresProductos = precios.map(p => p.Descripcion.toLowerCase());
  const productoMencionado = nombresProductos.find(nombre =>
    pregunta.toLowerCase().includes(nombre)
  );

  let textoParaGPT;
  if (productoMencionado) {
    const calculos = calcularPrecio(productoMencionado);
    textoParaGPT = `El producto "${productoMencionado}" tiene:
- Precio regular: $${calculos.precioRegular.toFixed(2)}
- Precio con descuento: $${calculos.precioDescuento.toFixed(2)}
- Pago mensual: $${calculos.pagoMensual.toFixed(2)}
- Pago semanal: $${calculos.pagoSemanal.toFixed(2)}

Responde máximo 2 frases de forma clara y profesional.`;
  } else {
    textoParaGPT = `No tengo el precio exacto del producto, pero puedo ayudar con otros productos.`;
  }

  // Generar sugerencia de cierre
  const cierre = sugerirCierre(pregunta);
  const respuestaFinal = cierre ? textoParaGPT + "\n\n" + cierre : textoParaGPT;

  // Guardar en historial
  historial.push({ role: "user", content: pregunta });
  historial.push({ role: "assistant", content: respuestaFinal });

  res.json({ respuesta: respuestaFinal });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor corriendo en puerto ${PORT}`));
