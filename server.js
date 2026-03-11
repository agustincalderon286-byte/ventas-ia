import express from "express";
import cors from "cors";

const app = express();

app.use(cors());
app.use(express.json());

app.post("/chat", async (req, res) => {

const pregunta = req.body.pregunta;

const response = await fetch("https://api.openai.com/v1/chat/completions",{
method:"POST",
headers:{
"Content-Type":"application/json",
"Authorization":`Bearer ${process.env.OPENAI_API_KEY}`
},
body:JSON.stringify({
model:"gpt-5-mini",
messages:[
{
role:"system",
content:"Eres un asistente en ventas Royal Prestige que ayuda a cerrar ventas y manejar objeciones tu nombre es Agustin 2.0 Reglas:
1. Responde **máximo 2 oraciones**.
2. Si el usuario pregunta precio, busca el producto en la lista de precios.
3. **Cálculo de precios:**
   - Precio regular = (Precio + Recargo Arancelario) * 4
   - Precio con descuento = (Precio + Recargo Arancelario) * 3.5
   - Tax = 10% del precio
   - Envío = 5% del precio
   - Precio final = precio + tax + envío
   - Pago mensual = Precio final * 0.05
4. Si no encuentras el producto, responde: "No tengo el precio exacto, pero puedo ayudar con otros productos".

Lista de precios (JSON):
${JSON.stringify(precios)}"
},
{
role:"user",
content:pregunta
}
]
})
});

const data = await response.json();

res.json(data);

});

app.listen(3000, ()=>{
console.log("Servidor funcionando");
});
