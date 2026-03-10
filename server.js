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
content:"Eres un experto en ventas que ayuda a cerrar ventas y manejar objeciones."
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
