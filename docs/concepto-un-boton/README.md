# TEC · concepto «un solo botón» (25-nov-2026)

Idea de Agustín: todo el negocio del vendedor se opera desde un solo botón en la pantalla.

## El concepto
- **Un botón.** Lo tocas y hablas. Sirve para grabar la demo, para prospectar y para hablar con Coach Jorge.
- **Jorge entiende qué fue.** No se eligen modos ni pestañas: por lo que oye, Jorge sabe si fue una demo, una plática de prospección o un recado para él.
- **Cada dato a su lugar.** Hoja del 4 en 14, Google Calendar, CRM y ventas, igual que hoy con la demo grabada.
- **Jorge pregunta solo lo que falta**, y se le contesta con el mismo botón.
- **Conexión guiada.** Un solo permiso de Google; TEC crea la hoja y el calendario; si falta algo, Jorge lo dice con un botón ahí mismo.

Imagen del concepto: `tec-un-boton.png` (se genera desde `tec-un-boton.html`).

## Lo que ya existe y sirve de base
Grabar la demo con la pantalla apagada, Siri, el micrófono de un toque, la tarjeta de preguntas de Jorge y el guardado automático (publicado el 25-nov-2026).

## Lo que falta para llegar al concepto
1. Una pantalla de inicio con solo el botón.
2. Que Jorge clasifique lo grabado (demo, prospección o recado).
3. Un solo permiso de Google con todo junto, y TEC armando la hoja y el calendario.
4. Revisar en la cuenta de Google si la aplicación está verificada; sin eso, los vendedores ven una advertencia al conectar.

## Ideas apartadas (hallazgos guardados, no se van a construir ahora)

### Grabar en ciclo, como las cámaras
Guardar solo las últimas horas en el teléfono, tomar muestras cortas para saber qué pasa (carro, plática ajena, prospección, demo) y, al detectar una demo, regresar a capturarla completa. Resuelve capacidad y ruido, y rescata la demo que se olvidó grabar. No resuelve el permiso: en Illinois, grabar una conversación privada requiere que todos lo sepan. Consultar a un abogado antes de un modo «todo el día». Falta medir la batería de una jornada.

### Aparato aparte
- **Bee (Amazon):** sin puerta pública para que otra aplicación reciba el audio o el texto. No sirve de base.
- **Looki L1:** sin verificar.
- **Omi:** unos 129 dólares, código abierto, dos micrófonos, 512 megas de memoria en anillo. Batería: sus páginas dicen 10–14 horas en una y 2–3 días en otra.
  - Camino A: su app manda el audio (cada pocos segundos) o el texto en vivo a una dirección de TEC. Solo en vivo, con el teléfono cerca, y el audio pasa por sus servidores.
  - Camino B: la app de TEC se conecta directo al aparato por Bluetooth. Sin terceros; más trabajo; no está documentado si funciona en segundo plano en iPhone ni cómo leer el audio guardado.
  - Su documentación no habla del consentimiento de las personas grabadas.

Fuentes: docs.omi.me (AudioStreaming, Integrations, Protocol, sdk/ReactNative, sdk/swift, hardware/OmiConsumer, apps/Submitting, info/Privacy), omi.me/pages/pendant, aboutamazon.com (Bee Frontier), techcrunch.com (Bee, 15-oct-2026), tech-insider.org (wearables y leyes de grabación).

## Prototipo web (25-nov-2026, listo y sin publicar)
- Dirección: `totalelitecoaching.com/boton` (abre la misma app en `/coach/app/?boton=1`; así comparte la sesión y no hace falta tocar el dominio). Solo cuentas del piloto.
- Una pantalla con un solo botón encima de la app de siempre (`public/coach/one-button.js`).
  - **Un toque:** hablas y tocas otra vez. Jorge decide (`/api/coach/boton/intent`, `src/coach-one-button.js`): si anuncias una visita, empieza a grabarla con la grabadora de siempre; si no, es un mensaje para él y contesta en pantalla y con voz. Si hay preguntas pendientes de una visita, tu respuesta las contesta.
  - **Mantener presionado:** empieza a grabar la visita de una vez, sin decir nada.
  - Mientras graba: reloj y un toque para terminar. Al quedar lista, Jorge dice qué guardó y qué le falta, ahí mismo.
  - «Abrir TEC completo» quita la pantalla del botón.
- Pruebas: `tests/coach-one-button.test.js`. Revisado en pantalla de teléfono con datos de ejemplo; sin probar con micrófono real.

## iPhone: lo que la web no puede hacer (propuesto, sin construir)
Dentro de la app TEC que ya tienes (misma firma; una app aparte exigiría una firma nueva):
1. Abrir directo en el botón.
2. Reloj de la visita en la pantalla bloqueada y en la isla dinámica, con «Terminar».
3. Botón de TEC en la pantalla bloqueada o el Centro de control para empezar sin abrir la app.
4. Botón de acción del iPhone y Siri (ya existen los atajos).
5. Vibración al empezar y al terminar.
