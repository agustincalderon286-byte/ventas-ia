# Coach «prender y apagar»: auditoría y plan (25-nov-2026)

Meta de Agustín: el vendedor solo prende y apaga la grabación de la demo. Todo llega solo a su lugar
(hoja 4 en 14, Google Calendar, CRM, encuesta, ventas). Coach Jorge entiende el contexto, pregunta solo
lo importante, las preguntas se ven mientras el vendedor contesta con un audio, y la respuesta se guarda
sola. «Nadie quiere el taladro, solo el agujero.»

## Auditoría: qué cumple hoy

Cumple: grabación con pantalla apagada (iPhone/Android), Siri, transcripción en la Mac, referidos a la
hoja, seguimiento y cita instantánea a Calendar, avisos de Jorge durante la demo. Sin publicar todavía:
anfitrión automático, citas con detalles, fecha/ciudad/relación en la hoja.

No cumple (evidencia en el código):

### A. El dato se queda a medio camino
1. La venta nunca se registra desde la grabación. `closeFacts.outcome/saleAmount` solo son sugerencias del
   diálogo «Cierre de la visita»; `confirm` (src/coach-demo-close.js:92-110) solo escribe `closeConfirmed`;
   el `CoachDemoOutcome` real lo crea el navegador (public/coach/demo-close-tools.js:147-180) con ficha
   abierta, formulario y casilla.
2. La encuesta no se guarda si el anfitrión se reconoció después: `createDemoSurveyWriter`
   (src/coach-demo-survey.js:101-110) exige `closeCrmId` y `autoClose.crmOwner`.
3. El cierre automático no toca el CRM del anfitrión (estado, próxima acción, cita, notas). `rememberEvent`
   no llama a `syncLead` (src/coach-followup-store.js:196-198).
4. Los referidos solo van a la hoja: no cuentan en «Mi día» (src/coach-my-day.js:74-83).
5. La cita instantánea no marca Estado «Cita» ni «Próxima llamada» en la hoja, ni fila en «Agenda»: el
   telemarketing puede llamar a quien ya tiene demo.
6. Lo no guardado se pierde: `report.autoClose` se borra tras guardar (server.js `finishLocalCoachDemo`);
   no hay reintento si faltaba la conexión de Google, y las conexiones se congelan al iniciar la grabación
   (server.js ~32229; src/coach-demo-auto-close.js saveCalendar/saveReferrals comparan el `grant`).

### B. Jorge no cierra el círculo
7. Jorge solo pregunta precio, seguimiento y tres campos de encuesta (`closeDoubts`,
   src/coach-demo-close.js:30-43). Referido dudoso/incompleto y cita instantánea dudosa nunca se preguntan,
   aunque el recibo dice «Coach Jorge te lo pregunta en el chat» (public/coach/app.js ~17036).
8. La respuesta del vendedor solo llena `closeDraft` (`clarify`, :133-135) y contesta «Revisa el cierre y
   elige qué guardar». Nada llega a hoja/calendario/CRM sin formularios.
9. Máximo 2 preguntas a la vez, en burbuja de chat que se pierde al hablar.
10. El micrófono es «mantener presionado», máximo 90 s, y se borra si se bloquea la pantalla
    (public/coach/coach-voice.js:3,267,379,515).
11. Mientras hay un cierre pendiente, cualquier mensaje va a `/clarify`; una pregunta distinta recibe «No
    identifiqué un dato nuevo… di «ahora no»» (public/coach/demo-close-chat.js:95-111).

### C. No es todavía «un botón»
12. Tres toques y la casilla de consentimiento se borra cada vez (app.js:17120, 17792, 17928).
13. Tope de 90 minutos en teléfono y servidor (app.js:17098; server.js ~32342).
14. Si el análisis falla no se reintenta (src/coach-demo-local.js:145-147); intentos máx. 2; trabajos
    caducan a las 24 h; arrendamiento de 30 min que una demo larga puede rebasar.
15. Una sola Mac sin vigilancia: si está apagada o el disco externo no está montado, nadie se entera.
16. No hay aviso cuando el resultado está listo; solo se revisa con la app abierta (app.js:18073-18078).
17. Solo funciona para cuentas con Torre de Control y en `COACH_DEMO_LOCAL_EMAILS` (server.js ~32222).

### D. El vendedor no ve «mis citas, mis ventas, mis leads»
18. «Mi día» solo muestra citas de hoy; ventas solo de `CoachDemoOutcome` confirmado; leads solo de TEC.

### E. Jorge en el chat general
19. Error: `matches()` no acepta `number` (src/coach-agent-loop.js:13-16), así que `preparar_resultado_cita`
    siempre falla con `argumentos_invalidos`.
20. Respuestas de callejón: «No encontré una herramienta…», «Prueba «Agenda una llamada…»», formatos
    «AAAA-MM-DD» (src/coach-conversation-plan.js:211-221; src/coach-calendar-agent.js:11,37;
    src/coach-followup-input.js:174-183).
21. El asistente nunca escribe sin botón (decisión del 22-nov; choca con la filosofía nueva).

## Diseño: una sola «visita» con un solo camino de guardado

Hoy hay dos sistemas paralelos (cierre automático y formulario «Cierre de la visita»). El plan los une:

- **Visita**: el servidor arma, tras el análisis, anfitrión, resultado, venta, seguimiento, referidos,
  citas instantáneas, encuesta, productos. Cada dato tiene estado: `guardado`, `pregunta`, `en espera`
  (falta conexión) o `descartado`.
- **Pendientes de la visita** (nuevo, cifrado, caduca a los 14 días): guarda lo que no se pudo guardar,
  con su evidencia. Habilita reintentos, las preguntas de Jorge y aplicar las respuestas.
- **Un aplicador** (`applyVisit`): la única función que escribe a hoja, Calendar, CRM, encuesta y
  resultado. La usan el cierre automático, el reintento y las respuestas a Jorge. Idempotente (mismos
  `eventId`/marcadores de hoy).
- **Preguntas**: salen de los pendientes, ordenadas por importancia, todas visibles en una tarjeta fija.
  Un audio puede contestar varias; la IA reparte la respuesta por pregunta y el aplicador guarda.

## Plan de trabajo (todo se publica junto al final)

### Bloque 1 · Base
- `src/coach-visit-pending.js`: almacén cifrado de pendientes (modelo propio, TTL 14 días).
- `finishLocalCoachDemo`: tras reconocer al anfitrión, ligar la grabación a su cliente (`closeCrmId`,
  `autoClose.crmOwner`) para que encuesta y cierre lo usen. Anfitrión sin teléfono: pregunta de Jorge.
- Conexiones de Google: usar la conexión vigente al terminar si es la misma cuenta; si falta, estado
  «en espera» y reintento automático al conectar (ruta de conexión llama al reintento).
- Reintento del análisis: 3 intentos con espera; arrendamiento renovado por pedazo; trabajos 72 h.

### Bloque 2 · Cada dato a su lugar
- Anfitrión → CRM: último contacto, nota de la visita (productos y precios), próxima acción y cita
  (`syncLead`), estado según resultado.
- Seguimiento y cita instantánea → Calendar (ya) + hoja: Estado «Cita», «Próxima llamada» y fila en
  «Agenda» cuando la hoja es la Hoja TEC.
- Referidos → hoja (ya) + conteo en «Mi día» (registro de programa 4 en 14 en TEC, sin duplicar).
- Encuesta → cliente reconocido.
- Resultado → `CoachDemoOutcome` desde el servidor: «seguimiento» y «no venta» solos; «venta» según la
  decisión de Agustín (ver Decisiones).

### Bloque 3 · Jorge pregunta y guarda
- Preguntas nuevas: referido con teléfono dudoso, referido incompleto, cita instantánea dudosa, anfitrión
  no reconocido, venta, hoja/calendario sin conectar.
- Tarjeta fija «Jorge necesita N datos» sobre el chat, visible mientras se graba el audio.
- Micrófono de un toque para empezar y otro para enviar, hasta 3 minutos, que no se borra al bloquear.
- `answerVisit`: la IA lee todas las preguntas y el audio, reparte respuestas, valida (teléfono completo,
  fecha que cuadra) y el aplicador guarda. Contesta «Guardé: …» y deja solo lo que siga en duda.
- Un mensaje que no contesta nada pasa al chat normal; «después» pospone sin frases exactas.
- Quitar del recibo lo que hoy promete y no cumple.

### Bloque 4 · Un botón de verdad
- Inicio en 2 toques: «Grabar demo» → «Ya avisé que voy a grabar, empezar». Se conserva el aviso porque
  grabar una conversación privada requiere el consentimiento de los presentes.
- Hasta 3 horas (45 pedazos, tope de texto 300 000, índice de pedazo ya admite 2 dígitos).
- Vigilancia de la Mac: latido del trabajador; si calla 15 min con demos en cola, aviso a Agustín; los
  reportes finales van antes que la revisión en vivo cuando hay cola.
- Aviso «Tu demo está lista» (web push existente; en las apps, aviso local al volver a abrir y mientras
  la app siga viva).
- Recuperación sola: `local-error` y `needs-review` se reintentan sin botones cuando es seguro.

### Bloque 5 · Lo que ve el vendedor
- Tarjeta de resultado en lenguaje llano al abrir TEC: «Visita con María López: 3 referidos en tu hoja ·
  cita el sábado 10:00 · encuesta guardada · Jorge necesita 2 datos».
- «Mi actividad»: próximas citas (14 días, no solo hoy), ventas y leads de la semana contando referidos.

### Bloque 6 · Jorge en el chat
- Arreglar `number` en `matches()` y cubrirlo con una prueba que pase por `readArguments`.
- Sustituir las respuestas de callejón por una pregunta útil o por lo más cercano que sí puede hacer.
- Escritura directa según la decisión de Agustín.

### Bloque 7 · Prueba de campo
- Guion con datos ficticios (555-01xx): demo completa de 15 min con anfitrión nuevo, 3 referidos (uno
  dudoso), cita instantánea, seguimiento, venta; luego audio de respuesta a Jorge.
- Lista de comprobación: hoja, Calendar, CRM, «Mi actividad», avisos.
- iPhone: volver a firmar antes del 28-nov por la noche. Android 0.4.1 por USB.

## Decisiones de Agustín pendientes
1. Venta: ¿se registra sola o Jorge pide un «sí»? (Recomendado: un «sí», porque se anuncia al equipo.)
2. Mac apagada: ¿esperar con aviso, o pasar a transcribir en la nube tras 2 horas? (Recomendado: esperar
   con aviso; la nube no hace la segunda lectura de cifras.)
3. Chat de Jorge: ¿guardar directo en su hoja y calendario sin botón? (Recomendado: sí para sus propios
   datos; botón solo para mensajes a clientes y para borrar.)
4. Piloto de campo: ¿solo Agustín y Rigoberto, o también los vendedores de prueba?
