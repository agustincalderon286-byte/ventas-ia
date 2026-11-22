# Coach TEC · Plan del nuevo cerebro del agente

Propuesta del 22 de noviembre de 2026, sobre la rama `codex/coach-hycite-piloto` en `825140a`. Es un plan para aprobar: todavía no se ha cambiado código. Complementa a `docs/coach-agente-piloto.md`.

## Resumen para Agustín

**Meta:** que cada vendedor tenga un asistente al que le habla con sus propias palabras y que haga el trabajo con sus cuentas: revisar la agenda, el correo, el 4 en 14 y los leads, recomendar a quién llamar, y dejar lista la cita, el correo o la nota para que él solo pulse «Confirmar».

**Qué cambia:** hoy el modelo elige una herramienta de un menú de siete y no ve el resultado. Con este plan el modelo trabaja en ciclo: pide un dato, lo lee, decide el siguiente paso y contesta con lo que encontró.

**Qué no cambia:** las conexiones de Google por vendedor, la pantalla del Coach, y la regla de que nada se envía, reserva ni guarda sin el botón del vendedor.

**Tres fases:**
1. El asistente **lee** con libertad (agenda, correo, 4 en 14, leads, ventas, contactos, trayectos).
2. El asistente **prepara cambios** dentro de la misma conversación (cita, correo, nota o resultado de llamada, alta, WhatsApp) y el vendedor confirma.
3. Memoria de la conversación, misma inteligencia para Jorge por voz y apertura a más vendedores.

**Decisiones que necesito de ti** (detalle al final): aviso de privacidad, presupuesto mensual por vendedor, permiso para correr la comparación de modelos con llamadas reales, y quién implementa.

## Diagnóstico

| Síntoma | Causa en el código |
| --- | --- |
| «Hay que decir la frase exacta» | Contactos, Trayectos, WhatsApp, Ofertas, Calendar y Gmail se activan con patrones de palabras (`coach-mail-intent.js`, `coach-followup-input.js`, `coach-wa-chat-input.js`). |
| «Dice que no tiene esa conexión» | El modelo debe elegir una sola herramienta de siete (`coach-plan-responses.js:94`, `tool_choice:'required'`, sin llamadas en paralelo). Si el pedido no cabe, el servidor responde con la lista de conexiones (`coach-conversation-plan.js:201`). |
| «Contesta antes de revisar» | Si ningún agente reclama el mensaje, cae en el chat general, que no tiene herramientas. |
| No encadena pasos ni entiende «¿y el segundo?» | El modelo nunca recibe el resultado de la herramienta; el servidor arma la respuesta con textos fijos. Tampoco recibe el historial de la conversación. |
| Solo funciona bien en dos cuentas | La lista de vendedores con modelo está escrita en `coach-agent-pilot.js:5`. |

## Diseño

### El ciclo

Módulo nuevo `src/coach-agent-loop.js`, sobre la misma API Responses que ya usa el piloto:

1. Recibe el mensaje del vendedor, el historial corto de la conversación y el estado de sus conexiones.
2. El modelo llama una o varias herramientas (`tool_choice:'auto'`; las lecturas pueden ir en paralelo).
3. El servidor ejecuta cada herramienta con la cuenta del vendedor y **devuelve el resultado al modelo**.
4. Se repite hasta que el modelo redacta la respuesta, con un tope de 6 pasos y 40 segundos por mensaje.
5. El servidor añade los botones de las propuestas pendientes y la línea de fuentes consultadas.

Se mantiene `store:false` (OpenAI no guarda la conversación).

### Herramientas de lectura (sin confirmación)

Todas reutilizan funciones que ya existen y están probadas.

| Herramienta | Función existente |
| --- | --- |
| `consultar_agenda` (día o rango) | `readCoachAgenda` (`coach-agenda.js`) |
| `buscar_horarios_libres` | `availableSlots` (`coach-calendar-slots.js`) |
| `leer_bandeja` (remitente, asunto, período, solo Hy Cite) | `gmail.inbox` |
| `leer_hilo` | `gmail.thread` |
| `revisar_ventas_hycite` (período) | `createPostSaleReader`, `gmail.notices` |
| `buscar_referidos` (nombre, anfitrión, estado, 4 en 14 o boletas) | `sheetTools.search` |
| `priorizar_llamadas` | `sheetTools.nextCall` con `shortlist` |
| `revisar_postventa` | `sheetTools.postSale` |
| `buscar_contactos` | servicio de `coach-contacts-connections.js` |
| `calcular_trayecto` | `createTrafficRoutes` (`coach-route-google.js`) |
| `estado_conexiones` | `agentConnectionReader` |

Las herramientas devuelven datos estructurados con identificadores opacos generados por el servidor. El modelo nunca recibe ni envía números de fila, identificadores de Google ni tokens.

### Herramientas de propuesta (preparan, no ejecutan)

| Herramienta | Función existente | Qué confirma el vendedor |
| --- | --- | --- |
| `proponer_cita` | `channels.appointment` | Botón «Confirmar cita»; Calendar vuelve a comprobar el hueco. |
| `proponer_correo` | `channels.email` | Botón «Enviar correo» con destinatario y texto exactos. |
| `proponer_nota` (incluye resultado de llamada y próxima fecha) | `sheetTools.prepareNote` | Botón «Guardar nota». |
| `proponer_alta` | `sheetTools.prepareAdd` (revisa duplicados) | Botón «Agregar prospecto». |
| `proponer_whatsapp` | `coach-wa-drafts.js` | El vendedor termina el envío en su WhatsApp. |
| `corregir_propuesta` | `merge` e `invalidate` del plan | Genera una versión nueva; los botones viejos caducan. |

Cada propuesta se guarda en el estado del plan (`coach-plan-store.js`) y se ejecuta únicamente con el botón `TECPLAN <versión> apply`, igual que hoy. **No existe herramienta de confirmar**: el modelo no puede enviar, reservar ni guardar por su cuenta.

### Reglas de veracidad

- La respuesta termina con las fuentes realmente consultadas en ese mensaje, calculadas por el servidor.
- «No está conectado» solo se puede decir cuando una herramienta devolvió ese error. Si el texto del modelo lo afirma sin ese respaldo, el servidor descarta la frase y vuelve a pedir la respuesta.
- Si una herramienta falla, el asistente dice cuál falló y por qué (sin conexión, sin permiso, Google no respondió), con el botón «Mis conexiones» cuando corresponda.

### Seguridad

- **Sesión:** se comprueba que la sesión siga siendo la del vendedor antes de cada herramienta y antes de responder (`current()`), como en el plan actual.
- **Órdenes ocultas:** el contenido de correos, hojas y contactos llega al modelo marcado como datos de terceros. Aunque un correo intente dar órdenes, lo máximo que puede provocar es una propuesta que el vendedor ve completa antes de confirmar.
- **Tamaño:** cada correo se recorta (texto plano, sin enlaces ni adjuntos, 2.000 caracteres) y cada búsqueda devuelve 12 resultados por página.
- **«No consultes…»:** `excludedReads` se aplica antes del ciclo y retira las herramientas de la fuente excluida.
- **Presupuesto:** reserva y liquidación por turno con el mismo patrón de `createLiveLedger` (`coach-live-jorge.js`), generalizado a un límite mensual por vendedor.

### Privacidad

Hoy el modelo recibe solo el mensaje del vendedor con teléfonos y correos enmascarados. Con el ciclo recibirá el contenido que lea para razonar. Medidas:

- Los teléfonos y direcciones de correo siguen enmascarados con marcadores que el servidor sustituye al responder.
- Se necesita un consentimiento del vendedor en Mis conexiones y una actualización del aviso de privacidad antes de activar la Fase 1 con datos reales.

### Reparto de mensajes

Para los vendedores con el ciclo activo, `dispatch` (`coach-connections.js`) queda así:

1. Botones con código (`TECPLAN …`, confirmar o cancelar correo, elegir horario) → ejecución directa, como hoy.
2. Todo lo demás → ciclo.
3. Si el modelo falla o se agota el presupuesto → camino actual (`offlineRead` y agentes por patrones) como respaldo.

Los demás vendedores siguen exactamente como hoy.

### Activación

- Variable `COACH_AGENT_LOOP_EMAILS` con la lista de vendedores. Se retira la lista escrita en `coach-agent-pilot.js`.
- El cliente no cambia en las fases 1 y 2: se conserva `coachPlanVersion: 1` y el formato de respuesta y botones. No hace falta regenerar el manifiesto sin conexión.

## Fases

### Fase 0 · Medición y preparación
- Ampliar `scripts/coach-agent-eval.mjs` de 20 a unas 60 expresiones: las mismas peticiones dichas de varias formas, con faltas de ortografía y dictado, pedidos de varios pasos, negaciones («no agendes nada») y correos ficticios con órdenes ocultas.
- Medir por modelo: herramienta correcta, respuestas falsas de «sin conexión», afirmaciones sin lectura, costo y tiempo.
- Generalizar el libro de presupuesto y mover la lista del piloto a configuración.
- **Aceptación:** línea base del sistema actual medida con el conjunto nuevo.

**Estado al 22 de noviembre de 2026:**
- Conjunto: 84 expresiones ficticias en 16 grupos (`scripts/coach-agent-cases.mjs`). Cada caso declara qué debe consultarse y qué debe quedar propuesto, sin depender de la arquitectura, así que sirve igual para el sistema actual y para el ciclo.
- Medidor: `scripts/coach-agent-bench.mjs`, con pruebas en `tests/coach-agent-bench.test.js`. No ejecuta lecturas ni cambios reales.
- Línea base de la capa de patrones, sin ningún modelo: **26 de 84 (31 %)**, cero acciones indebidas. Es lo que queda cuando el modelo falla. Contactos no se pudo medir en esta capa porque su detector no está exportado; sus 3 casos cuentan como no resueltos.
- Pendiente: línea base del selector actual con `gpt-6.2-sol` y `gpt-6-luna`. Necesita `OPENAI_API_KEY` en el entorno local (168 llamadas):

```bash
NODE_ENV=test node scripts/coach-agent-bench.mjs --system router --run
```

- El libro de presupuesto compartido y la lista del piloto en configuración pasan a la Fase 1, donde se usan por primera vez. La lista actual no se quita hasta confirmar que Render tiene las variables.

### Fase 1 · Lecturas con ciclo
- `coach-agent-loop.js`, las once herramientas de lectura, reglas de veracidad, respaldo y presupuesto.
- **Aceptación:** al menos 90 % de las expresiones de lectura resueltas, cero respuestas falsas de «sin conexión», cero datos afirmados sin lectura, y las pruebas actuales del Coach siguen en verde.
- Prueba local con las cuentas ficticias antes de cualquier publicación.

**Estado al 22 de noviembre de 2026: construida y apagada.** Se enciende por vendedor con `COACH_AGENT_LOOP_EMAILS` (vacía por defecto). No se ha probado contra el modelo real porque el entorno local no tiene `OPENAI_API_KEY`; todas las pruebas usan un modelo simulado.

- `src/coach-agent-loop.js`: ciclo sobre Responses, 6 pasos, 40 s, 4 herramientas por paso, argumentos validados contra el esquema.
- `src/coach-agent-tools.js`: 9 lecturas (`consultar_agenda`, `buscar_horarios_libres`, `leer_bandeja`, `leer_hilo`, `revisar_ventas_hycite`, `buscar_referidos`, `priorizar_llamadas`, `buscar_contactos`, `estado_conexiones`) y `pasar_al_coach`.
- `src/coach-agent-seller.js`: instrucciones, memoria de los 3 últimos intercambios (cifrada, 20 minutos), fuentes consultadas, corrección de afirmaciones sin respaldo y respaldo al flujo actual.
- `src/coach-agent-budget.js`: límite mensual por vendedor, `COACH_AGENT_LOOP_MONTHLY_MICROS` (5 USD por defecto).
- `src/coach-connections.js`: el ciclo va después de los botones y antes de Trayectos, Contactos y el plan.
- `src/coach-sheets-tools.js`: `search` acepta `detail` para devolver estado e historial.

Cambios respecto al diseño:
- **Privacidad más estricta que lo previsto.** El modelo recibe remitente, asunto y un extracto de cada correo, nunca el texto completo, y nunca recibe teléfonos ni correos de fichas o contactos. En su lugar recibe marcadores como `[[tel:ab12cd]]`; el servidor pone el teléfono, la dirección o el correo reales en la respuesta que ve el vendedor (`markerPattern` en `src/coach-agent-tools.js`). La memoria guarda la respuesta con marcadores y, aparte y cifrados, los datos de los últimos 80.
- **Trayectos, postventa y cambios de contactos se derivan** al flujo actual con `pasar_al_coach`, igual que el consejo de ventas y toda petición de preparar o guardar algo. Sus flujos dependen de varias preguntas encadenadas y entran en la Fase 2.
- **Con un borrador, una pregunta o una propuesta abierta el ciclo no interviene**, para no invalidar los botones de confirmación.
- **La memoria corta se adelantó de la Fase 3**, porque sin ella no entiende «¿y pasado mañana?».
- Tras leer la bandeja, el ciclo entrega la misma ficha que el agente de Gmail, así que «Responde al primero» sigue funcionando.

**Consentimiento (hecho el 22 de noviembre de 2026).** `src/coach-agent-consent.js` guarda el permiso por vendedor con versión y fecha. Mis conexiones muestra la sección «Asistente que consulta tus cuentas» solo a los correos de `COACH_AGENT_LOOP_EMAILS`, con los botones «Permitir…» y «Retirar el permiso» (`/api/coach/conexiones/asistente/`). Sin permiso vigente el ciclo no se ejecuta y el Coach responde como hoy. La política de privacidad (`public/coach/privacidad/index.html`) describe qué se envía a OpenAI y lleva la fecha del cambio. Si cambia lo que las herramientas entregan al modelo, hay que actualizar ese texto y subir `CONSENT_VERSION`.

**Publicado y encendido el 22 de noviembre de 2026** para las dos cuentas del piloto, por orden de Agustín y sin medición previa con el modelo real. En uso real respondió con datos de la hoja.

**Trabajo con leads (22 de noviembre de 2026).** A partir del uso real se añadió:
- `leads_por_zona`: sin zonas devuelve las ciudades y códigos postales de la hoja con su número de fichas; con zonas devuelve las fichas en ese orden y, dentro de cada zona, por urgencia. El modelo ordena las zonas por cercanía con su conocimiento de geografía. Es una aproximación por ciudad, no una distancia medida; la distancia real exigiría geocodificar direcciones con Google Maps.
- `buscar_referidos` busca también en las notas (`notas`) y tolera nombres incompletos o con una letra distinta cuando no hay coincidencia exacta. La tolerancia solo existe en lecturas: los cambios siguen exigiendo el nombre exacto.
- El modelo recibe ahora la ciudad o el código postal de las fichas, por lo que `CONSENT_VERSION` subió a 2 y cada vendedor debe volver a dar su permiso.

Pendiente: la medición con el modelo real.

```bash
NODE_ENV=test node scripts/coach-agent-bench.mjs --system loop --run
```

### Fase 2 · Propuestas dentro del ciclo
- Las seis herramientas de propuesta y la corrección de borradores por conversación.
- **Aceptación:** cero escrituras sin botón en todo el conjunto de evaluación, incluidos los casos de órdenes ocultas; cita, correo, nota y alta completados con cuentas ficticias.

**Estado (22 de noviembre de 2026): primera parte hecha en el código, sin publicar y sin medir con el modelo real.**

El asistente ya deja listas cuatro propuestas. Ninguna herramienta escribe: la propuesta se guarda en el mismo registro y con la misma forma que el plan de conversación (`initialPlan` y `planView` de `src/coach-conversation-plan.js`), y el botón `TECPLAN … apply` la confirma allí, con las comprobaciones de siempre.

| Herramienta | Qué prepara | Botón |
| --- | --- | --- |
| `preparar_nota` | Resultado de una llamada o visita en el historial de la ficha y su próximo seguimiento. | Guardar nota |
| `preparar_cita` | Cita, demostración o llamada en «Seguimientos TEC», con el horario comprobado. | Confirmar cita |
| `preparar_alta` | Un referido con su anfitrión o una boleta. Una persona por propuesta. | Agregar prospecto |
| `preparar_mensaje` | Mensaje de texto para alguien de la hoja. No pasa por el plan: el chat muestra el texto y un enlace `sms:` que abre Mensajes en el teléfono con el número y el texto puestos; el vendedor lo envía. | Abrir Mensajes |

Decisiones de esta parte:
- **Referencias de ficha estables.** Cada ficha llega al modelo con una `ref` derivada de su identidad (`f3a9c1`), igual en todas las búsquedas y turnos. Las herramientas de propuesta solo aceptan una `ref` que este vendedor ya leyó, y vuelven a leer la fila en ese momento (`sheetTools.card`), así que respetan «no llamar» y la versión vigente.
- **Una propuesta trata de una sola persona** y puede llevar su nota y su cita juntas. Un alta va sola. Otra persona en el mismo turno se rechaza; en un turno nuevo sustituye a la propuesta anterior.
- **El asistente sigue atendiendo su propia propuesta** (marca `agentLoop` en el estado): la corrige por conversación («mejor a las 5») y la mantiene vigente tras una lectura. Se aparta mientras hay una confirmación en curso o una que quedó sin resultado claro, y ante cualquier propuesta del flujo existente.
- **El modelo no puede dar nada por hecho.** Si su texto dice que guardó, agendó o envió algo, se le corrige una vez; si insiste, la propuesta se entrega con un texto neutro. Sin ninguna herramienta, esa afirmación no se entrega.
- **Los correos siguen en el flujo existente** (`pasar_al_coach`). Su redactor recibe la dirección del destinatario, y el permiso del asistente dice que los correos de las fichas no se envían al modelo. Para traerlos al ciclo hay que redactar sin la dirección o cambiar el texto del permiso y subir `CONSENT_VERSION`.
- **El permiso no cambió de versión**: el modelo recibe los mismos datos que antes. Se actualizó el texto de Mis conexiones y de la política de privacidad, que decían «solo lee».

Pendiente de la Fase 2: correos dentro del ciclo, varias altas a la vez y la hoja del 4 en 14 por foto, WhatsApp, contactos, trayectos y postventa; los casos de propuesta en `scripts/coach-agent-bench.mjs` (su ejecutor ficticio aún no conoce las herramientas nuevas); y probar el enlace de Mensajes en un teléfono real.

### Fase 3 · Continuidad y alcance
- Historial corto cifrado por sesión para que entienda «el segundo», «ese mismo» y «mándaselo también a ella».
- Jorge en vivo usa el mismo ciclo, de modo que voz y texto responden igual.
- Apertura gradual a más vendedores y, cuando Meta esté listo, WhatsApp Business como una herramienta más. Konverta sigue siendo manual.

## Archivos

| Archivo | Cambio |
| --- | --- |
| `src/coach-agent-loop.js` | Nuevo: ciclo, topes y veracidad. |
| `src/coach-agent-tools.js` | Nuevo: catálogo de herramientas sobre las funciones existentes. |
| `src/coach-connections.js` | `dispatch` envía al ciclo a los vendedores activados. |
| `src/coach-conversation-plan.js` | Expone el estado y la vista de propuestas para que el ciclo las use; la confirmación no se toca. |
| `src/coach-plan-responses.js` | Se conserva `excludedReads`; el selector de una herramienta queda como respaldo. |
| `src/coach-agent-pilot.js` | La lista de vendedores pasa a configuración. |
| `src/coach-live-jorge.js` | Se extrae el libro de presupuesto a un módulo compartido. |
| `scripts/coach-agent-eval.mjs`, `tests/coach-agent-*.test.js` | Evaluación ampliada y pruebas del ciclo. |

No se tocan: OAuth, `coach-plan-channels.js`, el envío de Gmail ni la captura del 4 en 14. En `coach-sheets-tools.js` solo se añadieron lecturas (`byZone`, `card`, búsqueda con detalle); las escrituras no cambiaron.

## Decisiones para Agustín

1. **Privacidad:** ¿apruebas que el asistente lea el contenido de correos y hojas del vendedor para responder, con consentimiento en Mis conexiones y aviso actualizado?
2. **Presupuesto:** límite mensual por vendedor para el asistente de texto. La Fase 0 entrega el costo medido por mensaje para fijarlo con datos.
3. **Comparación de modelos:** la evaluación necesita llamadas reales a la API, con costo. ¿La autorizas, y contra qué segundo modelo?
4. **Implementación:** ¿la hace este agente, Codex, o se reparte por fases? Dos agentes no deben editar los mismos archivos a la vez.
5. **Publicación:** cada fase se publica solo con tu orden expresa.
