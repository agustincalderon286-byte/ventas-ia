# Coach TEC · Agente del vendedor (piloto)

Nota de continuidad actualizada el 22 de noviembre de 2026 tras la auditoría del agente. Base: rama `codex/coach-hycite-piloto`, revisión `825140a`. La rama de despliegue es `codex-metalworks-crm-deploy` (servicio de Render `cmwf-crm-api`). Para publicar se necesita una orden expresa de Agustín.

## Objetivo

Cada vendedor de Royal Prestige pasa del entrenamiento a la acción desde su propio Coach en totalelitecoaching.com. Le pide al Coach preparar el siguiente paso (correo, cita, nota, alta de un prospecto o WhatsApp), revisa exactamente qué va a ocurrir y lo confirma. Cada vendedor conecta sus propias cuentas en **Mis conexiones** (`/mis-conexiones/`) y conserva el control de sus mensajes, citas y hojas. Nada se envía, reserva ni guarda sin su confirmación explícita.

## Conexiones

### Activas en el piloto
- **Gmail por vendedor** (`gmail.readonly`; `gmail.send` opcional y aparte con `enable-send`): lectura de la bandeja, clasificación de Hy Cite (ventas confirmadas frente a avisos que requieren revisión), respuesta en el mismo hilo, edición del borrador y confirmación de una versión concreta antes del envío.
- **Google Calendar** (`calendar.freebusy`, `calendar.app.created`): consulta la disponibilidad y «Seguimientos TEC», propone dos opciones de demo y vuelve a comprobar el hueco al confirmar.
- **Gmail + Calendar (ofertas)**: ofrece dos horarios por correo y revisa la respuesta en el mismo hilo. **Un horario ofrecido no está reservado**: la reserva requiere que el vendedor la confirme.
- **Google Sheets** (`drive.file`): hoja personal de boletas y referidos elegida por el vendedor; búsqueda, notas, altas con revisión de duplicados y priorización de llamadas del 4 en 14.
- **Google Contactos** (`contacts`): búsqueda y preparación de cambios en los contactos del vendedor.
- **Trayectos** (Google Routes): comprueba tiempos entre visitas con un tope mensual de consultas.
- **4 en 14 en TEC**: captura personal de la hoja (formulario o foto revisada), guardada por cuenta y exportable en CSV. Konverta se abre aparte, sin intercambio de datos.
- **WhatsApp personal**: el Coach prepara el texto y el vendedor termina el envío en su propio WhatsApp.
- **Jorge en vivo**: conversación por voz con lectura de correo y 4 en 14, con presupuesto por vendedor (5 USD compartidos entre sus sesiones).

### En pausa
- **WhatsApp Business por Meta Cloud (envío directo)**: el código existe (`coach-wa-cloud*.js`), pero el alta quedó en pausa por decisión de Agustín mientras registra el negocio. Solo se activa si existen las variables `COACH_WA_CLOUD_*`. No reactivarla sin su orden.

## Cómo decide el agente

`registerCoachConnections` (`src/coach-connections.js`) reparte cada mensaje en este orden:

1. **Botones con código y correcciones de un borrador abierto** van directo a Gmail o WhatsApp (`directDraftInput`).
2. **Contactos y Trayectos**, si reconocen la petición.
3. **Planificador conversacional** (`src/coach-conversation-plan.js`). Para los vendedores del piloto, un modelo de OpenAI (`COACH_AGENT_PILOT_MODEL`, por defecto `gpt-6.2-sol`) solo **elige una herramienta** (`src/coach-plan-responses.js`): `consultar_agenda`, `leer_bandeja`, `revisar_ventas`, `priorizar_referidos`, `preparar_plan`, `aclarar` o `derivar`. No existe herramienta para confirmar, y el modelo recibe solo el estado de la propuesta, no el contenido de los correos ni de las hojas.
4. **Agentes especializados**: Sheets → WhatsApp → Ofertas → Calendar → Gmail.

Los cambios solo se ejecutan con el botón `TECPLAN <versión> apply <pasos>`. Antes de ejecutar se comprueba que la versión siga vigente, que la sesión no haya cambiado, que la conexión de Sheets sea la misma y que el contacto siga existiendo; el doble clic ejecuta una sola vez. Si el modelo no responde, solo quedan disponibles lecturas simples de agenda, bandeja y ventas (`offlineRead`).

## Mapa del código

| Archivo | Papel |
| --- | --- |
| `src/coach-connections.js` | Rutas de Mis conexiones (OAuth, sincronización, desconexión, perfil de WhatsApp) y reparto del mensaje entre agentes. |
| `src/coach-chat-pending.js` | Una ficha pendiente por canal (`coachMailPending`, `coachCalendarPending`, `coachWhatsAppPending`) más `coachDraftFocus`. |
| `src/coach-conversation-plan.js` | Propuestas combinadas de Sheets, Calendar y Gmail, con selección y confirmación por pasos. |
| `src/coach-plan-responses.js` | Selección de herramienta con Responses, esquema estricto y respeto de «no consultes…» (`excludedReads`). |
| `src/coach-plan-channels.js` | Ejecución confirmada de la cita y del correo del plan. Impide que el texto diga que la cita ya quedó reservada. |
| `src/coach-agent-pilot.js` | Vendedores con modelo, estado de conexiones y lecturas sin modelo. |
| `src/coach-mail-agent.js` | Consulta, borrador, revisión y envío confirmado de Gmail. |
| `src/coach-offer-agent.js` | Correo → dos opciones → respuesta del prospecto → confirmación de la cita. El correo del prospecto solo se compara con las opciones y nunca da órdenes al Coach. |
| `src/coach-calendar-agent.js` | Búsqueda de horarios, propuesta y reserva con nueva comprobación. |
| `src/coach-sheets-*.js` | Conexión, lectura y escritura revisada de la hoja personal. |
| `src/coach-contacts-*.js`, `src/coach-route-*.js` | Contactos de Google y trayectos entre visitas. |
| `src/coach-referral-photo.js`, `public/coach/referral-sheet*.js` | Lectura de la foto del 4 en 14, revisión y CSV. |
| `src/coach-live-jorge.js` | Jorge en vivo y su presupuesto por vendedor. |

## Cómo correr las pruebas

Se necesita Node 24 (`engines`). Desde la raíz del repositorio:

```bash
NODE_ENV=test node --test tests/coach-*.test.js
```

Resultado del 22 de noviembre de 2026 en `825140a`: **117 archivos, 1.334 pruebas, 1.334 aprobadas y 0 fallidas**.

Suite completa del repositorio:

```bash
npm test
```

## Pendientes conocidos

- La prueba real entre los dos vendedores del piloto (envío y reserva reales) la lleva Codex.
- Los correos del piloto están escritos en el código (`src/coach-agent-pilot.js` y valores por defecto de `connectionConfig`). Sumar un vendedor exige cambiar el código y publicar; conviene moverlos a variables de entorno.
- 4 en 14: se aceptan teléfonos de 7 dígitos y el teléfono del anfitrión puede repetirse como referido.
- En la tarjeta de hoja guardada, los datos resumidos (`.health-survey-folder-chip`) tienen poco contraste.

## Reglas de trabajo

- Las revisiones se prueban con cuentas y textos ficticios. Las credenciales y los datos reales permanecen en sus servicios: no se copian al repositorio ni a esta nota.
- La prueba real entre los dos vendedores del piloto la sigue Codex. No intervenir en ese hilo ni en su reserva.
- Agustín indica qué funcionalidad implementar y cuándo publicar.
