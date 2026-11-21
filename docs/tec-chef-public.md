# Chef público y atribución de leads

Actualización solicitada el 17 de noviembre de 2026: el Chef queda disponible al público y a los vendedores. Esta autorización sustituye la limitación previa a una prueba administrativa para las rutas nuevas.

## Dos superficies separadas

- Mi cuenta → Chef abre `/coach/chef/`, con sesión operativa de TEC. Cada vendedor obtiene su URL canónica `/chef/:slug/`, un QR generado por el servidor y su lista de contactos. El botón Ver Chef del cliente abre otra pestaña.
- `/chef/` y `/chef/:slug/` sirven un documento exclusivo para clientes. No hay menú, enlaces al Coach/Torre, cierre de sesión ni listado de leads. No se usa la sesión del distribuidor para iniciar el chat. El cliente puede preguntar sin dejar datos ni iniciar sesión.
- El piloto anterior y su API siguen limitados a administradores; su página permanece en `/coach/chef/admin/`. WhatsApp, SMS y campañas anteriores no se reactivan. El nuevo flujo público no envía correo al destinatario fijo del piloto.

## Propiedad y supervisión

El servidor resuelve el perfil activo por el slug del enlace. El cliente no elige ni modifica ownerUserId, generatedByUserId, correo del destinatario o estado. Un enlace personal inexistente, deshabilitado o sin acceso operativo falla; no se redirige a otro vendedor. El destino predeterminado de `/chef/` se resuelve con la configuración existente. Si falta un destinatario predeterminado, el chat general funciona y no acepta solicitudes que quedarían sin asignación.

Cada solicitud crea un CoachLeadInbox con el propietario raíz de facturación en ownerUserId y el vendedor del QR en generatedByUserId; conserva chefSlug y source chef_personal. Este es el contrato existente de los registros de campo. También se llama a la sincronización del CRM e invalidación de cachés. Si el CRM no responde, el contacto permanece guardado y visible en Chef.

Mis leads consulta solo generatedByUserId de la cuenta autenticada. Mi equipo usa descendientes reales por parentUserId, con el recorrido acotado y detección de ciclos existentes. Todo upline real puede consultar los leads del Chef de su descendencia, incluso cuando su cuenta de vendedor no tenga el permiso global de supervisión; esa excepción es solo de lectura del Chef. No cambia teamSupervisionEnabled, Torre, administración de cuentas, encuestas ni permisos del resto de TEC. No se concede visibilidad por compartir dueño de facturación, territorio o comisiones.

El formulario del cliente identifica al vendedor y explica que su equipo de supervisión verá la solicitud. Requiere autorización explícita y teléfono o correo válidos. La demostración es opcional; su horario es una preferencia y queda solicitada, nunca confirmada. Los campos del formulario no se envían al modelo de IA.

## Sesiones, límites y reintentos

Una cookie HttpOnly/SameSite Lax contiene un token aleatorio; el servidor almacena solo su hash y expira las sesiones anónimas a los 14 días. Las conversaciones públicas están separadas de las administrativas y se acotan por vendedor + visitante + conversación; conservan 24 mensajes y expiran a los 30 días. Un visitante no obtiene acceso al panel privado.

Hay validación de origen/JSON y límites por visitante e IP, por proceso, para sesiones, consultas y solicitudes. No son una garantía de presupuesto diario entre reinicios o varias instancias. Las solicitudes excesivas se rechazan antes de crear conversaciones. Las claves del proveedor permanecen en el servidor; el Chef usa exclusivamente el contexto público y el motor de cocina existente.

La clave idempotente de cada lead combina visitante, vendedor y requestId. El índice único existente ownerUserId + offlineRequestId impide duplicados concurrentes; se inicializa antes de escuchar HTTP junto con los índices nuevos. Reutilizar una clave con otros datos responde 409. El navegador conserva el formulario ante errores y guarda únicamente el hash del contenido y la clave de reintento en sessionStorage; también conserva esa clave en memoria si el almacenamiento está bloqueado. No almacena allí el nombre, teléfono ni correo.

Las listas y QR privados usan no-store y validación de cuenta para pestañas con sesión cambiada. El panel borra su contenido y solicita recargar si cambia la cuenta. Actualiza cada 20 segundos cuando está visible. No incorpora notificaciones push con TEC cerrado.

## Solicitud de demostración por voz en Alexa (19 de noviembre de 2026)

Alexa no puede leer el QR físico. Cada vendedor tiene un **código para Alexa de seis dígitos** (`chefVoiceCode`, único, generado junto al slug y al código de compartir en `asegurarCoachDistributorProfile`). El panel del vendedor lo muestra debajo de su QR y la página `/chef/<slug>/` lo muestra al cliente. El cliente lo dice dígito a dígito («cuatro ocho dos nueve uno tres»). `spokenVoiceCode` lo reconoce y `resolverCoachChefContextPorCodigoVoz` lo resuelve con las mismas comprobaciones que el enlace del QR. También se acepta el slug dicho (`spokenSellerCandidates`), con frases como «mi vendedor es…» o «mi vendedor se llama…». Solo se acepta un único vendedor activo; si una frase lleva a dos, responde 409 y no adivina.

Flujo en la skill (`alexa-local/lambda/demo_request.py`):

1. «Quiero una demostración» → Alexa pide el código para Alexa o el identificador.
2. TEC resuelve el vendedor y Alexa lee su nombre. Debe confirmarse con un sí. Un «no» descarta ese vendedor y, al segundo intento fallido, la skill envía al formulario.
3. Alexa pregunta nombre, teléfono, ciudad, producto de interés y día u horario preferidos. Dos respuestas no entendidas seguidas en la misma pregunta envían al formulario sin guardar nada.
4. Alexa lee el resumen y pregunta si el cliente autoriza que el vendedor y su equipo de supervisión reciban los datos y lo contacten. Solo un sí explícito envía. Un no o una respuesta ambigua no guarda nada.
5. `POST /api/chef/public/alexa/leads` escribe en CoachLeadInbox con `storeLead`, el mismo camino del formulario web. Se guardan `ownerUserId` del contexto, `generatedByUserId` del vendedor, `chefSlug`, `source chef_personal`, `sourceDetail «TEC Chef · Alexa por voz»`, la etiqueta `alexa_voz`, el producto en `interest` y `appointmentStatus solicitada`. La preferencia queda como «Preferencia: …», con una nota que pide confirmar disponibilidad. El cliente no puede fijar dueño, vendedor ni estado. La ruta pública del formulario rechaza `origin: alexa-voz`, así que solo la ruta autenticada de Alexa puede marcar una solicitud como hecha por voz.

Fuera de una solicitud abierta, las frases de ciudad, producto u horario («estoy en el paso tres», «me interesa el pollo») se responden como preguntas de cocina. Las de nombre o teléfono no se guardan ni se envían a TEC. El modelo no usa portadoras amplias como «puedo…», «soy…» o «estoy en…».

Respaldo: si no hay código, el vendedor no se verifica, TEC no responde o la voz no está configurada, Alexa deja la tarjeta con `/chef/alexa/cita/`, el formulario existente, y no guarda nada.

Protección de datos: los endpoints de voz exigen el encabezado `X-TEC-Alexa-Key`, que se compara por hash en tiempo constante con `TEC_CHEF_ALEXA_VOICE_KEY`. Sin esa variable responden 503 y la skill sigue usando solo el formulario. TEC recibe un hash del usuario de Alexa, nunca el identificador de Amazon. Los datos de contacto viven solo en los atributos de la sesión de Alexa hasta que se envían o cancelan. No se escriben en DynamoDB, en registros ni en la tarjeta. Hay límites por usuario de Alexa y globales para búsquedas y solicitudes.

Duplicados: la clave de la solicitud es un hash de vendedor + respuestas + fecha UTC y se conserva en la sesión. Un reintento después de perder la respuesta, un «sí» repetido o la misma solicitud en otra sesión del mismo día devuelven el lead existente (`duplicate: true`). El índice único `ownerUserId + offlineRequestId` cubre la concurrencia. Reutilizar la clave con otros datos responde 409.

Aviso de privacidad, condiciones y la página `/chef/alexa/cita/` ya describen la solicitud por voz (publicados el 19 de noviembre con el commit 9b74012 y actualizados aquí con el código para Alexa y el producto).

### Pendientes

- Configurar `TEC_CHEF_ALEXA_VOICE_KEY` con el mismo valor secreto en Render y en la Lambda. Mientras no exista, producción se comporta como antes.
- Probar en el simulador y en el Echo que Alexa reconoce el código de seis dígitos, los teléfonos de 10 dígitos (AMAZON.NUMBER) y las frases de producto en es-US.
- Política si un vendedor queda inactivo entre la confirmación y el envío. Hoy no se reasigna: se envía al formulario y no se guarda.
- Cuestionario de certificación de Amazon: declarar que la skill recopila nombre, teléfono, ciudad y producto por voz. No se publicó ni se envió a certificación.

## Verificación

Pruebas HTTP/VM con cuentas ficticias verifican aislamiento de visitante y vendedor, consentimiento, campos de propietario falsificados, idempotencia y concurrencia, fallo del CRM, restricciones de origen, límites, contenido del QR, lectura por ascendencia y cambio de cuenta. La revisión visual local verificó consulta de precio, formulario para Eric Ejemplo, pérdida de respuesta después del guardado, reintento con un solo lead, recepción por Eric, lectura por Lorena y exclusión de Verónica.

La herramienta de navegador se desactivó durante la revisión móvil. La captura obtenida en ese intento todavía tenía viewport de escritorio; no cuenta como validación de 390 px. No se afirma prueba visual móvil completada. Ningún cliente real, correo o mensaje de WhatsApp se utiliza en las pruebas.
