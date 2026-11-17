# Chat privado del equipo en Coach

En Mensajes, «Chat del equipo» abre una conversación grupal para preguntas, coordinación y reportes de ventas escritos. La primera versión admite texto (hasta 2000 caracteres), historial paginado, mensajes nuevos, borradores y reintentos explícitos. Los mensajes nuevos se consultan aproximadamente cada 3 segundos mientras se usa el área; fuera de ella, cada 30 segundos para el contador. No se consulta en segundo plano. Esta entrega no incluye archivos, audios de grupo ni notificaciones push con TEC cerrado. El audio del Coach AI es una función independiente.

## Equipos y permisos

El grupo utiliza el propietario del espacio de trabajo existente: `owner` conserva su propio grupo; `seat` y `leader` usan `teamOwnerUserId`, aunque su `parentUserId` sea un líder intermedio. No se fusionan por territorio, patrocinador o red de referidos. Una referencia a un propietario inexistente o que no sea `owner` se rechaza; una cuenta delegada sin propietario no crea un grupo alterno.

Todas las rutas requieren cuenta operativa. El servidor calcula el grupo; el identificador opaco recibido del navegador solo detecta pestañas antiguas o cambios de equipo. Las escrituras requieren `accountId` y `roomId` coincidentes; las lecturas envían también la cuenta esperada. Nombre y remitente se obtienen de la sesión. Los boletines, privados, soporte, ventas y formularios existentes conservan sus datos y reglas. Escribir «vendí…» en el grupo no crea ni modifica un registro de venta.

## Persistencia y lectura

`CoachTeamChatMessage` guarda mensajes; un índice único de grupo/remitente/UUID impide duplicados incluso si se pierde la respuesta o coinciden reintentos. `CoachTeamChatReader` guarda un cursor monotónico por grupo y cuenta. Los índices se inicializan antes de aceptar peticiones al arrancar el servidor. Se entregan los 50 mensajes más recientes en orden ascendente y se pueden cargar anteriores.

La interfaz guarda borrador y UUID pendiente en una clave local por cuenta y grupo, cuando el navegador permite almacenamiento. Un envío sin confirmar conserva el texto y exige «Reintentar envío» con el mismo UUID. No se reenvía automáticamente al recuperar conexión ni se envía un pendiente a otra cuenta. Si el almacenamiento local falla, el texto sigue en la ventana; no se garantiza conservación tras recargar.

La lectura automática requiere pestaña visible, historial al final, último mensaje dentro de la pantalla y continuidad de lo que se cargó. Un diálogo del Coach AI sobre el grupo pausa las confirmaciones. Una llegada mientras se leen mensajes anteriores muestra «Ver mensajes nuevos» y conserva la posición. Enviar un mensaje propio no marca como leídos mensajes ajenos aún no recibidos. «Marcar todo como leído» es una acción explícita que incluye el grupo con un cursor recién consultado.

El contador de cabecera suma avisos, privados, soporte y grupo. Cada parte actualiza su propio componente; leer un boletín no borra el pendiente del grupo. El acceso rápido a Noticias conserva los avisos originales. Se compactó el resumen y se ajustó la altura móvil para dejar el envío accesible junto a los accesos flotantes.

## Validación y publicación

Trabajo dividido entre tres subagentes: backend, interfaz y revisión independiente, con integración y publicación centralizadas. 84 pruebas cubren aislamiento entre equipos, sesión/origen/cuenta/grupo, nombres no falsificables, UUID/reintentos, paginación, lecturas monotónicas, historial continuo, mensajes fuera de pantalla, diálogo AI, borradores por cuenta y combinación de contadores. Se ejecutan además las regresiones de voz, tarjetas, navegación y formularios sin conexión.

La vista local utiliza la plantilla y módulos reales, cuentas ficticias Ana/Beto/Carla y almacenes en memoria. Se probaron envío y recarga, pérdida de confirmación con reintento después de recargar sin duplicados, borradores separados, otro equipo aislado, historial anterior, llegada de compañero sin cambiar la posición y lectura explícita total. La vista de 390×844 mantiene el chat dentro del ancho y el envío accesible.

Las comprobaciones de producción son de apertura y lectura; no se envían mensajes de prueba a compañeros reales.
