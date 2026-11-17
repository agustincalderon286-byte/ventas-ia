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

## Verificación

Pruebas HTTP/VM con cuentas ficticias verifican aislamiento de visitante y vendedor, consentimiento, campos de propietario falsificados, idempotencia y concurrencia, fallo del CRM, restricciones de origen, límites, contenido del QR, lectura por ascendencia y cambio de cuenta. La revisión visual local verificó consulta de precio, formulario para Eric Ejemplo, pérdida de respuesta después del guardado, reintento con un solo lead, recepción por Eric, lectura por Lorena y exclusión de Verónica.

La herramienta de navegador se desactivó durante la revisión móvil. La captura obtenida en ese intento todavía tenía viewport de escritorio; no cuenta como validación de 390 px. No se afirma prueba visual móvil completada. Ningún cliente real, correo o mensaje de WhatsApp se utiliza en las pruebas.
