# Membresía de Audiolibros TEC

La landing vive en `/audiolibros/` en el mismo servidor de Total Elite Coaching. Su API, sesión, clientes de Stripe y colecciones de cuentas y progreso son independientes de Coach. La biblioteca comparte el catálogo editorial, las portadas, el lector sincronizado y los archivos completos ya instalados en GridFS.

## Activación en Render

1. En la cuenta de Stripe que usa el servidor, crea un producto de Audiolibros TEC con un precio fijo recurrente de 20 USD por mes. Crea una configuración específica del portal de clientes con cancelación habilitada al final del periodo, actualización del medio de pago e historial de facturas. Deshabilita los cambios de plan en esa configuración.
2. En el servicio Render de TEC, asigna el ID del precio a `STRIPE_AUDIO_PRICE_ID` y el ID del portal a `STRIPE_AUDIO_PORTAL_CONFIG_ID`. Usa el mismo modo de Stripe que `STRIPE_SECRET_KEY`. El correo de confirmación y recuperación requiere `AUDIO_RESEND_API_KEY` con permiso de envío, `AUDIO_RESEND_FROM_EMAIL` de un dominio verificado y el `APP_BASE_URL` del servidor.
3. Verifica el flujo completo en un entorno de prueba con los IDs del modo de pruebas. Inicia Checkout sin cuenta, completa el pago con una tarjeta de prueba, configura usuario y contraseña desde el correo de confirmación y comprueba el acceso a ambos libros y al lector, comprueba avance y marcadores desde otro navegador y cancela desde el portal. Una cancelación programada conserva acceso durante el periodo ya pagado. Un estado vencido o impagado debe rechazar el audio completo.
4. Despliega la rama aprobada en el servicio existente. Comprueba `/api/healthz`, `/audiolibros/` y ambos archivos de muestra. Sin una sesión de audiolibros, las rutas del audio y del texto completos deben responder 401. Para cuentas con sesión y sin membresía, deben responder 403.

Stripe confirma el acceso directamente con el estado de la suscripción. Las confirmaciones activas se reutilizan durante un máximo de 30 segundos para servir los rangos de audio. Los eventos del producto no se usan para asignar acceso en Coach.

## Muestras

Las muestras públicas son archivos AAC/M4A independientes, sin capítulos incrustados, recortados desde el comienzo de las ediciones con SHA-256 fijado en el catálogo. Las duraciones verificadas son 289,101 s para *Como un hombre piensa* y 549,530 s para *Acres de diamantes*. El audio completo sigue en el bucket privado existente.

Para incorporar otra muestra, toma la edición con el hash del catálogo, recórtala con FFmpeg al 10 % de su duración y guarda únicamente ese recorte en `public/audiolibros/muestras/<id>.m4a`. Valida la duración del contenedor antes de publicarlo.

## Verificación local

Ejecuta `npm test` con Node 24. La prueba de membresía usa correo, base y Stripe simulados para verificar el flujo HTTP sin cobros ni mensajes externos.

## Cuentas de clientes después del pago (10 de noviembre de 2026)

El registro público por enlace fue sustituido por usuario o correo y contraseña. No se crean cuentas de biblioteca por pedir un enlace ni por abrir Checkout. La muestra del 10 % permanece pública. Al pulsar Suscribirme se pide el correo y se abre Stripe a 20 USD/mes. Un cliente o intento de Checkout en Stripe no equivale a una cuenta ni concede acceso al catálogo.

El webhook firmado existente y la comprobación de retorno invocan la misma función idempotente. Recupera Checkout, suscripción y factura desde Stripe y exige el flujo `paid-account-v1`, modo correcto, precio de Audiolibros TEC, pago positivo real y suscripción activa. Crea entonces el registro pendiente, conserva progreso legado y envía una confirmación de un solo uso por 24 horas. No reasigna un cliente de facturación a otra cuenta. Las cuentas existentes con cliente de Stripe deben ingresar o recuperar acceso antes de volver a pagar; las sesiones abiertas se reutilizan.

Después del correo, `POST /credenciales` asigna un usuario canónico único y una contraseña de 15 a 128 caracteres, confirma el email y abre la sesión. El hash usa scrypt N=32768, r=8, p=3, sal aleatoria de 16 bytes, salida de 64 bytes y un máximo de dos hashes simultáneos. Se permiten frases, pegado y gestores de contraseñas. Los índices únicos de usuario y hash de enlace están listos antes de asignar credenciales.

`POST /recuperar` devuelve la misma respuesta pública para un correo desconocido, un registro sin pago y una cuenta existente, con espera mínima de 700 ms y límites por IP y por cuenta. Las cuentas pagadas antiguas sin contraseña reciben confirmación de alta después de comprobar su historial de pagos. Las cuentas con contraseña reciben un enlace de restablecimiento de 30 minutos. Los tokens se guardan solo como SHA-256; la actualización de credenciales consume el token atómicamente. Restablecer cambia la versión de credencial, invalida sesiones anteriores y requiere ingresar otra vez. No cambia el usuario ni convierte una membresía vencida en activa.

El remitente principal es el buzón exacto `agustin@totalelitecoaching.com`, con nombre visible Audiolibros TEC. Una alternativa Resend solo se acepta si el remitente pertenece al dominio de TEC; se rechaza un buzón personal. `createAudioAccountMailer` compone confirmación, recuperación y notificación de cambio sin incluir contraseñas.

Un registro legado que abandonó Checkout sin pagar recibe un enlace limitado a continuar el pago después de demostrar posesión del correo; no puede elegir contraseña ni abrir sesión con él. Así se puede reutilizar su cliente de facturación sin exponerlo a alguien que solo conozca el email. El alta de credenciales continúa exigiendo una factura realmente cobrada. La entrega de recuperación sigue en segundo plano para que la latencia del proveedor no revele la existencia de la cuenta; si el servidor se reinicia antes de completarla, el lector puede solicitar otro enlace pasado el límite de un minuto.

Las cuentas privadas de prueba conservan su ruta y credenciales de operador en `?acceso=prueba`; ese formulario ya no aparece al cliente normal. No pueden usar la recuperación pública ni abrir cobros.

Comprobar `tests/tec-audio-auth.test.js`, el flujo HTTP de `tests/tec-audio-membership.test.js`, correo y regresiones de Stripe y cuentas de prueba. Las pruebas usan servicios simulados y no crean una compra real.

## Cuentas de prueba privadas (8 de noviembre de 2026)

Las cuentas de prueba viven sólo en `TecAudioMember`, con `_id=test:<usuario>`, `isTest=true`, `analyticsExcluded=true`, `accessType=test` y `testAccessEnabled=true`. No crean clientes, suscripciones ni facturas en Stripe y no dan acceso al Coach ni a la Torre. No existe un endpoint público para crearlas. Los informes comerciales que consulten miembros deben usar `{isTest: {$ne: true}}`; contar documentos de miembros nunca equivale a contar suscriptores de pago.

El operador prepara contraseñas aleatorias en el SSD, fuera del repositorio: `node scripts/manage-audio-test-accounts.mjs prepare <carpeta-privada>`. Entregar los mensajes individuales de esa carpeta. Subir únicamente los registros con hashes al servidor mediante la entrada estándar base64 de `node scripts/manage-audio-test-accounts.mjs apply`, ejecutado en la consola autenticada de Render; la URI de Mongo permanece en el entorno del servidor. El comando es idempotente para los mismos registros y rechaza sobrescribir cuentas distintas. No guardar contraseñas en Git ni en variables públicas. La base guarda únicamente scrypt con sal aleatoria; las sesiones siguen siendo tokens aleatorios HttpOnly, Secure y SameSite=Lax, de 30 días.

El enlace compartible `/audiolibros/?acceso=prueba` abre el formulario de usuario/contraseña y excluye la entrada de Analytics antes del inicio de sesión. Tras entrar, un marcador `tec_audio_test=1` sin identidad mantiene la exclusión en todas las páginas de audiolibros, incluso con consentimiento previo. Ese marcador no concede acceso: cada petición protegida verifica la sesión, la cuenta, la habilitación y la versión de credencial. El progreso y las insignias personales se guardan para funcionar; no son métricas comerciales. Los registros técnicos de servidor/CDN siguen existiendo.

Para retirar acceso: `node scripts/manage-audio-test-accounts.mjs disable <usuario>` desde Render. Deshabilita el permiso y cambia la versión de credencial, invalidando las sesiones existentes sin afectar las demás cuentas. Cualquier recuperación de contraseña debe hacerse por el operador, sin habilitar cobros ni convertirla en cuenta de pago.


## Audio directo desde R2 (11 noviembre 2026)

Las rutas de reproducción autorizan la membresía y verifican el objeto privado antes de emitir una URL firmada GET de 300 segundos. `?delivery=1` devuelve `{url, expiresAt}`; las peticiones de audio tradicionales reciben 307 sin bytes de audio. HEAD solo devuelve metadatos. El navegador hace sus propias solicitudes Range a R2; no se firman encabezados If-Match ni Range. El reproductor renueva enlaces caducados conservando posición y velocidad. R2 permanece privado; nunca se publican claves de servidor. Un enlace temporal puede utilizarse hasta que caduque.

Las muestras son archivos recortados independientes definidos en `tec-audio-previews.json`; sus rutas se atienden antes de express.static y también entregan directamente desde R2. Todos los libros publicados tienen claves R2 inmutables. No reemplazar objetos activos: subir otra clave con versión/hash y actualizar bytes, SHA256 y ETag verificados. CORS permite GET/HEAD desde totalelitecoaching.com y www.totalelitecoaching.com, con Range y los encabezados de respuesta de medios.

El generador interno de fragmentos mantiene su lector acotado de 32 MB; no sigue redirecciones que evadan ese límite. Las páginas, API y fragmentos generados breves siguen usando Render.
