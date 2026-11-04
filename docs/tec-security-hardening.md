# TEC: correcciones de seguridad, 4 de noviembre de 2026

## Alcance y validación

Node 24 LTS autorizado expresamente por Agustin después de las pruebas. Instalación limpia con Node 24.11.1 y npm 11.6.2: **837 pruebas aprobadas**, ninguna omitida o fallida. `npm audit --omit=dev`: **0 vulnerabilidades conocidas** al verificar. No se ejecutaron cargas, compras, grabaciones, ataques de caída ni cambios de cuentas reales en producción.

## Correcciones

- Express 4 captura rechazos asíncronos; cookies malformadas se ignoran por fragmento, sin tumbar autenticación. Tokens malformados se rechazan antes de MongoDB; errores no exponen stacks ni datos recibidos.
- body-parser y proxy-addr actualizados; qs sobreescrito a una versión corregida. Contraseñas con scrypt asíncrono y entrada acotada; registros limitados por correo e IP, cuentas administrativas y de prueba reservadas.
- Login y operaciones autenticadas rechazan solicitudes de sitios ajenos. Clientes nativos HTTPS y cuerpos binarios de audio conservan compatibilidad.
- Historial del Coach con namespace derivado de cuenta y sesión, autenticado antes de consultar RAM/Redis. Cambio de cuenta y logout renuevan identificador de chat. Memoria antigua de leads de Rifa deshabilitada por defecto; información de productos y garantías conserva su flujo.
- Pendientes offline se envían con identidad inmutable del propietario y se comprueban contra la sesión real en el servidor. Si cambia la cuenta, se rechaza el envío sin eliminar el pendiente. No hay migraciones ni borrado de IndexedDB, borradores, catálogos o audios.
- Destinos de leads, referencias y reclutamiento: HTTPS público, sin credenciales en URL ni puertos arbitrarios, DNS validado y fijado en cada conexión, redirects verificados. Se bloquean redes internas, loopback y rangos reservados. `COACH_OUTBOUND_ALLOWED_HOSTS` permite limitar hosts si se configura; por defecto siguen habilitados destinos HTTPS públicos para conservar integraciones.
- Torre y resumen global requieren correo administrativo explícito; ser upline o leader no basta. Rigoberto conserva el acceso administrativo previamente autorizado.
- API privada no-store, HSTS en HTTPS y CSP base aplicada (`object-src`, `base-uri`, `form-action`, `frame-ancestors`). La CSP completa de scripts/medios está en **Report-Only**, pendiente de endurecimiento después de comprobar todas las herramientas, PDF, mapas, AR y clientes nativos. No se declara protección CSP completa.

## Doble verificación de administración

`/coach/seguridad/` permite a cada administrador registrar sus propias passkeys con contraseña actual y confirmación del dispositivo. Se almacena solo clave pública; no contraseña biométrica, PIN ni clave privada. Desafíos de cinco minutos, de un solo uso, ligados a usuario y sesión; WebAuthn valida origen, RP, firma, presencia y verificación de usuario. Pruebas usan un autenticador criptográfico ficticio, sin cuentas reales.

**Requiere que cada administrador registre personalmente su primera llave. No se registró ninguna por cuenta del usuario.** Después del registro, los endpoints administrativos exigen verificación por sesión y cada ocho horas. El Coach operativo y las grabaciones conservan acceso normal, para evitar interrumpir clientes nativos. No reemplaza MFA de Render, GitHub, Stripe o MongoDB.

Registrar una segunda llave de respaldo antes de depender de esta protección. Agregar llaves después de la primera exige llave existente y contraseña; eliminarlas exige contraseña y factor verificado en los últimos cinco minutos. Eliminar la última desactiva la protección. Recuperación operativa por pérdida de todas las llaves requiere procedimiento manual y verificación de identidad; no existe bypass público.

## Publicación y operación

Cambios de aplicación/worker versionados `20261104-security1`, sin activación forzada ni recarga de una demo en curso. Ventanas antiguas que envíen pendientes sin identidad pueden recibir un rechazo de seguridad: conservar el pendiente, terminar grabación/trabajo y cerrar/reabrir TEC para actualizar. Nunca borrar la app ni sus datos para resolverlo.

La compilación y health checks de Render deben verificarse después del push, incluido el Node 24 utilizado realmente. Mantener rollback por commit anterior, sin borrar datos ni colecciones.

## Pendientes fuera de esta corrección

- Inscripción personal de passkeys de Agustin y Rigoberto y comprobación del segundo dispositivo de respaldo.
- Endurecer CSP completa cuando se hayan validado proveedores y herramientas; no se habilitó telemetría nueva de reportes.
- Revisar MFA de proveedores, red de MongoDB, restauración real de backups y recuperación administrativa en una tarea separada; no se han dado por verificadas.
- Repetir revisión de dependencias durante futuras actualizaciones. Cero avisos conocidos no demuestra ausencia de vulnerabilidades.
- Render muestra un aviso de pago fallido. El titular debe resolverlo desde su cuenta; no se modificaron tarjetas ni se realizó un pago.

## Fuentes primarias

- https://expressjs.com/en/4x/guide/error-handling/
- https://github.com/advisories/GHSA-27r3-jxmh-p828
- https://github.com/advisories/GHSA-jqcg-44mw-7w3h
- https://github.com/advisories/GHSA-6pq8-jhm2-2vcw
- https://nodejs.org/en/about/eol
- https://simplewebauthn.dev/docs/packages/server
- https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html
- https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html
