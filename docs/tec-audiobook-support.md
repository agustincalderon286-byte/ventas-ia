# Soporte de Audiolibros TEC — 7 de noviembre de 2026

Página pública: `/audiolibros/contacto/`. El correo `agustincalderon286@gmail.com` aparece con enlace mailto, además del formulario. Accesos desde el pie de biblioteca, libros, colecciones y guías, el ingreso y la membresía. No exige sesión.

`POST /api/audiolibros/contacto` envía texto plano por Resend con las variables existentes `AUDIO_RESEND_API_KEY` y `AUDIO_RESEND_FROM_EMAIL`. Destinatario fijo: propietario de TEC; Reply-To: correo validado del solicitante. No crea claves, modifica membresías ni guarda consultas en MongoDB. Resend acepta el envío antes de mostrar éxito; aceptación no garantiza recepción en bandeja principal.

Nombre opcional de hasta 100 caracteres, correo de hasta 254, tema permitido y mensaje de 10 a 4,000 caracteres. Bloqueo de origen cruzado, campo señuelo y límites en memoria de 8 intentos por IP y 3 consultas por correo cada 30 minutos. Los identificadores de límite se guardan como hashes y expiran. Son límites por instancia, se reinician al desplegar y no sustituyen una protección contra abuso distribuido si aumenta el volumen. No se registran mensajes, direcciones ni respuestas del proveedor en logs.

Sin analítica en el formulario, sin caché de respuestas, sin cola ni almacenamiento de borradores. Si falta conexión o falla el envío, conserva el texto en la página y ofrece correo directo. El mensaje se limpia únicamente al confirmar aceptación. Cerrar o recargar la página puede perder texto no enviado.

Verificación: 874 pruebas automatizadas aprobadas; validación de origen, campos, límites, errores, destinatario fijo y Reply-To; comportamiento del formulario sin internet y ante errores. Navegador: envío local aislado y vistas de 320 y 768 px sin desbordamiento y controles de al menos 44 px. No se cambiaron Stripe, grabaciones, mapas ni audio.
