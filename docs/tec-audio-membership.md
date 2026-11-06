# Membresía de Audiolibros TEC

La landing vive en `/audiolibros/` en el mismo servidor de Total Elite Coaching. Su API, sesión, clientes de Stripe y colecciones de cuentas y progreso son independientes de Coach. La biblioteca comparte el catálogo editorial, las portadas, el lector sincronizado y los archivos completos ya instalados en GridFS.

## Activación en Render

1. En la cuenta de Stripe que usa el servidor, crea un producto de Audiolibros TEC con un precio fijo recurrente de 20 USD por mes. Crea una configuración específica del portal de clientes con cancelación habilitada al final del periodo, actualización del medio de pago e historial de facturas. Deshabilita los cambios de plan en esa configuración.
2. En el servicio Render de TEC, asigna el ID del precio a `STRIPE_AUDIO_PRICE_ID` y el ID del portal a `STRIPE_AUDIO_PORTAL_CONFIG_ID`. Usa el mismo modo de Stripe que `STRIPE_SECRET_KEY`. El acceso por enlace requiere `RESEND_API_KEY`, `RESEND_FROM_EMAIL` de un dominio verificado y el `APP_BASE_URL` del servidor.
3. Verifica el flujo completo en un entorno de prueba con los IDs del modo de pruebas. Solicita un enlace de acceso, completa Checkout con una tarjeta de prueba, confirma el acceso a ambos libros y al lector, comprueba avance y marcadores desde otro navegador y cancela desde el portal. Una cancelación programada conserva acceso durante el periodo ya pagado. Un estado vencido o impagado debe rechazar el audio completo.
4. Despliega la rama aprobada en el servicio existente. Comprueba `/api/healthz`, `/audiolibros/` y ambos archivos de muestra. Sin una sesión de audiolibros, las rutas del audio y del texto completos deben responder 401. Para cuentas con sesión y sin membresía, deben responder 403.

Stripe confirma el acceso directamente con el estado de la suscripción. Las confirmaciones activas se reutilizan durante un máximo de 30 segundos para servir los rangos de audio. Los eventos del producto no se usan para asignar acceso en Coach.

## Muestras

Las muestras públicas son archivos AAC/M4A independientes, sin capítulos incrustados, recortados desde el comienzo de las ediciones con SHA-256 fijado en el catálogo. Las duraciones verificadas son 289,101 s para *Como un hombre piensa* y 549,530 s para *Acres de diamantes*. El audio completo sigue en el bucket privado existente.

Para incorporar otra muestra, toma la edición con el hash del catálogo, recórtala con FFmpeg al 10 % de su duración y guarda únicamente ese recorte en `public/audiolibros/muestras/<id>.m4a`. Valida la duración del contenedor antes de publicarlo.

## Verificación local

Ejecuta `npm test` con Node 24. La prueba de membresía usa correo, base y Stripe simulados para verificar el flujo HTTP sin cobros ni mensajes externos.
