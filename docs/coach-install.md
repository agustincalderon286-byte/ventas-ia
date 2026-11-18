# Instalación de TEC Coach

La entrada pública y Mi cuenta ofrecen «Instalar TEC». El botón abre una guía y, cuando Chrome emite `beforeinstallprompt`, ofrece la confirmación nativa únicamente tras pulsarlo. No se abre una ventana de instalación automáticamente. iPhone/iPad reciben los pasos de Compartir en Safari. `accepted` y `appinstalled` se presentan como solicitud aceptada; solo el modo `standalone` observado acredita que TEC está abierta como app independiente.

El manifiesto conserva `id: /coach/`, inicio `/coach/app/` y alcance `/coach/`; añade capturas reales públicas para teléfono/escritorio y declara la preferencia por la experiencia web independiente. No se copian capturas de cuentas privadas al manifiesto. El acceso y las membresías conservan sus comprobaciones.

El AVD existente TEC_Android_17 usa Android37 google_apis, sin imagen Google Play. Aunque aparece `com.android.vending`, su ruta es `/product/app/LicenseChecker/LicenseChecker.apk`, versión1.8, un verificador de licencias; no es Play Store. GMS y Chrome145 sí están. No se observó un paquete org.chromium.webapk tras el intento anterior. Esta configuración explica probablemente la imposibilidad de completar la instalación en ese simulador, pero no se conserva el callback concreto del fallo. No se cambiaron flags ni componentes para ocultar esa limitación. La validación Android independiente requiere un entorno compatible.

Referencias primarias: [criterios de Chrome](https://web.dev/articles/install-criteria), [solicitud de instalación](https://web.dev/learn/pwa/installation-prompt), [tipos de instalación Android](https://web.dev/learn/pwa/installation/#android-installation), [arquitectura WebAPK de Chromium](https://chromium.googlesource.com/chromium/src/+/main/chrome/browser/android/webapk/).
