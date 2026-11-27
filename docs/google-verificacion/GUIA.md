# Verificar TEC Coach con Google

Objetivo: que al conectar Google ya no salga «Google no ha verificado esta app».
Lo hace Agustín en la consola de Google (Google Cloud → «Google Auth Platform», antes «Pantalla de consentimiento de OAuth»), en el proyecto de TEC.
Todas las conexiones de TEC (Calendar, Sheets, Contactos y Gmail) usan el mismo proyecto.

## 1. Lo que ya está listo en la web (después de publicar)

- Página de inicio: `https://totalelitecoaching.com/tec/`
- Política de privacidad: `https://totalelitecoaching.com/tec/privacidad/`
- El dominio ya tiene un archivo de verificación de Google (`googlee19e7e608fd653e9.html`). Confirma en Google Search Console que `totalelitecoaching.com` aparece como verificado, con la misma cuenta que es dueña o editora del proyecto.

## 2. Marca (Branding)

- **Nombre de la app:** `TEC Coach`. Debe ser igual al de la página de inicio y la de privacidad.
- **Correo de soporte:** el tuyo de TEC.
- **Logo:** el ícono de TEC, de 120 × 120 píxeles (`public/coach/icons/icon-192.png`, reducido). Subir un logo también pide revisión de marca; si quieres ir más rápido, puedes dejarlo sin logo.
- **Página de inicio:** `https://totalelitecoaching.com/tec/`
- **Política de privacidad:** `https://totalelitecoaching.com/tec/privacidad/`
- **Dominio autorizado:** `totalelitecoaching.com`

## 3. Permisos que se mandan a revisar (Data Access)

Deja **solo** estos:

| Permiso | Para qué |
|---|---|
| `openid`, `email` | Saber qué cuenta de Google se conectó |
| `https://www.googleapis.com/auth/calendar.app.created` | El calendario «Seguimientos TEC» y sus citas |
| `https://www.googleapis.com/auth/calendar.freebusy` | Ver si estás libre u ocupado para proponer horarios |
| `https://www.googleapis.com/auth/drive.file` | Las hojas de referidos (solo las que eliges o crea TEC) |
| `https://www.googleapis.com/auth/contacts` | Buscar y guardar clientes en Contactos |

**No incluyas** `gmail.readonly` ni `gmail.send`. Leer Gmail obliga a una auditoría de seguridad que se paga y se repite cada año, y alarga la revisión a unas 6 semanas.
En TEC la conexión de Gmail ya está limitada a las cuentas del piloto (la lista `COACH_CONNECTIONS_PILOT_EMAILS` en Render). Esas cuentas seguirán viendo el aviso solo al conectar Gmail, y Google lo permite hasta 100 personas.

> Ojo: hoy la misma lista abre Gmail **y** Calendar. Antes de abrir Calendar a todos los vendedores hay que separar las dos listas en el código, para que Gmail no se abra con Calendar.

## 4. Por qué necesitamos cada permiso (para pegar en el formulario, en inglés)

- **calendar.app.created:** TEC Coach creates a dedicated «Seguimientos TEC» calendar in the user's Google Calendar and adds, updates or removes the appointments the sales representative schedules in the app (follow-ups and referral appointments). This scope is the narrowest one that allows creating and managing events only in calendars the app created.
- **calendar.freebusy:** Before proposing an appointment time, the AI coach checks when the user is free or busy across their calendars so it never suggests a time that conflicts with their existing events. Only free/busy blocks are read; no event titles, guests or details.
- **contacts:** The app looks up the user's existing contacts to avoid duplicating clients and referrals, and saves or updates the contacts the user asks it to. Read-only access would not allow saving clients; the app never deletes contacts.
- **drive.file** (no requiere justificación, pero sirve de contexto): the app only opens spreadsheets the user picks with the Google Picker or that the app creates, to read and write referrals and appointments.

## 5. El video (lo graba Agustín, 2 a 4 minutos)

Se sube a YouTube como «No listado» y se pega el enlace. Con datos de prueba (teléfonos 555-01xx). En inglés o con subtítulos en inglés, si se puede.

1. Abre `totalelitecoaching.com/tec/`, muestra la página y el enlace a la privacidad.
2. Entra a TEC y abre «Mis conexiones».
3. Conecta **Google Calendar**. Que se vea toda la pantalla de Google: el nombre «TEC Coach», tu correo y la lista de permisos. Muestra la barra de direcciones con `client_id`.
4. Acepta y muestra cómo TEC crea una cita y cómo aparece en el calendario «Seguimientos TEC».
5. Pide a Jorge un horario: muestra que propone uno libre (ese es el uso de libre/ocupado).
6. Conecta **Google Sheets**: muestra la pantalla de permisos, el selector de Google y la hoja con un referido de prueba.
7. Conecta **Contactos**: muestra la pantalla de permisos y un cliente de prueba guardado en Contactos.
8. Desconecta una conexión para mostrar que se puede quitar.

## 6. Enviar

1. Si la app está en «Testing», pásala a «In production» (Publicar app).
2. En «Verification Center» → «Prepare for verification» → llena todo y envía.
3. Google contesta por correo. Tiempos que da Google (sin garantía): marca 2 a 3 días hábiles; permisos que piden revisión unos 10 días hábiles.
4. Si Google pide cambios a la página de inicio (por ejemplo, que la dirección principal `totalelitecoaching.com` no sea solo la pantalla de entrar), avísame y la cambiamos.

## Lo que la política promete y Agustín debe poder cumplir

- Borrar la cuenta y los datos de quien lo pida por correo, en máximo 30 días.
- Avisar dentro de TEC antes de cambiar cómo se usan los datos de Google.
- No usar los datos de Google para publicidad ni para entrenar modelos de inteligencia artificial. TEC los manda a OpenAI solo para generar la respuesta de Jorge.
