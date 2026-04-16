const THREAD_STORAGE_KEY = "cmwf_live_chat_thread_v1"
const PROFILE_STORAGE_KEY = "cmwf_live_chat_profile_v1"
const POLL_INTERVAL_MS = 5000
const MAX_CHAT_PHOTO_FILES = 4
const MAX_CHAT_TOTAL_BYTES = 6 * 1024 * 1024
const INTRO_MESSAGE =
  "Hola, este chat va directo a Chicago Metal Works & Fencing. Cuentanos que necesitas y te respondemos desde el CRM."

const threadWrap = document.querySelector("[data-chat-thread]")
const chatForm = document.querySelector("[data-chat-form]")
const chatInput = document.querySelector("[data-chat-input]")
const chatSendButton = document.querySelector("[data-chat-send]")
const chatPhotoInput = document.querySelector("[data-chat-photo-input]")
const chatPhotoButton = document.querySelector("[data-chat-photo-button]")
const chatPhotoStrip = document.querySelector("[data-chat-photo-strip]")
const chatPhotoCount = document.querySelector("[data-chat-photo-count]")
const chatPhotoList = document.querySelector("[data-chat-photo-list]")
const chatFeedback = document.querySelector("[data-chat-feedback]")
const chatStatus = document.querySelector("[data-chat-status]")
const profileDetails = document.querySelector("[data-chat-profile]")
const profileSummary = document.querySelector("[data-chat-profile-summary]")
const profileNameInput = document.querySelector("[data-chat-profile-name]")
const profilePhoneInput = document.querySelector("[data-chat-profile-phone]")
const profileEmailInput = document.querySelector("[data-chat-profile-email]")

const state = {
  visitorId: "",
  sessionId: "",
  leadId: "",
  messages: [],
  sending: false,
  uploadingPhotos: false,
  photoFileNames: [],
  pollHandle: null,
}

function readStoredJson(key, fallback = null) {
  try {
    const raw = window.localStorage.getItem(key)

    if (!raw) {
      return fallback
    }

    const parsed = JSON.parse(raw)
    return parsed ?? fallback
  } catch {
    return fallback
  }
}

function writeStoredJson(key, value) {
  try {
    window.localStorage.setItem(key, JSON.stringify(value))
  } catch {}
}

function createClientId(prefix = "cmw") {
  if (window.crypto?.randomUUID) {
    return `${prefix}_${window.crypto.randomUUID()}`
  }

  return `${prefix}_${Math.random().toString(36).slice(2)}_${Date.now().toString(36)}`
}

function ensureThreadIdentity() {
  const stored = readStoredJson(THREAD_STORAGE_KEY, {}) || {}
  const visitorId = String(stored.visitorId || "").trim() || createClientId("visitor")
  const sessionId = String(stored.sessionId || "").trim() || createClientId("session")

  state.visitorId = visitorId
  state.sessionId = sessionId

  writeStoredJson(THREAD_STORAGE_KEY, {
    visitorId,
    sessionId,
  })
}

function getProfile() {
  const stored = readStoredJson(PROFILE_STORAGE_KEY, {}) || {}

  return {
    fullName: String(profileNameInput?.value || stored.fullName || "").trim(),
    phoneDisplay: String(profilePhoneInput?.value || stored.phoneDisplay || "").trim(),
    email: String(profileEmailInput?.value || stored.email || "").trim(),
  }
}

function persistProfile() {
  const profile = getProfile()
  writeStoredJson(PROFILE_STORAGE_KEY, profile)
  syncProfileSummary(profile)
  return profile
}

function hydrateProfileInputs() {
  const profile = readStoredJson(PROFILE_STORAGE_KEY, {}) || {}

  if (profileNameInput) {
    profileNameInput.value = String(profile.fullName || "").trim()
  }

  if (profilePhoneInput) {
    profilePhoneInput.value = String(profile.phoneDisplay || "").trim()
  }

  if (profileEmailInput) {
    profileEmailInput.value = String(profile.email || "").trim()
  }

  syncProfileSummary(profile)
}

function syncProfileSummary(profile = getProfile()) {
  if (!profileSummary) {
    return
  }

  const tags = [profile.fullName, profile.phoneDisplay, profile.email].filter(Boolean)

  profileSummary.textContent = tags.length
    ? `Datos guardados en este dispositivo: ${tags.join(" · ")}`
    : "Agrega tu nombre o telefono para un seguimiento mas rapido"
}

function escapeHtml(value = "") {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
}

function formatMessageTime(value = "") {
  if (!value) {
    return ""
  }

  const date = new Date(value)

  if (Number.isNaN(date.getTime())) {
    return ""
  }

  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  })
}

function setFeedback(message = "", tone = "") {
  if (!chatFeedback) {
    return
  }

  chatFeedback.textContent = message
  chatFeedback.dataset.tone = tone
}

function setStatus(message = "") {
  if (!chatStatus) {
    return
  }

  chatStatus.textContent = message || "Listo para empezar"
}

function renderPhotoFiles() {
  if (!chatPhotoStrip || !chatPhotoList || !chatPhotoCount) {
    return
  }

  const fileNames = Array.isArray(state.photoFileNames) ? state.photoFileNames.filter(Boolean) : []

  chatPhotoStrip.hidden = fileNames.length === 0
  chatPhotoCount.textContent = `${fileNames.length} foto${fileNames.length === 1 ? "" : "s"} guardada${fileNames.length === 1 ? "" : "s"}`
  chatPhotoList.innerHTML = fileNames
    .map((fileName) => `<span class="messages-photo-chip">${escapeHtml(fileName)}</span>`)
    .join("")
}

function autoResizeTextarea() {
  if (!chatInput) {
    return
  }

  chatInput.style.height = "0px"
  chatInput.style.height = `${Math.min(Math.max(chatInput.scrollHeight, 56), 180)}px`
}

function buildTrackingPayload() {
  const params = new URLSearchParams(window.location.search || "")

  return {
    gclid: params.get("gclid") || "",
    gbraid: params.get("gbraid") || "",
    wbraid: params.get("wbraid") || "",
    utmSource: params.get("utm_source") || "",
    utmMedium: params.get("utm_medium") || "",
    utmCampaign: params.get("utm_campaign") || "",
    utmTerm: params.get("utm_term") || "",
    utmContent: params.get("utm_content") || "",
    landingPath: window.location.pathname || "",
    landingUrl: window.location.href || "",
    referrer: document.referrer || "",
  }
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()

    reader.onload = () => {
      resolve(String(reader.result || ""))
    }

    reader.onerror = () => {
      reject(new Error(`No pude leer ${file?.name || "la imagen"}.`))
    }

    reader.readAsDataURL(file)
  })
}

async function apiRequest(url, options = {}) {
  const response = await fetch(url, {
    method: String(options.method || "GET").toUpperCase(),
    credentials: "same-origin",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
  })

  const data = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(data.error || "No pude completar esa accion.")
  }

  return data
}

function scrollThreadToBottom() {
  if (!threadWrap) {
    return
  }

  window.requestAnimationFrame(() => {
    threadWrap.scrollTop = threadWrap.scrollHeight
  })
}

function renderThread() {
  if (!threadWrap) {
    return
  }

  const safeMessages = Array.isArray(state.messages) ? state.messages : []
  const introRow = `
    <div class="messages-bubble-row" data-role="assistant">
      <article class="messages-bubble">
        <p>${escapeHtml(INTRO_MESSAGE)}</p>
        <div class="messages-meta">Chicago Metal Works</div>
      </article>
    </div>
  `
  const body = safeMessages
    .map((message) => {
      const role = message.role === "assistant" ? "assistant" : "user"
      const author = role === "assistant" ? "Chicago Metal Works" : "Tu"
      const timeLabel = formatMessageTime(message.createdAt)

      return `
        <div class="messages-bubble-row" data-role="${escapeHtml(role)}">
          <article class="messages-bubble">
            <p>${escapeHtml(message.content || "")}</p>
            <div class="messages-meta">
              ${escapeHtml(author)}${timeLabel ? ` · ${escapeHtml(timeLabel)}` : ""}
            </div>
          </article>
        </div>
      `
    })
    .join("")

  const emptyState = safeMessages.length
    ? ""
    : '<p class="messages-empty">Empieza con una pregunta, una idea del proyecto o la reparacion que necesitas.</p>'

  threadWrap.innerHTML = `${introRow}${body}${emptyState}`
  scrollThreadToBottom()
}

function syncComposerState() {
  if (chatSendButton) {
    chatSendButton.disabled = state.sending || state.uploadingPhotos
    chatSendButton.textContent = state.sending ? "Enviando..." : "Enviar"
  }

  if (chatPhotoButton) {
    chatPhotoButton.disabled = state.sending || state.uploadingPhotos
    chatPhotoButton.textContent = state.uploadingPhotos ? "Subiendo fotos..." : "Agregar fotos"
  }
}

async function loadThread({ silent = false } = {}) {
  try {
    const result = await apiRequest("/api/public/metalworks/live-chat/thread", {
      method: "POST",
      body: {
        visitorId: state.visitorId,
        sessionId: state.sessionId,
      },
    })

    const nextThread = result.thread || null
    state.leadId = String(nextThread?.leadId || "").trim()
    state.messages = Array.isArray(nextThread?.messages) ? nextThread.messages : []
    state.photoFileNames = Array.isArray(nextThread?.photoFileNames) ? nextThread.photoFileNames : []
    renderThread()
    renderPhotoFiles()

    if (state.leadId) {
      setStatus("Conversacion conectada al CRM")
      if (!silent) {
        setFeedback("", "")
      }
    } else {
      setStatus("Listo para empezar")
    }
  } catch (error) {
    if (!silent) {
      setFeedback(error.message || "No pude cargar esta conversacion.", "error")
    }

    if (state.messages.length) {
      setStatus("Reconectando el chat...")
    }
  }
}

async function handleSendMessage(event) {
  event.preventDefault()

  if (!chatInput) {
    return
  }

  const message = String(chatInput.value || "").trim()

  if (!message) {
    setFeedback("Escribe tu mensaje primero.", "error")
    return
  }

  state.sending = true
  syncComposerState()
  setFeedback("Mandando tu mensaje...", "muted")
  setStatus("Enviando al CRM")

  try {
    const profile = persistProfile()
    const result = await apiRequest("/api/public/metalworks/live-chat/messages", {
      method: "POST",
      body: {
        message,
        visitorId: state.visitorId,
        sessionId: state.sessionId,
        profile,
        pageTitle: document.title || "",
        pagePath: window.location.pathname || "",
        pageUrl: window.location.href || "",
        referrer: document.referrer || "",
        tracking: buildTrackingPayload(),
      },
    })

    const nextThread = result.thread || null
    state.leadId = String(nextThread?.leadId || "").trim()
    state.messages = Array.isArray(nextThread?.messages) ? nextThread.messages : []
    state.photoFileNames = Array.isArray(nextThread?.photoFileNames) ? nextThread.photoFileNames : []
    renderThread()
    renderPhotoFiles()

    chatInput.value = ""
    autoResizeTextarea()
    setFeedback("Tu mensaje ya llego a Chicago Metal Works.", "success")
    setStatus("Conversacion conectada al CRM")

    if (profileDetails && profile.fullName) {
      profileDetails.open = false
    }
  } catch (error) {
    setFeedback(error.message || "No pude mandar tu mensaje.", "error")
    setStatus("No se pudo enviar")
  } finally {
    state.sending = false
    syncComposerState()
  }
}

async function handlePhotoSelection(event) {
  const selectedFiles = Array.from(event.target?.files || [])

  if (!selectedFiles.length) {
    return
  }

  if (selectedFiles.length > MAX_CHAT_PHOTO_FILES) {
    setFeedback(`Puedes subir hasta ${MAX_CHAT_PHOTO_FILES} fotos por envio.`, "error")
    if (chatPhotoInput) {
      chatPhotoInput.value = ""
    }
    return
  }

  if (selectedFiles.some((file) => !String(file?.type || "").startsWith("image/"))) {
    setFeedback("Solo se permiten imagenes.", "error")
    if (chatPhotoInput) {
      chatPhotoInput.value = ""
    }
    return
  }

  const totalBytes = selectedFiles.reduce((sum, file) => sum + (Number(file?.size || 0) || 0), 0)

  if (totalBytes > MAX_CHAT_TOTAL_BYTES) {
    setFeedback("El total de imagenes es demasiado grande. Intenta con menos fotos.", "error")
    if (chatPhotoInput) {
      chatPhotoInput.value = ""
    }
    return
  }

  state.uploadingPhotos = true
  syncComposerState()
  setFeedback("Subiendo fotos al CRM...", "muted")
  setStatus("Guardando fotos en el CRM")

  try {
    const profile = persistProfile()
    const files = await Promise.all(
      selectedFiles.map(async (file) => ({
        fileName: file.name || "project-photo.jpg",
        mimeType: file.type || "image/jpeg",
        dataUrl: await readFileAsDataUrl(file),
      })),
    )
    const result = await apiRequest("/api/public/metalworks/live-chat/photos", {
      method: "POST",
      body: {
        visitorId: state.visitorId,
        sessionId: state.sessionId,
        profile,
        files,
        pageTitle: document.title || "",
        pagePath: window.location.pathname || "",
        pageUrl: window.location.href || "",
        referrer: document.referrer || "",
        tracking: buildTrackingPayload(),
      },
    })

    const nextThread = result.thread || null
    state.leadId = String(nextThread?.leadId || state.leadId || "").trim()
    state.messages = Array.isArray(nextThread?.messages) ? nextThread.messages : state.messages
    state.photoFileNames = Array.isArray(nextThread?.photoFileNames)
      ? nextThread.photoFileNames
      : state.photoFileNames
    renderThread()
    renderPhotoFiles()
    setFeedback("Tus fotos ya quedaron guardadas en el lead.", "success")
    setStatus("Fotos guardadas en el CRM")
  } catch (error) {
    setFeedback(error.message || "No pude subir las fotos.", "error")
    setStatus("No se pudieron guardar las fotos")
  } finally {
    state.uploadingPhotos = false
    syncComposerState()
    if (chatPhotoInput) {
      chatPhotoInput.value = ""
    }
  }
}

function startPolling() {
  if (state.pollHandle) {
    window.clearInterval(state.pollHandle)
  }

  state.pollHandle = window.setInterval(() => {
    if (document.hidden || state.sending) {
      return
    }

    loadThread({ silent: true }).catch(() => {})
  }, POLL_INTERVAL_MS)
}

function bindEvents() {
  chatForm?.addEventListener("submit", handleSendMessage)
  chatPhotoButton?.addEventListener("click", () => {
    chatPhotoInput?.click()
  })
  chatPhotoInput?.addEventListener("change", handlePhotoSelection)
  chatInput?.addEventListener("input", autoResizeTextarea)
  chatInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault()
      chatForm?.requestSubmit()
    }
  })

  ;[profileNameInput, profilePhoneInput, profileEmailInput].forEach((input) => {
    input?.addEventListener("input", () => {
      persistProfile()
    })
  })

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      loadThread({ silent: true }).catch(() => {})
    }
  })
}

async function init() {
  ensureThreadIdentity()
  hydrateProfileInputs()
  bindEvents()
  autoResizeTextarea()
  renderThread()
  renderPhotoFiles()
  syncComposerState()
  await loadThread({ silent: true })
  startPolling()
}

init().catch((error) => {
  console.error(error)
  setFeedback("No pude iniciar este chat en este momento.", "error")
  setStatus("Intenta otra vez")
})
