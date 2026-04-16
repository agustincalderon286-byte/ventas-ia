const THREAD_STORAGE_KEY = "cmwf_live_chat_thread_v1"
const PROFILE_STORAGE_KEY = "cmwf_live_chat_profile_v1"
const POLL_INTERVAL_MS = 5000
const MAX_CHAT_PHOTO_FILES = 4
const MAX_CHAT_PHOTO_BYTES = 2 * 1024 * 1024
const MAX_CHAT_TOTAL_BYTES = 6 * 1024 * 1024
const MAX_CHAT_PHOTO_DIMENSION = 2200
const MIN_CHAT_PHOTO_TARGET_BYTES = 350 * 1024
const CHAT_PUSH_SW_PATH = "/metalworks-chat/chat-sw.js"
const CHAT_PUSH_SW_SCOPE = "/metalworks-chat/"
const INTRO_MESSAGE =
  "This chat goes directly to Chicago Metal Works & Fencing. Tell us what you need and we will reply from the CRM."

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
const chatInstallButton = document.querySelector("[data-chat-install]")
const chatEnablePushButton = document.querySelector("[data-chat-enable-push]")
const chatInstallHint = document.querySelector("[data-chat-install-hint]")
const profileDetails = document.querySelector("[data-chat-profile]")
const profileSummary = document.querySelector("[data-chat-profile-summary]")
const profileNameInput = document.querySelector("[data-chat-profile-name]")
const profilePhoneInput = document.querySelector("[data-chat-profile-phone]")
const profileEmailInput = document.querySelector("[data-chat-profile-email]")

const state = {
  visitorId: "",
  sessionId: "",
  threadKey: "",
  leadId: "",
  messages: [],
  sending: false,
  uploadingPhotos: false,
  photoFileNames: [],
  pushConfig: null,
  pushRegistration: null,
  pushSubscription: null,
  pushBusy: false,
  installPrompt: null,
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

function normalizeThreadKey(value = "") {
  return String(value || "")
    .trim()
    .replace(/[^A-Za-z0-9_-]/g, "")
    .slice(0, 120)
}

function readThreadKeyFromUrl() {
  const params = new URLSearchParams(window.location.search || "")
  return normalizeThreadKey(params.get("thread") || params.get("t") || "")
}

function persistThreadIdentity() {
  writeStoredJson(THREAD_STORAGE_KEY, {
    visitorId: state.visitorId,
    sessionId: state.sessionId,
    threadKey: state.threadKey,
  })
}

function syncThreadUrl() {
  const safeThreadKey = normalizeThreadKey(state.threadKey)

  if (!safeThreadKey || !window.history?.replaceState) {
    return
  }

  const nextUrl = new URL(window.location.href)
  const currentThreadKey = normalizeThreadKey(
    nextUrl.searchParams.get("thread") || nextUrl.searchParams.get("t") || "",
  )

  if (currentThreadKey === safeThreadKey && !nextUrl.searchParams.has("t")) {
    return
  }

  nextUrl.searchParams.set("thread", safeThreadKey)
  nextUrl.searchParams.delete("t")
  window.history.replaceState({}, "", nextUrl.toString())
}

function ensureThreadIdentity() {
  const stored = readStoredJson(THREAD_STORAGE_KEY, {}) || {}
  const initialThreadKey = readThreadKeyFromUrl() || normalizeThreadKey(stored.threadKey || "")
  const visitorId = String(stored.visitorId || "").trim() || createClientId("visitor")
  const sessionId = String(stored.sessionId || "").trim() || createClientId("session")

  state.visitorId = visitorId
  state.sessionId = sessionId
  state.threadKey = initialThreadKey

  persistThreadIdentity()
  syncThreadUrl()
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
    ? `Saved on this device: ${tags.join(" · ")}`
    : "Add your name or phone number for faster follow-up"
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

  chatStatus.textContent = message || "Ready to start"
}

function renameFileExtension(fileName = "", extension = ".jpg") {
  const safeExtension = String(extension || ".jpg").startsWith(".")
    ? String(extension || ".jpg")
    : `.${String(extension || "jpg")}`
  const baseName = String(fileName || "project-photo")
    .trim()
    .replace(/\.[A-Za-z0-9]+$/, "")

  return `${baseName || "project-photo"}${safeExtension}`
}

function canvasToBlob(canvas, mimeType = "image/jpeg", quality = 0.82) {
  return new Promise((resolve, reject) => {
    canvas.toBlob(
      (blob) => {
        if (blob) {
          resolve(blob)
          return
        }

        reject(new Error("Could not prepare this image for upload."))
      },
      mimeType,
      quality,
    )
  })
}

function loadImageFromFile(file) {
  return new Promise((resolve, reject) => {
    const objectUrl = URL.createObjectURL(file)
    const image = new Image()

    image.onload = () => {
      URL.revokeObjectURL(objectUrl)
      resolve(image)
    }

    image.onerror = () => {
      URL.revokeObjectURL(objectUrl)
      reject(new Error(`Could not process ${file?.name || "this image"}.`))
    }

    image.src = objectUrl
  })
}

async function optimizeImageFile(file, { targetBytes = MAX_CHAT_PHOTO_BYTES } = {}) {
  const safeTargetBytes = Math.max(
    MIN_CHAT_PHOTO_TARGET_BYTES,
    Math.min(MAX_CHAT_PHOTO_BYTES, Number(targetBytes || MAX_CHAT_PHOTO_BYTES) || MAX_CHAT_PHOTO_BYTES),
  )

  if (!(file instanceof File)) {
    throw new Error("Could not read this image file.")
  }

  if (!String(file.type || "").startsWith("image/")) {
    throw new Error("Only image uploads are allowed.")
  }

  if (/^image\/gif$/i.test(String(file.type || ""))) {
    if (file.size <= safeTargetBytes) {
      return file
    }

    throw new Error(`${file.name || "This image"} is too large. Please choose a smaller file.`)
  }

  if (
    file.size <= safeTargetBytes &&
    file.size <= MAX_CHAT_PHOTO_BYTES &&
    !/^image\/(?:heic|heif)$/i.test(String(file.type || ""))
  ) {
    return file
  }

  const image = await loadImageFromFile(file)
  const originalWidth = Number(image.naturalWidth || image.width || 0) || 1
  const originalHeight = Number(image.naturalHeight || image.height || 0) || 1
  const baseScale = Math.min(1, MAX_CHAT_PHOTO_DIMENSION / Math.max(originalWidth, originalHeight))
  const canvas = document.createElement("canvas")
  const context = canvas.getContext("2d", { alpha: false })

  if (!context) {
    throw new Error("This device could not prepare the image for upload.")
  }

  const dimensionScales = [1, 0.88, 0.76, 0.64, 0.52]
  const qualitySteps = [0.9, 0.82, 0.74, 0.66, 0.58, 0.5, 0.42]
  let bestBlob = null

  for (const dimensionScale of dimensionScales) {
    const width = Math.max(1, Math.round(originalWidth * baseScale * dimensionScale))
    const height = Math.max(1, Math.round(originalHeight * baseScale * dimensionScale))

    canvas.width = width
    canvas.height = height
    context.fillStyle = "#ffffff"
    context.fillRect(0, 0, width, height)
    context.drawImage(image, 0, 0, width, height)

    for (const quality of qualitySteps) {
      const blob = await canvasToBlob(canvas, "image/jpeg", quality)

      if (!bestBlob || blob.size < bestBlob.size) {
        bestBlob = blob
      }

      if (blob.size <= safeTargetBytes && blob.size <= MAX_CHAT_PHOTO_BYTES) {
        return new File([blob], renameFileExtension(file.name, ".jpg"), {
          type: "image/jpeg",
          lastModified: Date.now(),
        })
      }
    }
  }

  if (bestBlob && bestBlob.size <= MAX_CHAT_PHOTO_BYTES) {
    return new File([bestBlob], renameFileExtension(file.name, ".jpg"), {
      type: "image/jpeg",
      lastModified: Date.now(),
    })
  }

  throw new Error(
    `${file.name || "This image"} is still too large after compression. Try cropping it or taking the photo a little closer.`,
  )
}

async function preparePhotoFilesForUpload(selectedFiles = []) {
  const perFileTarget = Math.max(
    MIN_CHAT_PHOTO_TARGET_BYTES,
    Math.min(MAX_CHAT_PHOTO_BYTES, Math.floor(MAX_CHAT_TOTAL_BYTES / Math.max(selectedFiles.length, 1))),
  )

  let optimizedFiles = await Promise.all(
    selectedFiles.map((file) => optimizeImageFile(file, { targetBytes: perFileTarget })),
  )

  let totalBytes = optimizedFiles.reduce((sum, file) => sum + (Number(file?.size || 0) || 0), 0)

  if (totalBytes <= MAX_CHAT_TOTAL_BYTES) {
    return optimizedFiles
  }

  const tighterTarget = Math.max(MIN_CHAT_PHOTO_TARGET_BYTES, Math.floor(perFileTarget * 0.82))
  optimizedFiles = await Promise.all(
    optimizedFiles.map((file) => optimizeImageFile(file, { targetBytes: tighterTarget })),
  )
  totalBytes = optimizedFiles.reduce((sum, file) => sum + (Number(file?.size || 0) || 0), 0)

  if (totalBytes <= MAX_CHAT_TOTAL_BYTES) {
    return optimizedFiles
  }

  throw new Error("These photos are still too large. Try fewer photos or crop them before uploading.")
}

function detectBrowserName() {
  const userAgent = String(navigator.userAgent || "")

  if (/edg\//i.test(userAgent)) return "Edge"
  if (/opr\//i.test(userAgent)) return "Opera"
  if (/chrome\//i.test(userAgent) && !/edg\//i.test(userAgent)) return "Chrome"
  if (/firefox\//i.test(userAgent)) return "Firefox"
  if (/safari\//i.test(userAgent) && !/chrome\//i.test(userAgent)) return "Safari"
  return "Browser"
}

function isAppleMobileDevice() {
  return /iphone|ipad|ipod/i.test(String(navigator.userAgent || ""))
}

function isStandaloneApp() {
  try {
    return (
      window.matchMedia("(display-mode: standalone)").matches ||
      window.navigator.standalone === true
    )
  } catch {
    return window.navigator.standalone === true
  }
}

function needsHomeScreenInstallForPush() {
  return isAppleMobileDevice() && !isStandaloneApp()
}

function supportsWebPush() {
  return Boolean(
    window.isSecureContext &&
      "Notification" in window &&
      "serviceWorker" in navigator &&
      "PushManager" in window,
  )
}

function urlBase64ToUint8Array(value = "") {
  const padding = "=".repeat((4 - (value.length % 4)) % 4)
  const base64 = `${value}${padding}`.replace(/-/g, "+").replace(/_/g, "/")
  const rawData = window.atob(base64)
  return Uint8Array.from(rawData, (char) => char.charCodeAt(0))
}

function buildPushDeviceName() {
  const platform = String(navigator.platform || "device").trim()
  return `${detectBrowserName()} on ${platform}`.slice(0, 120)
}

function syncInstallUi() {
  if (chatInstallButton) {
    const canPrompt = Boolean(state.installPrompt)
    const canGuide = isAppleMobileDevice() && !isStandaloneApp()
    chatInstallButton.disabled = isStandaloneApp()
    chatInstallButton.textContent =
      canPrompt || canGuide
        ? "Save app"
        : isStandaloneApp()
          ? "App installed"
          : "How to install"
  }

  if (chatInstallHint) {
    if (needsHomeScreenInstallForPush()) {
      chatInstallHint.textContent =
        "On iPhone, tap Share and then Add to Home Screen to save this as an app and enable alerts."
      return
    }

    if (state.installPrompt) {
      chatInstallHint.textContent =
        "You can save this chat as an app and get alerts when Chicago Metal Works replies."
      return
    }

    chatInstallHint.textContent =
      "Save this page as an app to open your chat faster and get alerts when Chicago Metal Works replies."
  }
}

function syncPushButton() {
  if (!chatEnablePushButton) {
    return
  }

  const permission = supportsWebPush() ? Notification.permission : "unsupported"
  const configured = Boolean(state.pushConfig?.webPushConfigured)
  const enabled = Boolean(state.pushSubscription && permission === "granted")
  chatEnablePushButton.disabled =
    state.pushBusy ||
    !supportsWebPush() ||
    !configured ||
    needsHomeScreenInstallForPush()
  chatEnablePushButton.textContent = enabled
    ? "Alerts on"
    : needsHomeScreenInstallForPush()
      ? "Install app"
      : permission === "denied"
        ? "Alerts blocked"
        : "Enable alerts"
}

async function registerChatServiceWorker() {
  if (!("serviceWorker" in navigator)) {
    return null
  }

  if (state.pushRegistration) {
    return state.pushRegistration
  }

  const registration = await navigator.serviceWorker.register(CHAT_PUSH_SW_PATH, {
    scope: CHAT_PUSH_SW_SCOPE,
  })
  state.pushRegistration = await navigator.serviceWorker.ready
  registration.update().catch(() => null)
  return state.pushRegistration
}

async function refreshPushSubscription({ syncServer = false } = {}) {
  if (!supportsWebPush()) {
    state.pushSubscription = null
    syncPushButton()
    return null
  }

  const registration = await registerChatServiceWorker()
  const subscription = await registration.pushManager.getSubscription()
  state.pushSubscription = subscription

  if (
    syncServer &&
    subscription &&
    Notification.permission === "granted" &&
    state.pushConfig?.webPushConfigured
  ) {
    await apiRequest("/api/public/metalworks/live-chat/push/register", {
      method: "POST",
      body: {
        subscription: subscription.toJSON(),
        visitorId: state.visitorId,
        sessionId: state.sessionId,
        threadKey: state.threadKey,
        leadId: state.leadId,
        deviceName: buildPushDeviceName(),
        browserName: detectBrowserName(),
        notificationPath: "/metalworks-chat/",
        authorizationStatus: Notification.permission,
        notificationsEnabled: true,
      },
    })
  }

  syncPushButton()
  return subscription
}

async function loadPushConfig({ silent = false } = {}) {
  if (needsHomeScreenInstallForPush()) {
    if (!silent) {
      setFeedback(
        "Install this app on your Home Screen so iPhone can send you alerts.",
        "muted",
      )
    }
    syncPushButton()
    syncInstallUi()
    return null
  }

  if (!supportsWebPush()) {
    syncPushButton()
    syncInstallUi()
    return null
  }

  try {
    state.pushConfig = await apiRequest("/api/public/metalworks/live-chat/push/config")
    await refreshPushSubscription({ syncServer: true })

    if (!silent) {
      if (!state.pushConfig?.webPushConfigured) {
        setFeedback("Alerts still need VAPID keys on the server.", "warning")
      } else if (state.pushSubscription && Notification.permission === "granted") {
        setFeedback("Chat alerts are active on this device.", "success")
      }
    }

    syncPushButton()
    syncInstallUi()
    return state.pushConfig
  } catch (error) {
    if (!silent) {
      setFeedback(error.message || "Could not load chat alerts.", "error")
    }
    syncPushButton()
    syncInstallUi()
    return null
  }
}

async function handleEnablePush() {
  if (needsHomeScreenInstallForPush()) {
    setFeedback(
      "Install the app on your Home Screen first to enable alerts on iPhone.",
      "warning",
    )
    syncPushButton()
    return
  }

  if (!supportsWebPush()) {
    setFeedback("This browser does not support secure push alerts.", "error")
    return
  }

  if (!state.pushConfig?.webPushConfigured || !state.pushConfig?.vapidPublicKey) {
    setFeedback("Alerts are not configured on the server yet.", "warning")
    syncPushButton()
    return
  }

  state.pushBusy = true
  syncPushButton()
  setFeedback("Turning on alerts...", "muted")

  try {
    const permission = await Notification.requestPermission()

    if (permission !== "granted") {
      setFeedback(
        permission === "denied"
          ? "Alerts were blocked on this device."
          : "Alert permission was not granted.",
        "warning",
      )
      return
    }

    const registration = await registerChatServiceWorker()
    let subscription = await registration.pushManager.getSubscription()

    if (!subscription) {
      subscription = await registration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(state.pushConfig.vapidPublicKey),
      })
    }

    await apiRequest("/api/public/metalworks/live-chat/push/register", {
      method: "POST",
      body: {
        subscription: subscription.toJSON(),
        visitorId: state.visitorId,
        sessionId: state.sessionId,
        threadKey: state.threadKey,
        leadId: state.leadId,
        deviceName: buildPushDeviceName(),
        browserName: detectBrowserName(),
        notificationPath: "/metalworks-chat/",
        authorizationStatus: permission,
        notificationsEnabled: true,
      },
    })

    state.pushSubscription = subscription
    setFeedback("Done. This device will receive live replies.", "success")
  } catch (error) {
    setFeedback(error.message || "Could not enable alerts on this device.", "error")
  } finally {
    state.pushBusy = false
    syncPushButton()
  }
}

async function handleInstall() {
  if (state.installPrompt) {
    state.installPrompt.prompt()
    await state.installPrompt.userChoice.catch(() => null)
    state.installPrompt = null
    syncInstallUi()
    return
  }

  if (isAppleMobileDevice() && !isStandaloneApp()) {
    setFeedback(
      "On iPhone, tap Share and then Add to Home Screen to save this as an app.",
      "muted",
    )
    return
  }

  setFeedback("If your browser allows it, use its install or save app option.", "muted")
}

function renderPhotoFiles() {
  if (!chatPhotoStrip || !chatPhotoList || !chatPhotoCount) {
    return
  }

  const fileNames = Array.isArray(state.photoFileNames) ? state.photoFileNames.filter(Boolean) : []

  chatPhotoStrip.hidden = fileNames.length === 0
  chatPhotoCount.textContent = `${fileNames.length} photo${fileNames.length === 1 ? "" : "s"} saved`
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
      reject(new Error(`Could not read ${file?.name || "this image"}.`))
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
    throw new Error(data.error || "Could not complete this action.")
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
      const author = role === "assistant" ? "Chicago Metal Works" : "You"
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
    : '<p class="messages-empty">Start with a question, a project idea, or the repair you need.</p>'

  threadWrap.innerHTML = `${introRow}${body}${emptyState}`
  scrollThreadToBottom()
}

function syncComposerState() {
  if (chatSendButton) {
    chatSendButton.disabled = state.sending || state.uploadingPhotos
    chatSendButton.textContent = state.sending ? "Sending..." : "Send"
  }

  if (chatPhotoButton) {
    chatPhotoButton.disabled = state.sending || state.uploadingPhotos
    chatPhotoButton.textContent = state.uploadingPhotos ? "Uploading photos..." : "Add photos"
  }

  syncPushButton()
}

function applyThread(nextThread = null) {
  const safeThread = nextThread && typeof nextThread === "object" ? nextThread : null
  const nextThreadKey = normalizeThreadKey(safeThread?.threadKey || state.threadKey || readThreadKeyFromUrl())

  state.threadKey = nextThreadKey
  state.leadId = String(safeThread?.leadId || "").trim()
  state.messages = Array.isArray(safeThread?.messages) ? safeThread.messages : []
  state.photoFileNames = Array.isArray(safeThread?.photoFileNames) ? safeThread.photoFileNames : []

  persistThreadIdentity()
  syncThreadUrl()
  renderThread()
  renderPhotoFiles()
}

async function loadThread({ silent = false } = {}) {
  try {
    const result = await apiRequest("/api/public/metalworks/live-chat/thread", {
      method: "POST",
      body: {
        visitorId: state.visitorId,
        sessionId: state.sessionId,
        threadKey: state.threadKey,
      },
    })

    applyThread(result.thread || null)
    if (state.pushSubscription) {
      refreshPushSubscription({ syncServer: true }).catch(() => {})
    }

    if (state.leadId) {
      setStatus("Conversation connected to the CRM")
      if (!silent) {
        setFeedback("", "")
      }
    } else {
      setStatus("Ready to start")
    }
  } catch (error) {
    if (!silent) {
      setFeedback(error.message || "Could not load this conversation.", "error")
    }

    if (state.messages.length) {
      setStatus("Reconnecting chat...")
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
    setFeedback("Type your message first.", "error")
    return
  }

  state.sending = true
  syncComposerState()
  setFeedback("Sending your message...", "muted")
  setStatus("Sending to the CRM")

  try {
    const profile = persistProfile()
    const result = await apiRequest("/api/public/metalworks/live-chat/messages", {
      method: "POST",
      body: {
        message,
        visitorId: state.visitorId,
        sessionId: state.sessionId,
        threadKey: state.threadKey,
        profile,
        pageTitle: document.title || "",
        pagePath: window.location.pathname || "",
        pageUrl: window.location.href || "",
        referrer: document.referrer || "",
        tracking: buildTrackingPayload(),
      },
    })

    applyThread(result.thread || null)
    if (state.pushSubscription) {
      refreshPushSubscription({ syncServer: true }).catch(() => {})
    }

    chatInput.value = ""
    autoResizeTextarea()
    setFeedback("Your message reached Chicago Metal Works.", "success")
    setStatus("Conversation connected to the CRM")

    if (profileDetails && profile.fullName) {
      profileDetails.open = false
    }
  } catch (error) {
    setFeedback(error.message || "Could not send your message.", "error")
    setStatus("Message not sent")
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
    setFeedback(`You can upload up to ${MAX_CHAT_PHOTO_FILES} photos at a time.`, "error")
    if (chatPhotoInput) {
      chatPhotoInput.value = ""
    }
    return
  }

  if (selectedFiles.some((file) => !String(file?.type || "").startsWith("image/"))) {
    setFeedback("Only image uploads are allowed.", "error")
    if (chatPhotoInput) {
      chatPhotoInput.value = ""
    }
    return
  }

  state.uploadingPhotos = true
  syncComposerState()
  setFeedback("Preparing photos for upload...", "muted")
  setStatus("Saving photos to the CRM")

  try {
    const profile = persistProfile()
    const preparedFiles = await preparePhotoFilesForUpload(selectedFiles)
    const files = await Promise.all(
      preparedFiles.map(async (file) => ({
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
        threadKey: state.threadKey,
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
    applyThread(
      nextThread || {
        leadId: state.leadId,
        threadKey: state.threadKey,
        messages: state.messages,
        photoFileNames: state.photoFileNames,
      },
    )
    if (state.pushSubscription) {
      refreshPushSubscription({ syncServer: true }).catch(() => {})
    }
    setFeedback("Your photos were saved to the lead.", "success")
    setStatus("Photos saved to the CRM")
  } catch (error) {
    setFeedback(error.message || "Could not upload these photos.", "error")
    setStatus("Photos were not saved")
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
  chatInstallButton?.addEventListener("click", handleInstall)
  chatEnablePushButton?.addEventListener("click", handleEnablePush)
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

  window.addEventListener("beforeinstallprompt", (event) => {
    event.preventDefault()
    state.installPrompt = event
    syncInstallUi()
  })

  window.addEventListener("appinstalled", () => {
    state.installPrompt = null
    syncInstallUi()
  })
}

async function init() {
  ensureThreadIdentity()
  hydrateProfileInputs()
  bindEvents()
  syncInstallUi()
  autoResizeTextarea()
  renderThread()
  renderPhotoFiles()
  syncComposerState()
  registerChatServiceWorker().catch(() => null)
  await loadPushConfig({ silent: true })
  await loadThread({ silent: true })
  startPolling()
}

init().catch((error) => {
  console.error(error)
  setFeedback("Could not start this chat right now.", "error")
  setStatus("Try again")
})
