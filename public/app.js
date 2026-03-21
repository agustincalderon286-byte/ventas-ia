const form = document.getElementById("chatForm");
const input = document.getElementById("messageInput");
const messages = document.getElementById("messages");
const sendButton = document.getElementById("sendButton");
const statusBadge = document.getElementById("statusBadge");
const promptChips = document.querySelectorAll(".prompt-chip");
const CHAT_API_URL = "/chat";
const SESSION_STORAGE_KEY = "ventas-ia-session-id";

const conversation = [
  {
    role: "assistant",
    content:
      "Hi! I’m your website assistant. Ask me about services, pricing, support, or what your business offers."
  }
];

function getSessionId() {
  const existingSessionId = window.localStorage.getItem(SESSION_STORAGE_KEY);

  if (existingSessionId) {
    return existingSessionId;
  }

  const newSessionId = `web_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
  window.localStorage.setItem(SESSION_STORAGE_KEY, newSessionId);
  return newSessionId;
}

function autoResize() {
  input.style.height = "auto";
  input.style.height = `${Math.min(input.scrollHeight, 180)}px`;
}

function addMessage(role, content) {
  const card = document.createElement("article");
  card.className = `message ${role}`;

  const paragraph = document.createElement("p");
  paragraph.textContent = content;
  card.appendChild(paragraph);

  messages.appendChild(card);
  messages.scrollTop = messages.scrollHeight;
}

function setLoadingState(isLoading) {
  sendButton.disabled = isLoading;
  input.disabled = isLoading;
  statusBadge.textContent = isLoading ? "Thinking..." : "Ready";
}

async function sendMessage(content) {
  const text = content.trim();

  if (!text) {
    return;
  }

  addMessage("user", text);
  conversation.push({ role: "user", content: text });
  input.value = "";
  autoResize();
  setLoadingState(true);

  try {
    const response = await fetch(CHAT_API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        pregunta: text,
        sessionId: getSessionId()
      })
    });

    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.error || "Something went wrong.");
    }

    const reply = payload.respuesta || "I’m sorry, but I don’t have a reply yet.";
    conversation.push({ role: "assistant", content: reply });
    addMessage("assistant", reply);
  } catch (error) {
    addMessage(
      "assistant",
      error.message || "The assistant hit an unexpected issue. Please try again in a moment."
    );
  } finally {
    setLoadingState(false);
    input.focus();
  }
}

form.addEventListener("submit", event => {
  event.preventDefault();
  sendMessage(input.value);
});

input.addEventListener("input", autoResize);

input.addEventListener("keydown", event => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    sendMessage(input.value);
  }
});

promptChips.forEach(chip => {
  chip.addEventListener("click", () => {
    sendMessage(chip.textContent || "");
  });
});

autoResize();
