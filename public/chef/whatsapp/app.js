const PLATFORM_CONFIG_API_URL = "/api/platform/config";

async function hydrateChefWhatsAppPage() {
  const openLink = document.querySelector("[data-chef-whatsapp-open]");
  const note = document.querySelector("[data-chef-whatsapp-note]");

  if (!openLink || !note) {
    return;
  }

  try {
    const response = await fetch(PLATFORM_CONFIG_API_URL, {
      headers: {
        Accept: "application/json"
      }
    });

    const config = await response.json();
    const directUrl = String(config?.whatsapp?.chefUrl || "").trim();
    const enabled = Boolean(config?.whatsapp?.chefEnabled && directUrl);

    if (!enabled) {
      openLink.setAttribute("aria-disabled", "true");
      openLink.removeAttribute("href");
      note.textContent = "Activa WhatsApp del Chef para que este acceso abra el chat directo.";
      return;
    }

    openLink.href = directUrl;
    openLink.removeAttribute("aria-disabled");
    note.textContent = "Toca el botón verde y entrarás al chat de WhatsApp con el mensaje listo para empezar.";
  } catch (error) {
    openLink.setAttribute("aria-disabled", "true");
    openLink.removeAttribute("href");
    note.textContent = "No pude cargar el acceso de WhatsApp en este momento.";
  }
}

hydrateChefWhatsAppPage();
