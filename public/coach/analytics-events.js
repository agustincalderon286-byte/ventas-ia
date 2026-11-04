// Fixed vocabulary only. Never derive parameters from text, inputs, URLs or event details.
export const MEASUREMENT_ID = "G-DZCX3RMST7";
export const ANALYTICS_PROPERTY_ID = "562322027"; // Filled from the property created for TEC.
export const AREAS = Object.freeze(["inicio", "login", "planes", "cierre", "actividad", "mapa", "equipo", "biblioteca", "privacidad"]);
export const EVENTS = Object.freeze(["page_view", "workspace_view", "calculator_open", "survey_open", "survey_submit_requested", "catalog_open", "video_open", "coach_open", "route_open", "recording_open", "checkout_start_requested", "login_submit_requested", "registration_submit_requested"]);
export function safeEvent(name, area, surface) {
  if (!EVENTS.includes(name) || !AREAS.includes(area) || !["web", "pwa"].includes(surface)) return null;
  return { name, area, surface };
}
export function safePage(area) {
  const paths = { inicio: "", login: "login/", planes: "planes/", privacidad: "privacidad/" };
  return "https://totalelitecoaching.com/coach/" + (Object.hasOwn(paths, area) ? paths[area] : "app/");
}
