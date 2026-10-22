export const PROPERTY_VISIT_STATUSES = [
  { value: "not_answered", label: "No contestaron", color: "#7b667d" },
  { value: "answered", label: "Nos atendieron", color: "#22745e" },
  { value: "return_later", label: "Volver después", color: "#a36615" },
  { value: "appointment", label: "Cita agendada", color: "#245fb0" },
  { value: "do_not_visit", label: "No volver a visitar", color: "#7b3139" }
];

export function propertyVisitKey(property = {}) {
  const normalize = value => String(value || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toUpperCase().replace(/\./g, "").replace(/\s+/g, " ").trim();
  return `${normalize(property.address)}|${normalize(property.city)}|IL`;
}

export function mergePropertyVisits(remote = [], pending = []) {
  const result = new Map(remote.map(record => [record.propertyKey, { ...record, pending: Boolean(record.pending) }]));
  for (const record of pending) {
    const current = result.get(record.propertyKey);
    if (!current || String(record.clientUpdatedAt) >= String(current.clientUpdatedAt)) {
      result.set(record.propertyKey, { ...record, linkedLeadId: current?.linkedLeadId || record.linkedLeadId || "", pending: true });
    }
  }
  return [...result.values()].sort((a, b) => String(b.clientUpdatedAt).localeCompare(String(a.clientUpdatedAt)));
}

export function propertyVisitFromQueuedItem(item) {
  if (!["property-visit", "property-lead"].includes(item?.kind) || !item.body?.property) return null;
  if (item.kind === "property-lead") return null;
  return { ...item.body, propertyKey: propertyVisitKey(item.body.property), requestId: item.id, pending: true };
}
