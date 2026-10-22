// In-memory event store for isolated tests and loopback previews only.
export function memoryPropertyVisitModel() {
  const events = [];
  return {
    events,
    findOne(query) { return { lean: async () => events.find(row => Object.entries(query).every(([key, value]) => String(row[key]) === String(value))) || null }; },
    async create(record) {
      if (events.some(row => String(row.ownerUserId) === String(record.ownerUserId) && row.requestId === record.requestId)) throw Object.assign(new Error("Duplicate"), { code: 11000 });
      const row = { ...record, _id: String(events.length + 1).padStart(24, "0") };
      events.push(row); return row;
    },
    async aggregate(pipeline) {
      const filter = pipeline[0].$match;
      const matching = events.filter(row => String(row.ownerUserId) === String(filter.ownerUserId) && (!filter.propertyKey || filter.propertyKey.$in.includes(row.propertyKey)))
        .sort((a, b) => new Date(b.clientUpdatedAt) - new Date(a.clientUpdatedAt) || new Date(b.createdAt) - new Date(a.createdAt) || b._id.localeCompare(a._id));
      const grouped = new Map();
      for (const row of matching) {
        if (!grouped.has(row.propertyKey)) grouped.set(row.propertyKey, { record: row, linkedLeadId: "" });
        if (row.linkedLeadId) grouped.get(row.propertyKey).linkedLeadId = row.linkedLeadId;
      }
      return [...grouped.values()].slice(0, pipeline.at(-1).$limit);
    }
  };
}
