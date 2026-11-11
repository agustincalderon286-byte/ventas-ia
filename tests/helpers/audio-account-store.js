// Deterministic document store for HTTP/auth integration tests; no external DB.
export function audioAccountStore(seed = []) {
  const rows = new Map(seed.map(row => [row._id, structuredClone(row)]));
  const matches = (row, filter) => Object.entries(filter).every(([key, expected]) => {
    if (key === '$or') return expected.some(part => matches(row, part));
    const actual = row[key];
    if (expected && typeof expected === 'object' && !(expected instanceof Date)) {
      return Object.entries(expected).every(([operator, value]) => {
        if (operator === '$exists') return (actual !== undefined) === value;
        if (operator === '$ne') return actual !== value;
        if (operator === '$lte') return actual <= value;
        if (operator === '$gt') return actual > value;
        throw new Error('Unsupported test query: ' + operator);
      });
    }
    return actual === expected;
  });
  const lean = run => ({ async lean() { return structuredClone(run()); } });
  const change = (filter, update, options = {}) => {
    const previous = [...rows.values()].find(row => matches(row, filter));
    if (!previous && !options.upsert) return null;
    if (!previous && rows.has(filter._id)) throw Object.assign(new Error('duplicate id'), { code: 11000 });
    const row = { ...(previous || { _id: filter._id, ...update.$setOnInsert }), ...update.$set };
    for (const key of Object.keys(update.$unset || {})) delete row[key];
    for (const name of ['usernameCanonical', 'authGrantDigest']) {
      if (typeof row[name] === 'string' && [...rows.values()].some(other => other._id !== row._id && other[name] === row[name])) {
        throw Object.assign(new Error('duplicate unique field'), { code: 11000 });
      }
    }
    rows.set(row._id, structuredClone(row));
    return row;
  };
  return {
    rows,
    model: {
      async init() {},
      findById: id => lean(() => rows.get(id)),
      findOne: filter => lean(() => [...rows.values()].find(row => matches(row, filter))),
      findOneAndUpdate: (filter, update, options) => lean(() => change(filter, update, options)),
      async updateOne(filter, update, options) { return change(filter, update, options); },
      async create(row) { if (rows.has(row._id)) throw Object.assign(new Error('duplicate'), { code: 11000 }); rows.set(row._id, structuredClone(row)); },
      async deleteOne(filter) { const row = [...rows.values()].find(row => matches(row, filter)); if (row) rows.delete(row._id); }
    }
  };
}
