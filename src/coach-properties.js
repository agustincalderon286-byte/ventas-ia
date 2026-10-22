const DATA_ORIGIN = "https://datacatalog.cookcountyil.gov";
const ADDRESS_DATASET = "3723-97qp";
const PARCEL_DATASET = "pabr-t5kh";
const ADDRESS_FIELDS = "pin,year,prop_address_full,prop_address_city_name,prop_address_state,prop_address_zipcode_1,owner_address_name";

function propertyError(status, message) {
  return Object.assign(new Error(message), { status });
}

export function normalizePropertyAddress(value) {
  if (typeof value !== "string" || value.length > 160) throw propertyError(400, "Escribe la calle y el número de la cita.");
  const replacements = { NORTH: "N", SOUTH: "S", EAST: "E", WEST: "W", STREET: "ST", AVENUE: "AVE", ROAD: "RD", DRIVE: "DR", BOULEVARD: "BLVD", LANE: "LN", COURT: "CT", PLACE: "PL", TERRACE: "TER", PARKWAY: "PKWY" };
  const result = value.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toUpperCase().replace(/\./g, "").replace(/\s+/g, " ").trim()
    .replace(/\b(NORTH|SOUTH|EAST|WEST|STREET|AVENUE|ROAD|DRIVE|BOULEVARD|LANE|COURT|PLACE|TERRACE|PARKWAY)\b/g, word => replacements[word]);
  if (!/^\d[\dA-Z /-]*\s+[A-Z0-9]/.test(result) || result.length < 5 || !/^[A-Z0-9 #/'-]+$/.test(result)) {
    throw propertyError(400, "Escribe sólo número y calle. La ciudad va en el campo Ciudad.");
  }
  return result;
}

export function normalizePropertyCity(value = "HILLSIDE") {
  if (typeof value !== "string" || value.length > 60) throw propertyError(400, "Revisa la ciudad de la cita.");
  const city = value.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toUpperCase().replace(/\s+/g, " ").trim();
  if (!/^[A-Z][A-Z .'-]{1,59}$/.test(city)) throw propertyError(400, "Escribe una ciudad de Cook County, Illinois.");
  return city;
}

function literal(value) { return `'${String(value).replaceAll("'", "''")}'`; }
function clean(value) { return String(value || "").trim().replace(/\s+/g, " "); }
function ownerName(value) { const name = clean(value); return /^(NULL|UNKNOWN|N\/A|NONE)$/i.test(name) ? "" : name; }
function coordinates(row) {
  if (row?.lat == null || row?.lon == null || row.lat === "" || row.lon === "") return null;
  const lat = Number(row.lat), lon = Number(row.lon);
  return Number.isFinite(lat) && Number.isFinite(lon) && lat > 41 && lat < 43 && lon > -89 && lon < -87 ? { lat, lon } : null;
}

export function propertyDistanceMeters(a, b) {
  const rad = n => n * Math.PI / 180;
  const h = Math.sin(rad(b.lat - a.lat) / 2) ** 2 + Math.cos(rad(a.lat)) * Math.cos(rad(b.lat)) * Math.sin(rad(b.lon - a.lon) / 2) ** 2;
  return 6371000 * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(Math.max(0, 1 - h)));
}

function serializeProperty(row, coordinateRow) {
  const location = coordinates(coordinateRow);
  if (!location || !/^\d{14}$/.test(row?.pin || "") || !clean(row.prop_address_full)) return null;
  const address = clean(row.prop_address_full), city = clean(row.prop_address_city_name), zipCode = clean(row.prop_address_zipcode_1);
  return {
    pin: row.pin, address, city, state: "IL", zipCode,
    formattedAddress: [address, city, "IL", zipCode].filter(Boolean).join(", "),
    ownerName: ownerName(row.owner_address_name),
    year: Number(row.year), coordinateYear: Number(coordinateRow.year),
    ...location
  };
}

export function createCoachPropertyService({ fetchImpl = fetch, now = Date.now } = {}) {
  const cache = new Map();
  let activeRequests = 0;

  async function cached(key, loader, ttl = 5 * 60 * 1000) {
    const old = cache.get(key);
    if (old && old.expires > now()) return old.promise;
    if (cache.size >= 100) cache.delete(cache.keys().next().value);
    const entry = { expires: now() + ttl, promise: Promise.resolve().then(loader) };
    cache.set(key, entry);
    try { return await entry.promise; } catch (error) { if (cache.get(key) === entry) cache.delete(key); throw error; }
  }

  async function request(dataset, params) {
    if (activeRequests >= 8) throw propertyError(503, "Hay varias búsquedas en curso. Intenta de nuevo en unos segundos.");
    const url = new URL(`/resource/${dataset}.json`, DATA_ORIGIN);
    for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
    activeRequests++;
    try {
      const response = await fetchImpl(url, { headers: { Accept: "application/json" }, signal: AbortSignal.timeout(20000) });
      if (!response.ok) throw propertyError(503, "Cook County no está respondiendo en este momento. Intenta nuevamente.");
      const rows = await response.json();
      if (!Array.isArray(rows)) throw propertyError(503, "No pude interpretar la respuesta de Cook County.");
      return rows;
    } catch (error) {
      if (error.status) throw error;
      throw propertyError(503, "No pude consultar Cook County. Revisa la conexión e intenta nuevamente.");
    } finally { activeRequests--; }
  }

  async function latestYear(city) {
    return cached(`year:${city}`, async () => {
      const [row] = await request(ADDRESS_DATASET, { $select: "max(year) as year", $where: `prop_address_city_name = ${literal(city)}` });
      const year = Number(row?.year);
      if (!Number.isInteger(year) || year < 2000 || year > 2100) throw propertyError(404, "No encontré esa ciudad en Cook County. Prueba el nombre oficial en inglés.");
      return year;
    }, 60 * 60 * 1000);
  }

  async function getCoordinates(pins) {
    if (!pins.length) return [];
    return request(PARCEL_DATASET, { $select: "pin,year,lat,lon", $where: `pin in (${pins.map(literal).join(",")})`, $limit: String(pins.length) });
  }

  async function search({ address: inputAddress, city: inputCity }) {
    const address = normalizePropertyAddress(inputAddress), city = normalizePropertyCity(inputCity);
    return cached(`search:${city}:${address}`, async () => {
      const year = await latestYear(city);
      const base = `year = ${year} AND prop_address_city_name = ${literal(city)}`;
      let rows = await request(ADDRESS_DATASET, { $select: ADDRESS_FIELDS, $where: `${base} AND prop_address_full = ${literal(address)}`, $limit: "13", $order: "pin" });
      if (!rows.length) rows = await request(ADDRESS_DATASET, {
        $select: ADDRESS_FIELDS,
        $where: `${base} AND (starts_with(prop_address_full, ${literal(address + " ")}) OR starts_with(prop_address_full, ${literal(address + " UNIT")}))`,
        $limit: "13", $order: "pin"
      });
      const truncated = rows.length > 12;
      rows = rows.slice(0, 12).filter(row => /^\d{14}$/.test(row.pin || ""));
      const coordRows = await getCoordinates(rows.map(row => row.pin));
      const byPin = new Map(coordRows.map(row => [row.pin, row]));
      const candidates = rows.map(row => serializeProperty(row, byPin.get(row.pin))).filter(Boolean);
      return { candidates, year, truncated, missingCoordinates: rows.length - candidates.length, queriedAt: new Date(now()).toISOString(), source: "Cook County Assessor" };
    });
  }

  async function nearby({ pin, radius: inputRadius = 200 }) {
    if (typeof pin !== "string" || !/^\d{14}$/.test(pin)) throw propertyError(400, "Selecciona primero una dirección encontrada.");
    const radius = Number(inputRadius);
    if (![100, 200, 300].includes(radius)) throw propertyError(400, "Elige un radio de 100, 200 o 300 metros.");
    return cached(`nearby:${pin}:${radius}`, async () => {
      const [anchorRow] = await request(ADDRESS_DATASET, { $select: ADDRESS_FIELDS, $where: `pin = ${literal(pin)}`, $order: "year DESC", $limit: "1" });
      if (!anchorRow) throw propertyError(404, "No encontré esa propiedad. Busca nuevamente la dirección.");
      const [anchorCoordinates] = await getCoordinates([pin]);
      const anchor = serializeProperty(anchorRow, anchorCoordinates);
      if (!anchor) throw propertyError(404, "Esta propiedad no tiene una ubicación disponible en el mapa.");
      const latDelta = radius / 110574, lonDelta = radius / (111320 * Math.cos(anchor.lat * Math.PI / 180));
      const bounds = await request(PARCEL_DATASET, {
        $select: "pin,year,lat,lon",
        $where: `lat between ${anchor.lat - latDelta} and ${anchor.lat + latDelta} AND lon between ${anchor.lon - lonDelta} and ${anchor.lon + lonDelta}`,
        $limit: "501", $order: "pin"
      });
      const matches = bounds.slice(0, 500).filter(row => row.pin !== pin && /^\d{14}$/.test(row.pin || "") && coordinates(row))
        .map(row => ({ ...row, distance: propertyDistanceMeters(anchor, coordinates(row)) }))
        .filter(row => row.distance <= radius).sort((a, b) => a.distance - b.distance);
      const nearest = matches.slice(0, 100);
      const addressRows = nearest.length ? await request(ADDRESS_DATASET, {
        $select: ADDRESS_FIELDS, $where: `year = ${Number(anchor.year)} AND pin in (${nearest.map(row => literal(row.pin)).join(",")})`, $limit: "100"
      }) : [];
      const byPin = new Map(addressRows.map(row => [row.pin, row]));
      const groups = new Map();
      for (const row of nearest) {
        const property = serializeProperty(byPin.get(row.pin), row);
        if (!property || property.formattedAddress === anchor.formattedAddress) continue;
        const existing = groups.get(property.formattedAddress);
        if (existing) {
          existing.parcelCount++;
          if (property.ownerName && !existing.ownerNames.includes(property.ownerName)) existing.ownerNames.push(property.ownerName);
          continue;
        }
        groups.set(property.formattedAddress, { ...property, distanceMeters: Math.round(row.distance), parcelCount: 1, ownerNames: property.ownerName ? [property.ownerName] : [] });
      }
      return {
        anchor, properties: [...groups.values()].slice(0, 60), radiusMeters: radius,
        truncated: bounds.length > 500 || matches.length > 100 || groups.size > 60,
        unavailableAddresses: nearest.filter(row => !byPin.get(row.pin)?.prop_address_full).length,
        year: anchor.year, queriedAt: new Date(now()).toISOString(), source: "Cook County Assessor",
        sources: [`${DATA_ORIGIN}/d/${ADDRESS_DATASET}`, `${DATA_ORIGIN}/d/${PARCEL_DATASET}`]
      };
    });
  }
  return { search, nearby };
}

export function registerCoachPropertyRoutes(app, { requireCoachActivo, service = createCoachPropertyService(), now = Date.now }) {
  const requests = new Map();
  async function handle(req, res, method) {
    res.set("Cache-Control", "private, no-store");
    const auth = await requireCoachActivo(req, res);
    if (!auth) return;
    const userId = String(auth.user?._id || "");
    if (!userId) return res.status(403).json({ error: "Entra a tu cuenta para consultar propiedades." });
    const time = now();
    for (const [key, entry] of requests) if (entry.expires <= time) requests.delete(key);
    if (!requests.has(userId) && requests.size >= 2000) return res.status(503).json({ error: "Hay muchas consultas en curso. Intenta más tarde." });
    const bucket = requests.get(userId) || { count: 0, expires: time + 5 * 60 * 1000 };
    bucket.count++;
    requests.set(userId, bucket);
    if (bucket.count > 30) {
      res.set("Retry-After", String(Math.ceil((bucket.expires - time) / 1000)));
      return res.status(429).json({ error: "Alcanzaste el límite de búsquedas. Espera unos minutos antes de continuar." });
    }
    try { res.json(await service[method](req.query)); }
    catch (error) { res.status(error.status || 503).json({ error: error.status ? error.message : "No pude consultar las propiedades. Intenta nuevamente." }); }
  }
  app.get("/api/coach/properties/search", (req, res, next) => handle(req, res, "search").catch(next));
  app.get("/api/coach/properties/nearby", (req, res, next) => handle(req, res, "nearby").catch(next));
}
