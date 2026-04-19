import crypto from "node:crypto";
import fs from "node:fs";

export const DEFAULT_GOOGLE_ADS_CONFIG_PATH =
  "/Users/monse/Documents/New project/private/google-ads.local.json";

function base64Url(input) {
  return Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function signJwt(serviceAccount) {
  const now = Math.floor(Date.now() / 1000);
  const header = {
    alg: "RS256",
    typ: "JWT",
  };
  const payload = {
    iss: serviceAccount.client_email,
    scope: "https://www.googleapis.com/auth/adwords",
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now,
  };

  const unsigned = `${base64Url(JSON.stringify(header))}.${base64Url(
    JSON.stringify(payload),
  )}`;

  const signer = crypto.createSign("RSA-SHA256");
  signer.update(unsigned);
  signer.end();

  const signature = signer.sign(serviceAccount.private_key);
  return `${unsigned}.${signature
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "")}`;
}

export function maskCustomerId(id) {
  if (!id) return "";
  const value = String(id).replace(/\D/g, "");
  return `${value.slice(0, 3)}-${value.slice(3, 6)}-${value.slice(6)}`;
}

export function loadGoogleAdsConfig(configPath = DEFAULT_GOOGLE_ADS_CONFIG_PATH) {
  const config = JSON.parse(fs.readFileSync(configPath, "utf8"));
  const serviceAccount = JSON.parse(
    fs.readFileSync(config.serviceAccountKeyPath, "utf8"),
  );

  if (!config.developerToken) {
    throw new Error("Missing developerToken in local Google Ads config.");
  }

  return {
    config,
    serviceAccount,
  };
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const text = await response.text();

  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }

  if (!response.ok) {
    const error = new Error(`HTTP ${response.status} ${response.statusText}`);
    error.status = response.status;
    error.body = json;
    throw error;
  }

  return json;
}

async function getAccessToken(serviceAccount) {
  const assertion = signJwt(serviceAccount);

  const oauth = await fetchJson("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion,
    }),
  });

  if (!oauth.access_token) {
    throw new Error("OAuth token exchange returned no access token.");
  }

  return oauth.access_token;
}

export async function createGoogleAdsClient(
  configPath = DEFAULT_GOOGLE_ADS_CONFIG_PATH,
) {
  const { config, serviceAccount } = loadGoogleAdsConfig(configPath);
  const accessToken = await getAccessToken(serviceAccount);

  const baseHeaders = {
    authorization: `Bearer ${accessToken}`,
    "developer-token": config.developerToken,
    "login-customer-id": config.managerCustomerId,
    "content-type": "application/json",
  };

  async function request(url, { method = "GET", body } = {}) {
    return fetchJson(url, {
      method,
      headers: baseHeaders,
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  async function search(customerId, query) {
    const chunks = await request(
      `https://googleads.googleapis.com/v22/customers/${customerId}/googleAds:searchStream`,
      {
        method: "POST",
        body: { query },
      },
    );

    const rows = [];
    if (Array.isArray(chunks)) {
      for (const chunk of chunks) {
        if (Array.isArray(chunk.results)) {
          rows.push(...chunk.results);
        }
      }
    }
    return rows;
  }

  async function mutate(customerId, serviceName, operations) {
    return request(
      `https://googleads.googleapis.com/v22/customers/${customerId}/${serviceName}:mutate`,
      {
        method: "POST",
        body: { operations },
      },
    );
  }

  async function listAccessibleCustomers() {
    return request(
      "https://googleads.googleapis.com/v22/customers:listAccessibleCustomers",
      {
        method: "GET",
      },
    );
  }

  async function suggestGeoTargets(names, { countryCode = "US", locale = "en" } = {}) {
    return request("https://googleads.googleapis.com/v22/geoTargetConstants:suggest", {
      method: "POST",
      body: {
        locale,
        countryCode,
        locationNames: {
          names,
        },
      },
    });
  }

  return {
    config,
    baseHeaders,
    request,
    search,
    mutate,
    listAccessibleCustomers,
    suggestGeoTargets,
  };
}
