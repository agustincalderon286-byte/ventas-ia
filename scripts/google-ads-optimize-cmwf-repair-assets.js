import {
  createGoogleAdsClient,
  DEFAULT_GOOGLE_ADS_CONFIG_PATH,
  maskCustomerId,
} from "./google-ads-api.js";

const dryRun = process.argv.includes("--dry-run");
const explicitConfigPath = process.argv
  .slice(2)
  .find((arg) => arg && !arg.startsWith("--"));
const resolvedConfigPath = explicitConfigPath || DEFAULT_GOOGLE_ADS_CONFIG_PATH;

const repairCampaignNames = [
  "Search | CMWF Emergency | Chicago | 20260414",
  "Search | CMWF Core | Chicago | 20260414",
  "Search | CMWF Code Safety | Chicago | 20260414",
];

const removeAssetNames = new Set([
  "CMWF | Sitelink | Mobile Welding",
  "CMWF | Sitelink | Handrails",
  "CMWF | Structured Snippet | Services",
]);

function escapeGaql(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function normalizeBaseUrl(url) {
  return String(url || "").replace(/\/+$/, "");
}

function uniqueByName(items) {
  const map = new Map();
  for (const item of items) {
    map.set(item.name, item);
  }
  return [...map.values()];
}

async function main() {
  const client = await createGoogleAdsClient(resolvedConfigPath);
  const { config } = client;
  const customerId = config.clientCustomerId;
  const websiteUrl = normalizeBaseUrl(config.websiteUrl);
  const campaignNameList = repairCampaignNames
    .map((name) => `'${escapeGaql(name)}'`)
    .join(", ");

  const assetBlueprints = uniqueByName([
    {
      name: "CMWF | Sitelink | Handrail Repair",
      fieldType: "SITELINK",
      create: {
        name: "CMWF | Sitelink | Handrail Repair",
        finalUrls: [`${websiteUrl}/handrails-chicago.html`],
        sitelinkAsset: {
          linkText: "Handrail Repair",
          description1: "Porch and stair rail fixes",
          description2: "Chicago metal railing repair",
        },
      },
    },
    {
      name: "CMWF | Sitelink | Gate Repair",
      fieldType: "SITELINK",
      create: {
        name: "CMWF | Sitelink | Gate Repair",
        finalUrls: [`${websiteUrl}/gate-repair-chicago.html`],
        sitelinkAsset: {
          linkText: "Gate Repair",
          description1: "Sagging, dragging, broken",
          description2: "Metal gates repaired fast",
        },
      },
    },
    {
      name: "CMWF | Sitelink | Fence Repair",
      fieldType: "SITELINK",
      create: {
        name: "CMWF | Sitelink | Fence Repair",
        finalUrls: [`${websiteUrl}/fence-repair-chicago.html`],
        sitelinkAsset: {
          linkText: "Fence Repair",
          description1: "Metal fence sections repaired",
          description2: "Rust and damage fixes",
        },
      },
    },
    {
      name: "CMWF | Sitelink | Porch Repair",
      fieldType: "SITELINK",
      create: {
        name: "CMWF | Sitelink | Porch Repair",
        finalUrls: [`${websiteUrl}/metal-porch-repair-chicago.html`],
        sitelinkAsset: {
          linkText: "Porch Repair",
          description1: "Rusted porch and landing fixes",
          description2: "Chicago metal porch repair",
        },
      },
    },
    {
      name: "CMWF | Sitelink | Wrought Iron Repair",
      fieldType: "SITELINK",
      create: {
        name: "CMWF | Sitelink | Wrought Iron Repair",
        finalUrls: [`${websiteUrl}/wrought-iron-repair-chicago.html`],
        sitelinkAsset: {
          linkText: "Wrought Iron Repair",
          description1: "Repair rails, gates and fence",
          description2: "Rust and break damage fixes",
        },
      },
    },
    {
      name: "CMWF | Sitelink | Send Photos",
      fieldType: "SITELINK",
      create: {
        name: "CMWF | Sitelink | Send Photos",
        finalUrls: [`${websiteUrl}/whatsapp-quote-chicago.html`],
        sitelinkAsset: {
          linkText: "Send Photos",
          description1: "WhatsApp photos for pricing",
          description2: "Fast quote-ready contact",
        },
      },
    },
    {
      name: "CMWF | Sitelink | View Projects",
      fieldType: "SITELINK",
      create: {
        name: "CMWF | Sitelink | View Projects",
        finalUrls: [`${websiteUrl}/projects.html`],
        sitelinkAsset: {
          linkText: "View Projects",
          description1: "See real before-and-after work",
          description2: "Railings, gates and fences",
        },
      },
    },
    {
      name: "CMWF | Structured Snippet | Repair Services",
      fieldType: "STRUCTURED_SNIPPET",
      create: {
        name: "CMWF | Structured Snippet | Repair Services",
        structuredSnippetAsset: {
          header: "Services",
          values: [
            "Handrail Repair",
            "Gate Repair",
            "Fence Repair",
            "Porch Repair",
            "Wrought Iron Repair",
            "Guardrail Repair",
          ],
        },
      },
    },
  ]);

  const existingAssetMap = new Map();
  for (const asset of assetBlueprints) {
    const rows = await client.search(
      customerId,
      `SELECT asset.resource_name, asset.name
       FROM asset
       WHERE asset.name = '${escapeGaql(asset.name)}'
       LIMIT 1`,
    );

    const existing = rows[0]?.asset || null;
    if (existing?.resourceName) {
      existingAssetMap.set(asset.name, existing.resourceName);
    }
  }

  const createOperations = assetBlueprints
    .filter((asset) => !existingAssetMap.has(asset.name))
    .map((asset) => ({ create: asset.create }));

  if (!dryRun && createOperations.length > 0) {
    const result = await client.mutate(customerId, "assets", createOperations);
    const createdAssets = result.results || [];
    const missingCreates = assetBlueprints.filter((asset) => !existingAssetMap.has(asset.name));

    createdAssets.forEach((item, index) => {
      const assetName = missingCreates[index]?.name;
      if (assetName && item.resourceName) {
        existingAssetMap.set(assetName, item.resourceName);
      }
    });
  }

  const campaignRows = await client.search(
    customerId,
    `SELECT campaign.name, campaign.resource_name
     FROM campaign
     WHERE campaign.name IN (${campaignNameList})
     ORDER BY campaign.name`,
  );

  const campaigns = campaignRows
    .map((row) => row.campaign)
    .filter((campaign) => campaign?.resourceName);

  const removeOperations = [];
  const createCampaignAssetOperations = [];
  const plan = [];

  for (const campaign of campaigns) {
    const linkedRows = await client.search(
      customerId,
      `SELECT campaign_asset.resource_name, campaign_asset.asset, campaign_asset.field_type, asset.name
       FROM campaign_asset
       WHERE campaign_asset.campaign = '${escapeGaql(campaign.resourceName)}'`,
    );

    const linkedPairs = new Set(
      linkedRows.map((row) => `${row.campaignAsset.asset}::${row.campaignAsset.fieldType}`),
    );

    const removedAssetNames = [];
    for (const row of linkedRows) {
      const assetName = row.asset.name || "";
      const resourceName = row.campaignAsset.resourceName || "";
      if (!resourceName || !removeAssetNames.has(assetName)) {
        continue;
      }
      removeOperations.push({ remove: resourceName });
      removedAssetNames.push(assetName);
    }

    const linkedAssetNames = [];
    for (const asset of assetBlueprints) {
      const assetResourceName = existingAssetMap.get(asset.name);
      if (!assetResourceName) {
        continue;
      }
      const key = `${assetResourceName}::${asset.fieldType}`;
      linkedAssetNames.push(asset.name);
      if (linkedPairs.has(key)) {
        continue;
      }
      createCampaignAssetOperations.push({
        create: {
          campaign: campaign.resourceName,
          asset: assetResourceName,
          fieldType: asset.fieldType,
        },
      });
    }

    plan.push({
      campaignName: campaign.name,
      removedAssetNames,
      desiredAssetNames: linkedAssetNames,
    });
  }

  if (!dryRun && removeOperations.length > 0) {
    await client.mutate(customerId, "campaignAssets", removeOperations);
  }

  if (!dryRun && createCampaignAssetOperations.length > 0) {
    await client.mutate(customerId, "campaignAssets", createCampaignAssetOperations);
  }

  const verificationRows = await client.search(
    customerId,
    `SELECT campaign.name, campaign_asset.field_type, campaign_asset.status, asset.name
     FROM campaign_asset
     WHERE campaign.name IN (${campaignNameList})
     ORDER BY campaign.name, campaign_asset.field_type, asset.name`,
  );

  console.log(
    JSON.stringify(
      {
        status: "ok",
        mode: dryRun ? "dry_run" : "applied",
        customerId: maskCustomerId(customerId),
        createdAssets: assetBlueprints
          .map((asset) => asset.name)
          .filter((name) => !existingAssetMap.has(name) && dryRun),
        plannedCampaignChanges: plan,
        verification: verificationRows.map((row) => ({
          campaign: row.campaign.name,
          fieldType: row.campaignAsset.fieldType,
          status: row.campaignAsset.status,
          asset: row.asset.name,
        })),
      },
      null,
      2,
    ),
  );
}

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        status: "error",
        message: error.message,
        httpStatus: error.status || null,
        body: error.body || null,
      },
      null,
      2,
    ),
  );
  process.exit(1);
});
