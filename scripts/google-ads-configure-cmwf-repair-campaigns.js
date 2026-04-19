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

function usdToMicros(usd) {
  return String(Math.round(Number(usd || 0) * 1_000_000));
}

function buildUpdateOperation(resourceName, fields = {}) {
  const fieldNames = Object.keys(fields);

  return {
    update: {
      resourceName,
      ...fields,
    },
    updateMask: fieldNames.join(","),
  };
}

const activeCampaignBudgets = new Map([
  ["Search | CMWF Emergency | Chicago | 20260414", 5],
  ["Search | CMWF Core | Chicago | 20260414", 3],
  ["Search | CMWF Code Safety | Chicago | 20260414", 2],
]);

const campaignsToPause = new Set([
  "Chicago metal works and fence",
  "Search | CMWF Core | Chicago | 20260401",
  "Search | CMWF Mobile Welding | Chicago | 20260414",
]);

const activeCampaigns = new Set(activeCampaignBudgets.keys());

const pauseKeywordTextsByCampaign = new Map([
  [
    "Search | CMWF Core | Chicago | 20260414",
    new Set([
      "chicago handrails",
      "porch railing chicago",
      "railing contractor chicago",
      "railing installation chicago",
      "stair railing chicago",
      "custom metal gates",
    ]),
  ],
  [
    "Search | CMWF Emergency | Chicago | 20260414",
    new Set([
      "emergency welding chicago",
      "mobile welding chicago",
    ]),
  ],
]);

async function main() {
  const client = await createGoogleAdsClient(resolvedConfigPath);
  const customerId = client.config.clientCustomerId;
  const searchRows = await client.search(
    customerId,
    "SELECT campaign.id, campaign.name, campaign.resource_name, campaign.status, campaign_budget.resource_name, campaign_budget.amount_micros FROM campaign WHERE campaign.advertising_channel_type = SEARCH ORDER BY campaign.name",
  );

  const campaignRows = searchRows
    .map((row) => ({
      id: row.campaign.id,
      name: row.campaign.name || "",
      resourceName: row.campaign.resourceName || "",
      status: row.campaign.status || "",
      budgetResourceName: row.campaignBudget.resourceName || "",
      budgetMicros: String(row.campaignBudget.amountMicros || "0"),
    }))
    .filter(
      (item) => activeCampaigns.has(item.name) || campaignsToPause.has(item.name),
    );

  const missingCampaigns = [...activeCampaigns, ...campaignsToPause].filter(
    (name) => !campaignRows.some((item) => item.name === name),
  );

  if (missingCampaigns.length) {
    throw new Error(`Missing expected campaigns: ${missingCampaigns.join(", ")}`);
  }

  const budgetOperations = [];
  const campaignOperations = [];
  const campaignPlan = campaignRows.map((item) => {
    const targetStatus = activeCampaigns.has(item.name) ? "ENABLED" : "PAUSED";
    const targetBudgetUsd = activeCampaignBudgets.get(item.name) ?? Number(item.budgetMicros || 0) / 1_000_000;
    const targetBudgetMicros = usdToMicros(targetBudgetUsd);

    if (activeCampaigns.has(item.name) && item.budgetMicros !== targetBudgetMicros) {
      budgetOperations.push(
        buildUpdateOperation(item.budgetResourceName, {
          amountMicros: targetBudgetMicros,
        }),
      );
    }

    if (item.status !== targetStatus) {
      campaignOperations.push(
        buildUpdateOperation(item.resourceName, {
          status: targetStatus,
        }),
      );
    }

    return {
      name: item.name,
      statusBefore: item.status,
      statusAfter: targetStatus,
      budgetBeforeUsd: Number(item.budgetMicros || 0) / 1_000_000,
      budgetAfterUsd: targetBudgetUsd,
    };
  });

  const keywordRows = await client.search(
    customerId,
    "SELECT campaign.name, ad_group.name, ad_group_criterion.resource_name, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, ad_group_criterion.status FROM keyword_view WHERE campaign.advertising_channel_type = SEARCH AND ad_group_criterion.status != REMOVED",
  );

  const keywordOperations = [];
  const pausedKeywords = [];

  for (const row of keywordRows) {
    const campaignName = row.campaign.name || "";
    const text = String(row.adGroupCriterion.keyword?.text || "").trim().toLowerCase();
    const resourceName = row.adGroupCriterion.resourceName || "";
    const currentStatus = row.adGroupCriterion.status || "";
    const pauseSet = pauseKeywordTextsByCampaign.get(campaignName);

    if (!pauseSet || !pauseSet.has(text) || !resourceName || currentStatus === "PAUSED") {
      continue;
    }

    keywordOperations.push(
      buildUpdateOperation(resourceName, {
        status: "PAUSED",
      }),
    );
    pausedKeywords.push({
      campaignName,
      adGroupName: row.adGroup.name || "",
      text: row.adGroupCriterion.keyword?.text || "",
      matchType: row.adGroupCriterion.keyword?.matchType || "",
    });
  }

  if (!dryRun) {
    if (budgetOperations.length) {
      await client.mutate(customerId, "campaignBudgets", budgetOperations);
    }

    if (campaignOperations.length) {
      await client.mutate(customerId, "campaigns", campaignOperations);
    }

    if (keywordOperations.length) {
      await client.mutate(customerId, "adGroupCriteria", keywordOperations);
    }
  }

  console.log(
    JSON.stringify(
      {
        status: "ok",
        mode: dryRun ? "dry_run" : "applied",
        customerId: maskCustomerId(customerId),
        totalDailyBudgetUsd: [...activeCampaignBudgets.values()].reduce((sum, value) => sum + value, 0),
        activeCampaigns: campaignPlan.filter((item) => item.statusAfter === "ENABLED"),
        pausedCampaigns: campaignPlan.filter((item) => item.statusAfter === "PAUSED"),
        pausedKeywords,
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
