const ENDPOINT =
  "https://fapi.binance.com/fapi/v1/indexInfo?symbol=BTCDOMUSDT";

type BaseAssetEntry = {
  baseAsset?: string;
  weightInPercentage?: string;
  weightInQuantity?: string;
};

type IndexInfoResponse = {
  symbol?: string;
  baseAssetList?: BaseAssetEntry[];
  [key: string]: unknown;
};

type ComponentRow = {
  component: string;
  weight: number;
};

async function fetchIndexInfo(): Promise<IndexInfoResponse> {
  let response: Response;

  try {
    response = await fetch(ENDPOINT);
  } catch (err) {
    console.error("Network or fetch error while calling Binance indexInfo:", err);
    process.exitCode = 1;
    throw err;
  }

  const status = response.status;
  const statusText = response.statusText;

  if (!response.ok) {
    let bodyText: string;
    try {
      bodyText = await response.text();
    } catch (err) {
      bodyText = `<failed to read body: ${String(err)}>`;
    }

    console.error(
      `Non-200 response from Binance: ${status} ${statusText ?? ""}`.trim(),
    );
    console.error("Response body:");
    console.error(bodyText);
    process.exitCode = 1;
    throw new Error(`Binance API returned status ${status}`);
  }

  const rawText = await response.text();

  try {
    const json = JSON.parse(rawText) as IndexInfoResponse;
    return json;
  } catch (err) {
    console.error("Failed to parse JSON from Binance response:", err);
    console.error("Raw response body:");
    console.error(rawText);
    process.exitCode = 1;
    throw err;
  }
}

function validateAndExtractComponents(
  data: IndexInfoResponse,
): ComponentRow[] {
  if (!data || typeof data !== "object") {
    console.error("Unexpected response schema: top-level JSON is not an object.");
    console.error("Received:", data);
    process.exit(1);
  }

  if (!Array.isArray(data.baseAssetList)) {
    console.error(
      "Unexpected response schema: `baseAssetList` is missing or not an array.",
    );
    console.error(
      "Top-level keys:",
      Object.keys(data as Record<string, unknown>),
    );
    process.exit(1);
  }

  const rows: ComponentRow[] = [];

  for (const entry of data.baseAssetList) {
    if (!entry || typeof entry !== "object") {
      continue;
    }

    const component = entry.baseAsset;
    const weightStr =
      entry.weightInPercentage ?? entry.weightInQuantity ?? undefined;

    if (!component || typeof component !== "string") {
      console.warn("Skipping entry without a valid `baseAsset`:", entry);
      continue;
    }

    if (!weightStr || typeof weightStr !== "string") {
      console.warn(
        `Skipping entry for ${component} without a valid weight string.`,
        entry,
      );
      continue;
    }

    const weight = Number.parseFloat(weightStr);
    if (!Number.isFinite(weight)) {
      console.warn(
        `Skipping entry for ${component} due to non-numeric weight: ${weightStr}`,
      );
      continue;
    }

    rows.push({ component, weight });
  }

  if (rows.length === 0) {
    console.error(
      "No valid components found in `baseAssetList` after validation.",
    );
    process.exit(1);
  }

  return rows;
}

function formatAndPrintTable(components: ComponentRow[]): void {
  // Sort by descending weight
  components.sort((a, b) => b.weight - a.weight);

  const totalWeight = components.reduce((sum, row) => sum + row.weight, 0);

  const headerComponent = "Component";
  const headerWeight = "Weight (%)";

  const componentWidth = Math.max(
    headerComponent.length,
    ...components.map((row) => row.component.length),
  );

  const weightStrings = components.map((row) =>
    row.weight.toFixed(4),
  );

  const weightWidth = Math.max(
    headerWeight.length,
    ...weightStrings.map((w) => w.length),
  );

  const padRight = (value: string, width: number) =>
    value.padEnd(width, " ");
  const padLeft = (value: string, width: number) =>
    value.padStart(width, " ");

  const headerLine =
    `${padRight(headerComponent, componentWidth)}  ` +
    `${padLeft(headerWeight, weightWidth)}`;
  const separatorLine =
    `${"-".repeat(componentWidth)}  ${"-".repeat(weightWidth)}`;

  console.log(headerLine);
  console.log(separatorLine);

  for (let i = 0; i < components.length; i += 1) {
    const row = components[i];
    const weightStr = weightStrings[i];

    const line =
      `${padRight(row.component, componentWidth)}  ` +
      `${padLeft(weightStr, weightWidth)}`;
    console.log(line);
  }

  console.log();
  console.log(`Total weight: ${totalWeight.toFixed(4)}%`);
}

async function main() {
  try {
    const data = await fetchIndexInfo();
    const components = validateAndExtractComponents(data);
    formatAndPrintTable(components);
  } catch {
    // Errors are already logged in helpers; ensure non-zero exit for failures.
    if (process.exitCode === undefined) {
      process.exitCode = 1;
    }
  }
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main();

