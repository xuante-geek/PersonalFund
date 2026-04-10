import fs from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

const ROOT_DIR = path.resolve(process.cwd());
const HOLDINGS_CSV = path.join(ROOT_DIR, "data/input/current_holdings.csv");
const PRODUCT_REF_CSV = path.join(ROOT_DIR, "data/reference/product_variable.csv");
const ASSETS_REF_CSV = path.join(ROOT_DIR, "data/reference/assets_variable.csv");
const INDUSTRY_REF_CSV = path.join(ROOT_DIR, "data/reference/industry_variable.csv");
const OUTPUT_CSV = path.join(ROOT_DIR, "data/output/daily_data.csv");
const ARCHIVE_DIR = path.join(ROOT_DIR, "data/archive/daily_data");

const DATE_TEXT = new Date().toISOString().slice(0, 10);
const NAVIGATE_TIMEOUT_MS = 30000;
const LOCATOR_TIMEOUT_MS = 6000;

const TOP_CONTAINER_SELECTORS = [
  '[id*="QuoteHeader"]',
  '[class*="quote-header"]',
  '[class*="quote"]',
  '[class*="Quote"]',
  '[class*="header"]',
  "#main",
  "main",
  "body"
];

function decodeBuffer(buffer) {
  const encodings = ["utf-8", "gb18030", "gbk"];
  for (const encoding of encodings) {
    try {
      return new TextDecoder(encoding, { fatal: true }).decode(buffer);
    } catch (_error) {
      continue;
    }
  }
  return buffer.toString("utf8");
}

function parseCsvText(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;

  const normalized = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  for (let index = 0; index < normalized.length; index += 1) {
    const char = normalized[index];

    if (char === '"') {
      if (inQuotes && normalized[index + 1] === '"') {
        cell += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(cell);
      cell = "";
      continue;
    }

    if (char === "\n" && !inQuotes) {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      continue;
    }

    cell += char;
  }

  if (cell.length > 0 || row.length > 0) {
    row.push(cell);
    rows.push(row);
  }
  return rows;
}

function toCsvText(rows) {
  const escapedRows = rows.map((row) =>
    row
      .map((cell) => {
        const text = cell === null || cell === undefined ? "" : String(cell);
        if (/[",\n]/.test(text)) {
          return `"${text.replace(/"/g, '""')}"`;
        }
        return text;
      })
      .join(",")
  );
  return `\ufeff${escapedRows.join("\n")}\n`;
}

function normalizeCode(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (/^-?\d+(\.0+)?$/.test(text)) {
    return String(Math.trunc(Number(text)));
  }
  return text;
}

function parseFloatSafe(value) {
  const text = String(value || "").replace(/,/g, "").replace(/，/g, "").trim();
  if (!text) return null;
  const numberMatch = text.match(/-?\d+(\.\d+)?/);
  if (!numberMatch) return null;
  const parsed = Number(numberMatch[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function findDecimalValues(text) {
  const values = [];
  const matches = String(text || "").match(/\d+\.\d+/g) || [];
  for (const raw of matches) {
    const parsed = Number(raw);
    if (Number.isFinite(parsed)) {
      values.push(parsed);
    }
  }
  return values;
}

async function loadCsv(csvPath) {
  const buf = await fs.readFile(csvPath);
  return parseCsvText(decodeBuffer(buf));
}

async function loadVariableMap(csvPath) {
  const rows = await loadCsv(csvPath);
  const mapping = new Map();
  for (const row of rows.slice(1)) {
    const label = String(row[0] || "").trim();
    const code = normalizeCode(row[1]);
    if (code) {
      mapping.set(code, label);
    }
  }
  return mapping;
}

async function loadHoldings() {
  const rows = await loadCsv(HOLDINGS_CSV);
  const holdings = [];
  for (let idx = 1; idx < rows.length; idx += 1) {
    const row = rows[idx] || [];
    const padded = [...row, "", "", "", "", "", "", "", ""].slice(0, 8);
    if (padded.every((v) => String(v).trim() === "")) continue;
    const hasLocator = /^-?\d+(\.0+)?$/.test(String(padded[5] || "").trim()) &&
      /^-?\d+(\.0+)?$/.test(String(padded[6] || "").trim());

    holdings.push({
      lineNo: idx + 1,
      targetName: String(padded[0] || "").trim(),
      targetAmountRaw: String(padded[1] || "").trim(),
      targetCostRaw: String(padded[2] || "").trim(),
      quoteUrl: String(padded[3] || "").trim(),
      priceLocator: hasLocator ? String(padded[4] || "").trim() : "",
      productVariable: normalizeCode(hasLocator ? padded[5] : padded[4]),
      assetsVariable: normalizeCode(hasLocator ? padded[6] : padded[5]),
      industryVariable: normalizeCode(hasLocator ? padded[7] : padded[6])
    });
  }
  return holdings;
}

async function pickTopContainer(page) {
  for (const selector of TOP_CONTAINER_SELECTORS) {
    const locator = page.locator(selector).first();
    const count = await locator.count();
    if (!count) continue;
    const text = await locator.innerText({ timeout: LOCATOR_TIMEOUT_MS }).catch(() => "");
    if (text && text.trim().length > 0) {
      return locator;
    }
  }
  return page.locator("body").first();
}

async function collectAnchorTexts(container, anchorRegex) {
  const blocks = container.locator("div,section,article,li,tr,p,span,strong").filter({
    hasText: anchorRegex
  });
  const count = await blocks.count();
  const limit = Math.min(count, 24);
  const texts = [];
  for (let i = 0; i < limit; i += 1) {
    const text = await blocks.nth(i).innerText({ timeout: LOCATOR_TIMEOUT_MS }).catch(() => "");
    if (text && text.trim()) {
      texts.push(text.trim());
    }
  }
  return texts;
}

function choosePriceByStrategy(productVariable, anchorValues, containerValues) {
  if (productVariable === "3") {
    if (anchorValues.length > 0) return anchorValues[0];
    return null;
  }
  if (anchorValues.length > 0) return anchorValues[0];
  if (containerValues.length > 0) {
    return Math.max(...containerValues);
  }
  return null;
}

async function fetchPriceWithLocator(page, holding) {
  if (!holding.quoteUrl) {
    throw new Error("network: quote_url is empty");
  }
  await page.goto(holding.quoteUrl, {
    waitUntil: "domcontentloaded",
    timeout: NAVIGATE_TIMEOUT_MS
  });
  await page.waitForTimeout(900);

  const topContainer = await pickTopContainer(page);
  const anchorRegex = holding.productVariable === "3" ? /净值/i : /元/i;
  const anchorTexts = await collectAnchorTexts(topContainer, anchorRegex);

  const anchorValues = [];
  for (const text of anchorTexts) {
    anchorValues.push(...findDecimalValues(text));
  }

  const containerText = await topContainer.innerText({ timeout: LOCATOR_TIMEOUT_MS }).catch(() => "");
  const containerValues = findDecimalValues(containerText);
  const selected = choosePriceByStrategy(holding.productVariable, anchorValues, containerValues);

  if (selected === null) {
    throw new Error(
      `outerhtml_locator: no price matched; anchor_hits=${anchorValues.length}; container_hits=${containerValues.length}`
    );
  }

  return {
    targetPrice: selected,
    fetchNote: `ok: anchor_hits=${anchorValues.length}; container_hits=${containerValues.length}`
  };
}

async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

async function main() {
  const holdings = await loadHoldings();
  const productMap = await loadVariableMap(PRODUCT_REF_CSV);
  const assetsMap = await loadVariableMap(ASSETS_REF_CSV);
  const industryMap = await loadVariableMap(INDUSTRY_REF_CSV);

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  const rows = [
    [
      "date",
      "target_name",
      "target_amount",
      "target_cost",
      "target_price",
      "target_value",
      "target_pnl",
      "target_return_rate",
      "quote_url",
      "price_locator",
      "product_variable",
      "product_name",
      "assets_variable",
      "assets_name",
      "industry_variable",
      "industry_name",
      "fetch_status",
      "fetch_note"
    ]
  ];

  let successCount = 0;
  let errorCount = 0;
  let totalCost = 0;
  let totalValue = 0;

  for (const holding of holdings) {
    const amount = parseFloatSafe(holding.targetAmountRaw);
    const cost = parseFloatSafe(holding.targetCostRaw);
    const productName = productMap.get(holding.productVariable) || "";
    const assetsName = assetsMap.get(holding.assetsVariable) || "";
    const industryName = industryMap.get(holding.industryVariable) || "";

    let targetPrice = "";
    let targetValue = "";
    let targetPnl = "";
    let targetReturnRate = "";
    let fetchStatus = "ok";
    let fetchNote = "";

    try {
      if (amount === null || cost === null) {
        throw new Error(`data: invalid amount/cost at line ${holding.lineNo}`);
      }
      const fetched = await fetchPriceWithLocator(page, holding);
      targetPrice = fetched.targetPrice;
      targetValue = amount * fetched.targetPrice;
      targetPnl = targetValue - cost;
      targetReturnRate = cost !== 0 ? ((targetValue / cost - 1) * 100) : "";
      fetchNote = fetched.fetchNote;
      totalCost += cost;
      totalValue += targetValue;
      successCount += 1;
    } catch (error) {
      fetchStatus = "error";
      fetchNote = `line ${holding.lineNo}: ${error.message || String(error)}`;
      errorCount += 1;
    }

    rows.push([
      DATE_TEXT,
      holding.targetName,
      amount === null ? holding.targetAmountRaw : amount,
      cost === null ? holding.targetCostRaw : cost,
      targetPrice,
      targetValue,
      targetPnl,
      targetReturnRate,
      holding.quoteUrl,
      holding.priceLocator,
      holding.productVariable,
      productName,
      holding.assetsVariable,
      assetsName,
      holding.industryVariable,
      industryName,
      fetchStatus,
      fetchNote
    ]);
  }

  rows.push([
    DATE_TEXT,
    "__TOTAL__",
    "",
    successCount > 0 ? totalCost : "",
    "",
    successCount > 0 ? totalValue : "",
    successCount > 0 ? totalValue - totalCost : "",
    successCount > 0 && totalCost !== 0 ? ((totalValue / totalCost - 1) * 100) : "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "summary",
    `success=${successCount}; error=${errorCount}; mode=playwright_locator`
  ]);

  await ensureDir(path.dirname(OUTPUT_CSV));
  await fs.writeFile(OUTPUT_CSV, toCsvText(rows), "utf8");

  await ensureDir(ARCHIVE_DIR);
  const archivePath = path.join(ARCHIVE_DIR, `daily_data_${DATE_TEXT}.csv`);
  await fs.writeFile(archivePath, toCsvText(rows), "utf8");

  await page.close();
  await context.close();
  await browser.close();

  process.stdout.write(`Wrote: ${OUTPUT_CSV}\n`);
  process.stdout.write(`Archived: ${archivePath}\n`);
  process.stdout.write(`Success: ${successCount}, Error: ${errorCount}\n`);
  process.exit(errorCount > 0 ? 2 : 0);
}

main().catch((error) => {
  process.stderr.write(`${error.stack || String(error)}\n`);
  process.exit(1);
});
