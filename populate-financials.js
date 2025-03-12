const yahooFinance = require('yahoo-finance2').default;
yahooFinance.suppressNotices(['yahooSurvey']);
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const fsPromises = require('fs').promises;
const { parse } = require('csv-parse/sync');
const cron = require('node-cron');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');

// Constants
const CSV_FILE = 'public/data/companies.csv';
const LOG_FILE = 'financial_population_log.txt';
const ERROR_LOG_FILE = 'financial_population_errors.txt';
const DISCREPANCY_LOG_FILE = 'financial_discrepancies_log.txt';
const LOCK_FILE = path.join(__dirname, 'financials_update.lock');
const DELAY_BETWEEN_CALLS = 150; // 15 seconds for multi-source
const MAX_RETRIES = 4;
const SKIP_IF_UPDATED_WITHIN_HOURS = 12;
const LOCK_FILE_TIMEOUT = 24 * 60 * 60 * 1000;

// Initialize SQLite database
const db = new sqlite3.Database('./mining_companies.db', (err) => {
  if (err) console.error(`[${new Date().toISOString()}] ERROR: Database connection failed: ${err.message}`);
  else console.log(`[${new Date().toISOString()}] INFO: Connected to database for financial population`);
});

// Track if shutdown is in progress
let isShuttingDown = false;

// Utility Functions
async function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizeCompanyName(name) {
  return name?.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
}

function cleanFinancialValue(value) {
  if (!value || value === 'Infinity' || value === '-Infinity' || isNaN(value)) return null;
  return value;
}

// Fetch Data Functions
async function fetchYahooFinancials(ticker) {
  if (isShuttingDown) return null;
  console.log(`[${new Date().toISOString()}] INFO: Fetching Yahoo Finance data for ${ticker}`);
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const quoteSummary = await yahooFinance.quoteSummary(ticker, {
        modules: ['financialData', 'defaultKeyStatistics', 'incomeStatementHistory'],
        validation: { logErrors: true, coerceTypes: true }
      });
      const financialData = quoteSummary.financialData || {};
      const keyStats = quoteSummary.defaultKeyStatistics || {};
      const incomeHistory = quoteSummary.incomeStatementHistory?.incomeStatementHistory[0] || {};
      return {
        market_cap_value: cleanFinancialValue(keyStats.marketCap),
        enterprise_value_value: cleanFinancialValue(keyStats.enterpriseValue),
        trailing_pe: cleanFinancialValue(keyStats.trailingPE),
        forward_pe: cleanFinancialValue(keyStats.forwardPE),
        peg_ratio: cleanFinancialValue(keyStats.pegRatio),
        price_to_sales: cleanFinancialValue(keyStats.priceToSalesTrailing12Months),
        price_to_book: cleanFinancialValue(keyStats.priceToBook),
        enterprise_to_revenue: cleanFinancialValue(keyStats.enterpriseToRevenue),
        enterprise_to_ebitda: cleanFinancialValue(keyStats.enterpriseToEbitda),
        revenue_value: cleanFinancialValue(incomeHistory.totalRevenue),
        cost_of_revenue: cleanFinancialValue(incomeHistory.costOfRevenue),
        gross_profit: cleanFinancialValue(incomeHistory.grossProfit),
        operating_expense: cleanFinancialValue(incomeHistory.totalOperatingExpenses),
        operating_income: cleanFinancialValue(incomeHistory.operatingIncome),
        net_income_value: cleanFinancialValue(incomeHistory.netIncome),
        ebitda: cleanFinancialValue(financialData.ebitda),
        cash_value: cleanFinancialValue(financialData.totalCash),
        debt_value: cleanFinancialValue(financialData.totalDebt),
        currency: financialData.currency || 'USD',
        source: 'Yahoo'
      };
    } catch (e) {
      if (e.message.includes('Quote not found') || e.message.includes('No fundamentals data found')) {
        console.error(`[${new Date().toISOString()}] ERROR: Yahoo Finance failed for ${ticker}: ${e.message}`);
        await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: ${e.message}\n`);
        return null;
      }
      console.error(`[${new Date().toISOString()}] ERROR: Attempt ${attempt + 1} failed for ${ticker}: ${e.message}`);
      if (attempt < MAX_RETRIES - 1) await delay(5000 * Math.pow(2, attempt));
      else return null;
    }
  }
}

async function fetchGoogleFinance(ticker, companyName) {
  if (isShuttingDown) return null;
  console.log(`[${new Date().toISOString()}] INFO: Fetching Google Finance data for ${ticker}${companyName ? ` (${companyName})` : ''}`);
  try {
    const tickerPermutations = [ticker.replace('.V', ':CVE'), ticker.replace('.TO', ':TSE'), ticker];
    let financialsUrl, financialsResponse;
    for (const perm of tickerPermutations) {
      financialsUrl = `https://www.google.com/finance/quote/${perm}?hl=en`;
      try {
        financialsResponse = await axios.get(financialsUrl);
        if (financialsResponse.status === 200) break;
      } catch (e) {
        continue;
      }
    }
    if (!financialsResponse && companyName) {
      const searchUrl = `https://www.google.com/finance/search?q=${encodeURIComponent(companyName)}`;
      const searchResponse = await axios.get(searchUrl);
      const $ = cheerio.load(searchResponse.data);
      const tickerLink = $('a[data-type="symbol"]').attr('href');
      if (tickerLink) {
        financialsUrl = `https://www.google.com/finance/quote/${tickerLink.split('/').pop()}?hl=en`;
        financialsResponse = await axios.get(financialsUrl);
      }
    }
    if (!financialsResponse) throw new Error('No data found');
    const $ = cheerio.load(financialsResponse.data);
    const marketCap = $('div:contains("Market cap")').next().text().split(' ')[0];
    const revenue = $('div:contains("Revenue")').next().text().split(' ')[0];
    const netIncome = $('div:contains("Net income")').next().text().split(' ')[0];
    return {
      market_cap_value: cleanFinancialValue(marketCap),
      revenue_value: cleanFinancialValue(revenue),
      net_income_value: cleanFinancialValue(netIncome),
      currency: 'USD', // Adjust if needed
      source: 'Google'
    };
  } catch (e) {
    console.error(`[${new Date().toISOString()}] ERROR: Google Finance fetch failed for ${ticker}: ${e.message}`);
    return null;
  }
}

async function fetchJuniorMiningNetwork(companyName) {
  if (isShuttingDown || !companyName) return null;
  const normalizedName = normalizeCompanyName(companyName);
  const url = `https://www.juniorminingnetwork.com/market-data/stock-quote/${normalizedName}.html`;
  console.log(`[${new Date().toISOString()}] INFO: Fetching Junior Mining Network data for ${companyName} at ${url}`);
  try {
    const response = await axios.get(url);
    const $ = cheerio.load(response.data);
    const marketCap = $('span:contains("Market Cap")').next().text().split(' ')[0];
    const revenue = $('span:contains("Revenue")').next().text().split(' ')[0];
    const netIncome = $('span:contains("Net Income")').next().text().split(' ')[0];
    return {
      market_cap_value: cleanFinancialValue(marketCap),
      revenue_value: cleanFinancialValue(revenue),
      net_income_value: cleanFinancialValue(netIncome),
      currency: 'USD', // Adjust if needed
      source: 'JuniorMiningNetwork'
    };
  } catch (e) {
    console.error(`[${new Date().toISOString()}] ERROR: Junior Mining Network fetch failed for ${companyName}: ${e.message}`);
    return null;
  }
}

function mergeData(yahooData, googleData, jmnData) {
  return {
    market_cap_value: yahooData?.market_cap_value || googleData?.market_cap_value || jmnData?.market_cap_value || null,
    enterprise_value_value: yahooData?.enterprise_value_value || null,
    trailing_pe: yahooData?.trailing_pe || null,
    forward_pe: yahooData?.forward_pe || null,
    peg_ratio: yahooData?.peg_ratio || null,
    price_to_sales: yahooData?.price_to_sales || null,
    price_to_book: yahooData?.price_to_book || null,
    enterprise_to_revenue: yahooData?.enterprise_to_revenue || null,
    enterprise_to_ebitda: yahooData?.enterprise_to_ebitda || null,
    revenue_value: yahooData?.revenue_value || googleData?.revenue_value || jmnData?.revenue_value || null,
    cost_of_revenue: yahooData?.cost_of_revenue || null,
    gross_profit: yahooData?.gross_profit || null,
    operating_expense: yahooData?.operating_expense || null,
    operating_income: yahooData?.operating_income || null,
    net_income_value: yahooData?.net_income_value || googleData?.net_income_value || jmnData?.net_income_value || null,
    ebitda: yahooData?.ebitda || null,
    cash_value: yahooData?.cash_value || null,
    debt_value: yahooData?.debt_value || null,
    currency: yahooData?.currency || googleData?.currency || jmnData?.currency || 'USD',
    source: yahooData ? 'Yahoo' : googleData ? 'Google' : 'JuniorMiningNetwork'
  };
}

// Main Update Function
async function updateFinancials() {
  console.log(`[${new Date().toISOString()}] INFO: Starting financial data update process`);
  let companies;

  try {
    const csvData = await fsPromises.readFile(CSV_FILE, 'utf8');
    const cleanedCsvData = csvData.trim().replace(/^\ufeff/, '');
    companies = parse(cleanedCsvData, { columns: true, skip_empty_lines: true, trim: true });
    console.log(`[${new Date().toISOString()}] INFO: Parsed ${companies.length} companies: ${companies.map(c => c.TICKER).join(', ')}`);
  } catch (err) {
    console.error(`[${new Date().toISOString()}] ERROR: Failed to read CSV file: ${err.message}`);
    await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] Failed to read CSV: ${err.message}\n`);
    return;
  }

  for (const company of companies) {
    if (isShuttingDown) break;
    const ticker = company.TICKER;
    const companyName = company.NAME || company.COMPANY_NAME;
    if (!ticker || ticker === 'undefined') {
      console.error(`[${new Date().toISOString()}] ERROR: Invalid ticker found: ${JSON.stringify(company)}`);
      await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] Skipping invalid ticker: ${JSON.stringify(company)}\n`);
      continue;
    }

    let companyId;
    try {
      companyId = await new Promise((resolve, reject) => {
        db.get('SELECT company_id FROM companies WHERE tsx_code = ?', [ticker], (err, row) => {
          if (err) reject(err);
          else resolve(row ? row.company_id : null);
        });
      });
      if (!companyId) {
        console.error(`[${new Date().toISOString()}] ERROR: No company_id for ${ticker}`);
        await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: No company_id found\n`);
        continue;
      }
    } catch (err) {
      console.error(`[${new Date().toISOString()}] ERROR: DB error fetching company_id for ${ticker}: ${err.message}`);
      await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: DB error - ${err.message}\n`);
      continue;
    }

    try {
      const row = await new Promise((resolve, reject) => {
        db.get('SELECT last_updated FROM financials WHERE company_id = ? AND market_cap_value IS NOT NULL AND revenue_value IS NOT NULL AND net_income_value IS NOT NULL', [companyId], (err, row) => {
          if (err) reject(err);
          else resolve(row);
        });
      });
      if (row && row.last_updated) {
        const lastUpdated = new Date(row.last_updated);
        const twelveHoursAgo = new Date(Date.now() - SKIP_IF_UPDATED_WITHIN_HOURS * 60 * 60 * 1000);
        if (lastUpdated > twelveHoursAgo) {
          console.log(`[${new Date().toISOString()}] INFO: Skipping ${ticker}, updated at ${lastUpdated.toISOString()}`);
          continue;
        }
      }
    } catch (err) {
      console.error(`[${new Date().toISOString()}] ERROR: Failed to check last_updated for ${ticker}: ${err.message}`);
      await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: Error checking last_updated - ${err.message}\n`);
      continue;
    }

    const yahooData = await fetchYahooFinancials(ticker);
    const googleData = yahooData ? null : await fetchGoogleFinance(ticker, companyName);
    const jmnData = (yahooData || googleData) ? null : await fetchJuniorMiningNetwork(companyName);

    if (!yahooData && !googleData && !jmnData) {
      console.log(`[${new Date().toISOString()}] INFO: Skipping ${ticker} due to fetch failure from all sources`);
      continue;
    }

    const finalData = mergeData(yahooData, googleData, jmnData);
    const {
      market_cap_value, enterprise_value_value, trailing_pe, forward_pe, peg_ratio,
      price_to_sales, price_to_book, enterprise_to_revenue, enterprise_to_ebitda,
      revenue_value, cost_of_revenue, gross_profit, operating_expense, operating_income,
      net_income_value, ebitda, cash_value, debt_value, currency, source
    } = finalData;

    console.log(`[${new Date().toISOString()}] INFO: Successfully fetched financial data for ${ticker} from ${source}`);
    await fsPromises.appendFile(LOG_FILE, `[${new Date().toISOString()}] ${ticker}: Fetched market_cap=${market_cap_value}, revenue=${revenue_value}, net_income=${net_income_value} from ${source}\n`);

    try {
      await new Promise((resolve, reject) => {
        db.run(
          `INSERT INTO financials (
            company_id, market_cap_value, market_cap_currency, enterprise_value_value, enterprise_value_currency,
            trailing_pe, forward_pe, peg_ratio, price_to_sales, price_to_book,
            enterprise_to_revenue, enterprise_to_ebitda, revenue_value, revenue_currency,
            cost_of_revenue, gross_profit, operating_expense, operating_income,
            net_income_value, net_income_currency, ebitda, cash_value, cash_currency,
            debt_value, debt_currency, last_updated, data_source
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(company_id) DO UPDATE SET
            market_cap_value = excluded.market_cap_value,
            market_cap_currency = excluded.market_cap_currency,
            enterprise_value_value = excluded.enterprise_value_value,
            enterprise_value_currency = excluded.enterprise_value_currency,
            trailing_pe = excluded.trailing_pe,
            forward_pe = excluded.forward_pe,
            peg_ratio = excluded.peg_ratio,
            price_to_sales = excluded.price_to_sales,
            price_to_book = excluded.price_to_book,
            enterprise_to_revenue = excluded.enterprise_to_revenue,
            enterprise_to_ebitda = excluded.enterprise_to_ebitda,
            revenue_value = excluded.revenue_value,
            revenue_currency = excluded.revenue_currency,
            cost_of_revenue = excluded.cost_of_revenue,
            gross_profit = excluded.gross_profit,
            operating_expense = excluded.operating_expense,
            operating_income = excluded.operating_income,
            net_income_value = excluded.net_income_value,
            net_income_currency = excluded.net_income_currency,
            ebitda = excluded.ebitda,
            cash_value = excluded.cash_value,
            cash_currency = excluded.cash_currency,
            debt_value = excluded.debt_value,
            debt_currency = excluded.debt_currency,
            last_updated = excluded.last_updated,
            data_source = excluded.data_source`,
          [
            companyId, market_cap_value, currency, enterprise_value_value, currency,
            trailing_pe, forward_pe, peg_ratio, price_to_sales, price_to_book,
            enterprise_to_revenue, enterprise_to_ebitda, revenue_value, currency,
            cost_of_revenue, gross_profit, operating_expense, operating_income,
            net_income_value, currency, ebitda, cash_value, currency,
            debt_value, currency, new Date().toISOString(), source
          ],
          (err) => {
            if (err) reject(err);
            else resolve();
          }
        );
      });
    } catch (err) {
      console.error(`[${new Date().toISOString()}] ERROR: Database operation failed for ${ticker}: ${err.message}`);
      continue;
    }

    await delay(DELAY_BETWEEN_CALLS);
  }

  console.log(`[${new Date().toISOString()}] INFO: Financial data update process completed`);
}

// Cleanup Function
async function cleanup() {
  if (fs.existsSync(LOCK_FILE)) {
    fs.unlinkSync(LOCK_FILE);
    console.log(`[${new Date().toISOString()}] INFO: Removed lock file`);
  }
  return new Promise((resolve) => {
    db.close((err) => {
      if (err) console.error(`[${new Date().toISOString()}] ERROR: Failed to close database: ${err.message}`);
      else console.log(`[${new Date().toISOString()}] INFO: Database connection closed`);
      resolve();
    });
  });
}

// Lock File and Execution Logic
async function runWithLockCheck() {
  if (fs.existsSync(LOCK_FILE)) {
    const stats = fs.statSync(LOCK_FILE);
    const lockAge = Date.now() - stats.mtimeMs;
    if (lockAge > LOCK_FILE_TIMEOUT) {
      console.log(`[${new Date().toISOString()}] INFO: Lock file is stale (older than 24 hours), removing it`);
      fs.unlinkSync(LOCK_FILE);
    } else {
      console.log(`[${new Date().toISOString()}] INFO: Another instance is running, exiting`);
      await cleanup();
      return;
    }
  }

  fs.writeFileSync(LOCK_FILE, '');
  console.log(`[${new Date().toISOString()}] INFO: Starting immediate financial update`);
  try {
    await updateFinancials();
  } finally {
    await cleanup();
  }
}

// Graceful Shutdown Handlers
async function shutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`[${new Date().toISOString()}] INFO: Received ${signal}, shutting down gracefully`);
  await cleanup();
  process.exit(0);
}

process.on('uncaughtException', async (err) => {
  console.error(`[${new Date().toISOString()}] ERROR: Uncaught exception: ${err.message}`);
  await shutdown('uncaughtException');
});

process.on('SIGINT', async () => {
  await shutdown('SIGINT');
});

runWithLockCheck();

cron.schedule('0 3 * * *', async () => {
  if (isShuttingDown) return;
  console.log(`[${new Date().toISOString()}] INFO: Scheduled update starting`);
  if (fs.existsSync(LOCK_FILE)) {
    const stats = fs.statSync(LOCK_FILE);
    const lockAge = Date.now() - stats.mtimeMs;
    if (lockAge > LOCK_FILE_TIMEOUT) {
      console.log(`[${new Date().toISOString()}] INFO: Lock file is stale (older than 24 hours), removing it`);
      fs.unlinkSync(LOCK_FILE);
    } else {
      console.log(`[${new Date().toISOString()}] INFO: Another instance running, skipping scheduled update`);
      return;
    }
  }

  fs.writeFileSync(LOCK_FILE, '');
  try {
    await updateFinancials();
  } finally {
    await cleanup();
  }
});