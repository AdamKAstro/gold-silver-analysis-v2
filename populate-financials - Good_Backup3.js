#!/usr/bin/env node

const yahooFinance = require('yahoo-finance2').default;
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const fsPromises = fs.promises;
const { parse } = require('csv-parse/sync');
const cron = require('node-cron');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');
const similarity = require('string-similarity');

// Constants
const CSV_FILE = 'public/data/companies.csv';
const LOG_FILE = 'financial_population_log.txt';
const ERROR_LOG_FILE = 'financial_population_errors.txt';
const DISCREPANCY_LOG_FILE = 'financial_discrepancies_log.txt';
const LOCK_FILE = path.join(__dirname, 'financials_update.lock');
const DELAY_BETWEEN_CALLS = 150;
const MAX_RETRIES = 4;
const LOCK_FILE_TIMEOUT = 24 * 60 * 60 * 1000;
const HISTORICAL_DAYS = 30;
const DISCREPANCY_THRESHOLD = 0.1;

// Initialize SQLite database
const db = new sqlite3.Database('./mining_companies.db', (err) => {
  if (err) logError(`Database connection failed: ${err.message}`);
  else logInfo('Connected to database for financial population');
});

let isShuttingDown = false;

// Utility Functions
async function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function logInfo(message) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] INFO: ${message}`);
  fsPromises.appendFile(LOG_FILE, `[${timestamp}] INFO: ${message}\n`).catch(err => console.error(`Log write failed: ${err.message}`));
}

function logError(message) {
  const timestamp = new Date().toISOString();
  console.error(`[${timestamp}] ERROR: ${message}`);
  fsPromises.appendFile(ERROR_LOG_FILE, `[${timestamp}] ERROR: ${message}\n`).catch(err => console.error(`Error log write failed: ${err.message}`));
}

function logDiscrepancy(message) {
  const timestamp = new Date().toISOString();
  console.warn(`[${timestamp}] WARN: ${message}`);
  fsPromises.appendFile(DISCREPANCY_LOG_FILE, `[${timestamp}] ${message}\n`).catch(err => console.error(`Discrepancy log write failed: ${err.message}`));
}

function normalizeName(name) {
  if (!name) return '';
  return name.toLowerCase().replace(/[^a-z0-9 ]/g, '').replace(/\s+/g, ' ').trim();
}

function cleanFinancialValue(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return value; // Preserve Infinity, -Infinity, NaN
  if (typeof value === 'string') {
    if (value === 'Infinity') return Infinity;
    if (value === '-Infinity') return -Infinity;
    const num = parseFloat(value);
    return isNaN(num) ? null : num;
  }
  if (typeof value === 'object' && 'raw' in value) return cleanFinancialValue(value.raw);
  return null;
}

function parseFinancialString(value) {
  if (!value) return null;
  const cleaned = value.replace(/[^0-9.TBMK-]/g, '');
  const multiplier = cleaned.match(/[TBMK]/i);
  let num = parseFloat(cleaned.replace(/[TBMK]/i, ''));
  if (isNaN(num)) return null;
  if (multiplier) {
    switch (multiplier[0].toUpperCase()) {
      case 'T': num *= 1e12; break;
      case 'B': num *= 1e9; break;
      case 'M': num *= 1e6; break;
      case 'K': num *= 1e3; break;
    }
  }
  return num;
}

async function retryFetch(fn, ticker, retries = MAX_RETRIES, delayMs = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (e) {
      if (i === retries - 1) {
        logError(`[${ticker}] Fetch failed after ${retries} retries: ${e.message}`);
        return null;
      }
      logInfo(`[${ticker}] Retry ${i + 1}/${retries} after error: ${e.message}`);
      await delay(delayMs * (i + 1));
    }
  }
}

// Fetch Functions
async function fetchYahooFinancials(ticker, companyName, nameAlt) {
  return retryFetch(async () => {
    let quote, quoteSummary, historical;
    try {
      quote = await yahooFinance.quote(ticker);
    } catch (e) {
      logError(`[${ticker}] Yahoo quote fetch failed: ${e.message}`);
    }
    try {
      quoteSummary = await yahooFinance.quoteSummary(ticker, {
        modules: ['financialData', 'defaultKeyStatistics', 'incomeStatementHistory', 'summaryDetail', 'balanceSheetHistory']
      });
    } catch (e) {
      logError(`[${ticker}] Yahoo quoteSummary fetch failed: ${e.message}`);
    }
    try {
      historical = await yahooFinance.historical(ticker, { period1: new Date(Date.now() - HISTORICAL_DAYS * 24 * 60 * 60 * 1000), interval: '1d' });
    } catch (e) {
      logError(`[${ticker}] Yahoo historical fetch failed: ${e.message}`);
    }

    if (!quote && !quoteSummary && !historical) {
      throw new Error('All Yahoo fetches failed');
    }

    const normalizedFetchedName = normalizeName(quote?.shortName || '');
    const expectedName = normalizeName(companyName);
    const altName = nameAlt ? normalizeName(nameAlt) : null;

    const nameMatch = similarity.compareTwoStrings(normalizedFetchedName, expectedName) > 0.7 ||
                     (altName && similarity.compareTwoStrings(normalizedFetchedName, altName) > 0.7);
    if (!nameMatch && quote?.shortName) {
      logInfo(`[${ticker}] Yahoo name mismatch: expected "${companyName}" or "${nameAlt}", got "${quote.shortName}"`);
      if (quote?.symbol !== ticker) throw new Error('Ticker mismatch');
    }

    const financialData = quoteSummary?.financialData || {};
    const keyStats = quoteSummary?.defaultKeyStatistics || {};
    const summaryDetail = quoteSummary?.summaryDetail || {};
    const incomeHistory = quoteSummary?.incomeStatementHistory?.incomeStatementHistory?.[0] || {};
    const balanceSheet = quoteSummary?.balanceSheetHistory?.balanceSheetStatements?.[0] || {};

    const data = {
      market_cap_value: cleanFinancialValue(summaryDetail.marketCap), // Updated to handle both number and object
      enterprise_value_value: cleanFinancialValue(financialData.enterpriseValue?.raw),
      trailing_pe: cleanFinancialValue(financialData.trailingPE),
      forward_pe: cleanFinancialValue(keyStats.forwardPE),
      peg_ratio: cleanFinancialValue(keyStats.pegRatio),
      price_to_sales: cleanFinancialValue(summaryDetail.priceToSalesTrailing12Months),
      price_to_book: cleanFinancialValue(keyStats.priceToBook),
      enterprise_to_revenue: cleanFinancialValue(financialData.enterpriseToRevenue),
      enterprise_to_ebitda: cleanFinancialValue(financialData.enterpriseToEbitda),
      revenue_value: cleanFinancialValue(incomeHistory.totalRevenue?.raw),
      cost_of_revenue: cleanFinancialValue(incomeHistory.costOfRevenue?.raw),
      gross_profit: cleanFinancialValue(incomeHistory.grossProfit?.raw),
      operating_expense: cleanFinancialValue(incomeHistory.totalOperatingExpenses?.raw),
      operating_income: cleanFinancialValue(incomeHistory.operatingIncome?.raw),
      net_income_value: cleanFinancialValue(incomeHistory.netIncome?.raw),
      ebitda: cleanFinancialValue(financialData.ebitda),
      cash_value: cleanFinancialValue(financialData.totalCash || balanceSheet.cash?.raw),
      debt_value: cleanFinancialValue(financialData.totalDebt || balanceSheet.totalLiab?.raw),
      currency: summaryDetail.currency || 'USD',
      price: cleanFinancialValue(quote?.regularMarketPrice),
      shares_outstanding: cleanFinancialValue(quote?.sharesOutstanding || summaryDetail.sharesOutstanding?.raw),
      historical_prices: historical ? historical.map(h => ({
        date: new Date(h.date).toISOString().split('T')[0],
        close: cleanFinancialValue(h.close)
      })) : []
    };

    logInfo(`[${ticker}] Yahoo Finance fetched: market_cap=${data.market_cap_value}, price=${data.price}, shares=${data.shares_outstanding}, historical_prices=${data.historical_prices.length} days`);
    if (!data.market_cap_value) {
      logInfo(`[${ticker}] Yahoo raw summaryDetail: ${JSON.stringify(summaryDetail, null, 2)}`);
    }
    return data;
  }, ticker);
}

async function fetchGoogleFinance(ticker, companyName) {
  return retryFetch(async () => {
    const exchange = ticker.endsWith('.TO') ? ':TSE' : ticker.endsWith('.V') ? ':CVE' : '';
    const url = `https://www.google.com/finance/quote/${ticker}${exchange}`;
    const response = await axios.get(url, { timeout: 10000 });
    const $ = cheerio.load(response.data);

    const nameFromPage = $('div.zzDege').text().trim() || '';
    const normalizedPageName = normalizeName(nameFromPage);
    const expectedName = normalizeName(companyName);
    if (nameFromPage && similarity.compareTwoStrings(normalizedPageName, expectedName) < 0.7) {
      logInfo(`[${ticker}] Google Finance name mismatch: expected "${companyName}", got "${nameFromPage}"`);
    }

    const marketCapText = $('div:contains("Market cap")').next('.P6K39c').text().trim();
    const marketCap = parseFinancialString(marketCapText);
    const priceText = $('div.YMlKec.fxKbKc').text().replace(/[^0-9.]/g, '');
    const price = parseFloat(priceText) || null;

    const data = { market_cap_value: marketCap, price };
    logInfo(`[${ticker}] Google Finance fetched: market_cap=${marketCap}, price=${price}`);
    if (!marketCap) {
      logInfo(`[${ticker}] Google raw market cap element: ${$('div:contains("Market cap")').parent().html()}`);
    }
    return data;
  }, ticker);
}

async function fetchJuniorMiningNetwork(ticker, companyName) {
  return retryFetch(async () => {
    const urls = [
      'https://www.juniorminingnetwork.com/mining-stocks/gold-mining-stocks.html',
      'https://www.juniorminingnetwork.com/mining-stocks/silver-mining-stocks.html'
    ];
    let data = null;

    for (const url of urls) {
      const response = await axios.get(url, { timeout: 10000 });
      const $ = cheerio.load(response.data);

      const row = $('table.stock-table tbody tr').filter((i, el) => {
        const rowTicker = $(el).find('.ticker').text().trim();
        const rowName = $(el).find('.company').text().trim();
        return rowTicker === ticker || similarity.compareTwoStrings(normalizeName(rowName), normalizeName(companyName)) > 0.7;
      });

      if (row.length) {
        const priceText = row.find('.last-trade').text().replace(/[^0-9.]/g, '');
        const price = parseFloat(priceText) || null;
        const marketCapText = row.find('.market-cap').text().trim();
        const marketCap = parseFinancialString(marketCapText);
        data = { market_cap_value: marketCap, price };
        logInfo(`[${ticker}] JMN fetched from ${url.split('/').pop()}: market_cap=${marketCap}, price=${price}`);
        break;
      }
    }

    if (!data) {
      logInfo(`[${ticker}] JMN no data found across group pages`);
      logInfo(`[${ticker}] JMN raw table content: ${$('table.stock-table').html() || 'No table found'}`);
    }
    return data;
  }, ticker);
}

// Merge Data with Cross-Verification
function mergeData(ticker, yahoo, google, jmn) {
  const finalData = {
    market_cap_value: null,
    enterprise_value_value: yahoo?.enterprise_value_value ?? null,
    trailing_pe: yahoo?.trailing_pe ?? null,
    forward_pe: yahoo?.forward_pe ?? null,
    peg_ratio: yahoo?.peg_ratio ?? null,
    price_to_sales: yahoo?.price_to_sales ?? null,
    price_to_book: yahoo?.price_to_book ?? null,
    enterprise_to_revenue: yahoo?.enterprise_to_revenue ?? null,
    enterprise_to_ebitda: yahoo?.enterprise_to_ebitda ?? null,
    revenue_value: yahoo?.revenue_value ?? null,
    cost_of_revenue: yahoo?.cost_of_revenue ?? null,
    gross_profit: yahoo?.gross_profit ?? null,
    operating_expense: yahoo?.operating_expense ?? null,
    operating_income: yahoo?.operating_income ?? null,
    net_income_value: yahoo?.net_income_value ?? null,
    ebitda: yahoo?.ebitda ?? null,
    cash_value: yahoo?.cash_value ?? null,
    debt_value: yahoo?.debt_value ?? null,
    currency: yahoo?.currency ?? 'USD',
    price: null,
    shares_outstanding: yahoo?.shares_outstanding ?? null,
    historical_prices: yahoo?.historical_prices ?? []
  };

  const marketCaps = [yahoo?.market_cap_value, google?.market_cap_value, jmn?.market_cap_value].filter(v => v != null);
  if (marketCaps.length > 0) {
    finalData.market_cap_value = yahoo?.market_cap_value ?? google?.market_cap_value ?? jmn?.market_cap_value;
    if (marketCaps.length > 1) {
      const max = Math.max(...marketCaps);
      const min = Math.min(...marketCaps);
      const variance = (max - min) / (max || 1);
      if (variance > DISCREPANCY_THRESHOLD) {
        logDiscrepancy(`[${ticker}] Market cap variance (${(variance * 100).toFixed(2)}%): Yahoo=${yahoo?.market_cap_value ?? 'N/A'}, Google=${google?.market_cap_value ?? 'N/A'}, JMN=${jmn?.market_cap_value ?? 'N/A'}`);
      }
    }
  }

  const prices = [yahoo?.price, google?.price, jmn?.price].filter(v => v != null);
  if (prices.length > 0) {
    finalData.price = yahoo?.price ?? google?.price ?? jmn?.price;
    if (prices.length > 1) {
      const max = Math.max(...prices);
      const min = Math.min(...prices);
      const variance = (max - min) / (max || 1);
      if (variance > DISCREPANCY_THRESHOLD) {
        logDiscrepancy(`[${ticker}] Price variance (${(variance * 100).toFixed(2)}%): Yahoo=${yahoo?.price ?? 'N/A'}, Google=${google?.price ?? 'N/A'}, JMN=${jmn?.price ?? 'N/A'}`);
      }
    }
  }

  logInfo(`[${ticker}] Merged data: market_cap=${finalData.market_cap_value}, price=${finalData.price}, shares=${finalData.shares_outstanding}, historical_prices=${finalData.historical_prices.length} days`);
  return finalData;
}

// Database Update
async function updateDatabase(companyId, finalData) {
  const {
    market_cap_value, enterprise_value_value, trailing_pe, forward_pe, peg_ratio,
    price_to_sales, price_to_book, enterprise_to_revenue, enterprise_to_ebitda,
    revenue_value, cost_of_revenue, gross_profit, operating_expense, operating_income,
    net_income_value, ebitda, cash_value, debt_value, currency, price, shares_outstanding,
    historical_prices
  } = finalData;

  const lastUpdated = new Date().toISOString();

  // Update financials table
  const existingFinancial = await new Promise((resolve, reject) => {
    db.get('SELECT financial_id FROM financials WHERE company_id = ?', [companyId], (err, row) => {
      if (err) reject(err);
      else resolve(row ? row.financial_id : null);
    });
  });

  if (existingFinancial) {
    await new Promise((resolve, reject) => {
      db.run(
        `UPDATE financials SET
          market_cap_value = ?, market_cap_currency = ?, enterprise_value_value = ?, enterprise_value_currency = ?,
          trailing_pe = ?, forward_pe = ?, peg_ratio = ?, price_to_sales = ?, price_to_book = ?,
          enterprise_to_revenue = ?, enterprise_to_ebitda = ?, revenue_value = ?, revenue_currency = ?,
          cost_of_revenue = ?, gross_profit = ?, operating_expense = ?, operating_income = ?,
          net_income_value = ?, net_income_currency = ?, ebitda = ?, cash_value = ?, cash_currency = ?,
          debt_value = ?, debt_currency = ?, last_updated = ?, data_source = ?, shares_outstanding = ?
        WHERE financial_id = ?`,
        [
          market_cap_value, currency, enterprise_value_value, currency,
          trailing_pe, forward_pe, peg_ratio, price_to_sales, price_to_book,
          enterprise_to_revenue, enterprise_to_ebitda, revenue_value, currency,
          cost_of_revenue, gross_profit, operating_expense, operating_income,
          net_income_value, currency, ebitda, cash_value, currency,
          debt_value, currency, lastUpdated, 'Merged (Yahoo, Google, JMN)', shares_outstanding,
          existingFinancial
        ],
        (err) => {
          if (err) reject(err);
          else resolve();
        }
      );
    });
    logInfo(`[${companyId}] Updated financial record (financial_id: ${existingFinancial})`);
  } else {
    await new Promise((resolve, reject) => {
      db.run(
        `INSERT INTO financials (
          company_id, market_cap_value, market_cap_currency, enterprise_value_value, enterprise_value_currency,
          trailing_pe, forward_pe, peg_ratio, price_to_sales, price_to_book,
          enterprise_to_revenue, enterprise_to_ebitda, revenue_value, revenue_currency,
          cost_of_revenue, gross_profit, operating_expense, operating_income,
          net_income_value, net_income_currency, ebitda, cash_value, cash_currency,
          debt_value, debt_currency, last_updated, data_source, shares_outstanding
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          companyId, market_cap_value, currency, enterprise_value_value, currency,
          trailing_pe, forward_pe, peg_ratio, price_to_sales, price_to_book,
          enterprise_to_revenue, enterprise_to_ebitda, revenue_value, currency,
          cost_of_revenue, gross_profit, operating_expense, operating_income,
          net_income_value, currency, ebitda, cash_value, currency,
          debt_value, currency, lastUpdated, 'Merged (Yahoo, Google, JMN)', shares_outstanding
        ],
        (err) => {
          if (err) reject(err);
          else resolve();
        }
      );
    });
    logInfo(`[${companyId}] Inserted new financial record`);
  }

  // Update stock_prices table
  for (const price of historical_prices) {
    await new Promise((resolve, reject) => {
      db.run(
        `INSERT INTO stock_prices (company_id, price_date, price_value, price_currency, last_updated)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(company_id, price_date) DO UPDATE SET
           price_value = excluded.price_value,
           last_updated = excluded.last_updated`,
        [companyId, price.date, price.close, currency, lastUpdated],
        (err) => {
          if (err) reject(err);
          else resolve();
        }
      );
    });
  }

  logInfo(`[${companyId}] Database updated: financials and ${historical_prices.length} stock prices`);
}

// Main Update Function
async function updateFinancials() {
  logInfo('Starting financial data update process');
  let companies;

  try {
    const csvData = await fsPromises.readFile(CSV_FILE, 'utf8');
    companies = parse(csvData.trim().replace(/^\ufeff/, ''), { columns: true, skip_empty_lines: true, trim: true });
    logInfo(`Parsed ${companies.length} companies from CSV`);
  } catch (err) {
    logError(`Failed to read CSV: ${err.message}`);
    return;
  }

  for (const company of companies) {
    if (isShuttingDown) {
      logInfo('Shutdown detected, stopping update process');
      break;
    }

    const ticker = company.TICKER;
    const companyName = company.NAME || company.COMPANY_NAME;
    const nameAlt = company.NAME_ALT;
    if (!ticker || ticker === 'undefined') {
      logError(`Invalid ticker: ${JSON.stringify(company)}`);
      continue;
    }

    const companyId = await new Promise((resolve) => {
      db.get('SELECT company_id FROM companies WHERE tsx_code = ?', [ticker], (err, row) => {
        if (err) {
          logError(`[${ticker}] DB query failed: ${err.message}`);
          resolve(null);
        } else {
          resolve(row ? row.company_id : null);
        }
      });
    });

    if (!companyId) {
      logError(`No company_id found for ticker ${ticker}`);
      continue;
    }

    const [yahooData, googleData, jmnData] = await Promise.all([
      fetchYahooFinancials(ticker, companyName, nameAlt),
      fetchGoogleFinance(ticker, companyName),
      fetchJuniorMiningNetwork(ticker, companyName)
    ]);

    if (!yahooData && !googleData && !jmnData) {
      logInfo(`[${ticker}] Skipping - all data sources failed`);
      continue;
    }

    const finalData = mergeData(ticker, yahooData, googleData, jmnData);
    await updateDatabase(companyId, finalData);
    await delay(DELAY_BETWEEN_CALLS);
  }

  logInfo('Financial data update process completed');
}

// Cleanup and Execution Logic
async function cleanup() {
  if (fs.existsSync(LOCK_FILE)) {
    fs.unlinkSync(LOCK_FILE);
    logInfo('Lock file removed');
  }
  return new Promise((resolve) => {
    db.close((err) => {
      if (err) logError(`Failed to close database: ${err.message}`);
      else logInfo('Database connection closed');
      resolve();
    });
  });
}

async function runWithLockCheck() {
  if (fs.existsSync(LOCK_FILE)) {
    const stats = fs.statSync(LOCK_FILE);
    if (Date.now() - stats.mtimeMs > LOCK_FILE_TIMEOUT) {
      fs.unlinkSync(LOCK_FILE);
      logInfo('Stale lock file removed');
    } else {
      logInfo('Another instance is running, exiting');
      await cleanup();
      return;
    }
  }

  fs.writeFileSync(LOCK_FILE, '');
  logInfo('Lock file created');
  try {
    await updateFinancials();
  } catch (err) {
    logError(`Unexpected error in update process: ${err.stack}`);
  } finally {
    await cleanup();
  }
}

// Execution
if (process.argv.includes('--once')) {
  logInfo('Running in --once mode');
  runWithLockCheck().then(() => {
    logInfo('Execution completed, exiting');
    process.exit(0);
  });
} else {
  logInfo('Starting in scheduled mode');
  runWithLockCheck();
  cron.schedule('0 3 * * *', async () => {
    if (isShuttingDown) return;
    logInfo('Scheduled run triggered');
    await runWithLockCheck();
  });
}

// Graceful Shutdown
process.on('SIGINT', async () => {
  logInfo('Received SIGINT, shutting down gracefully');
  isShuttingDown = true;
  await cleanup();
  process.exit(0);
});

process.on('uncaughtException', async (err) => {
  logError(`Uncaught exception: ${err.stack}`);
  isShuttingDown = true;
  await cleanup();
  process.exit(1);
});