require('dotenv').config();
const yahooFinance = require('yahoo-finance2').default;
const axios = require('axios');
const cheerio = require('cheerio');
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const path = require('path');
const fs = require('fs');
const cron = require('node-cron');

// Configure logging
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => `${timestamp} [${level.toUpperCase()}]: ${message}`)
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'fetch_data.log' }),
    new winston.transports.File({ filename: 'fetch_errors.log', level: 'error' })
  ]
});

// Global Configuration
const LOCK_FILE = path.join(__dirname, 'fetch_data.lock');
const LOCK_FILE_TIMEOUT = 24 * 60 * 60 * 1000; // 24 hours
const DISCREPANCY_THRESHOLD = 0.1; // 10% variance
let isShuttingDown = false;

// Verify sqlite3
if (!sqlite3 || typeof sqlite3.Database !== 'function') {
  console.error('Failed to load sqlite3 correctly. Please run "npm install sqlite3@latest"');
  process.exit(1);
}

// Database setup
const dbPath = process.env.DB_PATH || path.join(__dirname, 'mining_companies.db');
let db;
try {
  db = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE, (err) => {
    if (err) {
      logger.error(`Failed to connect to database: ${err.message}`);
      process.exit(1);
    }
    logger.info(`Connected to SQLite database: ${dbPath}`);
  });
} catch (err) {
  logger.error(`Failed to initialize sqlite3 Database: ${err.message}`);
  process.exit(1);
}

// Utility Functions
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function sanitizeValue(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'string') {
    if (value === 'Infinity' || value === '-Infinity') return null;
    const num = parseFloat(value.replace(/[^0-9.-]/g, ''));
    return isNaN(num) ? null : num;
  }
  if (typeof value === 'object' && 'raw' in value) return sanitizeValue(value.raw);
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

async function retryFetch(fn, ticker, retries = 4, baseDelay = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (e) {
      if (i === retries - 1) {
        logger.error(`[${ticker}] Failed after ${retries} retries: ${e.message}`);
        return null;
      }
      const delayMs = baseDelay * (i + 1);
      logger.warn(`[${ticker}] Retry ${i + 1}/${retries} after error: ${e.message}. Waiting ${delayMs}ms`);
      await delay(delayMs);
    }
  }
}

// Fetch Data from Yahoo Finance (Primary)
async function fetchYahooData(ticker) {
  return retryFetch(async () => {
    logger.info(`[${ticker}] Fetching Yahoo quote summary...`);
    const quoteSummary = await yahooFinance.quoteSummary(ticker, {
      modules: [
        'price',
        'summaryProfile',
        'financialData',
        'defaultKeyStatistics',
        'incomeStatementHistory',
        'balanceSheetHistory'
      ]
    });

    const startDate = new Date();
    startDate.setMonth(startDate.getMonth() - 1);
    logger.info(`[${ticker}] Fetching Yahoo historical data from ${startDate.toISOString().split('T')[0]}...`);
    const historical = await yahooFinance.historical(ticker, {
      period1: startDate,
      interval: '1d',
    });

    logger.info(`[${ticker}] Yahoo fetched ${historical.length} days of historical data`);
    return { quoteSummary, historical };
  }, ticker);
}

// Fetch Data from Google Finance (Verification)
async function fetchGoogleData(ticker) {
  return retryFetch(async () => {
    const exchange = ticker.endsWith('.TO') ? ':TSE' : ticker.endsWith('.V') ? ':CVE' : '';
    const url = `https://www.google.com/finance/quote/${ticker}${exchange}`;
    logger.info(`[${ticker}] Fetching Google verification data from ${url}...`);
    const response = await axios.get(url, { timeout: 10000 });
    const $ = cheerio.load(response.data);

    const priceText = $('div.YMlKec.fxKbKc').text().trim();
    const price = sanitizeValue(priceText);
    const marketCapText = $('div.P6K39c:contains("Market cap")').prev().text().trim();
    const marketCap = parseFinancialString(marketCapText);

    logger.info(`[${ticker}] Google fetched: price=${price}, market_cap=${marketCap}`);
    return { price, marketCap };
  }, ticker);
}

// Database Functions
async function runQuery(query, params = []) {
  return new Promise((resolve, reject) => {
    if (isShuttingDown) return reject(new Error('Database operation aborted due to shutdown'));
    db.run(query, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

async function getQuery(query, params = []) {
  return new Promise((resolve, reject) => {
    if (isShuttingDown) return reject(new Error('Database operation aborted due to shutdown'));
    db.get(query, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

async function allQuery(query, params = []) {
  return new Promise((resolve, reject) => {
    if (isShuttingDown) return reject(new Error('Database operation aborted due to shutdown'));
    db.all(query, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

async function cleanup() {
  if (fs.existsSync(LOCK_FILE)) {
    fs.unlinkSync(LOCK_FILE);
    logger.info('Lock file removed');
  }
  return new Promise((resolve) => {
    db.close((err) => {
      if (err) logger.error(`Failed to close database: ${err.message}`);
      else logger.info('Database connection closed');
      resolve();
    });
  });
}

// Merge and Verify Data
function mergeAndVerifyData(ticker, yahooData, googleData) {
  const finalData = yahooData || {};
  const source = yahooData ? 'Yahoo Finance' : 'Google Finance (fallback)';
  
  if (yahooData && googleData) {
    const yahooPrice = yahooData.quoteSummary?.price?.regularMarketPrice || yahooData.historical?.[0]?.close;
    const googlePrice = googleData.price;
    if (yahooPrice && googlePrice) {
      const variance = Math.abs(yahooPrice - googlePrice) / (yahooPrice || 1);
      if (variance > DISCREPANCY_THRESHOLD) {
        logger.warn(`[${ticker}] Price discrepancy detected: Yahoo=${yahooPrice}, Google=${googlePrice}, Variance=${(variance * 100).toFixed(2)}%`);
      }
    }

    const yahooMarketCap = yahooData.quoteSummary?.price?.marketCap;
    const googleMarketCap = googleData.marketCap;
    if (yahooMarketCap && googleMarketCap) {
      const variance = Math.abs(yahooMarketCap - googleMarketCap) / (yahooMarketCap || 1);
      if (variance > DISCREPANCY_THRESHOLD) {
        logger.warn(`[${ticker}] Market cap discrepancy detected: Yahoo=${yahooMarketCap}, Google=${googleMarketCap}, Variance=${(variance * 100).toFixed(2)}%`);
      }
    }
  }

  if (!yahooData && googleData) {
    finalData.price = googleData.price;
    finalData.marketCap = googleData.marketCap;
    logger.info(`[${ticker}] Using Google data as Yahoo failed`);
  }

  return { data: finalData, source };
}

// Update Companies Table
async function updateCompaniesTable(companyId, ticker, data) {
  const profile = data.quoteSummary?.summaryProfile || {};
  const headquarters = [
    profile.address1,
    profile.city,
    profile.state,
    profile.country
  ].filter(Boolean).join(', ') || null;
  const description = profile.longBusinessSummary || null;

  const existing = await getQuery('SELECT headquarters, description FROM companies WHERE company_id = ?', [companyId]);
  if (!existing.headquarters || !existing.description) {
    const updates = {
      headquarters: existing.headquarters || headquarters,
      description: existing.description || description,
      last_updated: new Date().toISOString()
    };
    await runQuery(
      'UPDATE companies SET headquarters = ?, description = ?, last_updated = ? WHERE company_id = ?',
      [updates.headquarters, updates.description, updates.last_updated, companyId]
    );
    logger.info(`[${ticker}] Updated companies: headquarters=${updates.headquarters}, description=${updates.description ? 'set' : 'null'}`);
  } else {
    logger.info(`[${ticker}] Companies table already up-to-date`);
  }
}

// Update Financials Table
async function updateFinancialsTable(companyId, ticker, data, source) {
  const price = data.quoteSummary?.price || {};
  const financials = data.quoteSummary?.financialData || {};
  const stats = data.quoteSummary?.defaultKeyStatistics || {};
  const income = data.quoteSummary?.incomeStatementHistory?.incomeStatementHistory?.[0] || {};
  const balance = data.quoteSummary?.balanceSheetHistory?.balanceSheetStatements?.[0] || {};

  const currency = price.currency || 'CAD';
  const financialData = {
    company_id: companyId,
    cash_value: sanitizeValue(financials.totalCash || balance.cash),
    cash_currency: currency,
    cash_date: null,
    liabilities: sanitizeValue(balance.totalLiab),
    liabilities_currency: currency,
    market_cap_value: sanitizeValue(price.marketCap || data.marketCap),
    market_cap_currency: currency,
    enterprise_value_value: sanitizeValue(financials.enterpriseValue),
    enterprise_value_currency: currency,
    trailing_pe: sanitizeValue(financials.trailingPE),
    forward_pe: sanitizeValue(stats.forwardPE),
    peg_ratio: sanitizeValue(stats.pegRatio),
    price_to_sales: sanitizeValue(stats.priceToSalesTrailing12Months),
    price_to_book: sanitizeValue(stats.priceToBook),
    enterprise_to_revenue: sanitizeValue(financials.enterpriseToRevenue),
    enterprise_to_ebitda: sanitizeValue(financials.enterpriseToEbitda),
    revenue_value: sanitizeValue(income.totalRevenue),
    revenue_currency: currency,
    cost_of_revenue: sanitizeValue(income.costOfRevenue),
    gross_profit: sanitizeValue(income.grossProfit),
    operating_expense: sanitizeValue(income.totalOperatingExpenses),
    operating_income: sanitizeValue(income.operatingIncome),
    net_income_value: sanitizeValue(income.netIncome),
    net_income_currency: currency,
    ebitda: sanitizeValue(financials.ebitda),
    debt_value: sanitizeValue(financials.totalDebt || balance.totalLiab),
    debt_currency: currency,
    shares_outstanding: sanitizeValue(stats.sharesOutstanding),
    last_updated: new Date().toISOString(),
    data_source: source
  };

  const existing = await getQuery('SELECT financial_id, last_updated FROM financials WHERE company_id = ?', [companyId]);
  if (existing) {
    await runQuery(
      `UPDATE financials SET ${Object.keys(financialData).map(k => `${k} = ?`).join(', ')} 
       WHERE company_id = ? AND last_updated < ?`,
      [...Object.values(financialData), companyId, financialData.last_updated]
    );
    logger.info(`[${ticker}] Updated financials: market_cap=${financialData.market_cap_value}, shares=${financialData.shares_outstanding}`);
  } else {
    await runQuery(
      `INSERT INTO financials (${Object.keys(financialData).join(', ')}) 
       VALUES (${Object.keys(financialData).map(() => '?').join(', ')})`,
      Object.values(financialData)
    );
    logger.info(`[${ticker}] Inserted financials: market_cap=${financialData.market_cap_value}, shares=${financialData.shares_outstanding}`);
  }
}

// Update Stock Prices Table
async function updateStockPricesTable(companyId, ticker, data, source) {
  const currency = 'CAD';
  const historical = data.historical || [];
  const latestPrice = data.price || (historical.length > 0 ? historical[0].close : null);

  await db.serialize(async () => {
    await runQuery('BEGIN TRANSACTION');
    try {
      if (latestPrice) {
        const priceDate = new Date().toISOString().split('T')[0];
        const priceValue = sanitizeValue(latestPrice);
        const lastUpdated = new Date().toISOString();

        const existing = await getQuery(
          'SELECT price_id FROM stock_prices WHERE company_id = ? AND price_date = ?',
          [companyId, priceDate]
        );

        if (!existing) {
          await runQuery(
            'INSERT INTO stock_prices (company_id, price_date, price_value, price_currency, last_updated) VALUES (?, ?, ?, ?, ?)',
            [companyId, priceDate, priceValue, currency, lastUpdated]
          );
          logger.info(`[${ticker}] Inserted stock price for ${priceDate}: ${priceValue}`);
        } else {
          logger.debug(`[${ticker}] Stock price for ${priceDate} already exists`);
        }
      }

      if (historical.length > 0) {
        for (const day of historical) {
          const priceDate = new Date(day.date).toISOString().split('T')[0];
          const priceValue = sanitizeValue(day.close);
          const lastUpdated = new Date().toISOString();

          const existing = await getQuery(
            'SELECT price_id FROM stock_prices WHERE company_id = ? AND price_date = ?',
            [companyId, priceDate]
          );

          if (!existing) {
            await runQuery(
              'INSERT INTO stock_prices (company_id, price_date, price_value, price_currency, last_updated) VALUES (?, ?, ?, ?, ?)',
              [companyId, priceDate, priceValue, currency, lastUpdated]
            );
            logger.info(`[${ticker}] Inserted stock price for ${priceDate}: ${priceValue}`);
          } else {
            logger.debug(`[${ticker}] Stock price for ${priceDate} already exists`);
          }
        }
      }
      await runQuery('COMMIT');
    } catch (err) {
      await runQuery('ROLLBACK');
      logger.error(`[${ticker}] Failed to update stock prices: ${err.message}`);
    }
  });
}

// Update Company URLs Table
async function updateCompanyUrlsTable(companyId, ticker, data) {
  const profile = data.quoteSummary?.summaryProfile || {};
  const website = profile.website;

  if (!website) {
    logger.warn(`[${ticker}] No website found`);
    return;
  }

  const existing = await getQuery(
    'SELECT url_id FROM company_urls WHERE company_id = ? AND url = ?',
    [companyId, website]
  );

  if (!existing) {
    const urlData = {
      company_id: companyId,
      url_type: 'website',
      url: website,
      last_validated: new Date().toISOString()
    };
    await runQuery(
      'INSERT INTO company_urls (company_id, url_type, url, last_validated) VALUES (?, ?, ?, ?)',
      Object.values(urlData)
    );
    logger.info(`[${ticker}] Inserted company URL: ${website}`);
  } else {
    logger.info(`[${ticker}] Company URL already exists: ${website}`);
  }
}

// Update Capital Structure Table
async function updateCapitalStructureTable(companyId, ticker, data) {
  const stats = data.quoteSummary?.defaultKeyStatistics || {};
  const existingShares = sanitizeValue(stats.sharesOutstanding);

  if (!existingShares) {
    logger.warn(`[${ticker}] No shares outstanding data available`);
    return;
  }

  const existing = await getQuery('SELECT capital_id FROM capital_structure WHERE company_id = ?', [companyId]);
  const capitalData = {
    company_id: companyId,
    existing_shares: existingShares,
    fully_diluted_shares: null,
    in_the_money_options: null,
    options_revenue: null,
    options_revenue_currency: null,
    last_updated: new Date().toISOString()
  };

  if (existing) {
    await runQuery(
      `UPDATE capital_structure SET ${Object.keys(capitalData).map(k => `${k} = ?`).join(', ')} 
       WHERE company_id = ? AND last_updated < ?`,
      [...Object.values(capitalData), companyId, capitalData.last_updated]
    );
    logger.info(`[${ticker}] Updated capital structure: existing_shares=${existingShares}`);
  } else {
    await runQuery(
      `INSERT INTO capital_structure (${Object.keys(capitalData).join(', ')}) 
       VALUES (${Object.keys(capitalData).map(() => '?').join(', ')})`,
      Object.values(capitalData)
    );
    logger.info(`[${ticker}] Inserted capital structure: existing_shares=${existingShares}`);
  }
}

// Main Processing Function
async function processCompany(ticker, companyId) {
  if (isShuttingDown) {
    logger.info(`[${ticker}] Skipping due to shutdown signal`);
    return;
  }
  logger.info(`Processing ${ticker} (ID: ${companyId})...`);

  const yahooData = await fetchYahooData(ticker);
  const googleData = await fetchGoogleData(ticker);
  const { data, source } = mergeAndVerifyData(ticker, yahooData, googleData);

  if (!data) {
    logger.error(`[${ticker}] Skipping due to fetch failure from both sources`);
    return;
  }

  await updateCompaniesTable(companyId, ticker, data);
  await updateFinancialsTable(companyId, ticker, data, source);
  await updateStockPricesTable(companyId, ticker, data, source);
  await updateCompanyUrlsTable(companyId, ticker, data);
  await updateCapitalStructureTable(companyId, ticker, data);
  logger.info(`[${ticker}] Processing completed successfully with source: ${source}`);
}

// Update Financials (Main Logic)
async function updateFinancials() {
  logger.info('Starting data fetch (Yahoo primary, Google verification)...');
  const companies = await allQuery('SELECT company_id, tsx_code FROM companies');
  if (!companies.length) {
    logger.error('No companies found in database');
    return;
  }
  logger.info(`Found ${companies.length} companies to process`);

  logger.info('Running sequentially');
  for (const { company_id, tsx_code } of companies) {
    await processCompany(tsx_code, company_id);
  }
  logger.info('All companies processed successfully');
}

// Lock Check and Execution
async function runWithLockCheck() {
  if (fs.existsSync(LOCK_FILE)) {
    const stats = fs.statSync(LOCK_FILE);
    if (Date.now() - stats.mtimeMs > LOCK_FILE_TIMEOUT) {
      fs.unlinkSync(LOCK_FILE);
      logger.info('Stale lock file removed');
    } else {
      logger.info('Another instance is running, exiting');
      await cleanup();
      return;
    }
  }

  fs.writeFileSync(LOCK_FILE, '');
  logger.info('Lock file created');
  try {
    await updateFinancials();
  } catch (err) {
    logger.error(`Unexpected error in update process: ${err.stack}`);
  } finally {
    await cleanup();
  }
}

// Execution
if (process.argv.includes('--once')) {
  logger.info('Running in --once mode');
  runWithLockCheck().then(() => {
    logger.info('Execution completed, exiting');
    process.exit(0);
  });
} else {
  logger.info('Starting in scheduled mode');
  runWithLockCheck();
  cron.schedule('0 3 * * *', async () => {
    if (isShuttingDown) return;
    logger.info('Scheduled run triggered');
    await runWithLockCheck();
  });
}

// Graceful Shutdown
process.on('SIGINT', async () => {
  logger.info('Received SIGINT, shutting down gracefully');
  isShuttingDown = true;
  await cleanup();
  process.exit(0);
});

process.on('uncaughtException', async (err) => {
  logger.error(`Uncaught exception: ${err.stack}`);
  isShuttingDown = true;
  await cleanup();
  process.exit(1);
});