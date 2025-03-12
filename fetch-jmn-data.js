require('dotenv').config();
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
    new winston.transports.File({ filename: 'jmn_fetch.log' }),
    new winston.transports.File({ filename: 'jmn_errors.log', level: 'error' })
  ]
});

// Global Configuration
const LOCK_FILE = path.join(__dirname, 'fetch_jmn.lock');
const LOCK_FILE_TIMEOUT = 24 * 60 * 60 * 1000; // 24 hours
const DISCREPANCY_THRESHOLD = 0.1; // 10% variance for conflict detection
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

function parsePercentage(text) {
  const match = text.match(/(\d+\.?\d*)%/);
  return match ? parseFloat(match[1]) : null;
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

// Generate Slugs for Individual Pages
function generateSlugs(companyName) {
  const baseName = companyName.toLowerCase().replace(/[^a-z0-9\s&]/g, '').trim();
  const withoutSuffix = baseName.replace(/\b(ltd|inc|corp|limited|incorporated)\b/gi, '').trim();
  const slugs = [
    baseName.replace(/\s+/g, '-'),
    baseName.replace(/\s+/g, ''),
    withoutSuffix.replace(/\s+/g, '-'),
    withoutSuffix.replace(/\s+/g, ''),
    baseName.replace(/\s+/g, '-').replace(/&/g, '--'),
    withoutSuffix.replace(/\s+/g, '-').replace(/&/g, '--'),
    baseName.replace(/\s+/g, '').replace(/&/g, ''),
    withoutSuffix.replace(/\s+/g, '').replace(/&/g, ''),
  ];
  return [...new Set(slugs)].filter(slug => slug.length > 0);
}

// Fetch Basic Data from JMN List Pages
async function fetchJMNBasicData(ticker) {
  return retryFetch(async () => {
    const baseTicker = ticker.split('.')[0]; // e.g., "AAUC" from "AAUC.TO"
    const urls = [
      'https://www.juniorminingnetwork.com/mining-stocks/gold-mining-stocks.html',
      'https://www.juniorminingnetwork.com/mining-stocks/silver-mining-stocks.html'
    ];
    let data = null;

    for (const url of urls) {
      logger.info(`[${ticker}] Fetching basic JMN data from ${url}...`);
      const response = await axios.get(url, { timeout: 10000 });
      const $ = cheerio.load(response.data);

      const rows = $('table.stock-table tbody tr');
      logger.debug(`[${ticker}] Found ${rows.length} rows on ${url.split('/').pop()}`);
      for (const row of rows) {
        const rowTicker = $(row).find('.ticker').text().trim();
        logger.debug(`[${ticker}] Checking row ticker: ${rowTicker}`);
        if (rowTicker === baseTicker) {
          const priceText = $(row).find('.last-trade').text().replace(/[^0-9.]/g, '');
          const price = sanitizeValue(priceText);
          const marketCapText = $(row).find('.market-cap').text().trim();
          const marketCap = parseFinancialString(marketCapText);
          data = { price, marketCap };
          logger.info(`[${ticker}] Fetched basic data: price=${price}, market_cap=${marketCap}`);
          break;
        }
      }
      if (data) break;
    }

    if (!data) {
      logger.warn(`[${ticker}] No basic data found on JMN list pages for base ticker ${baseTicker}`);
    }
    return data;
  }, ticker);
}

// Fetch Detailed Data from JMN Individual Pages
const dataPoints = {
  construction_costs: { selector: '.costs-section .construction-costs', parser: parseFinancialString },
  aisc_last_quarter: { selector: '.financials .aisc-last-quarter', parser: parseFinancialString },
  aisc_last_year: { selector: '.financials .aisc-last-year', parser: parseFinancialString },
  aisc_future: { selector: '.financials .aisc-future', parser: parseFinancialString },
  aic_last_quarter: { selector: '.financials .aic-last-quarter', parser: parseFinancialString },
  aic_last_year: { selector: '.financials .aic-last-year', parser: parseFinancialString },
  tco_current: { selector: '.costs-section .tco-current', parser: parseFinancialString },
  tco_future: { selector: '.costs-section .tco-future', parser: parseFinancialString },
  percent_gold: { selector: '.production .percent-gold', parser: parsePercentage },
  percent_silver: { selector: '.production .percent-silver', parser: parsePercentage }
};

async function fetchJMNDetailData(ticker, companyName) {
  return retryFetch(async () => {
    const slugs = generateSlugs(companyName);
    let url = null;
    const baseTicker = ticker.split('.')[0];

    for (const slug of slugs) {
      const candidateUrl = `https://www.juniorminingnetwork.com/market-data/stock-quote/${slug}.html`;
      logger.debug(`[${ticker}] Checking URL: ${candidateUrl}`);
      const response = await axios.get(candidateUrl, { timeout: 10000 }).catch(() => null);
      if (response) {
        const $ = cheerio.load(response.data);
        const pageTicker = $('.ticker').text().trim() || $('body').text();
        if (pageTicker.includes(baseTicker) || pageTicker.includes(ticker)) {
          url_STATS

url = candidateUrl;
          logger.info(`[${ticker}] Valid URL found: ${url}`);
          break;
        }
      }
    }

    if (!url) {
      logger.warn(`[${ticker}] No valid JMN page found for ${companyName}`);
      return null;
    }

    const response = await axios.get(url, { timeout: 10000 });
    const $ = cheerio.load(response.data);
    const data = { last_updated: new Date().toISOString() };

    for (const [key, { selector, parser }] of Object.entries(dataPoints)) {
      const element = $(selector);
      const text = element.length ? element.text().trim() : '';
      data[key] = text ? parser(text) : null;
      if (key.includes('currency')) data[key] = 'CAD';
      logger.debug(`[${ticker}] ${key}: ${data[key] || 'Not found'}`);
    }

    return data;
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

// Update Financials Table
async function updateFinancialsTable(companyId, ticker, data) {
  const currency = 'CAD';
  const newData = {
    market_cap_value: sanitizeValue(data.marketCap),
    market_cap_currency: currency,
    last_updated: data.last_updated || new Date().toISOString(),
    data_source: 'Junior Mining Network'
  };

  const existing = await getQuery(
    'SELECT market_cap_value, last_updated, data_source FROM financials WHERE company_id = ?',
    [companyId]
  );

  if (existing) {
    if (existing.market_cap_value !== null && newData.market_cap_value !== null) {
      const variance = Math.abs(existing.market_cap_value - newData.market_cap_value) / (existing.market_cap_value || 1);
      if (variance > DISCREPANCY_THRESHOLD) {
        logger.warn(`[${ticker}] Market cap conflict: Existing=${existing.market_cap_value} (${existing.data_source}, ${existing.last_updated}), JMN=${newData.market_cap_value}. Not overwriting.`);
        return;
      }
    }

    if (existing.market_cap_value === null || new Date(existing.last_updated) < new Date(newData.last_updated)) {
      await runQuery(
        `UPDATE financials SET market_cap_value = ?, market_cap_currency = ?, last_updated = ?, data_source = ? 
         WHERE company_id = ?`,
        [newData.market_cap_value, newData.market_cap_currency, newData.last_updated, newData.data_source, companyId]
      );
      logger.info(`[${ticker}] Updated financials: market_cap=${newData.market_cap_value}`);
    } else {
      logger.info(`[${ticker}] Financials not updated: existing data preferred or newer`);
    }
  } else {
    await runQuery(
      `INSERT INTO financials (company_id, market_cap_value, market_cap_currency, last_updated, data_source) 
       VALUES (?, ?, ?, ?, ?)`,
      [companyId, newData.market_cap_value, newData.market_cap_currency, newData.last_updated, newData.data_source]
    );
    logger.info(`[${ticker}] Inserted financials: market_cap=${newData.market_cap_value}`);
  }
}

// Update Stock Prices Table
async function updateStockPricesTable(companyId, ticker, data) {
  if (!data.price) {
    logger.warn(`[${ticker}] No price data to process`);
    return;
  }

  const currency = 'CAD';
  const priceDate = new Date().toISOString().split('T')[0];
  const priceValue = sanitizeValue(data.price);
  const lastUpdated = data.last_updated || new Date().toISOString();

  const existing = await getQuery(
    'SELECT price_value, last_updated FROM stock_prices WHERE company_id = ? AND price_date = ?',
    [companyId, priceDate]
  );

  if (existing) {
    if (existing.price_value !== null && priceValue !== null) {
      const variance = Math.abs(existing.price_value - priceValue) / (existing.price_value || 1);
      if (variance > DISCREPANCY_THRESHOLD) {
        logger.warn(`[${ticker}] Price conflict for ${priceDate}: Existing=${existing.price_value} (${existing.last_updated}), JMN=${priceValue}. Not overwriting.`);
        return;
      }
    }

    if (existing.price_value === null || new Date(existing.last_updated) < new Date(lastUpdated)) {
      await runQuery(
        `UPDATE stock_prices SET price_value = ?, price_currency = ?, last_updated = ? 
         WHERE company_id = ? AND price_date = ?`,
        [priceValue, currency, lastUpdated, companyId, priceDate]
      );
      logger.info(`[${ticker}] Updated stock price for ${priceDate}: ${priceValue}`);
    } else {
      logger.info(`[${ticker}] Stock price for ${priceDate} not updated: existing data newer`);
    }
  } else {
    await runQuery(
      'INSERT INTO stock_prices (company_id, price_date, price_value, price_currency, last_updated) VALUES (?, ?, ?, ?, ?)',
      [companyId, priceDate, priceValue, currency, lastUpdated]
    );
    logger.info(`[${ticker}] Inserted stock price for ${priceDate}: ${priceValue}`);
  }
}

// Update Costs Table
async function updateCostsTable(companyId, ticker, data) {
  const existing = await getQuery('SELECT * FROM costs WHERE company_id = ?', [companyId]);
  const newData = {
    construction_costs: data.construction_costs,
    construction_costs_currency: 'CAD',
    aisc_last_quarter: data.aisc_last_quarter,
    aisc_last_quarter_currency: 'CAD',
    aisc_last_year: data.aisc_last_year,
    aisc_last_year_currency: 'CAD',
    aisc_future: data.aisc_future,
    aisc_future_currency: 'CAD',
    aic_last_quarter: data.aic_last_quarter,
    aic_last_quarter_currency: 'CAD',
    aic_last_year: data.aic_last_year,
    aic_last_year_currency: 'CAD',
    tco_current: data.tco_current,
    tco_current_currency: 'CAD',
    tco_future: data.tco_future,
    tco_future_currency: 'CAD',
    last_updated: data.last_updated
  };

  if (existing) {
    let updates = [];
    for (const [key, value] of Object.entries(newData)) {
      if (value !== null && (existing[key] === null || new Date(existing.last_updated) < new Date(data.last_updated))) {
        if (existing[key] !== null && !key.includes('currency')) {
          const variance = Math.abs(existing[key] - value) / (existing[key] || 1);
          if (variance > DISCREPANCY_THRESHOLD) {
            logger.warn(`[${ticker}] Conflict in ${key}: Existing=${existing[key]}, JMN=${value}. Skipping.`);
            continue;
          }
        }
        updates.push(`${key} = ?`);
      }
    }
    if (updates.length) {
      const query = `UPDATE costs SET ${updates.join(', ')}, last_updated = ? WHERE company_id = ?`;
      const values = updates.map(upd => newData[upd.split(' =')[0]]).concat([data.last_updated, companyId]);
      await runQuery(query, values);
      logger.info(`[${ticker}] Updated costs table with ${updates.length} fields`);
    } else {
      logger.info(`[${ticker}] No updates needed for costs table`);
    }
  } else {
    const fields = Object.keys(newData).filter(key => newData[key] !== null);
    const values = fields.map(key => newData[key]);
    const placeholders = fields.map(() => '?').join(', ');
    await runQuery(
      `INSERT INTO costs (company_id, ${fields.join(', ')}) VALUES (?, ${placeholders})`,
      [companyId, ...values]
    );
    logger.info(`[${ticker}] Inserted new row in costs table`);
  }
}

// Main Processing Function
async function processCompany(ticker, companyId) {
  if (isShuttingDown) {
    logger.info(`[${ticker}] Skipping due to shutdown signal`);
    return;
  }
  logger.info(`Processing ${ticker} (ID: ${companyId})...`);

  const company = await getQuery('SELECT company_name FROM companies WHERE company_id = ?', [companyId]);
  const companyName = company?.company_name;

  if (!companyName) {
    logger.warn(`[${ticker}] No company name found in database, skipping detailed fetch`);
  }

  const basicData = await fetchJMNBasicData(ticker);
  if (basicData) {
    await updateFinancialsTable(companyId, ticker, basicData);
    await updateStockPricesTable(companyId, ticker, basicData);
  } else {
    logger.warn(`[${ticker}] No basic data fetched`);
  }

  if (companyName) {
    const detailData = await fetchJMNDetailData(ticker, companyName);
    if (detailData) {
      await updateCostsTable(companyId, ticker, detailData);
      // Add updateProductionTable here if percent_gold/percent_silver belong to production table
    } else {
      logger.warn(`[${ticker}] No detailed data fetched`);
    }
  }

  logger.info(`[${ticker}] Processing completed successfully`);
}

// Update Financials (Main Logic)
async function updateFinancials() {
  logger.info('Starting JMN data fetch...');
  const companies = await allQuery('SELECT company_id, tsx_code as ticker FROM companies');
  if (!companies.length) {
    logger.error('No companies found in database');
    return;
  }
  logger.info(`Found ${companies.length} companies to process`);

  logger.info('Running sequentially');
  for (const company of companies) {
    await processCompany(company.ticker, company.company_id);
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