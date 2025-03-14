require('dotenv').config();
const axios = require('axios');
const cheerio = require('cheerio');
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const path = require('path');
const fs = require('fs');

// Configure logging
const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => `${timestamp} [${level.toUpperCase()}]: ${message}`)
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'barrons_fetch.log' }),
    new winston.transports.File({ filename: 'barrons_errors.log', level: 'error' })
  ]
});

// Global Configuration
const LOCK_FILE = path.join(__dirname, 'fetch_barrons.lock');
const LOCK_FILE_TIMEOUT = 24 * 60 * 60 * 1000;
let isShuttingDown = false;

// Database setup
const dbPath = process.env.DB_PATH || path.join(__dirname, 'mining_companies.db');
const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE, (err) => {
  if (err) logger.error(`Failed to connect to database: ${err.message}`);
  else logger.info(`Connected to SQLite database: ${dbPath}`);
});

// Utility Functions
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

const sanitizeValue = (value, field) => {
  if (!value || value === 'N/A' || value === '--') {
    logger.debug(`Sanitize: ${field} is null/invalid: ${value}`);
    return null;
  }
  let numStr = value.toString().replace(/[^0-9.-]/g, '');
  let multiplier = 1;
  if (value.includes('B')) multiplier = 1e9;
  else if (value.includes('M')) multiplier = 1e6;
  else if (value.includes('K')) multiplier = 1e3;
  const num = parseFloat(numStr) * multiplier;
  logger.debug(`Sanitize: ${field} parsed as ${num} from ${value} (multiplier: ${multiplier})`);
  return Number.isFinite(num) ? num : null;
};

const parsePercentage = (value, field) => {
  if (!value || value === 'N/A' || value === '--') {
    logger.debug(`ParsePercentage: ${field} is null/invalid: ${value}`);
    return null;
  }
  const match = value.match(/(-?\d+\.?\d*)%/);
  const num = match ? parseFloat(match[1]) : null;
  logger.debug(`ParsePercentage: ${field} parsed as ${num} from ${value}`);
  return num;
};

async function loadCookies() {
  try {
    const cookiesString = await fs.promises.readFile(path.join(__dirname, 'cookies.json'), 'utf8');
    const cookies = JSON.parse(cookiesString);
    return cookies.map(c => `${c.name}=${c.value}`).join('; ');
  } catch (err) {
    logger.error(`Failed to load cookies: ${err.message}`);
    throw err;
  }
}

async function fetchWithCookies(url, retries = 3, baseDelay = 1000) {
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Cookie': await loadCookies()
  };
  for (let i = 0; i < retries; i++) {
    try {
      logger.info(`Fetching ${url}`);
      const response = await axios.get(url, { headers, timeout: 10000 });
      logger.debug(`Fetched ${url}, length: ${response.data.length} chars`);
      return response.data;
    } catch (e) {
      if (i === retries - 1) throw e;
      const delayMs = baseDelay * (i + 1);
      logger.warn(`Retry ${i + 1}/${retries} for ${url}: ${e.message}. Waiting ${delayMs}ms`);
      await delay(delayMs);
    }
  }
}

// Database Functions
async function runQuery(query, params = []) {
  return new Promise((resolve, reject) => {
    if (isShuttingDown) return reject(new Error('Database operation aborted'));
    logger.debug(`Query: ${query} with params: ${JSON.stringify(params)}`);
    db.run(query, params, function (err) {
      if (err) reject(err);
      else {
        logger.debug(`Affected ${this.changes} rows`);
        resolve(this);
      }
    });
  });
}

async function getQuery(query, params = []) {
  return new Promise((resolve, reject) => {
    if (isShuttingDown) return reject(new Error('Database operation aborted'));
    logger.debug(`Get: ${query} with params: ${JSON.stringify(params)}`);
    db.get(query, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

// Data Extraction
const extractFinancialData = ($, ticker) => {
  const data = {};
  logger.info(`[${ticker}] Extracting data`);
  $('table tr').each((_, row) => {
    const label = $(row).find('th, td').first().text().trim();
    const value = $(row).find('td').last().text().trim();
    const key = label.toLowerCase().replace(/[^a-z0-9]/g, '');
    logger.debug(`[${ticker}] Row - Label: "${label}", Key: "${key}", Value: "${value}"`);
    
    if (label && value) {
      const mappings = {
        'cashequivalents': 'cash_value', 'cash': 'cash_value',
        'totalliabilities': 'liabilities',
        'marketvalue': 'market_cap_value', 'marketcap': 'market_cap_value',
        'enterprisevalue': 'enterprise_value_value',
        'trailingpe': 'trailing_pe', 'peratiottm': 'trailing_pe',
        'forwardpe': 'forward_pe',
        'pegratio': 'peg_ratio',
        'pricesales': 'price_to_sales',
        'pricebook': 'price_to_book',
        'evrevenue': 'enterprise_to_revenue',
        'evebitda': 'enterprise_to_ebitda',
        'revenue': 'revenue_value',
        'costofrevenue': 'cost_of_revenue',
        'grossprofit': 'gross_profit',
        'operatingexpense': 'operating_expense',
        'operatingincome': 'operating_income',
        'netincome': 'net_income_value',
        'ebitda': 'ebitda',
        'totaldebt': 'debt_value',
        'averagevolume': 'average_volume',
        'epsttm': 'eps_ttm',
        'dividendyield': 'dividend_yield',
        'shortinterest': 'short_interest',
        'shortinterestchange': 'short_interest_change',
        'percentoffloat': 'percent_of_float',
        'sharesoutstanding': 'shares_outstanding'
      };
      const field = Object.entries(mappings).find(([pattern]) => key.includes(pattern))?.[1];
      if (field) {
        const parsed = field.includes('dividend_yield') || field.includes('percent') 
          ? parsePercentage(value, field) 
          : sanitizeValue(value, field);
        data[field] = parsed;
        logger.info(`[${ticker}] ${label} -> ${field}: ${parsed}`);
      }
    }
  });
  logger.info(`[${ticker}] Extracted: ${JSON.stringify(data)}`);
  return data;
};

// Update Functions
async function updateFinancialsTable(companyId, ticker, data) {
  const currency = 'CAD';
  const lastUpdated = new Date().toISOString();
  
  const financialData = {
    company_id: companyId,
    cash_value: data.cash_value,
    cash_currency: currency,
    liabilities: data.liabilities,
    liabilities_currency: currency,
    market_cap_value: data.market_cap_value,
    market_cap_currency: currency,
    enterprise_value_value: data.enterprise_value_value,
    enterprise_value_currency: currency,
    trailing_pe: data.trailing_pe,
    forward_pe: data.forward_pe,
    peg_ratio: data.peg_ratio,
    price_to_sales: data.price_to_sales,
    price_to_book: data.price_to_book,
    enterprise_to_revenue: data.enterprise_to_revenue,
    enterprise_to_ebitda: data.enterprise_to_ebitda,
    revenue_value: data.revenue_value,
    revenue_currency: currency,
    cost_of_revenue: data.cost_of_revenue,
    gross_profit: data.gross_profit,
    operating_expense: data.operating_expense,
    operating_income: data.operating_income,
    net_income_value: data.net_income_value,
    net_income_currency: currency,
    ebitda: data.ebitda,
    debt_value: data.debt_value,
    debt_currency: currency,
    shares_outstanding: data.shares_outstanding,
    last_updated: lastUpdated,
    data_source: "Barron's"
  };

  const existing = await getQuery('SELECT * FROM financials WHERE company_id = ?', [companyId]);
  logger.debug(`[${ticker}] Existing financials: ${JSON.stringify(existing)}`);

  const updateData = {};
  for (const [key, value] of Object.entries(financialData)) {
    if (value !== null) {
      // Validate reasonableness
      if (key === 'market_cap_value' && value < 1e6) {
        logger.warn(`[${ticker}] ${key} too low: ${value}, skipping`);
        continue;
      }
      if (key === 'shares_outstanding' && value < 1e6) {
        logger.warn(`[${ticker}] ${key} too low: ${value}, skipping`);
        continue;
      }
      updateData[key] = value;
    } else if (existing && existing[key] !== null) {
      updateData[key] = existing[key];
      logger.debug(`[${ticker}] Preserving ${key}: ${existing[key]}`);
    }
  }

  if (Object.keys(updateData).length > 1) {
    const query = existing
      ? `UPDATE financials SET ${Object.keys(updateData).map(k => `${k} = ?`).join(', ')} WHERE company_id = ?`
      : `INSERT INTO financials (${Object.keys(updateData).join(', ')}) VALUES (${Object.keys(updateData).map(() => '?').join(', ')})`;
    const params = existing ? [...Object.values(updateData), companyId] : Object.values(updateData);
    
    await runQuery(query, params);
    logger.info(`[${ticker}] ${existing ? 'Updated' : 'Inserted'} financials: ${JSON.stringify(updateData)}`);
  } else {
    logger.warn(`[${ticker}] No valid data to update`);
  }
}

async function updateCapitalStructureTable(companyId, ticker, shares) {
  if (!shares || shares < 1e6) {
    logger.warn(`[${ticker}] Invalid shares_outstanding: ${shares}`);
    return;
  }

  const capitalData = {
    company_id: companyId,
    existing_shares: shares,
    last_updated: new Date().toISOString()
  };

  const existing = await getQuery('SELECT * FROM capital_structure WHERE company_id = ?', [companyId]);
  logger.debug(`[${ticker}] Existing capital: ${JSON.stringify(existing)}`);

  if (existing) {
    await runQuery(
      `UPDATE capital_structure SET existing_shares = ?, last_updated = ? WHERE company_id = ?`,
      [capitalData.existing_shares, capitalData.last_updated, companyId]
    );
    logger.info(`[${ticker}] Updated capital: ${JSON.stringify(capitalData)}`);
  } else {
    await runQuery(
      `INSERT INTO capital_structure (company_id, existing_shares, last_updated) VALUES (?, ?, ?)`,
      [companyId, shares, capitalData.last_updated]
    );
    logger.info(`[${ticker}] Inserted capital: ${JSON.stringify(capitalData)}`);
  }
}

// Process Company
async function processCompany({ baseTicker, fullTicker, companyId }) {
  if (isShuttingDown) return;
  
  const url = `https://www.barrons.com/market-data/stocks/${baseTicker}/financials?countrycode=ca`;
  try {
    const html = await fetchWithCookies(url);
    const $ = cheerio.load(html);
    const data = extractFinancialData($, fullTicker);
    
    await updateFinancialsTable(companyId, fullTicker, data);
    await updateCapitalStructureTable(companyId, fullTicker, data.shares_outstanding);
    logger.info(`[${fullTicker}] Processed`);
  } catch (err) {
    logger.error(`[${fullTicker}] Failed: ${err.message}`);
  }
}

// Main Execution
async function updateFinancials() {
  const companies = [
    { fullTicker: 'ITR.V', baseTicker: 'itr', companyId: 1 },
    { fullTicker: 'AAG.V', baseTicker: 'aag', companyId: 2 },
    { fullTicker: 'ITH.TO', baseTicker: 'ith', companyId: 3 },
    { fullTicker: 'IRV.V', baseTicker: 'irv', companyId: 4 },
    { fullTicker: 'AAB.TO', baseTicker: 'aab', companyId: 5 }
  ];

  for (const company of companies) {
    await processCompany(company);
    await delay(5000);
  }
}

async function cleanup() {
  if (fs.existsSync(LOCK_FILE)) fs.unlinkSync(LOCK_FILE);
  return new Promise(resolve => db.close(err => {
    if (err) logger.error(`Close failed: ${err.message}`);
    logger.info('Database closed');
    resolve();
  }));
}

async function runWithLockCheck() {
  if (fs.existsSync(LOCK_FILE)) {
    const stats = fs.statSync(LOCK_FILE);
    if (Date.now() - stats.mtimeMs > LOCK_FILE_TIMEOUT) fs.unlinkSync(LOCK_FILE);
    else {
      logger.info('Another instance running, exiting');
      await cleanup();
      return;
    }
  }

  fs.writeFileSync(LOCK_FILE, '');
  try {
    await updateFinancials();
  } catch (err) {
    logger.error(`Unexpected error: ${err.stack}`);
  } finally {
    await cleanup();
  }
}

// Execution
runWithLockCheck().then(() => {
  logger.info('Execution completed');
  process.exit(0);
});

process.on('SIGINT', async () => {
  isShuttingDown = true;
  await cleanup();
  process.exit(0);
});