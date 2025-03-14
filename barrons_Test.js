require('dotenv').config();
const puppeteer = require('puppeteer');
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const path = require('path');
const fs = require('fs');

// Structured logging
const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'barrons_fetch.log' }),
    new winston.transports.File({ filename: 'barrons_errors.log', level: 'error' })
  ]
});

// Constants
const LOCK_FILE = path.join(__dirname, 'fetch_barrons.lock');
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'mining_companies.db');
const CURRENCY = 'CAD';
const DATA_SOURCE = 'Barron\'s';
const FINANCIAL_FIELDS = [
  'company_id', 'cash_value', 'cash_currency', 'liabilities', 'liabilities_currency',
  'market_cap_value', 'market_cap_currency', 'revenue_value', 'revenue_currency',
  'operating_expense', 'operating_income', 'net_income_value', 'net_income_currency',
  'debt_value', 'debt_currency', 'shares_outstanding', 'last_updated', 'data_source'
];

// Utility Functions
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const sanitizeValue = (value, field) => {
  if (!value || value === 'N/A' || value === '--' || value === '-') {
    logger.debug({ message: `Sanitizing ${field}: null/invalid`, value });
    return null;
  }
  const cleanedValue = value.replace(/[^\d.-]/g, '');
  const num = parseFloat(cleanedValue);
  const multiplier = value.includes('B') ? 1e9 : value.includes('M') ? 1e6 : value.includes('K') ? 1e3 : 1;
  const parsed = num * multiplier;
  logger.debug({ message: `Sanitizing ${field}`, value, parsed, multiplier });
  return Number.isFinite(parsed) ? parsed : null;
};

async function loadCookies() {
  try {
    const cookiesString = await fs.promises.readFile(path.join(__dirname, 'cookies.json'), 'utf8');
    return JSON.parse(cookiesString);
  } catch (err) {
    logger.error({ message: 'Failed to load cookies', error: err.message });
    throw err;
  }
}

async function fetchWithPuppeteer(url) {
  try {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    const cookies = await loadCookies();
    await page.setCookie(...cookies);
    await page.goto(url, { waitUntil: 'networkidle2' });
    await delay(5000); // Wait for dynamic content

    const data = await page.evaluate(() => {
      const extractSummaryValue = (label) => {
        const table = document.querySelector('table'); // Summary table
        if (!table) return null;
        const row = Array.from(table.querySelectorAll('tr'))
          .find(tr => tr.textContent.includes(label));
        if (!row) return null;
        const value = row.querySelector('td:last-child')?.textContent.trim();
        return value || null;
      };

      const extractFinancialValue = (label) => {
        const rows = Array.from(document.querySelectorAll('.BarronsTable--tableRow'));
        const row = rows.find(r => r.textContent.includes(label));
        if (!row) return null;
        const cells = Array.from(row.querySelectorAll('.BarronsTable--cell'));
        const value = cells[cells.length - 1]?.textContent.trim(); // Latest year
        return value || null;
      };

      return {
        market_cap_value: extractSummaryValue('Market Value'),
        shares_outstanding: extractSummaryValue('Shares Outstanding'),
        revenue_value: extractFinancialValue('Sales/Revenue'),
        net_income_value: extractFinancialValue('Net Income'),
        cash_value: extractFinancialValue('Cash & Short Term Investments'),
        debt_value: extractFinancialValue('Total Debt'),
        liabilities: extractFinancialValue('Total Liabilities')
      };
    });

    logger.debug({ message: 'Extracted data from page', url, data });
    await browser.close();
    return data;
  } catch (err) {
    logger.error({ message: 'Puppeteer fetch failed', url, error: err.message });
    throw err;
  }
}

// Database Functions
const db = new sqlite3.Database(DB_PATH, sqlite3.OPEN_READWRITE, (err) => {
  if (err) {
    logger.error({ message: 'Database connection failed', error: err.message });
    process.exit(1);
  }
  logger.info({ message: 'Connected to database', path: DB_PATH });
});

async function runQuery(query, params = []) {
  return new Promise((resolve, reject) => {
    db.run(query, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

async function getQuery(query, params = []) {
  return new Promise((resolve, reject) => {
    db.get(query, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

// Data Processing
const processData = (rawData, ticker) => {
  const data = {};
  for (const [key, value] of Object.entries(rawData)) {
    if (value) {
      const parsed = sanitizeValue(value, key);
      if (parsed !== null) {
        data[key] = parsed;
        logger.info({ message: `Processed field`, ticker, key, value: parsed });
      }
    }
  }
  return data;
};

// Update Functions
async function updateFinancialsTable(companyId, ticker, data) {
  const financialData = {
    company_id: companyId,
    cash_value: data.cash_value || null,
    cash_currency: CURRENCY,
    liabilities: data.liabilities || null,
    liabilities_currency: CURRENCY,
    market_cap_value: data.market_cap_value || null,
    market_cap_currency: CURRENCY,
    revenue_value: data.revenue_value || null,
    revenue_currency: CURRENCY,
    net_income_value: data.net_income_value || null,
    net_income_currency: CURRENCY,
    debt_value: data.debt_value || null,
    debt_currency: CURRENCY,
    shares_outstanding: data.shares_outstanding || null,
    last_updated: new Date().toISOString(),
    data_source: DATA_SOURCE
  };

  const missingFields = Object.keys(financialData)
    .filter(k => !['company_id', 'last_updated', 'data_source'].includes(k))
    .filter(k => financialData[k] === null);
  logger.info({ message: 'Missing fields', ticker, fields: missingFields });

  try {
    const existing = await getQuery('SELECT * FROM financials WHERE company_id = ?', [companyId]);
    const updateData = {};
    for (const [key, value] of Object.entries(financialData)) {
      if (value !== null && FINANCIAL_FIELDS.includes(key)) {
        if ((key === 'market_cap_value' || key === 'shares_outstanding') && value < 1e6) {
          logger.warn({ message: `${key} too low, skipping`, ticker, value });
          continue;
        }
        updateData[key] = value;
      } else if (existing && existing[key] !== null && FINANCIAL_FIELDS.includes(key)) {
        updateData[key] = existing[key];
        logger.debug({ message: `Preserving ${key}`, ticker, value: existing[key] });
      }
    }

    if (Object.keys(updateData).length > 1) {
      const query = existing
        ? `UPDATE financials SET ${Object.keys(updateData).map(k => `${k} = ?`).join(', ')} WHERE company_id = ?`
        : `INSERT INTO financials (${Object.keys(updateData).join(', ')}) VALUES (${Object.keys(updateData).map(() => '?').join(', ')})`;
      const params = existing ? [...Object.values(updateData), companyId] : Object.values(updateData);
      await runQuery(query, params);
      logger.info({ message: `${existing ? 'Updated' : 'Inserted'} financials`, ticker, data: updateData });
    } else {
      logger.warn({ message: 'No valid data to update', ticker });
    }
  } catch (err) {
    logger.error({ message: 'Financials update failed', ticker, error: err.message });
  }
}

async function updateCapitalStructureTable(companyId, ticker, shares) {
  if (!shares || shares < 1e6) {
    logger.warn({ message: 'No valid shares_outstanding provided', ticker, shares });
    return;
  }

  const capitalData = {
    company_id: companyId,
    existing_shares: shares,
    last_updated: new Date().toISOString()
  };

  try {
    const existing = await getQuery('SELECT * FROM capital_structure WHERE company_id = ?', [companyId]);
    const query = existing
      ? 'UPDATE capital_structure SET existing_shares = ?, last_updated = ? WHERE company_id = ?'
      : 'INSERT INTO capital_structure (company_id, existing_shares, last_updated) VALUES (?, ?, ?)';
    const params = existing
      ? [capitalData.existing_shares, capitalData.last_updated, companyId]
      : [companyId, capitalData.existing_shares, capitalData.last_updated];
    await runQuery(query, params);
    logger.info({ message: `${existing ? 'Updated' : 'Inserted'} capital structure`, ticker, data: capitalData });
  } catch (err) {
    logger.error({ message: 'Capital structure update failed', ticker, error: err.message });
  }
}

// Process Company
async function processCompany({ baseTicker, fullTicker, companyId }) {
  const url = `https://www.barrons.com/market-data/stocks/${baseTicker}/financials?countrycode=ca&mod=searchresults_companyquotes&mod=searchbar&search_keywords=${baseTicker.toUpperCase()}&search_statement_type=suggested`;
  
  try {
    const rawData = await fetchWithPuppeteer(url);
    const data = processData(rawData, fullTicker);
    await updateFinancialsTable(companyId, fullTicker, data);
    await updateCapitalStructureTable(companyId, fullTicker, data.shares_outstanding);
    logger.info({ message: 'Processed company', ticker: fullTicker });
  } catch (err) {
    logger.error({ message: 'Company processing failed', ticker: fullTicker, error: err.message });
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

async function runWithLockCheck() {
  if (fs.existsSync(LOCK_FILE)) {
    logger.warn({ message: 'Removing stale lock file' });
    fs.unlinkSync(LOCK_FILE);
  }
  fs.writeFileSync(LOCK_FILE, '');
  logger.info({ message: 'Lock file created' });

  try {
    await updateFinancials();
    logger.info({ message: 'Execution completed successfully' });
  } catch (err) {
    logger.error({ message: 'Execution failed', error: err.message });
  } finally {
    fs.unlinkSync(LOCK_FILE);
    db.close((err) => {
      if (err) logger.error({ message: 'Database close failed', error: err.message });
      else logger.info({ message: 'Database closed' });
    });
  }
}

process.on('SIGINT', async () => {
  logger.info({ message: 'Received SIGINT, shutting down' });
  if (fs.existsSync(LOCK_FILE)) fs.unlinkSync(LOCK_FILE);
  db.close((err) => {
    if (err) logger.error({ message: 'Database close failed on shutdown', error: err.message });
    logger.info({ message: 'Shutdown complete' });
    process.exit(0);
  });
});

runWithLockCheck();