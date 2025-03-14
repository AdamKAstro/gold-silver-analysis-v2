require('dotenv').config();
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const path = require('path');
const fs = require('fs').promises;
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

// Custom delay function
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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
const DATA_SOURCE = "Barron's";
const FINANCIAL_FIELDS = [
  'company_id', 'cash_value', 'cash_currency', 'liabilities', 'liabilities_currency',
  'market_cap_value', 'market_cap_currency', 'revenue_value', 'revenue_currency',
  'net_income_value', 'net_income_currency', 'debt_value', 'debt_currency',
  'shares_outstanding', 'last_updated', 'data_source'
];

// Initialize the database with schema
const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    logger.error({ message: 'Failed to connect to database', error: err.message });
    throw err;
  }
  logger.info({ message: 'Connected to database', path: DB_PATH });

  db.serialize(() => {
    db.run(`
      CREATE TABLE IF NOT EXISTS financials (
        company_id INTEGER PRIMARY KEY,
        cash_value REAL, cash_currency TEXT,
        liabilities REAL, liabilities_currency TEXT,
        market_cap_value REAL, market_cap_currency TEXT,
        revenue_value REAL, revenue_currency TEXT,
        net_income_value REAL, net_income_currency TEXT,
        debt_value REAL, debt_currency TEXT,
        shares_outstanding REAL, last_updated TEXT, data_source TEXT
      )
    `);
    db.run(`
      CREATE TABLE IF NOT EXISTS capital_structure (
        company_id INTEGER PRIMARY KEY,
        existing_shares REAL, last_updated TEXT
      )
    `);
  });
});

// Database query helpers
const runQuery = (query, params) => {
  return new Promise((resolve, reject) => {
    db.run(query, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
};

const getQuery = (query, params) => {
  return new Promise((resolve, reject) => {
    db.get(query, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
};

// Sanitize values with chart noise removal
const sanitizeValue = (value, field, ticker) => {
  if (!value || value === 'N/A' || value === '--' || value === '-' || value.includes('Copyright')) {
    logger.debug({ message: `Sanitizing ${field}: null/invalid`, value, ticker });
    return null;
  }
  const cleanValue = value.replace(/ChartBar.*End of interactive chart\./i, '').trim();
  const isNegative = cleanValue.startsWith('(') && cleanValue.endsWith(')');
  const cleanedValue = cleanValue.replace(/[()$,]/g, '').replace(/[^\d.-]/g, '');
  const num = parseFloat(cleanedValue);
  if (!Number.isFinite(num)) {
    logger.debug({ message: `Sanitizing ${field}: non-numeric`, value, ticker });
    return null;
  }
  const multiplier = cleanValue.includes('B') ? 1e9 : cleanValue.includes('M') ? 1e6 : cleanValue.includes('K') ? 1e3 : 1;
  const parsed = num * multiplier * (isNegative ? -1 : 1);
  logger.debug({ message: `Sanitizing ${field}`, value, parsed, multiplier, ticker });
  logger.info({ message: 'Processed field', key: field, ticker, value: parsed });
  return parsed;
};

// Load cookies
async function loadCookies() {
  try {
    const cookiesString = await fs.readFile(path.join(__dirname, 'cookies.json'), 'utf8');
    return JSON.parse(cookiesString);
  } catch (err) {
    logger.error({ message: 'Failed to load cookies', error: err.message });
    return [];
  }
}

// Fetch data with Puppeteer (Fixed header-to-data mapping)
async function fetchWithPuppeteer(url, retries = 3) {
  let browser;
  try {
    browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    const cookies = await loadCookies();
    if (cookies.length > 0) await page.setCookie(...cookies);

    await page.goto(url, { waitUntil: 'networkidle2', timeout: 40000 });
    await page.waitForSelector('[data-id="FinancialTables_table"], .summary-table', { timeout: 40000 });
    await delay(15000 + Math.random() * 5000); // Randomize delay between 15-20s

    page.on('console', msg => logger.debug({ message: 'Browser console', text: msg.text() }));

    const data = await page.evaluate(() => {
      // Extract from summary table
      const extractSummaryValue = (labels) => {
        const rows = Array.from(document.querySelectorAll('.summary-table tr, .table__Row-sc-1djjifq-2, tr'));
        for (const label of labels) {
          const row = rows.find(r => r.textContent.toLowerCase().includes(label.toLowerCase()));
          if (row) {
            const value = row.querySelector('td:last-child, .table__Cell-sc-1djjifq-5:last-child')?.textContent.trim();
            return value && !labels.some(l => l.toLowerCase() === value.toLowerCase()) ? value : null;
          }
        }
        return null;
      };

      // Extract from financial table (Fixed version)
      const extractFinancialTable = () => {
        const tables = Array.from(document.querySelectorAll('[data-id="FinancialTables_table"]'));
        const financialTable = tables.find(table => {
          const rows = Array.from(table.querySelectorAll('.table__Row-sc-1djjifq-2'));
          return rows.some(row => row.textContent.match(/Sales|Revenue|Net Income|Free Cash Flow/));
        });

        if (!financialTable) {
          console.log('No financial table found');
          return {};
        }

        const rows = Array.from(financialTable.querySelectorAll('.table__Row-sc-1djjifq-2'));
        const headerKeywords = {
          'sales/revenue': 'revenue_value',
          'operating income': 'operating_income',
          'net income': 'net_income_value',
          'ebitda': 'ebitda',
          'free cash flow': 'free_cash_flow',
          'cash flow': 'cash_value',
          'total debt': 'debt_value',
          'liabilities': 'liabilities',
          'basic shares outstanding': 'shares_outstanding'
        };

        const headers = [];
        const dataRows = [];

        // Function to check if a value is numerical
        const isNumerical = (val) => {
          if (!val) return false;
          const cleanVal = val.replace(/[()$,]/g, '').trim();
          return !isNaN(parseFloat(cleanVal)) && isFinite(cleanVal);
        };

        rows.forEach((row, index) => {
          const text = row.textContent.trim().toLowerCase();
          const values = Array.from(row.querySelectorAll('.table__Cell-sc-1djjifq-5'))
            .map(cell => cell.textContent.trim())
            .filter(val => val && !val.includes('ChartBar') && !val.match(/%/) && !val.match(/Growth/i) && !val.match(/^\d{4}$/));

          console.log(`Row ${index + 1}: Text: "${text}", Values: [${values.join(', ')}]`);

          // Skip chart noise
          if (text.includes('chartbar chart')) return;

          // Collect headers (no values present)
          const headerKey = Object.keys(headerKeywords).find(key => text === key);
          if (headerKey && values.length === 0) {
            headers.push({ key: headerKeywords[headerKey], index });
            console.log(`Header found: '${headerKeywords[headerKey]}' at Row ${index + 1}`);
          }

          // Collect data rows with 5+ numerical values
          if (values.length >= 5 && values.every(isNumerical)) {
            dataRows.push({ values, index });
            console.log(`Data row found at Row ${index + 1}: [${values.join(', ')}]`);
          }
        });

        const result = {};
        headers.forEach((header, i) => {
          if (i < dataRows.length) {
            const latestValue = dataRows[i].values[dataRows[i].values.length - 1]; // Latest year (e.g., 2024)
            result[header.key] = latestValue;
            console.log(`Matched '${header.key}' to ${latestValue} from Row ${dataRows[i].index + 1}`);
          } else {
            console.log(`No data row for header '${header.key}' at index ${i}`);
          }
        });

        if (headers.length !== dataRows.length) {
          console.log(`Warning: Found ${headers.length} headers but ${dataRows.length} data rows`);
        }

        return result;
      };

      const financials = extractFinancialTable();
      return {
        market_cap_value: extractSummaryValue(['Market Value', 'Market Cap']),
        enterprise_value_value: extractSummaryValue(['Enterprise Value']),
        trailing_pe: extractSummaryValue(['P/E (Trailing)', 'Trailing P/E']),
        forward_pe: extractSummaryValue(['P/E (Forward)', 'Forward P/E']),
        price_to_sales: extractSummaryValue(['Price/Sales', 'P/S']),
        price_to_book: extractSummaryValue(['Price/Book', 'P/B']),
        shares_outstanding: financials.shares_outstanding || extractSummaryValue(['Shares Outstanding', 'Basic Shares']),
        revenue_value: financials.revenue_value || extractSummaryValue(['Sales/Revenue', 'Revenue', 'Sales']),
        net_income_value: financials.net_income_value || extractSummaryValue(['Net Income', 'Consolidated Net Income']),
        operating_income: financials.operating_income || extractSummaryValue(['Operating Income']),
        ebitda: financials.ebitda || extractSummaryValue(['EBITDA']),
        cash_value: financials.cash_value || extractSummaryValue(['Cash', 'Cash & Equivalents', 'Cash Flow']),
        debt_value: financials.debt_value || extractSummaryValue(['Total Debt', 'Debt']),
        liabilities: financials.liabilities || extractSummaryValue(['Liabilities', 'Total Liabilities']),
        free_cash_flow: financials.free_cash_flow || extractSummaryValue(['Free Cash Flow'])
      };
    });

    logger.debug({ message: 'Extracted data from page', url, data });
    return data;
  } catch (err) {
    logger.error({ message: 'Puppeteer fetch failed', url, error: err.message });
    if (err.message.includes('timeout') && retries > 0) {
      logger.warn({ message: 'Retrying due to timeout', url, retriesLeft: retries - 1 });
      await delay(10000);
      if (browser) await browser.close();
      return await fetchWithPuppeteer(url, retries - 1);
    }
    throw err;
  } finally {
    if (browser) await browser.close();
  }
}

// Process raw data from fetchWithPuppeteer
async function processData(rawData, ticker) {
  const processedData = {};
  const fieldsToSanitize = [
    'market_cap_value', 'enterprise_value_value', 'trailing_pe', 'forward_pe',
    'price_to_sales', 'price_to_book', 'shares_outstanding', 'revenue_value',
    'net_income_value', 'operating_income', 'ebitda', 'cash_value', 'debt_value',
    'liabilities', 'free_cash_flow'
  ];

  for (const field of fieldsToSanitize) {
    processedData[field] = rawData[field] ? sanitizeValue(rawData[field], field, ticker) : null;
  }

  const missingFields = fieldsToSanitize.filter(field => processedData[field] === null);
  if (missingFields.length > 0) {
    logger.info({ message: 'Missing fields', ticker, fields: missingFields });
  }

  return processedData;
}

// Update financials table
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

  const updateData = {};
  for (const [key, value] of Object.entries(financialData)) {
    if (FINANCIAL_FIELDS.includes(key)) {
      if (value !== null) {
        if ((key === 'market_cap_value' || key === 'shares_outstanding') && value < 1e6) {
          logger.warn({ message: `${key} too low, skipping`, ticker, value });
          continue;
        }
        updateData[key] = value;
      } else {
        updateData[key] = null;
      }
    }
  }

  try {
    const existing = await getQuery('SELECT * FROM financials WHERE company_id = ?', [companyId]);
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

// Update capital structure table
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

// Process a single company
async function processCompany({ baseTicker, fullTicker, companyId }) {
  const url = `https://www.barrons.com/market-data/stocks/${baseTicker}/financials?countrycode=ca&mod=searchresults_companyquotes&mod=searchbar&search_keywords=${baseTicker.toUpperCase()}&search_statement_type=suggested`;

  try {
    const rawData = await fetchWithPuppeteer(url);
    const data = await processData(rawData, fullTicker);
    await updateFinancialsTable(companyId, fullTicker, data);
    await updateCapitalStructureTable(companyId, fullTicker, data.shares_outstanding);
    logger.info({ message: 'Processed company', ticker: fullTicker });
  } catch (err) {
    logger.error({ message: 'Company processing failed', ticker: fullTicker, error: err.message });
  }
}

// Update financials for all companies
async function updateFinancials() {
  // const companies = [
    // { fullTicker: 'ITR.V', baseTicker: 'itr', companyId: 1 },
    // { fullTicker: 'AAG.V', baseTicker: 'aag', companyId: 2 },
    // { fullTicker: 'ITH.TO', baseTicker: 'ith', companyId: 3 },
    // { fullTicker: 'IRV.V', baseTicker: 'irv', companyId: 4 },
    // { fullTicker: 'AAB.TO', baseTicker: 'aab', companyId: 5 }
  // ];
  const companies = [
    { fullTicker: 'XOM', baseTicker: 'xom', companyId: 6 }
  ];



  for (const company of companies) {
    await processCompany(company);
    await delay(15000);
  }
}

// Run with lock check
async function runWithLockCheck() {
  try {
    if (await fs.access(LOCK_FILE).then(() => true).catch(() => false)) {
      logger.warn({ message: 'Removing stale lock file' });
      await fs.unlink(LOCK_FILE);
    }
    await fs.writeFile(LOCK_FILE, '');
    logger.info({ message: 'Lock file created' });

    await updateFinancials();
    logger.info({ message: 'Execution completed successfully' });
  } catch (err) {
    logger.error({ message: 'Execution failed', error: err.message });
  } finally {
    await fs.unlink(LOCK_FILE).catch(() => logger.warn({ message: 'Lock file already removed' }));
    db.close((err) => {
      if (err) logger.error({ message: 'Database close failed', error: err.message });
      else logger.info({ message: 'Database closed' });
    });
  }
}

// Handle SIGINT
process.on('SIGINT', async () => {
  logger.info({ message: 'Received SIGINT, shutting down' });
  await fs.unlink(LOCK_FILE).catch(() => {});
  db.close((err) => {
    if (err) logger.error({ message: 'Database close failed on shutdown', error: err.message });
    logger.info({ message: 'Shutdown complete' });
    process.exit(0);
  });
});

// Start the script
runWithLockCheck();