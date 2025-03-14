require('dotenv').config();
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const path = require('path');
const fs = require('fs');
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

// Initialize the database
const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    logger.error({ message: 'Failed to connect to database', error: err.message });
    throw err;
  }
  logger.info({ message: 'Connected to database', path: DB_PATH });
});

// Database query helpers
const runQuery = (query, params) => {
  return new Promise((resolve, reject) => {
    db.run(query, params, function (err) {
      if (err) {
        reject(err);
      } else {
        resolve(this);
      }
    });
  });
};

const getQuery = (query, params) => {
  return new Promise((resolve, reject) => {
    db.get(query, params, (err, row) => {
      if (err) {
        reject(err);
      } else {
        resolve(row);
      }
    });
  });
};

// Sanitize values
const sanitizeValue = (value, field, ticker) => {
  if (!value || value === 'N/A' || value === '--' || value === '-' || value.includes('Copyright')) {
    logger.debug({ message: `Sanitizing ${field}: null/invalid`, value, ticker });
    return null;
  }
  const cleanedValue = value.replace(/[^\d.-]/g, '');
  const num = parseFloat(cleanedValue);
  const multiplier = value.includes('B') ? 1e9 : value.includes('M') ? 1e6 : value.includes('K') ? 1e3 : 1;
  const parsed = num * multiplier;
  logger.debug({ message: `Sanitizing ${field}`, value, parsed, multiplier, ticker });
  const result = Number.isFinite(parsed) ? parsed : null;
  logger.info({ message: 'Processed field', key: field, ticker, value: result });
  return result;
};

// Load cookies
async function loadCookies() {
  try {
    const cookiesString = await fs.promises.readFile(path.join(__dirname, 'cookies.json'), 'utf8');
    return JSON.parse(cookiesString);
  } catch (err) {
    logger.error({ message: 'Failed to load cookies', error: err.message });
    return [];
  }
}

// Fetch data with Puppeteer
async function fetchWithPuppeteer(url) {
  let browser;
  try {
    browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    const cookies = await loadCookies();
    if (cookies.length > 0) await page.setCookie(...cookies);

    await page.goto(url, { waitUntil: 'networkidle2', timeout: 40000 });
    await page.waitForSelector('[data-id="FinancialTables_table"], .summary-table', { timeout: 40000 });
    await delay(5000);

    page.on('console', msg => logger.debug({ message: 'Browser console', text: msg.text() }));

    const data = await page.evaluate(() => {
      const extractSummaryValue = (labels) => {
        const rows = Array.from(document.querySelectorAll('.summary-table tr, .table__Row-sc-1djjifq-2'));
        for (const label of labels) {
          const row = rows.find(r => r.textContent.toLowerCase().includes(label.toLowerCase()));
          if (row) {
            const value = row.querySelector('td:last-child, .table__Cell-sc-1djjifq-5:last-child')?.textContent.trim();
            return value || null;
          }
        }
        return null;
      };

      const extractFinancialTable = () => {
        const tables = document.querySelectorAll('[data-id="FinancialTables_table"], table');
        const headerRowMap = new Map();
        const allDataRows = [];

        tables.forEach((table, tableIndex) => {
          const rows = Array.from(table.querySelectorAll('.table__Row-sc-1djjifq-2, tr'));
          rows.forEach((row, rowIndex) => {
            const cells = Array.from(row.querySelectorAll('.table__Cell-sc-1djjifq-5:not(.table__HeaderCell-sc-1djjifq-6)'));
            const values = cells.map(cell => cell.textContent.trim());
            console.log(`Table ${tableIndex}, Row ${rowIndex}: ${JSON.stringify(values)}`);

            if (values.length === 1 && values[0].match(/^(Sales|Revenue|Operating Income|Net Income|Basic Shares|Pretax|EBITDA|Free Cash Flow|Cash Flow|Total Debt|Liabilities)/i)) {
              if (!headerRowMap.has(values[0])) {
                headerRowMap.set(values[0], null);
                console.log(`Header found: ${values[0]} in Table ${tableIndex}, Row ${rowIndex}`);
              }
            } else if (values.length >= 3 && !values.every(v => v === '-' || v === '' || v.includes('%') || v === 'N/A')) {
              allDataRows.push({ values, tableIndex, rowIndex });
              console.log(`Collected data row: ${JSON.stringify(values)} in Table ${tableIndex}, Row ${rowIndex}`);
            }
          });
        });

        const assignRowToHeader = (header, condition) => {
          const row = allDataRows.find(r => condition(r.values));
          if (row) {
            headerRowMap.set(header, row);
            console.log(`Assigned data to header ${header}: ${JSON.stringify(row.values)}`);
            return row.values[row.values.length - 1];
          }
          return null;
        };

        const financialData = {
          revenue_value: assignRowToHeader('Sales/Revenue', values => values.some(val => parseFloat(val.replace(/[^\d.-]/g, '')) > 1e6)),
          net_income_value: assignRowToHeader('Net Income', values => values.some(val => val.includes('(')) || parseFloat(values[values.length - 1].replace(/[^\d.-]/g, '')) < 1e7),
          operating_income: assignRowToHeader('Operating Income', values => parseFloat(values[values.length - 1].replace(/[^\d.-]/g, '')) < 1e7 && !values.some(val => val.includes('('))),
          shares_outstanding: assignRowToHeader('Basic Shares Outstanding', values => values.some(val => parseFloat(val.replace(/[^\d.-]/g, '')) > 10e6)),
          ebitda: assignRowToHeader('EBITDA', values => parseFloat(values[values.length - 1].replace(/[^\d.-]/g, '')) < 1e8 && !values.some(val => val.includes('('))),
          free_cash_flow: assignRowToHeader('Free Cash Flow', values => values.some(val => val.includes('(')) || parseFloat(values[values.length - 1].replace(/[^\d.-]/g, '')) < 1e7),
          cash_value: assignRowToHeader('Cash Flow', values => values.every(val => !val.includes('%') && !val.includes('(')) && parseFloat(values[values.length - 1].replace(/[^\d.-]/g, '')) < 1e7) || null,
          debt_value: assignRowToHeader('Total Debt', values => values.some(val => parseFloat(val.replace(/[^\d.-]/g, '')) < 1e8 && !val.includes('('))) || null,
          liabilities: assignRowToHeader('Liabilities', values => values.some(val => parseFloat(val.replace(/[^\d.-]/g, '')) < 1e8)) || null,
        };

        console.log('Final headerRowMap:', JSON.stringify([...headerRowMap.entries()]));
        return financialData;
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
        revenue_value: financials.revenue_value,
        net_income_value: financials.net_income_value,
        operating_income: financials.operating_income,
        ebitda: financials.ebitda,
        cash_value: financials.cash_value,
        debt_value: financials.debt_value,
        liabilities: financials.liabilities,
        free_cash_flow: financials.free_cash_flow,
      };
    });

    logger.debug({ message: 'Extracted data from page', url, data });
    return data;
  } catch (err) {
    logger.error({ message: 'Puppeteer fetch failed', url, error: err.message });
    if (err.message.includes('timeout')) {
      logger.warn({ message: 'Retrying due to timeout', url });
      await delay(10000);
      return await fetchWithPuppeteer(url);
    }
    throw err;
  } finally {
    if (browser) await browser.close();
  }
}

// Update the sanitization logic in processData
async function processData(ticker, rawData) {
  const sanitizeValue = (value) => {
    if (!value || value === 'N/A' || value === '--' || value === '-') return null;
    const isNegative = value.includes('(');
    const cleanedValue = value.replace(/[$,()]/g, '').trim();
    const multiplier = cleanedValue.includes('B') ? 1e9 : cleanedValue.includes('M') ? 1e6 : cleanedValue.includes('K') ? 1e3 : 1;
    const num = parseFloat(cleanedValue.replace(/[BMK]/g, ''));
    const parsed = num * multiplier * (isNegative ? -1 : 1);
    logger.debug({ message: `Sanitizing value`, ticker, value, parsed });
    return Number.isFinite(parsed) ? parsed : null;
  };

  const processedData = {};
  const fieldsToSanitize = [
    'market_cap_value', 'enterprise_value_value', 'trailing_pe', 'forward_pe',
    'price_to_sales', 'price_to_book', 'shares_outstanding', 'revenue_value',
    'net_income_value', 'operating_income', 'ebitda', 'cash_value', 'debt_value',
    'liabilities', 'free_cash_flow'
  ];

  for (const field of fieldsToSanitize) {
    processedData[field] = rawData[field] ? sanitizeValue(rawData[field]) : null;
  }

  const missingFields = fieldsToSanitize.filter(field => processedData[field] === null);
  if (missingFields.length > 0) {
    logger.info({ message: 'Missing fields', ticker, fields: missingFields });
  }

  return processedData;
}


// Process raw data from fetchWithPuppeteer
function processData(rawData, ticker) {
  const data = {};
  const fieldsToSanitize = [
    'market_cap_value', 'shares_outstanding', 'revenue_value',
    'net_income_value', 'cash_value', 'debt_value',
    'liabilities', 'operating_income', 'pretax_income'
  ];

  for (const field of fieldsToSanitize) {
    data[field] = rawData[field] ? sanitizeValue(rawData[field], field, ticker) : null;
  }

  return data;
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
    enterprise_value_value: data.enterprise_value_value || null,
    enterprise_value_currency: CURRENCY,
    revenue_value: data.revenue_value || null,
    revenue_currency: CURRENCY,
    net_income_value: data.net_income_value || null,
    net_income_currency: CURRENCY,
    operating_income: data.operating_income || null,
    ebitda: data.ebitda || null,
    debt_value: data.debt_value || null,
    debt_currency: CURRENCY,
    shares_outstanding: data.shares_outstanding || null,
    trailing_pe: data.trailing_pe || null,
    forward_pe: data.forward_pe || null,
    price_to_sales: data.price_to_sales || null,
    price_to_book: data.price_to_book || null,
    last_updated: new Date().toISOString(),
    data_source: DATA_SOURCE
  };

  // Validation checks
  const updateData = {};
  for (const [key, value] of Object.entries(financialData)) {
    if (FINANCIAL_FIELDS.includes(key)) {
      if (value !== null) {
        if ((key === 'market_cap_value' || key === 'shares_outstanding') && value < 1e6) {
          logger.warn({ message: `${key} too low, skipping`, ticker, value });
          continue;
        }
        if (key.includes('pe') && (value < 0 || value > 1000)) {
          logger.warn({ message: `${key} out of reasonable range, skipping`, ticker, value });
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
    const data = processData(rawData, fullTicker);
    await updateFinancialsTable(companyId, fullTicker, data);
    await updateCapitalStructureTable(companyId, fullTicker, data.shares_outstanding);
    logger.info({ message: 'Processed company', ticker: fullTicker });
  } catch (err) {
    logger.error({ message: 'Company processing failed', ticker: fullTicker, error: err.message });
  }
}

// Update financials for all companies
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

// Run with lock check
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

// Handle SIGINT
process.on('SIGINT', async () => {
  logger.info({ message: 'Received SIGINT, shutting down' });
  if (fs.existsSync(LOCK_FILE)) fs.unlinkSync(LOCK_FILE);
  db.close((err) => {
    if (err) logger.error({ message: 'Database close failed on shutdown', error: err.message });
    logger.info({ message: 'Shutdown complete' });
    process.exit(0);
  });
});

// Start the script
runWithLockCheck();