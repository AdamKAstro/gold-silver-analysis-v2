const yahooFinance = require('yahoo-finance2').default;
yahooFinance.suppressNotices(['yahooSurvey']); // Suppress Yahoo survey notices
const sqlite3 = require('sqlite3').verbose(); // SQLite database library
const fs = require('fs'); // Synchronous file operations (for lock file)
const fsPromises = require('fs').promises; // Asynchronous file operations (for logging)
const { parse } = require('csv-parse/sync'); // Sync CSV parsing
const cron = require('node-cron'); // Cron scheduler
const path = require('path'); // Path utilities for lock file

// Constants
const CSV_FILE = 'public/data/companies.csv'; // CSV file with company tickers
const LOG_FILE = 'financial_population_log.txt'; // Success log file
const ERROR_LOG_FILE = 'financial_population_errors.txt'; // Error log file
const LOCK_FILE = path.join(__dirname, 'financials_update.lock'); // Lock file to prevent concurrent runs
const DELAY_BETWEEN_CALLS = 150; // 15-second delay between API calls
const MAX_RETRIES = 4; // Max retries for transient errors
const SKIP_IF_UPDATED_WITHIN_HOURS = 12; // Skip if updated within 12 hours
const LOCK_FILE_TIMEOUT = 24 * 60 * 60 * 1000; // 24-hour timeout for stale lock file

// Initialize SQLite database connection
const db = new sqlite3.Database('./mining_companies.db', (err) => {
  if (err) {
    console.error(`[${new Date().toISOString()}] ERROR: Database connection failed: ${err.message}`);
  } else {
    console.log(`[${new Date().toISOString()}] INFO: Connected to database for financial population`);
  }
});

// Utility function for delay (used for retries and rate limiting)
async function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Fetch financial data from Yahoo Finance with robust error handling
async function fetchYahooFinancials(ticker) {
  console.log(`[${new Date().toISOString()}] INFO: Fetching financial data for ${ticker}`);
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const quoteSummary = await yahooFinance.quoteSummary(ticker, {
        modules: ['financialData', 'defaultKeyStatistics', 'incomeStatementHistory']
      });
      console.log(`[${new Date().toISOString()}] INFO: Successfully fetched data for ${ticker} on attempt ${attempt + 1}`);

      const financialData = quoteSummary.financialData || {};
      const keyStats = quoteSummary.defaultKeyStatistics || {};
      const incomeHistory = quoteSummary.incomeStatementHistory?.incomeStatementHistory[0] || {};

      const data = {
        market_cap_value: keyStats.marketCap || null, // Market capitalization
        enterprise_value_value: keyStats.enterpriseValue || null, // Enterprise value
        trailing_pe: keyStats.trailingPE || null, // Trailing P/E ratio
        forward_pe: keyStats.forwardPE || null, // Forward P/E ratio
        peg_ratio: keyStats.pegRatio || null, // PEG ratio (5yr expected)
        price_to_sales: keyStats.priceToSalesTrailing12Months || null, // Price-to-sales ratio
        price_to_book: keyStats.priceToBook || null, // Price-to-book ratio
        enterprise_to_revenue: keyStats.enterpriseToRevenue || null, // Enterprise value to revenue
        enterprise_to_ebitda: keyStats.enterpriseToEbitda || null, // Enterprise value to EBITDA
        revenue_value: incomeHistory.totalRevenue || null, // Total revenue
        cost_of_revenue: incomeHistory.costOfRevenue || null, // Cost of revenue
        gross_profit: incomeHistory.grossProfit || null, // Gross profit
        operating_expense: incomeHistory.totalOperatingExpenses || null, // Operating expenses
        operating_income: incomeHistory.operatingIncome || null, // Operating income
        net_income_value: incomeHistory.netIncome || null, // Net income
        ebitda: financialData.ebitda || null, // EBITDA
        cash_value: financialData.totalCash || null, // Cash on hand
        debt_value: financialData.totalDebt || null, // Total debt
        currency: financialData.currency || 'USD' // Default currency if not provided
      };

      return data;
    } catch (e) {
      if (e.message.includes('Quote not found')) {
        console.error(`[${new Date().toISOString()}] ERROR: Quote not found for ${ticker}: ${e.message}`);
        await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: Quote not found - ${e.message}\n`);
        return null;
      } else if (e.message.includes('Failed Yahoo Schema validation')) {
        console.error(`[${new Date().toISOString()}] ERROR: Schema validation failed for ${ticker}: ${e.message}`);
        await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: Validation error - ${e.message}\n`);
        return null;
      } else if (e.message.includes('No fundamentals data found')) {
        console.error(`[${new Date().toISOString()}] ERROR: No financial data for ${ticker}: ${e.message}`);
        await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: No financial data - ${e.message}\n`);
        return null;
      } else {
        console.error(`[${new Date().toISOString()}] ERROR: Attempt ${attempt + 1} failed for ${ticker}: ${e.message}`);
        if (attempt < MAX_RETRIES - 1) {
          console.log(`[${new Date().toISOString()}] INFO: Retrying ${ticker} after delay`);
          await delay(5000 * Math.pow(2, attempt));
        } else {
          console.error(`[${new Date().toISOString()}] ERROR: Exhausted ${MAX_RETRIES} retries for ${ticker}`);
          await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: Failed after ${MAX_RETRIES} retries - ${e.message}\n`);
          return null;
        }
      }
    }
  }
}

// Cleanup function to remove lock file and close database
async function cleanupLockFile() {
  if (fs.existsSync(LOCK_FILE)) {
    fs.unlinkSync(LOCK_FILE);
    console.log(`[${new Date().toISOString()}] INFO: Removed lock file`);
  }
  db.close((err) => {
    if (err) {
      console.error(`[${new Date().toISOString()}] ERROR: Failed to close database: ${err.message}`);
    } else {
      console.log(`[${new Date().toISOString()}] INFO: Database connection closed`);
    }
  });
}

// Main function to update financials
async function updateFinancials() {
  console.log(`[${new Date().toISOString()}] INFO: Starting financial data update process`);
  let companies;

  // Parse companies from CSV file
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

  // Process each company
  for (const company of companies) {
    const ticker = company.TICKER;
    if (!ticker || ticker === 'undefined') {
      console.error(`[${new Date().toISOString()}] ERROR: Invalid ticker found: ${JSON.stringify(company)}`);
      await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] Skipping invalid ticker: ${JSON.stringify(company)}\n`);
      continue;
    }

    // Fetch company_id from the companies table
    let companyId;
    try {
      companyId = await new Promise((resolve, reject) => {
        db.get('SELECT company_id FROM companies WHERE tsx_code = ?', [ticker], (err, row) => {
          if (err) {
            reject(err);
          } else if (!row) {
            console.error(`[${new Date().toISOString()}] ERROR: No company_id found for ticker ${ticker} in companies table`);
            resolve(null);
          } else {
            console.log(`[${new Date().toISOString()}] INFO: Found company_id ${row.company_id} for ${ticker}`);
            resolve(row.company_id);
          }
        });
      });
    } catch (err) {
      console.error(`[${new Date().toISOString()}] ERROR: Database error fetching company_id for ${ticker}: ${err.message}`);
      await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: DB error fetching company_id - ${err.message}\n`);
      continue;
    }

    if (!companyId) {
      await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: Skipped - no company_id found\n`);
      continue;
    }

    // Check if fully updated in the last 12 hours
    try {
      const row = await new Promise((resolve, reject) => {
        db.get(
          'SELECT last_updated FROM financials WHERE company_id = ? AND market_cap_value IS NOT NULL AND revenue_value IS NOT NULL AND net_income_value IS NOT NULL',
          [companyId],
          (err, row) => {
            if (err) reject(err);
            else resolve(row);
          }
        );
      });
      if (row && row.last_updated) {
        const lastUpdated = new Date(row.last_updated);
        const twelveHoursAgo = new Date(Date.now() - SKIP_IF_UPDATED_WITHIN_HOURS * 60 * 60 * 1000);
        if (lastUpdated > twelveHoursAgo) {
          console.log(`[${new Date().toISOString()}] INFO: Skipping ${ticker}, successfully updated at ${lastUpdated.toISOString()}`);
          continue;
        }
      }
    } catch (err) {
      console.error(`[${new Date().toISOString()}] ERROR: Failed to check last_updated for ${ticker}: ${err.message}`);
      await fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: Error checking last_updated - ${err.message}\n`);
      continue;
    }

    // Fetch financial data
    const data = await fetchYahooFinancials(ticker);
    if (!data) {
      console.log(`[${new Date().toISOString()}] INFO: Skipping ${ticker} due to fetch failure`);
      continue;
    }

    const {
      market_cap_value, enterprise_value_value, trailing_pe, forward_pe, peg_ratio,
      price_to_sales, price_to_book, enterprise_to_revenue, enterprise_to_ebitda,
      revenue_value, cost_of_revenue, gross_profit, operating_expense, operating_income,
      net_income_value, ebitda, cash_value, debt_value, currency
    } = data;

    // Log successful data fetch
    console.log(`[${new Date().toISOString()}] INFO: Successfully fetched financial data for ${ticker}`);
    await fsPromises.appendFile(LOG_FILE, `[${new Date().toISOString()}] ${ticker}: Fetched market_cap=${market_cap_value}, revenue=${revenue_value}, net_income=${net_income_value}, etc.\n`);

    // Insert or update financials table
    try {
      await new Promise((resolve, reject) => {
        db.run(
          `INSERT INTO financials (
            company_id, market_cap_value, market_cap_currency, enterprise_value_value, enterprise_value_currency,
            trailing_pe, forward_pe, peg_ratio, price_to_sales, price_to_book,
            enterprise_to_revenue, enterprise_to_ebitda, revenue_value, revenue_currency,
            cost_of_revenue, gross_profit, operating_expense, operating_income,
            net_income_value, net_income_currency, ebitda, cash_value, cash_currency,
            debt_value, debt_currency, last_updated
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            last_updated = excluded.last_updated`,
          [
            companyId, market_cap_value, currency, enterprise_value_value, currency,
            trailing_pe, forward_pe, peg_ratio, price_to_sales, price_to_book,
            enterprise_to_revenue, enterprise_to_ebitda, revenue_value, currency,
            cost_of_revenue, gross_profit, operating_expense, operating_income,
            net_income_value, currency, ebitda, cash_value, currency,
            debt_value, currency, new Date().toISOString()
          ],
          (err) => {
            if (err) {
              console.error(`[${new Date().toISOString()}] ERROR: Failed to update financials for ${ticker}: ${err.message}`);
              fsPromises.appendFile(ERROR_LOG_FILE, `[${new Date().toISOString()}] ${ticker}: DB error - ${err.message}\n`);
              reject(err);
            } else {
              console.log(`[${new Date().toISOString()}] INFO: Updated financials for ${ticker} in database`);
              resolve();
            }
          }
        );
      });
    } catch (err) {
      console.error(`[${new Date().toISOString()}] ERROR: Database operation failed for ${ticker}: ${err.message}`);
      continue;
    }

    console.log(`[${new Date().toISOString()}] INFO: Waiting ${DELAY_BETWEEN_CALLS / 1000} seconds before next ticker`);
    await delay(DELAY_BETWEEN_CALLS);
  }

  console.log(`[${new Date().toISOString()}] INFO: Financial data update process completed`);
}

// Check and handle stale lock file synchronously at startup
async function runWithLockCheck() {
  if (fs.existsSync(LOCK_FILE)) {
    const stats = fs.statSync(LOCK_FILE);
    const lockAge = Date.now() - stats.mtimeMs;
    if (lockAge > LOCK_FILE_TIMEOUT) {
      console.log(`[${new Date().toISOString()}] INFO: Lock file is stale (older than 24 hours), removing it`);
      fs.unlinkSync(LOCK_FILE);
    } else {
      console.log(`[${new Date().toISOString()}] INFO: Another instance is running, exiting`);
      db.close();
      return;
    }
  }

  // Create lock file
  fs.writeFileSync(LOCK_FILE, '');
  console.log(`[${new Date().toISOString()}] INFO: Starting immediate financial update`);

  try {
    await updateFinancials();
  } catch (err) {
    console.error(`[${new Date().toISOString()}] ERROR: Financial update failed: ${err.message}`);
  } finally {
    await cleanupLockFile();
  }
}

// Handle unexpected exits
process.on('uncaughtException', async (err) => {
  console.error(`[${new Date().toISOString()}] ERROR: Uncaught exception: ${err.message}`);
  await cleanupLockFile();
  process.exit(1);
});

process.on('SIGINT', async () => {
  console.log(`[${new Date().toISOString()}] INFO: Received SIGINT (Ctrl+C), cleaning up`);
  await cleanupLockFile();
  process.exit(0);
});

// Run the script with lock check
runWithLockCheck();

// Schedule daily execution at 3 AM
cron.schedule('0 3 * * *', async () => {
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
  } catch (err) {
    console.error(`[${new Date().toISOString()}] ERROR: Scheduled update failed: ${err.message}`);
  } finally {
    await cleanupLockFile();
  }
});