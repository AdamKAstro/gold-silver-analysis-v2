const sqlite3 = require('sqlite3').verbose();
const fs = require('fs').promises;
const pLimitModule = require('p-limit');
const yahooFinance = require('yahoo-finance2').default;

// Command-line flag for forcing updates
const FORCE_UPDATE = process.argv.includes('--force');

// Extract pLimit function
const pLimit = pLimitModule.default;
if (typeof pLimit !== 'function') {
    console.error('Failed to resolve pLimit.default as a function. p-limit export:', pLimitModule);
    process.exit(1);
}

// SQLite database connection
let isDbClosed = false;
const db = new sqlite3.Database('./mining_companies.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) logError('Database connection error', { message: err.message, stack: err.stack });
    else logInfo('Connected to database', { forceUpdate: FORCE_UPDATE, dbPath: './mining_companies.db' });
});

// Configuration constants
const CONCURRENCY_LIMIT = 5;
const RETRY_DELAY = 5000; // ms
const MAX_RETRIES = 2;

// Concurrency limit
const limit = pLimit(CONCURRENCY_LIMIT);

// Logging helpers with JSON details
const now = () => new Date().toISOString();
const logInfo = async (message, details = {}) => {
    const logLine = `${now()} [INFO] ${message}: ${JSON.stringify(details, null, 2)}`;
    console.log(logLine);
    await fs.appendFile('scrape.log', `${logLine}\n`);
};
const logError = async (message, details = {}) => {
    const logLine = `${now()} [ERROR] ${message}: ${JSON.stringify(details, null, 2)}`;
    console.error(logLine);
    await fs.appendFile('scrape.log', `${logLine}\n`);
};

// Insert or update validated URL into company_urls
async function insertValidatedUrl(ticker, urlType, url) {
    if (isDbClosed) {
        await logError('Cannot insert URL - database closed', { ticker, urlType, url });
        return;
    }
    try {
        await logInfo('Preparing to insert/update URL', { ticker, urlType, url });
        const sql = 'INSERT OR REPLACE INTO company_urls (company_id, url_type, url, last_validated) VALUES ((SELECT company_id FROM companies WHERE tsx_code = ?), ?, ?, ?)';
        const params = [ticker, urlType, url, now()];
        await logInfo('Executing SQL for URL insert', { sql, params });
        await new Promise((resolve, reject) => {
            db.run(sql, params, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });
        await logInfo('Successfully inserted/updated URL', { ticker, urlType, url });
    } catch (error) {
        await logError('Database insert/update failed', { ticker, urlType, url, error: { message: error.message, stack: error.stack } });
    }
}

// Fetch financial data for a single ticker with retry logic
async function fetchYahooFinanceData(ticker, attempt = 1) {
    try {
        await logInfo('Validating ticker input', { ticker, type: typeof ticker });
        if (!ticker || typeof ticker !== 'string') {
            await logError('Invalid ticker', { ticker, reason: 'Must be a non-empty string' });
            return null;
        }

        await logInfo('Initiating API call', { ticker, attempt, maxRetries: MAX_RETRIES });
        const modules = ['price', 'summaryDetail', 'financialData', 'balanceSheetHistory', 'incomeStatementHistory'];
        await logInfo('Requesting Yahoo Finance data', { ticker, modules });
        const result = await yahooFinance.quoteSummary(ticker, { modules });

        await logInfo('Received raw API response', { ticker, responseSize: JSON.stringify(result).length, response: result });
        if (!result.price?.symbol) {
            await logError('No symbol in API response', { ticker, rawResponse: result });
            return null;
        }

        const data = { data_source: 'Yahoo Finance' };
        await logInfo('Initializing data object', { ticker, initialData: data });

        // Populate data with fallbacks
        data.market_cap_value = result.price?.marketCap || result.summaryDetail?.marketCap || null;
        data.market_cap_currency = result.price?.currency || 'CAD';
        data.trailing_pe = result.summaryDetail?.trailingPE || null;
        data.forward_pe = result.summaryDetail?.forwardPE || null;
        data.shares_outstanding = result.summaryDetail?.sharesOutstanding || result.price?.sharesOutstanding || null;
        data.enterprise_value_value = result.summaryDetail?.enterpriseValue || null;
        data.enterprise_value_currency = result.summaryDetail?.currency || 'CAD';

        data.revenue_value = result.financialData?.totalRevenue || (result.incomeStatementHistory?.incomeStatementHistory[0]?.totalRevenue) || null;
        data.revenue_currency = result.financialData?.currency || 'CAD';
        data.net_income_value = result.financialData?.netIncomeToCommon || (result.incomeStatementHistory?.incomeStatementHistory[0]?.netIncome) || null;
        data.net_income_currency = result.financialData?.currency || 'CAD';
        data.ebitda = result.financialData?.ebitda || null;
        data.cash_value = result.financialData?.totalCash || (result.balanceSheetHistory?.balanceSheetStatements[0]?.cash) || null;
        data.cash_currency = result.financialData?.currency || 'CAD';
        data.debt_value = result.financialData?.totalDebt || (result.balanceSheetHistory?.balanceSheetStatements[0]?.totalLiab) || null;
        data.debt_currency = result.financialData?.currency || 'CAD';

        await logInfo('Parsed raw data', { ticker, data });

        const isDataEmpty = Object.values(data).every(val => val === null || val === 'Yahoo Finance' || val === data.market_cap_currency);
        await logInfo('Checking data completeness', { ticker, isDataEmpty, data });
        if (isDataEmpty) {
            await logError('All parsed values are null or default', { ticker, data });
            return null;
        }

        await logInfo('Validated data', { ticker, status: 'Contains non-null values' });
        const url = `https://finance.yahoo.com/quote/${ticker}/`;
        await insertValidatedUrl(ticker, 'yahoo_finance', url);
        return data;
    } catch (error) {
        await logError('API fetch failed', { 
            ticker, 
            attempt, 
            maxRetries: MAX_RETRIES, 
            error: { message: error.message, stack: error.stack, response: error.response?.data }
        });
        if (attempt < MAX_RETRIES && (error.response?.status === 429 || error.message.includes('timeout'))) {
            await logInfo('Scheduling retry', { ticker, delay: RETRY_DELAY / 1000, nextAttempt: attempt + 1 });
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
            return await fetchYahooFinanceData(ticker, attempt + 1);
        }
        return null;
    }
}

// Update database function
async function updateDatabase() {
    const currentTime = now();
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

    try {
        await logInfo('Fetching companies from database', { sql: 'SELECT company_id, tsx_code, company_name FROM companies' });
        const companies = await new Promise((resolve, reject) => {
            db.all('SELECT company_id, tsx_code, company_name FROM companies', (err, rows) => {
                if (err) reject(new Error(`Database query failed: ${err.message}`));
                else resolve(rows);
            });
        });

        await logInfo('Retrieved companies', { count: companies.length, sample: companies.slice(0, 3) });
        const validCompanies = companies.filter(c => c.tsx_code && typeof c.tsx_code === 'string');
        await logInfo('Filtered valid companies', { count: validCompanies.length, invalidCount: companies.length - validCompanies.length });

        const tasks = validCompanies.map(company => limit(async () => {
            const { company_id, tsx_code, company_name } = company;
            await logInfo('Processing company', { company_id, tsx_code, company_name });

            try {
                await logInfo('Querying last update', { tsx_code, company_id });
                const row = await new Promise((resolve, reject) => {
                    db.get('SELECT last_updated FROM financials WHERE company_id = ?', [company_id], (err, row) => {
                        if (err) reject(new Error(`Financials query failed: ${err.message}`));
                        else resolve(row);
                    });
                });

                const lastUpdated = row ? new Date(row.last_updated) : null;
                const updateNeeded = FORCE_UPDATE || !lastUpdated || lastUpdated < new Date(yesterday);
                await logInfo('Determined update status', { 
                    tsx_code, 
                    lastUpdated: lastUpdated?.toISOString(), 
                    yesterday: yesterday, 
                    force: FORCE_UPDATE, 
                    updateNeeded 
                });

                if (updateNeeded) {
                    await logInfo('Starting update', { tsx_code, company_name, reason: FORCE_UPDATE ? 'Forced update' : 'Out of date' });
                    const data = await fetchYahooFinanceData(tsx_code);
                    if (!data) {
                        await logError('No data fetched - skipping update', { tsx_code });
                        return;
                    }

                    data.company_id = company_id;
                    data.last_updated = currentTime;
                    await logInfo('Prepared data for database', { tsx_code, data });

                    const sql = `
                        INSERT INTO financials (
                            company_id, cash_value, cash_currency, market_cap_value, market_cap_currency,
                            enterprise_value_value, enterprise_value_currency, trailing_pe, forward_pe,
                            revenue_value, revenue_currency, net_income_value, net_income_currency,
                            ebitda, debt_value, debt_currency, shares_outstanding, last_updated, data_source
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(company_id) DO UPDATE SET
                            cash_value = excluded.cash_value, cash_currency = excluded.cash_currency,
                            market_cap_value = excluded.market_cap_value, market_cap_currency = excluded.market_cap_currency,
                            enterprise_value_value = excluded.enterprise_value_value, enterprise_value_currency = excluded.enterprise_value_currency,
                            trailing_pe = excluded.trailing_pe, forward_pe = excluded.forward_pe,
                            revenue_value = excluded.revenue_value, revenue_currency = excluded.revenue_currency,
                            net_income_value = excluded.net_income_value, net_income_currency = excluded.net_income_currency,
                            ebitda = excluded.ebitda, debt_value = excluded.debt_value, debt_currency = excluded.debt_currency,
                            shares_outstanding = excluded.shares_outstanding, last_updated = excluded.last_updated,
                            data_source = excluded.data_source
                    `;
                    const params = [
                        data.company_id, data.cash_value, data.cash_currency, data.market_cap_value, data.market_cap_currency,
                        data.enterprise_value_value, data.enterprise_value_currency, data.trailing_pe, data.forward_pe,
                        data.revenue_value, data.revenue_currency, data.net_income_value, data.net_income_currency,
                        data.ebitda, data.debt_value, data.debt_currency, data.shares_outstanding, data.last_updated, data.data_source
                    ];
                    await logInfo('Executing financials update', { tsx_code, sqlSnippet: sql.slice(0, 50) + '...', params });

                    await new Promise((resolve, reject) => {
                        db.run(sql, params, (err) => {
                            if (err) reject(new Error(`Insert/update failed: ${err.message}`));
                            else resolve();
                        });
                    });
                    await logInfo('Financials table updated', { tsx_code });

                    await logInfo('Updating companies table', { tsx_code, company_id });
                    await new Promise((resolve, reject) => {
                        db.run('UPDATE companies SET last_updated = ? WHERE company_id = ?', [currentTime, company_id], (err) => {
                            if (err) reject(new Error(`Companies update failed: ${err.message}`));
                            else resolve();
                        });
                    });
                    await logInfo('Companies table updated', { tsx_code });
                } else {
                    await logInfo('Skipping update - up-to-date', { tsx_code, lastUpdated: lastUpdated.toISOString() });
                }
            } catch (err) {
                await logError('Processing failed', { tsx_code, error: { message: err.message, stack: err.stack } });
            }
        }));

        await logInfo('Launching concurrent updates', { totalTasks: validCompanies.length, concurrencyLimit: CONCURRENCY_LIMIT });
        await Promise.all(tasks);
        await logInfo('All updates completed successfully');
    } catch (err) {
        await logError('Critical error in updateDatabase', { error: { message: err.message, stack: err.stack } });
    }
}

// Run the script with top-level error handling
(async () => {
    try {
        await logInfo('Script execution started', { args: process.argv });
        await updateDatabase();
        await logInfo('Script execution completed');
    } catch (err) {
        await logError('Script execution failed', { error: { message: err.message, stack: err.stack } });
    } finally {
        isDbClosed = true;
        await logInfo('Closing database connection');
        db.close((err) => {
            if (err) logError('Error closing database', { error: { message: err.message, stack: err.stack } });
            else logInfo('Database connection closed');
            process.exit(err ? 1 : 0);
        });
    }
})();

// Graceful shutdown
process.on('SIGINT', () => {
    logInfo('Received SIGINT, shutting down');
    isDbClosed = true;
    db.close((err) => {
        if (err) logError('Error closing database', { error: { message: err.message, stack: err.stack } });
        else logInfo('Database connection closed');
        process.exit(0);
    });
});