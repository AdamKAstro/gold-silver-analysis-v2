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
        const modules = [
            'price',
            'summaryDetail',
            'financialData',
            'balanceSheetHistory',
            'incomeStatementHistory',
            'defaultKeyStatistics'
        ];
        await logInfo('Requesting Yahoo Finance data', { ticker, modules });
        const result = await yahooFinance.quoteSummary(ticker, { 
            modules,
            validateResult: false // Ensure robustness
        });

        await logInfo('Received raw API response', { 
            ticker, 
            responseSize: JSON.stringify(result).length, 
            modules: Object.keys(result).filter(k => result[k] && Object.keys(result[k]).length > 0),
            incomeStmtRaw: result.incomeStatementHistory,
            financialDataRaw: result.financialData // Log financialData for additional checks
        });

        if (!result.price?.symbol) {
            await logError('No symbol in API response', { ticker, rawResponse: result });
            return null;
        }

        const data = { 
            financials: { data_source: 'Yahoo Finance' },
            capital_structure: {},
            stock_prices: {}
        };
        await logInfo('Initializing data object', { ticker, initialData: data });

        const currency = result.financialData?.financialCurrency || 
                        result.price?.currency || 
                        result.summaryDetail?.currency || 
                        'CAD';
        const now = new Date().toISOString();

        // Financials table
        const incomeStmt = result.incomeStatementHistory?.incomeStatementHistory[0] || {};
        await logInfo('Processing income statement', { ticker, incomeStmt });

        data.financials.cash_value = result.financialData?.totalCash || result.balanceSheetHistory?.balanceSheetStatements[0]?.cash || null;
        data.financials.cash_currency = currency;
        data.financials.cash_date = null;
        data.financials.investments_json = null;
        data.financials.hedgebook = null;
        data.financials.liabilities = result.financialData?.totalDebt || result.balanceSheetHistory?.balanceSheetStatements[0]?.totalLiab || null;
        data.financials.liabilities_currency = currency;
        data.financials.debt_value = data.financials.liabilities;
        data.financials.debt_currency = currency;
        data.financials.other_financial_assets = result.balanceSheetHistory?.balanceSheetStatements[0]?.shortTermInvestments || result.balanceSheetHistory?.balanceSheetStatements[0]?.otherCurrentAssets || null;
        data.financials.other_financial_assets_currency = currency;
        data.financials.market_cap_value = result.price?.marketCap || result.summaryDetail?.marketCap || result.defaultKeyStatistics?.marketCap || null;
        data.financials.market_cap_currency = currency;
        data.financials.enterprise_value_value = result.defaultKeyStatistics?.enterpriseValue || result.summaryDetail?.enterpriseValue || 
                                                (data.financials.market_cap_value && data.financials.debt_value && data.financials.cash_value ? 
                                                 data.financials.market_cap_value + data.financials.debt_value - data.financials.cash_value : null) || null;
        data.financials.enterprise_value_currency = currency;
        data.financials.net_financial_assets = (data.financials.cash_value && data.financials.liabilities) ? (data.financials.cash_value - data.financials.liabilities) : null;
        data.financials.net_financial_assets_currency = currency;

        // Revenue and costs with multiple sources
        data.financials.revenue_value = result.financialData?.totalRevenue || incomeStmt.totalRevenue || null;
        data.financials.revenue_currency = currency;
        data.financials.cost_of_revenue = incomeStmt.costOfRevenue !== undefined ? incomeStmt.costOfRevenue : 
                                        (result.financialData?.costOfRevenue ? result.financialData.costOfRevenue : null); // Check financialData
        data.financials.gross_profit = incomeStmt.grossProfit !== undefined ? incomeStmt.grossProfit : 
                                      (result.financialData?.grossProfits !== undefined ? result.financialData.grossProfits : 
                                       (data.financials.revenue_value && data.financials.cost_of_revenue ? data.financials.revenue_value - data.financials.cost_of_revenue : null)) || null;
        data.financials.operating_expense = incomeStmt.totalOperatingExpenses !== undefined ? incomeStmt.totalOperatingExpenses : 
                                           (incomeStmt.researchDevelopment && incomeStmt.sellingGeneralAdministrative ? 
                                            incomeStmt.researchDevelopment + incomeStmt.sellingGeneralAdministrative : 
                                            (result.financialData?.totalOperatingExpenses ? result.financialData.totalOperatingExpenses : null)) || null;
        data.financials.operating_income = incomeStmt.operatingIncome !== undefined ? incomeStmt.operatingIncome : 
                                          (result.financialData?.operatingIncome ? result.financialData.operatingIncome : 
                                           (data.financials.gross_profit && data.financials.operating_expense ? data.financials.gross_profit - data.financials.operating_expense : null)) || null;
        data.financials.net_income_value = result.financialData?.netIncomeToCommon || incomeStmt.netIncome || result.defaultKeyStatistics?.netIncomeToCommon || incomeStmt.netIncomeApplicableToCommonShares || null;
        data.financials.net_income_currency = currency;
        data.financials.ebitda = result.financialData?.ebitda || (data.financials.operating_income && incomeStmt.depreciation ? data.financials.operating_income + incomeStmt.depreciation : null) || null;
        data.financials.free_cash_flow = result.financialData?.freeCashflow || null;
        data.financials.shares_outstanding = result.summaryDetail?.sharesOutstanding || result.defaultKeyStatistics?.sharesOutstanding || result.price?.sharesOutstanding || null;

        const epsActual = result.defaultKeyStatistics?.trailingEps || 
                         (data.financials.net_income_value && data.financials.shares_outstanding ? data.financials.net_income_value / data.financials.shares_outstanding : null) || null;
        const epsGrowth = (result.defaultKeyStatistics?.forwardEps && epsActual ? (result.defaultKeyStatistics.forwardEps - epsActual) / epsActual : null) || null;

        data.financials.trailing_pe = result.summaryDetail?.trailingPE || (result.price?.regularMarketPrice && epsActual ? result.price.regularMarketPrice / epsActual : null) || null;
        data.financials.forward_pe = result.summaryDetail?.forwardPE || result.defaultKeyStatistics?.forwardPE || null;
        data.financials.peg_ratio = result.defaultKeyStatistics?.pegRatio || (data.financials.trailing_pe && epsGrowth ? data.financials.trailing_pe / (epsGrowth * 100) : null) || null;
        data.financials.price_to_sales = result.summaryDetail?.priceToSalesTrailing12Months || (data.financials.market_cap_value && data.financials.revenue_value ? data.financials.market_cap_value / data.financials.revenue_value : null) || null;
        data.financials.price_to_book = result.defaultKeyStatistics?.priceToBook || null;
        data.financials.enterprise_to_revenue = result.defaultKeyStatistics?.enterpriseToRevenue || (data.financials.enterprise_value_value && data.financials.revenue_value ? data.financials.enterprise_value_value / data.financials.revenue_value : null) || null;
        data.financials.enterprise_to_ebitda = result.defaultKeyStatistics?.enterpriseToEbitda || (data.financials.enterprise_value_value && data.financials.ebitda ? data.financials.enterprise_value_value / data.financials.ebitda : null) || null;

        // Capital structure table
        data.capital_structure.existing_shares = data.financials.shares_outstanding;
        data.capital_structure.fully_diluted_shares = null; // Not directly available
        data.capital_structure.in_the_money_options = null; // Not directly available
        data.capital_structure.options_revenue = null; // Not directly available
        data.capital_structure.options_revenue_currency = null;
        data.capital_structure.last_updated = now;

        // Stock prices table
        data.stock_prices.price_date = now.split('T')[0]; // Current date
        data.stock_prices.price_value = result.price?.regularMarketPrice || result.summaryDetail?.regularMarketPrice || null;
        data.stock_prices.price_currency = currency;
        data.stock_prices.change_1yr_percent = result.summaryDetail?.fiftyTwoWeekChange || null; // Approx 1-year change
        data.stock_prices.last_updated = now;

        // Log null fields for debugging
        const nullFields = {
            financials: Object.entries(data.financials).filter(([key, value]) => value === null && key !== 'data_source' && !key.endsWith('_currency') && key !== 'cash_date').map(([key]) => key),
            capital_structure: Object.entries(data.capital_structure).filter(([key, value]) => value === null && key !== 'last_updated').map(([key]) => key),
            stock_prices: Object.entries(data.stock_prices).filter(([key, value]) => value === null && key !== 'last_updated').map(([key]) => key)
        };
        if (Object.values(nullFields).some(fields => fields.length > 0)) {
            await logInfo('Fields remaining null', { ticker, nullFields });
        }

        await logInfo('Parsed raw data', { ticker, data });

        const isDataEmpty = Object.values(data.financials).every(val => val === null || val === 'Yahoo Finance' || val === currency) &&
                           Object.values(data.capital_structure).every(val => val === null || val === now) &&
                           Object.values(data.stock_prices).every(val => val === null || val === now || val === currency);
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


// Update database function with totals tracking
async function updateDatabase() {
    const currentTime = now();
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

    // Track non-null updates for financials
    const updateTotals = {
        cash_value: 0, market_cap_value: 0, enterprise_value_value: 0, trailing_pe: 0, forward_pe: 0,
        peg_ratio: 0, price_to_sales: 0, price_to_book: 0, enterprise_to_revenue: 0, enterprise_to_ebitda: 0,
        revenue_value: 0, cost_of_revenue: 0, gross_profit: 0, operating_expense: 0, operating_income: 0,
        net_income_value: 0, ebitda: 0, debt_value: 0, shares_outstanding: 0, free_cash_flow: 0,
        liabilities: 0, net_financial_assets: 0
    };

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

                    // Financials table update
                    data.financials.company_id = company_id;
                    data.financials.last_updated = currentTime;
                    const financialsSql = `
                        INSERT INTO financials (
                            company_id, cash_value, cash_currency, cash_date, investments_json, hedgebook,
                            liabilities, liabilities_currency, other_financial_assets, other_financial_assets_currency,
                            market_cap_value, market_cap_currency, enterprise_value_value, enterprise_value_currency,
                            net_financial_assets, net_financial_assets_currency, trailing_pe, forward_pe,
                            peg_ratio, price_to_sales, price_to_book, enterprise_to_revenue, enterprise_to_ebitda,
                            revenue_value, revenue_currency, cost_of_revenue, gross_profit, operating_expense,
                            operating_income, net_income_value, net_income_currency, ebitda, debt_value, debt_currency,
                            shares_outstanding, free_cash_flow, last_updated, data_source
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(company_id) DO UPDATE SET
                            cash_value = excluded.cash_value, cash_currency = excluded.cash_currency,
                            cash_date = excluded.cash_date, investments_json = excluded.investments_json,
                            hedgebook = excluded.hedgebook, liabilities = excluded.liabilities,
                            liabilities_currency = excluded.liabilities_currency,
                            other_financial_assets = excluded.other_financial_assets,
                            other_financial_assets_currency = excluded.other_financial_assets_currency,
                            market_cap_value = excluded.market_cap_value, market_cap_currency = excluded.market_cap_currency,
                            enterprise_value_value = excluded.enterprise_value_value, enterprise_value_currency = excluded.enterprise_value_currency,
                            net_financial_assets = excluded.net_financial_assets, net_financial_assets_currency = excluded.net_financial_assets_currency,
                            trailing_pe = excluded.trailing_pe, forward_pe = excluded.forward_pe,
                            peg_ratio = excluded.peg_ratio, price_to_sales = excluded.price_to_sales,
                            price_to_book = excluded.price_to_book, enterprise_to_revenue = excluded.enterprise_to_revenue,
                            enterprise_to_ebitda = excluded.enterprise_to_ebitda, revenue_value = excluded.revenue_value,
                            revenue_currency = excluded.revenue_currency, cost_of_revenue = excluded.cost_of_revenue,
                            gross_profit = excluded.gross_profit, operating_expense = excluded.operating_expense,
                            operating_income = excluded.operating_income, net_income_value = excluded.net_income_value,
                            net_income_currency = excluded.net_income_currency, ebitda = excluded.ebitda,
                            debt_value = excluded.debt_value, debt_currency = excluded.debt_currency,
                            shares_outstanding = excluded.shares_outstanding, free_cash_flow = excluded.free_cash_flow,
                            last_updated = excluded.last_updated, data_source = excluded.data_source
                    `;
                    const financialsParams = [
                        data.financials.company_id, data.financials.cash_value, data.financials.cash_currency, data.financials.cash_date, 
                        data.financials.investments_json, data.financials.hedgebook, data.financials.liabilities, data.financials.liabilities_currency, 
                        data.financials.other_financial_assets, data.financials.other_financial_assets_currency, data.financials.market_cap_value, 
                        data.financials.market_cap_currency, data.financials.enterprise_value_value, data.financials.enterprise_value_currency, 
                        data.financials.net_financial_assets, data.financials.net_financial_assets_currency, data.financials.trailing_pe, 
                        data.financials.forward_pe, data.financials.peg_ratio, data.financials.price_to_sales, data.financials.price_to_book, 
                        data.financials.enterprise_to_revenue, data.financials.enterprise_to_ebitda, data.financials.revenue_value, 
                        data.financials.revenue_currency, data.financials.cost_of_revenue, data.financials.gross_profit, data.financials.operating_expense, 
                        data.financials.operating_income, data.financials.net_income_value, data.financials.net_income_currency, data.financials.ebitda, 
                        data.financials.debt_value, data.financials.debt_currency, data.financials.shares_outstanding, data.financials.free_cash_flow, 
                        data.financials.last_updated, data.financials.data_source
                    ];
                    await logInfo('Executing financials update', { tsx_code, sqlSnippet: financialsSql.slice(0, 50) + '...', params: financialsParams.length });
                    await new Promise((resolve, reject) => {
                        db.run(financialsSql, financialsParams, (err) => {
                            if (err) reject(new Error(`Financials insert/update failed: ${err.message}`));
                            else resolve();
                        });
                    });
                    await logInfo('Financials table updated', { tsx_code });

                    // Update totals for non-null fields
                    for (const [key, value] of Object.entries(data.financials)) {
                        if (value !== null && key in updateTotals) {
                            updateTotals[key]++;
                        }
                    }

                    // Capital structure table update
                    data.capital_structure.company_id = company_id;
                    const capitalSql = `
                        INSERT INTO capital_structure (
                            company_id, existing_shares, fully_diluted_shares, in_the_money_options, 
                            options_revenue, options_revenue_currency, last_updated
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(company_id) DO UPDATE SET
                            existing_shares = excluded.existing_shares,
                            fully_diluted_shares = excluded.fully_diluted_shares,
                            in_the_money_options = excluded.in_the_money_options,
                            options_revenue = excluded.options_revenue,
                            options_revenue_currency = excluded.options_revenue_currency,
                            last_updated = excluded.last_updated
                    `;
                    const capitalParams = [
                        data.capital_structure.company_id, data.capital_structure.existing_shares, 
                        data.capital_structure.fully_diluted_shares, data.capital_structure.in_the_money_options, 
                        data.capital_structure.options_revenue, data.capital_structure.options_revenue_currency, 
                        currentTime
                    ];
                    await logInfo('Executing capital structure update', { tsx_code, sqlSnippet: capitalSql.slice(0, 50) + '...', params: capitalParams.length });
                    await new Promise((resolve, reject) => {
                        db.run(capitalSql, capitalParams, (err) => {
                            if (err) reject(new Error(`Capital structure insert/update failed: ${err.message}`));
                            else resolve();
                        });
                    });
                    await logInfo('Capital structure table updated', { tsx_code });

                    // Stock prices table update
                    data.stock_prices.company_id = company_id;
                    const pricesSql = `
                        INSERT INTO stock_prices (
                            company_id, price_date, price_value, price_currency, change_1yr_percent, last_updated
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(company_id, price_date) DO UPDATE SET
                            price_value = excluded.price_value,
                            price_currency = excluded.price_currency,
                            change_1yr_percent = excluded.change_1yr_percent,
                            last_updated = excluded.last_updated
                    `;
                    const pricesParams = [
                        data.stock_prices.company_id, data.stock_prices.price_date, data.stock_prices.price_value, 
                        data.stock_prices.price_currency, data.stock_prices.change_1yr_percent, currentTime
                    ];
                    await logInfo('Executing stock prices update', { tsx_code, sqlSnippet: pricesSql.slice(0, 50) + '...', params: pricesParams.length });
                    await new Promise((resolve, reject) => {
                        db.run(pricesSql, pricesParams, (err) => {
                            if (err) reject(new Error(`Stock prices insert/update failed: ${err.message}`));
                            else resolve();
                        });
                    });
                    await logInfo('Stock prices table updated', { tsx_code });

                    // Update companies table
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

        // Print totals
        console.log('\n--- Update Totals ---');
        for (const [field, count] of Object.entries(updateTotals)) {
            console.log(`Total ${field.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())} updated: ${count}`);
        }
        await logInfo('Update totals', updateTotals);
    } catch (err) {
        await logError('Critical error in updateDatabase', { error: { message: err.message, stack: err.stack } });
    }
    return updateTotals; // Return for main execution
}

// Run the script with top-level error handling
(async () => {
    try {
        await logInfo('Script execution started', { args: process.argv });
        const totals = await updateDatabase();
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