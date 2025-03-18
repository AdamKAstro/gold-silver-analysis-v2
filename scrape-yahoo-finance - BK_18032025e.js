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
        const sql = 'INSERT OR REPLACE INTO company_urls (company_id, url_type, url, last_validated) VALUES ((SELECT company_id FROM companies WHERE tsx_code = ?), ?, ?, ?)';
        const params = [ticker, urlType, url, now()];
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
            'incomeStatementHistoryQuarterly',
            'defaultKeyStatistics'
        ];
        const result = await yahooFinance.quoteSummary(ticker, { 
            modules,
            validateResult: false
        });

        await logInfo('Received raw API response', { 
            ticker, 
            responseSize: JSON.stringify(result).length, 
            modules: Object.keys(result).filter(k => result[k] && Object.keys(result[k]).length > 0),
            financialDataRaw: {
                totalRevenue: result.financialData?.totalRevenue,
                grossProfits: result.financialData?.grossProfits,
                operatingIncome: result.financialData?.operatingIncome,
                operatingCashflow: result.financialData?.operatingCashflow
            },
            incomeStmtRaw: result.incomeStatementHistory?.incomeStatementHistory[0] ? {
                endDate: result.incomeStatementHistory.incomeStatementHistory[0].endDate,
                totalRevenue: result.incomeStatementHistory.incomeStatementHistory[0].totalRevenue,
                costOfRevenue: result.incomeStatementHistory.incomeStatementHistory[0].costOfRevenue,
                grossProfit: result.incomeStatementHistory.incomeStatementHistory[0].grossProfit,
                totalOperatingExpenses: result.incomeStatementHistory.incomeStatementHistory[0].totalOperatingExpenses,
                operatingIncome: result.incomeStatementHistory.incomeStatementHistory[0].operatingIncome
            } : null,
            quarterlyStmtRaw: result.incomeStatementHistoryQuarterly?.incomeStatementHistory.map(stmt => ({
                endDate: stmt.endDate,
                totalRevenue: stmt.totalRevenue,
                costOfRevenue: stmt.costOfRevenue,
                grossProfit: stmt.grossProfit,
                totalOperatingExpenses: stmt.totalOperatingExpenses,
                operatingIncome: stmt.operatingIncome
            }))
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

        const currency = result.financialData?.financialCurrency || 
                        result.price?.currency || 
                        result.summaryDetail?.currency || 
                        'CAD';
        const currentTime = now();

        // Financials table
        const incomeStmt = result.incomeStatementHistory?.incomeStatementHistory[0] || {};
        const quarterlyStmts = result.incomeStatementHistoryQuarterly?.incomeStatementHistory || [];
        const balanceSheet = result.balanceSheetHistory?.balanceSheetStatements[0] || {};

        // Aggregate TTM data from quarterly statements (last 4 quarters)
        let ttmRevenue = 0, ttmCostOfRevenue = 0, ttmGrossProfit = 0, ttmOperatingExpenses = 0, ttmOperatingIncome = 0;
        let ttmDataAvailable = false;
        if (quarterlyStmts.length >= 4) {
            const lastFourQuarters = quarterlyStmts.slice(0, 4);
            ttmDataAvailable = true;
            lastFourQuarters.forEach(stmt => {
                ttmRevenue += stmt.totalRevenue || 0;
                ttmCostOfRevenue += stmt.costOfRevenue || 0;
                ttmGrossProfit += stmt.grossProfit || 0;
                ttmOperatingExpenses += stmt.totalOperatingExpenses || 0;
                ttmOperatingIncome += stmt.operatingIncome || 0;
            });
        }

        await logInfo('TTM data aggregation', { 
            ticker,
            ttmDataAvailable,
            ttmRevenue,
            ttmCostOfRevenue,
            ttmGrossProfit,
            ttmOperatingExpenses,
            ttmOperatingIncome
        });

        data.financials.cash_value = result.financialData?.totalCash || balanceSheet.cash || null;
        data.financials.cash_currency = currency;
        data.financials.cash_date = balanceSheet.endDate ? new Date(balanceSheet.endDate * 1000).toISOString() : null;
        data.financials.investments_json = balanceSheet.shortTermInvestments ? JSON.stringify({ shortTermInvestments: balanceSheet.shortTermInvestments }) : null;
        data.financials.hedgebook = null;
        data.financials.liabilities = result.financialData?.totalDebt || balanceSheet.totalLiab || null;
        data.financials.liabilities_currency = currency;
        data.financials.debt_value = data.financials.liabilities;
        data.financials.debt_currency = currency;
        data.financials.other_financial_assets = balanceSheet.shortTermInvestments || balanceSheet.otherCurrentAssets || null;
        data.financials.other_financial_assets_currency = currency;
        data.financials.market_cap_value = result.price?.marketCap || result.summaryDetail?.marketCap || result.defaultKeyStatistics?.marketCap || null;
        data.financials.market_cap_currency = currency;
        data.financials.enterprise_value_value = result.defaultKeyStatistics?.enterpriseValue || 
                                                (data.financials.market_cap_value && data.financials.debt_value && data.financials.cash_value ? 
                                                 data.financials.market_cap_value + data.financials.debt_value - data.financials.cash_value : null);
        data.financials.enterprise_value_currency = currency;
        data.financials.net_financial_assets = (data.financials.cash_value && data.financials.liabilities) ? 
                                              (data.financials.cash_value - data.financials.liabilities) : null;
        data.financials.net_financial_assets_currency = currency;

        // Enhanced revenue and cost metrics
        data.financials.revenue_value = result.financialData?.totalRevenue || incomeStmt.totalRevenue || (ttmDataAvailable ? ttmRevenue : null);
        data.financials.revenue_currency = currency;
        data.financials.cost_of_revenue = incomeStmt.costOfRevenue !== undefined ? incomeStmt.costOfRevenue : 
                                        (ttmDataAvailable && ttmCostOfRevenue !== 0 ? ttmCostOfRevenue : 
                                         (result.financialData?.totalRevenue && result.financialData?.grossProfits ? 
                                          result.financialData.totalRevenue - result.financialData.grossProfits : null));
        data.financials.gross_profit = incomeStmt.grossProfit !== undefined ? incomeStmt.grossProfit : 
                                      (ttmDataAvailable && ttmGrossProfit !== 0 ? ttmGrossProfit : 
                                       result.financialData?.grossProfits || 
                                       (data.financials.revenue_value && data.financials.cost_of_revenue ? 
                                        data.financials.revenue_value - data.financials.cost_of_revenue : null));
        data.financials.operating_expense = incomeStmt.totalOperatingExpenses !== undefined ? incomeStmt.totalOperatingExpenses : 
                                          (ttmDataAvailable && ttmOperatingExpenses !== 0 ? ttmOperatingExpenses : 
                                           (incomeStmt.sellingGeneralAdministrative && incomeStmt.researchDevelopment ? 
                                            incomeStmt.sellingGeneralAdministrative + incomeStmt.researchDevelopment : 
                                            (result.financialData?.operatingCashflow ? Math.abs(result.financialData.operatingCashflow) : null))); // Fixed proxy
        data.financials.operating_income = incomeStmt.operatingIncome || 
                                          (ttmDataAvailable && ttmOperatingIncome !== 0 ? ttmOperatingIncome : 
                                           result.financialData?.operatingIncome || 
                                           (data.financials.gross_profit && data.financials.operating_expense ? 
                                            data.financials.gross_profit - data.financials.operating_expense : null));
        data.financials.net_income_value = result.financialData?.netIncomeToCommon || 
                                          incomeStmt.netIncome || 
                                          (ttmDataAvailable ? lastFourQuarters.reduce((sum, stmt) => sum + (stmt.netIncome || 0), 0) : null) || 
                                          result.defaultKeyStatistics?.netIncomeToCommon || null;
        data.financials.net_income_currency = currency;
        data.financials.ebitda = result.financialData?.ebitda || 
                                (data.financials.operating_income && incomeStmt.depreciation ? 
                                 data.financials.operating_income + incomeStmt.depreciation : null);
        data.financials.free_cash_flow = result.financialData?.freeCashflow || null;
        data.financials.shares_outstanding = result.summaryDetail?.sharesOutstanding || 
                                            result.defaultKeyStatistics?.sharesOutstanding || 
                                            result.price?.sharesOutstanding || null;

        const epsActual = result.defaultKeyStatistics?.trailingEps || 
                         (data.financials.net_income_value && data.financials.shares_outstanding ? 
                          data.financials.net_income_value / data.financials.shares_outstanding : null);
        const epsGrowth = result.defaultKeyStatistics?.forwardEps && epsActual ? 
                         (result.defaultKeyStatistics.forwardEps - epsActual) / epsActual : null;

        data.financials.trailing_pe = result.summaryDetail?.trailingPE || 
                                     (result.price?.regularMarketPrice && epsActual ? 
                                      result.price.regularMarketPrice / epsActual : null);
        data.financials.forward_pe = result.summaryDetail?.forwardPE || result.defaultKeyStatistics?.forwardPE || null;
        data.financials.peg_ratio = result.defaultKeyStatistics?.pegRatio || 
                                   (data.financials.trailing_pe && epsGrowth ? 
                                    data.financials.trailing_pe / (epsGrowth * 100) : null);
        data.financials.price_to_sales = result.summaryDetail?.priceToSalesTrailing12Months || 
                                        (data.financials.market_cap_value && data.financials.revenue_value ? 
                                         data.financials.market_cap_value / data.financials.revenue_value : null);
        data.financials.price_to_book = result.defaultKeyStatistics?.priceToBook || 
                                       (data.financials.market_cap_value && balanceSheet.totalStockholderEquity ? 
                                        data.financials.market_cap_value / balanceSheet.totalStockholderEquity : null);
        data.financials.enterprise_to_revenue = result.defaultKeyStatistics?.enterpriseToRevenue || 
                                               (data.financials.enterprise_value_value && data.financials.revenue_value ? 
                                                data.financials.enterprise_value_value / data.financials.revenue_value : null);
        data.financials.enterprise_to_ebitda = result.defaultKeyStatistics?.enterpriseToEbitda || 
                                              (data.financials.enterprise_value_value && data.financials.ebitda ? 
                                               data.financials.enterprise_value_value / data.financials.ebitda : null);

        await logInfo('Financial metrics computed', { 
            ticker,
            revenue_value: { rawFinancial: result.financialData?.totalRevenue, rawIncome: incomeStmt.totalRevenue, ttm: ttmRevenue, final: data.financials.revenue_value },
            cost_of_revenue: { rawIncome: incomeStmt.costOfRevenue, ttm: ttmCostOfRevenue, computed: data.financials.cost_of_revenue },
            gross_profit: { rawIncome: incomeStmt.grossProfit, ttm: ttmGrossProfit, rawFinancial: result.financialData?.grossProfits, computed: data.financials.gross_profit },
            operating_expense: { rawIncome: incomeStmt.totalOperatingExpenses, ttm: ttmOperatingExpenses, proxy: result.financialData?.operatingCashflow, computed: data.financials.operating_expense },
            operating_income: { rawIncome: incomeStmt.operatingIncome, ttm: ttmOperatingIncome, rawFinancial: result.financialData?.operatingIncome, computed: data.financials.operating_income },
            price_to_sales: { raw: result.summaryDetail?.priceToSalesTrailing12Months, computed: data.financials.price_to_sales },
            price_to_book: { raw: result.defaultKeyStatistics?.priceToBook, computed: data.financials.price_to_book }
        });

        // Capital structure table
        data.capital_structure.existing_shares = data.financials.shares_outstanding;
        data.capital_structure.fully_diluted_shares = null;
        data.capital_structure.in_the_money_options = null;
        data.capital_structure.options_revenue = null;
        data.capital_structure.options_revenue_currency = null;
        data.capital_structure.last_updated = currentTime;

        // Stock prices table
        data.stock_prices.price_date = currentTime.split('T')[0];
        data.stock_prices.price_value = result.price?.regularMarketPrice || result.summaryDetail?.regularMarketPrice || 0;
        data.stock_prices.price_currency = currency;
        data.stock_prices.change_1yr_percent = result.summaryDetail?.fiftyTwoWeekChange || null;
        data.stock_prices.last_updated = currentTime;

        const nullFields = {
            financials: Object.entries(data.financials).filter(([key, value]) => value === null && key !== 'data_source' && !key.endsWith('_currency') && key !== 'cash_date').map(([key]) => key),
            capital_structure: Object.entries(data.capital_structure).filter(([key, value]) => value === null && key !== 'last_updated').map(([key]) => key),
            stock_prices: Object.entries(data.stock_prices).filter(([key, value]) => value === null && key !== 'last_updated').map(([key]) => key)
        };
        if (Object.values(nullFields).some(fields => fields.length > 0)) {
            await logInfo('Fields remaining null', { ticker, nullFields });
        }

        const isDataEmpty = Object.values(data.financials).every(val => val === null || val === 'Yahoo Finance' || val === currency) &&
                           Object.values(data.capital_structure).every(val => val === null || val === currentTime) &&
                           Object.values(data.stock_prices).every(val => val === null || val === currentTime || val === currency || val === 0);
        if (isDataEmpty) {
            await logError('All parsed values are null or default', { ticker, data });
            return null;
        }

        await insertValidatedUrl(ticker, 'yahoo_finance', `https://finance.yahoo.com/quote/${ticker}/`);
        return data;
    } catch (error) {
        await logError('API fetch failed', { 
            ticker, 
            attempt, 
            maxRetries: MAX_RETRIES, 
            error: { message: error.message, stack: error.stack, response: error.response?.data }
        });
        if (attempt < MAX_RETRIES && (error.response?.status === 429 || error.message.includes('timeout'))) {
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
    const updateTotals = {
        cash_value: 0, market_cap_value: 0, enterprise_value_value: 0, trailing_pe: 0, forward_pe: 0,
        peg_ratio: 0, price_to_sales: 0, price_to_book: 0, enterprise_to_revenue: 0, enterprise_to_ebitda: 0,
        revenue_value: 0, cost_of_revenue: 0, gross_profit: 0, operating_expense: 0, operating_income: 0,
        net_income_value: 0, ebitda: 0, debt_value: 0, shares_outstanding: 0, free_cash_flow: 0,
        liabilities: 0, net_financial_assets: 0
    };

    try {
        const companies = await new Promise((resolve, reject) => {
            db.all('SELECT company_id, tsx_code, company_name FROM companies', (err, rows) => {
                if (err) reject(new Error(`Database query failed: ${err.message}`));
                else resolve(rows);
            });
        });

        const validCompanies = companies.filter(c => c.tsx_code && typeof c.tsx_code === 'string');
        await logInfo('Retrieved companies', { count: validCompanies.length });

        const tasks = validCompanies.map(company => limit(async () => {
            const { company_id, tsx_code, company_name } = company;
            const row = await new Promise((resolve, reject) => {
                db.get('SELECT last_updated FROM financials WHERE company_id = ?', [company_id], (err, row) => {
                    if (err) reject(new Error(`Financials query failed: ${err.message}`));
                    else resolve(row);
                });
            });

            const lastUpdated = row ? new Date(row.last_updated) : null;
            const updateNeeded = FORCE_UPDATE || !lastUpdated || lastUpdated < new Date(yesterday);

            if (!updateNeeded) {
                await logInfo('Skipping update - up-to-date', { tsx_code, lastUpdated: lastUpdated.toISOString() });
                return;
            }

            const data = await fetchYahooFinanceData(tsx_code);
            if (!data) {
                await logError('No data fetched - skipping update', { tsx_code });
                return;
            }

            // Financials table update with COALESCE
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
                    cash_value = COALESCE(excluded.cash_value, cash_value),
                    cash_currency = COALESCE(excluded.cash_currency, cash_currency),
                    cash_date = COALESCE(excluded.cash_date, cash_date),
                    investments_json = COALESCE(excluded.investments_json, investments_json),
                    hedgebook = COALESCE(excluded.hedgebook, hedgebook),
                    liabilities = COALESCE(excluded.liabilities, liabilities),
                    liabilities_currency = COALESCE(excluded.liabilities_currency, liabilities_currency),
                    other_financial_assets = COALESCE(excluded.other_financial_assets, other_financial_assets),
                    other_financial_assets_currency = COALESCE(excluded.other_financial_assets_currency, other_financial_assets_currency),
                    market_cap_value = COALESCE(excluded.market_cap_value, market_cap_value),
                    market_cap_currency = COALESCE(excluded.market_cap_currency, market_cap_currency),
                    enterprise_value_value = COALESCE(excluded.enterprise_value_value, enterprise_value_value),
                    enterprise_value_currency = COALESCE(excluded.enterprise_value_currency, enterprise_value_currency),
                    net_financial_assets = COALESCE(excluded.net_financial_assets, net_financial_assets),
                    net_financial_assets_currency = COALESCE(excluded.net_financial_assets_currency, net_financial_assets_currency),
                    trailing_pe = COALESCE(excluded.trailing_pe, trailing_pe),
                    forward_pe = COALESCE(excluded.forward_pe, forward_pe),
                    peg_ratio = COALESCE(excluded.peg_ratio, peg_ratio),
                    price_to_sales = COALESCE(excluded.price_to_sales, price_to_sales),
                    price_to_book = COALESCE(excluded.price_to_book, price_to_book),
                    enterprise_to_revenue = COALESCE(excluded.enterprise_to_revenue, enterprise_to_revenue),
                    enterprise_to_ebitda = COALESCE(excluded.enterprise_to_ebitda, enterprise_to_ebitda),
                    revenue_value = COALESCE(excluded.revenue_value, revenue_value),
                    revenue_currency = COALESCE(excluded.revenue_currency, revenue_currency),
                    cost_of_revenue = COALESCE(excluded.cost_of_revenue, cost_of_revenue),
                    gross_profit = COALESCE(excluded.gross_profit, gross_profit),
                    operating_expense = COALESCE(excluded.operating_expense, operating_expense),
                    operating_income = COALESCE(excluded.operating_income, operating_income),
                    net_income_value = COALESCE(excluded.net_income_value, net_income_value),
                    net_income_currency = COALESCE(excluded.net_income_currency, net_income_currency),
                    ebitda = COALESCE(excluded.ebitda, ebitda),
                    debt_value = COALESCE(excluded.debt_value, debt_value),
                    debt_currency = COALESCE(excluded.debt_currency, debt_currency),
                    shares_outstanding = COALESCE(excluded.shares_outstanding, shares_outstanding),
                    free_cash_flow = COALESCE(excluded.free_cash_flow, free_cash_flow),
                    last_updated = excluded.last_updated,
                    data_source = excluded.data_source
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
            await new Promise((resolve, reject) => {
                db.run(financialsSql, financialsParams, (err) => {
                    if (err) reject(new Error(`Financials insert/update failed: ${err.message}`));
                    else resolve();
                });
            });
            await logInfo('Financials table updated', { tsx_code });

            // Update totals
            for (const [key, value] of Object.entries(data.financials)) {
                if (value !== null && key in updateTotals) updateTotals[key]++;
            }

            // Capital structure table update
            data.capital_structure.company_id = company_id;
            const capitalSql = `
                INSERT INTO capital_structure (
                    company_id, existing_shares, fully_diluted_shares, in_the_money_options, 
                    options_revenue, options_revenue_currency, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_id) DO UPDATE SET
                    existing_shares = COALESCE(excluded.existing_shares, existing_shares),
                    fully_diluted_shares = COALESCE(excluded.fully_diluted_shares, fully_diluted_shares),
                    in_the_money_options = COALESCE(excluded.in_the_money_options, in_the_money_options),
                    options_revenue = COALESCE(excluded.options_revenue, options_revenue),
                    options_revenue_currency = COALESCE(excluded.options_revenue_currency, options_revenue_currency),
                    last_updated = excluded.last_updated
            `;
            const capitalParams = [
                data.capital_structure.company_id, data.capital_structure.existing_shares, 
                data.capital_structure.fully_diluted_shares, data.capital_structure.in_the_money_options, 
                data.capital_structure.options_revenue, data.capital_structure.options_revenue_currency, 
                currentTime
            ];
            await new Promise((resolve, reject) => {
                db.run(capitalSql, capitalParams, (err) => {
                    if (err) reject(new Error(`Capital structure insert/update failed: ${err.message}`));
                    else resolve();
                });
            });

            // Stock prices table update
            data.stock_prices.company_id = company_id;
            const pricesSql = `
                INSERT INTO stock_prices (
                    company_id, price_date, price_value, price_currency, change_1yr_percent, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_id, price_date) DO UPDATE SET
                    price_value = COALESCE(excluded.price_value, price_value),
                    price_currency = COALESCE(excluded.price_currency, price_currency),
                    change_1yr_percent = COALESCE(excluded.change_1yr_percent, change_1yr_percent),
                    last_updated = excluded.last_updated
            `;
            const pricesParams = [
                data.stock_prices.company_id, data.stock_prices.price_date, data.stock_prices.price_value, 
                data.stock_prices.price_currency, data.stock_prices.change_1yr_percent, currentTime
            ];
            await new Promise((resolve, reject) => {
                db.run(pricesSql, pricesParams, (err) => {
                    if (err) reject(new Error(`Stock prices insert/update failed: ${err.message}`));
                    else resolve();
                });
            });

            // Update companies table
            await new Promise((resolve, reject) => {
                db.run('UPDATE companies SET last_updated = ? WHERE company_id = ?', [currentTime, company_id], (err) => {
                    if (err) reject(new Error(`Companies update failed: ${err.message}`));
                    else resolve();
                });
            });
        }));

        await Promise.all(tasks);
        await logInfo('All updates completed successfully');
        console.log('\n--- Update Totals ---');
        for (const [field, count] of Object.entries(updateTotals)) {
            console.log(`Total ${field.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())} updated: ${count}`);
        }
        await logInfo('Update totals', updateTotals);
    } catch (err) {
        await logError('Critical error in updateDatabase', { error: { message: err.message, stack: err.stack } });
    }
}

// Run the script
(async () => {
    try {
        await logInfo('Script execution started', { args: process.argv });
        await updateDatabase();
        await logInfo('Script execution completed');
    } catch (err) {
        await logError('Script execution failed', { error: { message: err.message, stack: err.stack } });
    } finally {
        isDbClosed = true;
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