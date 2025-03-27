require('dotenv').config();
const yahooFinance = require('yahoo-finance2').default;
const axios = require('axios');
const cheerio = require('cheerio');
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const path = require('path');
const fs = require('fs');
const util = require('util');
const pLimit = require('p-limit').default;
const cron = require('node-cron');

// --- Configuration ---
const DB_FILE = path.resolve(__dirname, 'mining_companies.db');
const LOG_DIR = path.resolve(__dirname, 'logs');
const LOCK_FILE = path.resolve(__dirname, 'daily_updater.lock');

try {
    if (!fs.existsSync(LOG_DIR)) {
        fs.mkdirSync(LOG_DIR, { recursive: true });
    }
} catch (err) {
    console.error(`Error creating log directory: ${LOG_DIR}`, err);
    process.exit(1);
}

const CONCURRENT_FETCH_LIMIT = 5;
const fetchLimiter = pLimit(CONCURRENT_FETCH_LIMIT);
const PRICE_CHANGE_WARN_THRESHOLD = 0.25;
const MCAP_CHANGE_WARN_THRESHOLD = 0.20;
const SOURCE_DISCREPANCY_WARN_THRESHOLD = 0.10;
const RETRY_COUNT = 3;
const RETRY_DELAY_MS = 1500;
const CRON_SCHEDULE = '5 3 * * *';
const CRON_TIMEZONE = "America/Toronto";
const FETCH_TIMEOUT_MS = 20000; // 20 seconds timeout for API calls
const LOG_PROGRESS_INTERVAL = 25; // Log progress every 25 companies
const ENABLE_GOOGLE_VERIFICATION = process.env.ENABLE_GOOGLE_VERIFICATION === 'true'; // Control Google scraping

let isShuttingDown = false;
let isProcessing = false;
let exchangeRatesCache = {};
let db;

// --- Logger Setup ---
const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info', // Change to 'debug' for more verbose output
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }), // Added milliseconds
        winston.format.errors({ stack: true }),
        winston.format.printf(({ timestamp, level, message, stack, ticker }) => // Added ticker context
            `${timestamp} [${level.toUpperCase()}]${ticker ? ` [${ticker}]` : ''}: ${stack || message}`
        )
    ),
    transports: [
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.printf(({ timestamp, level, message, ticker }) => `${timestamp} [${level}]${ticker ? ` [${ticker}]` : ''}: ${message}`)
            )
        }),
        new winston.transports.File({ filename: path.join(LOG_DIR, 'daily_update.log'), maxsize: 5242880, maxFiles: 3, tailable: true }),
        new winston.transports.File({ filename: path.join(LOG_DIR, 'daily_update_errors.log'), level: 'error', maxsize: 5242880, maxFiles: 3, tailable: true })
    ],
    exceptionHandlers: [new winston.transports.File({ filename: path.join(LOG_DIR, 'exceptions.log') })],
    rejectionHandlers: [new winston.transports.File({ filename: path.join(LOG_DIR, 'rejections.log') })]
});

// Function to create a child logger with ticker context
const createTickerLogger = (ticker) => logger.child({ ticker });

// --- Database Setup & Promisification ---
// ... (connectDb, dbRun, dbGet, dbAll remain the same) ...
async function connectDb() {
    return new Promise((resolve, reject) => {
        if (db) {
           logger.debug("Reusing existing DB connection.");
           return resolve(db);
        }
        logger.debug("Attempting to open DB connection...");
        try {
            if (!fs.existsSync(DB_FILE)) {
                return reject(new Error(`Database file not found at ${DB_FILE}`));
            }
            const newDb = new sqlite3.Database(DB_FILE, sqlite3.OPEN_READWRITE, (err) => {
                if (err) {
                    logger.error(`Failed to connect to database: ${err.message}`, { stack: err.stack });
                    return reject(err);
                }
                logger.info(`Connected to SQLite database: ${DB_FILE}`);
                db = newDb; // Store the connection globally
                resolve(db);
            });
        } catch (err) {
            logger.error(`Failed to initialize sqlite3 Database: ${err.message}`, { stack: err.stack });
            reject(err);
        }
    });
}
async function dbRun(sql, params = []) {
    const localLogger = params && params.length > 0 && typeof params[0] === 'string' ? createTickerLogger(params[0]) : logger; // Hacky way to get ticker sometimes
    localLogger.debug(`Executing DB Run: ${sql.substring(0,100)}... Params: ${JSON.stringify(params).substring(0,100)}`);
    await connectDb();
    return util.promisify(db.run.bind(db))(sql, params);
}
async function dbGet(sql, params = []) {
    const localLogger = params && params.length > 0 && typeof params[0] === 'string' ? createTickerLogger(params[0]) : logger;
    localLogger.debug(`Executing DB Get: ${sql.substring(0,100)}... Params: ${JSON.stringify(params).substring(0,100)}`);
    await connectDb();
    return util.promisify(db.get.bind(db))(sql, params);
}
async function dbAll(sql, params = []) {
    const localLogger = params && params.length > 0 && typeof params[0] === 'string' ? createTickerLogger(params[0]) : logger;
     localLogger.debug(`Executing DB All: ${sql.substring(0,100)}... Params: ${JSON.stringify(params).substring(0,100)}`);
    await connectDb();
    return util.promisify(db.all.bind(db))(sql, params);
}


// --- Utility Functions (delay, sanitize, parse, retry) ---
// ... (retryOperation slightly adjusted to use ticker logger) ...
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function sanitizeFiniteNumber(value) { /* ... as before ... */ if (value === null || value === undefined) return null; let num; if (typeof value === 'number') { num = value; } else if (typeof value === 'string') { const cleaned = value.replace(/[, ]/g, ''); if (cleaned === '' || cleaned === '-' || cleaned === '.') return null; num = parseFloat(cleaned); } else if (typeof value === 'object' && value !== null && 'raw' in value) { return sanitizeFiniteNumber(value.raw); } else { return null; } return Number.isFinite(num) ? num : null; }
function parseFinancialString(value) { /* ... as before ... */ if (typeof value !== 'string' || !value) return null; const cleaned = value.replace(/[^0-9.TBMK-]/g, ''); const multiplier = cleaned.match(/[TBMK]/i); let num = parseFloat(cleaned.replace(/[TBMK]/i, '')); if (isNaN(num)) return null; if (multiplier) { switch (multiplier[0].toUpperCase()) { case 'T': num *= 1e12; break; case 'B': num *= 1e9; break; case 'M': num *= 1e6; break; case 'K': num *= 1e3; break; } } return sanitizeFiniteNumber(num); }

async function retryOperation(fn, operationName, ticker, retries = RETRY_COUNT, baseDelay = RETRY_DELAY_MS) {
    const tickerLogger = createTickerLogger(ticker); // Use ticker-specific logger
     for (let i = 0; i <= retries; i++) {
        if (isShuttingDown) throw new Error(`Operation ${operationName} aborted due to shutdown.`);
        try {
            tickerLogger.debug(`Attempting ${operationName}, try ${i+1}/${retries+1}`);
            const result = await fn();
            tickerLogger.debug(`${operationName} successful.`);
            return result;
        } catch (e) {
            const statusCode = e?.response?.status;
            const isHttpClientError = statusCode >= 400 && statusCode < 500;

            if (i === retries || isHttpClientError) {
                const reason = isHttpClientError ? `Client Error ${statusCode}` : `Max retries reached`;
                tickerLogger.error(`Failed ${operationName} (${reason}): ${e.message}`);
                if (!isHttpClientError || statusCode !== 404) {
                     tickerLogger.debug(`${operationName} Error Stack:`, { stack: e.stack });
                }
                return null; // Return null on final failure
            }

            const delayMs = baseDelay * Math.pow(2, i) + Math.random() * baseDelay;
            tickerLogger.warn(`Error during ${operationName}: ${e.message}. Retry ${i + 1}/${retries} in ${Math.round(delayMs)}ms...`);
            await delay(delayMs);
        }
    }
    return null;
}

// --- Data Fetching Functions ---
async function fetchYahooQuote(ticker) {
    const tickerLogger = createTickerLogger(ticker);
    const operation = () => fetchLimiter(async () => {
        tickerLogger.debug(`Fetching Yahoo quote...`);
        const result = await yahooFinance.quote(ticker, {
             fields: ['regularMarketPrice', 'currency', 'marketCap']
        }, { validateResult: false });
         if (!result || result.regularMarketPrice === undefined || result.marketCap === undefined) {
            throw new Error(`Incomplete data received`); // Let retry handle it
        }
        return result;
    });
    return retryOperation(operation, 'fetchYahooQuote', ticker);
}

async function fetchGoogleVerification(ticker) {
    if (!ENABLE_GOOGLE_VERIFICATION) {
        logger.debug(`[${ticker}] Google verification disabled.`);
        return null;
    }
    const tickerLogger = createTickerLogger(ticker);
     const operation = async () => {
        // ... (URL construction logic as before) ...
        const safeTicker = ticker.replace('.', '-');
        let exchangeSuffix = '';
        if (ticker.endsWith('.TO')) exchangeSuffix = 'TSE';
        else if (ticker.endsWith('.V') || ticker.endsWith('.CN') || ticker.endsWith('.NE')) exchangeSuffix = 'CVE';
        else if (ticker.endsWith('.L')) exchangeSuffix = 'LON';
        else if (ticker.endsWith('.AX')) exchangeSuffix = 'ASX';
        const url = `https://www.google.com/finance/quote/${safeTicker.split('.')[0]}${exchangeSuffix ? ':' + exchangeSuffix : ''}`;

        tickerLogger.debug(`Fetching Google verification from ${url}`);
        const { data } = await axios.get(url, {
            timeout: FETCH_TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36', 'Accept-Language': 'en-US,en;q=0.9', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8', 'Connection': 'keep-alive' }
        });
        const $ = cheerio.load(data);
        const priceText = $('div.YMlKec.fxKbKc').first().text().trim();
        const price = sanitizeFiniteNumber(priceText.replace(/[^0-9.-]/g, ''));
        let marketCapText = $('div[data-attrid="Market cap"]').find('div > div').first().text().trim();
        if (!marketCapText) { marketCapText = $('div.P6K39c:contains("Market cap")').prev().text().trim(); }
        const marketCap = parseFinancialString(marketCapText);
        if (price === null) {
             // fs.writeFileSync(`google_fail_${ticker}.html`, data); // Optional Debugging
             throw new Error('Google scrape failed to find price.'); }
        tickerLogger.debug(`Google fetched: price=${price}, market_cap=${marketCap}`);
        return { price, marketCap };
    };
    return retryOperation(operation, 'fetchGoogleVerification', ticker, 1, 500);
}

// --- Exchange Rate & Conversion Functions ---
// ... (loadExchangeRates, getExchangeRate, convertToUSD remain the same) ...
async function loadExchangeRates() { /* ... as before ... */ logger.info('Loading exchange rates from database...'); try { const rates = await dbAll('SELECT from_currency, to_currency, rate FROM exchange_rates'); exchangeRatesCache = rates.reduce((acc, row) => { if (!acc[row.from_currency]) { acc[row.from_currency] = {}; } acc[row.from_currency][row.to_currency] = row.rate; return acc; }, {}); logger.info(`Loaded ${rates.length} exchange rates into cache.`); if (!getExchangeRate('CAD','USD')) { logger.warn('CAD->USD rate missing, using fallback 0.73'); if(!exchangeRatesCache['CAD']) exchangeRatesCache['CAD'] = {}; exchangeRatesCache['CAD']['USD'] = 0.73; } if (!getExchangeRate('USD','CAD')) { logger.warn('USD->CAD rate missing, using fallback 1.37'); if(!exchangeRatesCache['USD']) exchangeRatesCache['USD'] = {}; exchangeRatesCache['USD']['CAD'] = 1.37; } if (!getExchangeRate('AUD','USD')) { logger.warn('AUD->USD rate missing, using fallback 0.66'); if(!exchangeRatesCache['AUD']) exchangeRatesCache['AUD'] = {}; exchangeRatesCache['AUD']['USD'] = 0.66; } } catch (err) { logger.error(`Failed to load exchange rates: ${err.message}`, { stack: err.stack }); exchangeRatesCache = { CAD: { USD: 0.73 }, USD: { CAD: 1.37 }, AUD: { USD: 0.66 } }; logger.warn('Using fallback exchange rates due to DB load error.'); } }
function getExchangeRate(fromCurrency, toCurrency) { if (!fromCurrency || !toCurrency) return null; if (fromCurrency === toCurrency) return 1.0; return exchangeRatesCache[fromCurrency]?.[toCurrency] || null; }
function convertToUSD(value, currency, operationName = 'Conversion', localLogger = logger) { const numericValue = sanitizeFiniteNumber(value); if (numericValue === null) return null; if (!currency) { localLogger.warn(`Missing currency for value ${numericValue} during ${operationName}. Assuming USD.`); currency = 'USD'; } currency = currency.toUpperCase(); if (currency === 'USD') return numericValue; const rate = getExchangeRate(currency, 'USD'); if (rate === null) { localLogger.error(`Cannot convert ${currency} to USD for ${operationName}: Exchange rate not found/loaded.`); return null; } return numericValue * rate; }


// --- Database Update Functions ---
async function updateStockPrice(companyId, ticker, fetchedPrice, fetchedCurrency) {
    const tickerLogger = createTickerLogger(ticker);
    const priceDateStr = new Date().toISOString().split('T')[0];
    const price = sanitizeFiniteNumber(fetchedPrice);
    const currency = fetchedCurrency?.toUpperCase() || 'USD';

    if (price === null || price <= 0) {
        tickerLogger.warn(`Skipping stock price update due to invalid fetched price: ${fetchedPrice}`);
        return false;
    }

    tickerLogger.debug(`Attempting stock price update for ${priceDateStr}: ${price} ${currency}`);
    try {
        const existingToday = await dbGet(
            'SELECT price_id FROM stock_prices WHERE company_id = ? AND date(price_date) = ?',
            [companyId, priceDateStr]
        );
        if (existingToday) {
            tickerLogger.debug(`Stock price for ${priceDateStr} already exists.`);
            return true;
        }

        const latestExisting = await dbGet(
            'SELECT price_value, price_currency, date(price_date) as date FROM stock_prices WHERE company_id = ? ORDER BY price_date DESC LIMIT 1',
            [companyId]
        );

        if (latestExisting?.price_value > 0 && latestExisting.date !== priceDateStr) {
             const latestExistingUSD = convertToUSD(latestExisting.price_value, latestExisting.price_currency, 'Threshold Check', tickerLogger);
             const priceUSD = convertToUSD(price, currency, 'Threshold Check', tickerLogger);

             if(latestExistingUSD !== null && priceUSD !== null) {
                const variance = Math.abs(priceUSD - latestExistingUSD) / latestExistingUSD;
                if (variance > PRICE_CHANGE_WARN_THRESHOLD) {
                    tickerLogger.warn(`Price change > ${PRICE_CHANGE_WARN_THRESHOLD * 100}% (USD Eq: ${latestExistingUSD.toFixed(2)} -> ${priceUSD.toFixed(2)}). Skipping update for ${priceDateStr}.`);
                    return false;
                }
             } else {
                 tickerLogger.warn(`Could not compare price change threshold due to currency conversion issue.`);
             }
        }

        await dbRun(
            'INSERT INTO stock_prices (company_id, price_date, price_value, price_currency, last_updated) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)',
            [companyId, priceDateStr, price, currency]
        );
        tickerLogger.info(`Inserted stock price for ${priceDateStr}: ${price} ${currency}`);
        return true;

    } catch (err) {
        tickerLogger.error(`Error updating stock_prices: ${err.message}`, { stack: err.stack });
        return false;
    }
}

async function updateFinancialMarketData(companyId, ticker, fetchedMCap, fetchedCurrency) {
    const tickerLogger = createTickerLogger(ticker);
    const marketCap = sanitizeFiniteNumber(fetchedMCap);
    const currency = fetchedCurrency?.toUpperCase() || 'USD';
    const now = new Date().toISOString();

    if (marketCap === null || marketCap <= 0) {
        tickerLogger.warn(`Skipping financial market data update due to invalid market cap: ${fetchedMCap}`);
        return false;
    }
    tickerLogger.debug(`Attempting financial market update: MCap=${marketCap} ${currency}`);
    try {
        const currentFinancials = await dbGet(
            `SELECT market_cap_value, market_cap_currency, cash_value, cash_currency,
                    debt_value, debt_currency, liabilities, liabilities_currency
             FROM financials WHERE company_id = ?`,
            [companyId]
        );

        if (!currentFinancials) {
            tickerLogger.warn(`No existing financial record found. Cannot update market data.`);
            return false;
        }

        // Check Market Cap Threshold (Compare in USD)
        if (currentFinancials.market_cap_value && currentFinancials.market_cap_value > 0) {
            const currentMCapUSD = convertToUSD(currentFinancials.market_cap_value, currentFinancials.market_cap_currency, 'MCap Threshold', tickerLogger);
            const fetchedMCapUSD = convertToUSD(marketCap, currency, 'MCap Threshold', tickerLogger);

            if (currentMCapUSD !== null && fetchedMCapUSD !== null) {
                const variance = Math.abs(fetchedMCapUSD - currentMCapUSD) / currentMCapUSD;
                if (variance > MCAP_CHANGE_WARN_THRESHOLD) {
                    tickerLogger.warn(`Market Cap change > ${MCAP_CHANGE_WARN_THRESHOLD * 100}% (USD Eq: ${currentMCapUSD.toFixed(0)} -> ${fetchedMCapUSD.toFixed(0)}). Skipping update.`);
                    return false;
                }
            } else {
                tickerLogger.warn(`Could not compare MCap threshold due to currency conversion issue.`);
            }
        }

        // Calculate new Enterprise Value in USD
        const cashUSD = convertToUSD(currentFinancials.cash_value, currentFinancials.cash_currency, 'EV Calc', tickerLogger) || 0;
        const debtValue = sanitizeFiniteNumber(currentFinancials.debt_value);
        const liabValue = sanitizeFiniteNumber(currentFinancials.liabilities);
        const debtToUse = (debtValue !== null && debtValue > 0) ? debtValue : (liabValue !== null && liabValue > 0 ? liabValue : 0);
        const debtCurrency = (debtValue !== null && debtValue > 0) ? currentFinancials.debt_currency : currentFinancials.liabilities_currency;
        const debtUSD = convertToUSD(debtToUse, debtCurrency, 'EV Calc', tickerLogger) || 0;
        const fetchedMCapUSD = convertToUSD(marketCap, currency, 'EV Calc', tickerLogger);

        let enterpriseValue = null;
        let evCurrency = null;

        if (fetchedMCapUSD !== null) {
            enterpriseValue = fetchedMCapUSD + debtUSD - cashUSD;
            evCurrency = 'USD';
        } else {
            tickerLogger.warn(`Could not calculate EV because MCap USD conversion failed.`);
        }

        await dbRun(
            `UPDATE financials
             SET market_cap_value = ?,
                 market_cap_currency = ?,
                 enterprise_value_value = ?,
                 enterprise_value_currency = ?,
                 last_updated = ?
             WHERE company_id = ?`,
            [marketCap, currency, sanitizeFiniteNumber(enterpriseValue), evCurrency, now, companyId]
        );
        tickerLogger.info(`Updated financials market data: MCap=${marketCap} ${currency}, EV=${sanitizeFiniteNumber(enterpriseValue)} ${evCurrency}`);
        return true;

    } catch (err) {
        tickerLogger.error(`Error updating financials market data: ${err.message}`, { stack: err.stack });
        return false;
    }
}

async function recalculateAndUpdateValuationMetrics(companyId, ticker) {
    const tickerLogger = createTickerLogger(ticker);
    tickerLogger.debug(`Recalculating valuation metrics...`);
    try {
        const finData = await dbGet(`SELECT * FROM financials WHERE company_id = ?`, [companyId]);
        const estData = await dbGet(`SELECT * FROM mineral_estimates WHERE company_id = ?`, [companyId]);
        const prodData = await dbGet(`SELECT * FROM production WHERE company_id = ?`, [companyId]);

        if (!finData || finData.market_cap_value === null || finData.enterprise_value_value === null) {
            tickerLogger.warn(`Skipping valuation metrics update: Missing required financial data (MCap/EV).`);
            return;
        }

        const mCapUSD = convertToUSD(finData.market_cap_value, finData.market_cap_currency, 'Valuation MCap', tickerLogger);
        const evUSD = convertToUSD(finData.enterprise_value_value, finData.enterprise_value_currency, 'Valuation EV', tickerLogger);

        if (mCapUSD === null || evUSD === null) {
             tickerLogger.warn(`Skipping valuation metrics update: Failed to convert MCap/EV to USD.`);
             return;
        }

        const safeDivide = (numerator, denominator) => { /* ... as before ... */ const num = sanitizeFiniteNumber(numerator); const den = sanitizeFiniteNumber(denominator); return (num !== null && den !== null && den !== 0) ? num / den : null; };

        const metrics = { /* ... (all 19 metric calculations as before) ... */
            company_id: companyId,
            mkt_cap_per_reserve_oz_precious: safeDivide(mCapUSD, (estData?.reserves_precious_aueq_moz || 0) * 1e6),
            mkt_cap_per_mi_oz_precious: safeDivide(mCapUSD, (estData?.measured_indicated_precious_aueq_moz || 0) * 1e6),
            mkt_cap_per_resource_oz_precious: safeDivide(mCapUSD, (estData?.resources_precious_aueq_moz || 0) * 1e6),
            mkt_cap_per_mineable_oz_precious: safeDivide(mCapUSD, (estData?.mineable_precious_aueq_moz || 0) * 1e6),
            mkt_cap_per_reserve_oz_all: safeDivide(mCapUSD, (estData?.reserves_total_aueq_moz || 0) * 1e6),
            mkt_cap_per_mi_oz_all: safeDivide(mCapUSD, (estData?.measured_indicated_total_aueq_moz || 0) * 1e6),
            mkt_cap_per_resource_oz_all: safeDivide(mCapUSD, (estData?.resources_total_aueq_moz || 0) * 1e6),
            mkt_cap_per_mineable_oz_all: safeDivide(mCapUSD, (estData?.mineable_total_aueq_moz || 0) * 1e6),
            ev_per_reserve_oz_precious: safeDivide(evUSD, (estData?.reserves_precious_aueq_moz || 0) * 1e6),
            ev_per_mi_oz_precious: safeDivide(evUSD, (estData?.measured_indicated_precious_aueq_moz || 0) * 1e6),
            ev_per_resource_oz_precious: safeDivide(evUSD, (estData?.resources_precious_aueq_moz || 0) * 1e6),
            ev_per_mineable_oz_precious: safeDivide(evUSD, (estData?.mineable_precious_aueq_moz || 0) * 1e6),
            ev_per_reserve_oz_all: safeDivide(evUSD, (estData?.reserves_total_aueq_moz || 0) * 1e6),
            ev_per_mi_oz_all: safeDivide(evUSD, (estData?.measured_indicated_total_aueq_moz || 0) * 1e6),
            ev_per_resource_oz_all: safeDivide(evUSD, (estData?.resources_total_aueq_moz || 0) * 1e6),
            ev_per_mineable_oz_all: safeDivide(evUSD, (estData?.mineable_total_aueq_moz || 0) * 1e6),
            mkt_cap_per_production_oz: safeDivide(mCapUSD, (prodData?.current_production_total_aueq_koz || 0) * 1e3),
            ev_per_production_oz: safeDivide(evUSD, (prodData?.current_production_total_aueq_koz || 0) * 1e3),
            last_updated: new Date().toISOString()
        };

        const columns = Object.keys(metrics);
        const placeholders = columns.map(() => '?').join(',');
        const values = Object.values(metrics);
        const sql = `INSERT OR REPLACE INTO valuation_metrics (${columns.join(', ')}) VALUES (${placeholders})`;

        await dbRun(sql, values);
        tickerLogger.info(`Updated valuation_metrics.`);

    } catch (err) {
        tickerLogger.error(`Error recalculating/updating valuation_metrics: ${err.message}`, { stack: err.stack });
    }
}


// --- Main Processing Logic ---
async function processCompanyUpdate(company, index, total) {
    const { company_id: companyId, tsx_code: ticker } = company;
    const tickerLogger = createTickerLogger(ticker); // Use child logger

    if (isShuttingDown) {
        tickerLogger.info(`Skipping due to shutdown signal.`);
        return;
    }
    tickerLogger.info(`Processing ${index + 1}/${total} (ID: ${companyId})...`);

    let yahooQuote = null;
    let googleData = null;
    let primarySource = 'None';
    let price = null;
    let marketCap = null;
    let currency = null;
    let updateFinancialsSuccess = false;

    try {
        // Step 1: Fetch Primary (Yahoo)
        yahooQuote = await fetchYahooQuote(ticker); // Returns null on final failure
        if (yahooQuote) {
            price = sanitizeFiniteNumber(yahooQuote.regularMarketPrice);
            marketCap = sanitizeFiniteNumber(yahooQuote.marketCap);
            currency = yahooQuote.currency?.toUpperCase() || 'USD';
            primarySource = 'Yahoo Finance';
            tickerLogger.debug(`Yahoo OK: Price=${price}, MCap=${marketCap}, Cur=${currency}`);
        } else {
            tickerLogger.warn(`Yahoo fetch failed or returned incomplete data.`);
        }

        // Step 2: Fetch Verification (Google) - Optional/Fallback
        if (ENABLE_GOOGLE_VERIFICATION || primarySource === 'None') {
            googleData = await fetchGoogleVerification(ticker);
            if (googleData && googleData.price !== null && primarySource === 'None') {
                price = googleData.price;
                marketCap = googleData.marketCap;
                currency = 'USD'; // Assumption
                primarySource = 'Google Finance (Fallback)';
                tickerLogger.info(`Using Google as fallback. Price=${price}, MCap=${marketCap}`);
            }
        }

        // Step 3: Verify Source Discrepancy
        if (ENABLE_GOOGLE_VERIFICATION && yahooQuote && googleData?.price !== null && price !== null && primarySource === 'Yahoo Finance') {
             // ... (verification logic as before, using tickerLogger) ...
             const yahooPriceUSD = convertToUSD(sanitizeFiniteNumber(yahooQuote.regularMarketPrice), yahooQuote.currency, 'Source Verify', tickerLogger);
             const googlePriceUSD = convertToUSD(googleData.price, 'USD', 'Source Verify', tickerLogger); // Assume Google needs conversion
             if (yahooPriceUSD !== null && googlePriceUSD !== null && yahooPriceUSD > 0) {
                 const variance = Math.abs(yahooPriceUSD - googlePriceUSD) / yahooPriceUSD;
                 if (variance > SOURCE_DISCREPANCY_WARN_THRESHOLD) {
                    tickerLogger.warn(`Source Price Discrepancy > ${SOURCE_DISCREPANCY_WARN_THRESHOLD*100}%: Yahoo=${yahooPriceUSD.toFixed(2)}, Google=${googlePriceUSD.toFixed(2)} (USD Equiv). Using Yahoo.`);
                 }
             } else { tickerLogger.debug(`Could not verify source price discrepancy.`); }
        }

        // Step 4: Check data validity
        if (price === null && marketCap === null) {
            tickerLogger.error(`Skipping updates. No valid price or market cap found.`);
            return; // Skip this company
        }

        // Step 5: Update DB
        let priceUpdateSkipped = false;
        if (price !== null) {
            const priceUpdateSuccess = await updateStockPrice(companyId, ticker, price, currency);
            if (!priceUpdateSuccess) priceUpdateSkipped = true;
        } else {
            tickerLogger.warn(`Skipping price update as price is null.`);
            priceUpdateSkipped = true; // Treat null price as a reason to skip dependent updates
        }

        if (marketCap !== null) {
            if (!priceUpdateSkipped) {
                 updateFinancialsSuccess = await updateFinancialMarketData(companyId, ticker, marketCap, currency);
            } else {
                tickerLogger.warn(`Skipping financial market data update because stock price update was skipped or price was null.`);
            }
        } else {
             tickerLogger.warn(`Skipping financial market data update as marketCap is null.`);
        }

        // Step 6: Recalculate Valuations
        if (updateFinancialsSuccess) {
             await recalculateAndUpdateValuationMetrics(companyId, ticker);
        } else {
             tickerLogger.debug(`Skipping valuation metrics update as financial market data was not updated.`);
        }

        tickerLogger.info(`Processing finished using ${primarySource}.`);

    } catch (error) {
        tickerLogger.error(`Unhandled error during processing company: ${error.message}`, { stack: error.stack });
    } finally {
        // Log progress intermittently
        if ((index + 1) % LOG_PROGRESS_INTERVAL === 0 || (index + 1) === total) {
            logger.info(`--- Progress: ${index + 1} / ${total} companies processed ---`);
        }
    }
}

async function runDailyUpdates() {
    if (isProcessing) {
        logger.warn("Update process already running. Skipping this trigger.");
        return;
    }
    isProcessing = true;
    logger.info('Starting DAILY data update run...');
    const startTime = Date.now();

    try {
        await connectDb();
        await loadExchangeRates();

        let companies = [];
        try {
            companies = await dbAll(`SELECT company_id, tsx_code FROM companies WHERE status != ? AND tsx_code IS NOT NULL AND tsx_code != '' ORDER BY company_id`, ['delisted']);
            if (!companies.length) {
                logger.error('No active companies with valid tickers found.');
                isProcessing = false;
                return;
            }
            logger.info(`Found ${companies.length} active companies to process.`);
        } catch (dbErr) {
             logger.error(`Failed to fetch companies from DB: ${dbErr.message}`, { stack: dbErr.stack });
             isProcessing = false;
             return;
        }

        // Process companies concurrently
        const promises = companies.map((company, index) =>
            fetchLimiter(() => processCompanyUpdate(company, index, companies.length))
                .catch(err => logger.error(`[${company.tsx_code}] FATAL error in processCompanyUpdate promise: ${err.message}`, { stack: err.stack }))
        );
        await Promise.allSettled(promises);

    } catch (error) {
        logger.error(`Error during daily update setup/run: ${error.message}`, { stack: error.stack });
    } finally {
        const duration = (Date.now() - startTime) / 1000;
        logger.info(`Daily data update run finished in ${duration.toFixed(1)} seconds.`);
        isProcessing = false;
    }
}


// --- Lock File and Execution / Scheduling ---
// ... (main, cleanup, handleShutdown, global error handlers remain mostly the same, ensure they await cleanup) ...
async function main(runNow = false) {
    if (fs.existsSync(LOCK_FILE)) {
        const lockContent = fs.readFileSync(LOCK_FILE, 'utf8');
        logger.warn(`Lock file exists. Another instance might be running (Started: ${lockContent.split(': ')[1] || 'unknown'}). Exiting.`);
        if (db) await connectDb().then(d => d.close()).catch(e => logger.error(`Error closing DB on lock exit: ${e.message}`));
        db = null;
        return;
    }

    let lockFd;
    try {
        // Try to get an exclusive lock immediately
        lockFd = fs.openSync(LOCK_FILE, 'wx'); // 'x' flag fails if path exists
        fs.writeSync(lockFd, `Running since: ${new Date().toISOString()} PID: ${process.pid}`);
        fs.closeSync(lockFd);
        logger.info('Lock file created.');

        if (runNow) {
             logger.info('Executing initial run (--run-now or --once)...');
             await runDailyUpdates();
             logger.info('Initial run complete.');
        }
    } catch (err) {
         if (err.code === 'EEXIST') {
             logger.warn('Lock file appeared after initial check. Another instance likely running. Exiting.');
             if (db) await connectDb().then(d => d.close()).catch(e => logger.error(`Error closing DB on lock exit: ${e.message}`));
             db = null;
         } else {
            logger.error(`Critical error during main setup/lock acquisition: ${err.message}`, { stack: err.stack });
            await cleanup(); // Attempt cleanup
         }
         return; // Exit if lock failed
    }
    // Note: If not running immediately, cleanup is handled by shutdown signals or after cron job finishes
     if (!runOnce && !runNowAndSchedule) {
        await cleanup(); // Close DB if just scheduling and not running now
     }
}

async function cleanup() {
    logger.info('Running cleanup...');
    try {
        if (fs.existsSync(LOCK_FILE)) {
            // Verify PID if possible before deleting - more robust for stale locks
            // const lockContent = fs.readFileSync(LOCK_FILE, 'utf8');
            // const pidMatch = lockContent.match(/PID: (\d+)/);
            // if (!pidMatch || parseInt(pidMatch[1], 10) === process.pid) { // Only delete if it's our PID or no PID found
                fs.unlinkSync(LOCK_FILE);
                logger.info('Lock file removed.');
            // } else {
            //     logger.warn(`Lock file owned by different PID (${pidMatch[1]}). Not removing.`);
            // }
        }
    } catch (unlinkErr) {
        logger.error(`Error removing lock file: ${unlinkErr.message}`);
    }
     return new Promise((resolve) => {
        if (db) {
            logger.debug("Attempting to close DB connection...");
            db.close((err) => {
                if (err) logger.error(`Failed to close database: ${err.message}`);
                else logger.info('Database connection closed.');
                db = null;
                resolve();
            });
        } else {
             logger.info('Database connection already closed or never opened.');
             resolve();
        }
    });
}

const runOnce = process.argv.includes('--once');
const runNowAndSchedule = process.argv.includes('--run-now');
let activeCronTask = null;

if (runOnce) {
    logger.info('Running in --once mode.');
    main(true).catch(async (e) => { // Ensure main errors are caught
         logger.error(`Exiting due to error in --once mode: ${e.message}`, { stack: e.stack });
         process.exitCode = 1;
    }).finally(async () => {
        logger.info('--- Run Once Mode Finished ---');
        await cleanup();
    });
} else {
    // Connect DB initially for scheduled mode readiness
    connectDb().then(() => {
        logger.info(`Starting in scheduled mode. Cron job: "${CRON_SCHEDULE}" (${CRON_TIMEZONE}).`);
        if (runNowAndSchedule) {
            logger.info("`--run-now` specified: Performing initial run immediately before scheduling.");
            main(true).catch(e => logger.error(`Error during initial run in scheduled mode: ${e.message}`, { stack: e.stack }));
        } else {
            logger.info("Waiting for schedule. Use --run-now to execute immediately as well.");
        }

        activeCronTask = cron.schedule(CRON_SCHEDULE, async () => {
            logger.info(`Scheduled daily update run triggered by cron at ${new Date().toISOString()}`);
            if (!isProcessing && !fs.existsSync(LOCK_FILE)) {
                 await main(true); // Run the full process with lock check
            } else {
                 logger.warn("Skipping scheduled run: Lock file exists or previous run still processing.");
            }
        }, {
            scheduled: true,
            timezone: CRON_TIMEZONE
        });
        logger.info('Cron job scheduled. Process will keep running. Press Ctrl+C to exit.');
        // Keep script alive
        process.stdin.resume();
    }).catch(e => {
        logger.error(`Failed initial DB connection for scheduled mode: ${e.message}`);
        process.exit(1);
    });

    process.on('SIGINT', () => handleShutdown('SIGINT', activeCronTask));
    process.on('SIGTERM', () => handleShutdown('SIGTERM', activeCronTask));
}

async function handleShutdown(signal, task = null) { /* ... as before ... */ if (isShuttingDown) return; isShuttingDown = true; logger.info(`Received ${signal}. Shutting down gracefully...`); if (task) { task.stop(); logger.info('Stopped scheduled cron task.'); } await cleanup(); logger.info('Shutdown complete.'); process.exit(0); }
process.on('uncaughtException', async (err) => { /* ... as before ... */ logger.error(`UNCAUGHT EXCEPTION: ${err.message}`, { stack: err.stack }); if (!isShuttingDown) { await handleShutdown('uncaughtException', activeCronTask); } process.exit(1); });
process.on('unhandledRejection', async (reason, promise) => { /* ... as before ... */ logger.error('UNHANDLED REJECTION:', { reason: reason?.message || reason, stack: reason?.stack }); if (!isShuttingDown) { await handleShutdown('unhandledRejection', activeCronTask); } process.exit(1); });