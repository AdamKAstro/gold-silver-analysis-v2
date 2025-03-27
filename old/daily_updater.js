require('dotenv').config();
const yahooFinance = require('yahoo-finance2').default;
const axios = require('axios');
const cheerio = require('cheerio');
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const path = require('path');
const fs = require('fs');
const util = require('util');
const pLimit = require('p-limit').default; // Re-enabled
const cron = require('node-cron');
const { URL } = require('url');

// --- Configuration ---
const DB_FILE = path.resolve(__dirname, 'mining_companies.db');
const LOG_DIR = path.resolve(__dirname, 'logs');
const LOCK_FILE = path.resolve(__dirname, 'daily_updater.lock');

// *** START WITH A LOW CONCURRENCY LIMIT FOR TESTING ***
const CONCURRENT_FETCH_LIMIT = 1; // Start testing with 2, increase later if stable
// ******************************************************

const fetchLimiter = pLimit(CONCURRENT_FETCH_LIMIT); // Re-enabled
const PRICE_CHANGE_WARN_THRESHOLD = 0.25;
const MCAP_CHANGE_WARN_THRESHOLD = 0.20;
const SOURCE_DISCREPANCY_WARN_THRESHOLD = 0.10;
const RETRY_COUNT = 2;
const RETRY_DELAY_MS = 2000;
const CRON_SCHEDULE = '5 3 * * *';
const CRON_TIMEZONE = "America/Toronto";
const FETCH_TIMEOUT_MS = 30000;
const LOG_PROGRESS_INTERVAL = 25; // Increased progress logging
const ENABLE_GOOGLE_VERIFICATION = process.env.ENABLE_GOOGLE_VERIFICATION === 'false'; // Keep google off for now

let isShuttingDown = false;
let isProcessing = false;
let exchangeRatesCache = {};
let db;

// Ensure log directory exists
try { /* ... */ if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true }); } catch (err) { console.error(`Log dir error: ${LOG_DIR}`, err); process.exit(1); }

// --- Logger Setup ---
const logger = winston.createLogger({ /* ... as before ... */ level: process.env.LOG_LEVEL || 'info', /* ... */ format: winston.format.combine( winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }), winston.format.errors({ stack: true }), winston.format.printf(({ timestamp, level, message, stack, ticker }) => `${timestamp} [${level.toUpperCase()}]${ticker ? ` [${ticker}]` : ''}: ${stack || message}` ) ), transports: [ new winston.transports.Console({ format: winston.format.combine(winston.format.colorize(), winston.format.printf(({ timestamp, level, message, ticker }) => `${timestamp} [${level}]${ticker ? ` [${ticker}]` : ''}: ${message}`)) }), new winston.transports.File({ filename: path.join(LOG_DIR, 'daily_update.log'), maxsize: 5242880, maxFiles: 3, tailable: true }), new winston.transports.File({ filename: path.join(LOG_DIR, 'daily_update_errors.log'), level: 'error', maxsize: 5242880, maxFiles: 3, tailable: true }) ], exceptionHandlers: [new winston.transports.File({ filename: path.join(LOG_DIR, 'exceptions.log') })], rejectionHandlers: [new winston.transports.File({ filename: path.join(LOG_DIR, 'rejections.log') })] });
const createTickerLogger = (ticker) => logger.child({ ticker });

// --- Database Setup & Promisification ---
async function connectDb() { /* ... as before ... */ return new Promise((resolve, reject) => { if (db) { logger.debug("Reusing DB connection."); return resolve(db); } logger.debug("Opening DB connection..."); try { if (!fs.existsSync(DB_FILE)) { return reject(new Error(`DB file not found: ${DB_FILE}`)); } const newDb = new sqlite3.Database(DB_FILE, sqlite3.OPEN_READWRITE, (err) => { if (err) { logger.error(`DB connect error: ${err.message}`, { stack: err.stack }); return reject(err); } logger.info(`Connected to DB: ${DB_FILE}`); db = newDb; resolve(db); }); } catch (err) { logger.error(`DB init error: ${err.message}`, { stack: err.stack }); reject(err); } }); }
async function dbRun(sql, params = [], localLogger = logger) { localLogger.debug(`DB Run: ${sql.substring(0,100)}...`); await connectDb(); return util.promisify(db.run.bind(db))(sql, params); }
async function dbGet(sql, params = [], localLogger = logger) { localLogger.debug(`DB Get: ${sql.substring(0,100)}...`); await connectDb(); return util.promisify(db.get.bind(db))(sql, params); }
async function dbAll(sql, params = [], localLogger = logger) { localLogger.debug(`DB All: ${sql.substring(0,100)}...`); await connectDb(); return util.promisify(db.all.bind(db))(sql, params); }


// --- Utility Functions ---
// ... (delay, sanitizeFiniteNumber, parseFinancialString, retryOperationWithTimeout, extractYahooTickerFromUrl, getTickerToUse) ...
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function sanitizeFiniteNumber(value) { if (value === null || value === undefined) return null; let num; if (typeof value === 'number') { num = value; } else if (typeof value === 'string') { const cleaned = value.replace(/[, ]/g, ''); if (cleaned === '' || cleaned === '-' || cleaned === '.') return null; num = parseFloat(cleaned); } else if (typeof value === 'object' && value !== null && 'raw' in value) { return sanitizeFiniteNumber(value.raw); } else { return null; } return Number.isFinite(num) ? num : null; }
function parseFinancialString(value) { if (typeof value !== 'string' || !value) return null; const cleaned = value.replace(/[^0-9.TBMK-]/g, ''); const multiplier = cleaned.match(/[TBMK]/i); let num = parseFloat(cleaned.replace(/[TBMK]/i, '')); if (isNaN(num)) return null; if (multiplier) { switch (multiplier[0].toUpperCase()) { case 'T': num *= 1e12; break; case 'B': num *= 1e9; break; case 'M': num *= 1e6; break; case 'K': num *= 1e3; break; } } return sanitizeFiniteNumber(num); }
async function retryOperationWithTimeout(fn, operationName, ticker, retries = RETRY_COUNT, baseDelay = RETRY_DELAY_MS, timeout = FETCH_TIMEOUT_MS) { const tickerLogger = createTickerLogger(ticker); for (let i = 0; i <= retries; i++) { if (isShuttingDown) throw new Error(`Operation ${operationName} aborted.`); let timeoutId; const timeoutPromise = new Promise((_, reject) => { timeoutId = setTimeout(() => { tickerLogger.warn(`${operationName} attempt ${i + 1} timed out.`); reject(new Error(`Operation timed out after ${timeout}ms`)); }, timeout); }); try { tickerLogger.debug(`Attempt ${operationName}, try ${i + 1}/${retries + 1}...`); const result = await Promise.race([fn(), timeoutPromise]); clearTimeout(timeoutId); tickerLogger.debug(`${operationName} success (try ${i + 1}).`); return result; } catch (e) { clearTimeout(timeoutId); const statusCode = e?.response?.status; const isHttpClientError = statusCode >= 400 && statusCode < 500; const isTimeoutError = e.message.includes('timed out'); if (i === retries || isHttpClientError || isTimeoutError) { const reason = isTimeoutError ? `Timeout` : isHttpClientError ? `Client Error ${statusCode}` : `Max retries`; tickerLogger.error(`Failed ${operationName} (${reason}): ${e.message}`); if (!isHttpClientError || (statusCode && statusCode !== 404)) { tickerLogger.debug(`${operationName} Stack:`, { stack: e.stack });} return null; } const delayMs = baseDelay * Math.pow(2, i) + Math.random() * baseDelay; tickerLogger.warn(`Error ${operationName}: ${e.message}. Retry ${i + 1}/${retries} in ${Math.round(delayMs)}ms...`); await delay(delayMs); } } return null; }
function extractYahooTickerFromUrl(yahooUrl) { if (!yahooUrl) return null; try { const urlObj = new URL(yahooUrl); const pathParts = urlObj.pathname.split('/').filter(part => part); if (pathParts.length >= 2 && pathParts[0].toLowerCase() === 'quote') { return pathParts[1]; } } catch (e) { logger.error(`Error parsing Yahoo URL "${yahooUrl}": ${e.message}`); } return null; }
async function getTickerToUse(companyId, fallbackTicker, localLogger = logger) { localLogger.debug(`Getting ticker for company ID ${companyId}...`); try { const urlRow = await dbGet( `SELECT url FROM company_urls WHERE company_id = ? AND url_type = 'yahoo_finance' ORDER BY last_validated DESC, url_id DESC LIMIT 1`, [companyId], localLogger); if (urlRow?.url) { const extractedTicker = extractYahooTickerFromUrl(urlRow.url); if (extractedTicker) { localLogger.debug(`Using validated ticker from DB URL: ${extractedTicker}`); return extractedTicker; } else { localLogger.warn(`Failed to extract ticker from DB URL: ${urlRow.url}. Fallback ${fallbackTicker}.`); } } else { localLogger.debug(`No validated URL found. Fallback ${fallbackTicker}.`); } } catch (dbErr) { localLogger.error(`Error fetching ticker: ${dbErr.message}`); } return fallbackTicker; }

// --- Data Fetching Functions ---
// Re-introducing p-limit wrapper
async function fetchYahooQuote(tickerToUse) {
    const tickerLogger = createTickerLogger(tickerToUse);
    const operation = async () => { // The actual fetch logic
        tickerLogger.debug(`Calling yahooFinance.quote...`);
        const startTime = Date.now();
        let result;
        try {
            result = await yahooFinance.quote(tickerToUse, {
                fields: ['regularMarketPrice', 'currency', 'marketCap']
            }, { validateResult: false });
        } finally {
            const duration = Date.now() - startTime;
            tickerLogger.debug(`yahooFinance.quote call took ${duration}ms.`);
        }
        if (!result || result.regularMarketPrice === undefined || result.marketCap === undefined) {
            throw new Error(`Incomplete data received`);
        }
        result.tickerUsed = tickerToUse;
        return result;
    };
    // Wrap the operation with p-limit and retry/timeout logic
    const quoteResult = await retryOperationWithTimeout(
        () => fetchLimiter(operation), // Wrap the async operation with limiter
        'fetchYahooQuote',
        tickerToUse
    );
    if (!quoteResult) {
        tickerLogger.error(`fetchYahooQuote failed definitively after retries/timeout.`);
    }
    return quoteResult;
}
// ... (fetchGoogleVerification remains the same, potentially disabled) ...
async function fetchGoogleVerification(tickerToUse) { /* ... */ if (!ENABLE_GOOGLE_VERIFICATION) { logger.debug(`[${tickerToUse}] Google verification disabled.`); return null; } const tickerLogger = createTickerLogger(tickerToUse); const operation = async () => { const safeTicker = tickerToUse.replace('.', '-'); let exchangeSuffix = ''; if (tickerToUse.endsWith('.TO')) exchangeSuffix = 'TSE'; else if (tickerToUse.endsWith('.V') || tickerToUse.endsWith('.CN') || tickerToUse.endsWith('.NE')) exchangeSuffix = 'CVE'; else if (tickerToUse.endsWith('.L')) exchangeSuffix = 'LON'; else if (tickerToUse.endsWith('.AX')) exchangeSuffix = 'ASX'; const url = `https://www.google.com/finance/quote/${safeTicker.split('.')[0]}${exchangeSuffix ? ':' + exchangeSuffix : ''}`; tickerLogger.debug(`Fetching Google verification from ${url}`); const { data } = await axios.get(url, { timeout: FETCH_TIMEOUT_MS, headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36', 'Accept-Language': 'en-US,en;q=0.9', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8', 'Connection': 'keep-alive' } }); const $ = cheerio.load(data); const priceText = $('div.YMlKec.fxKbKc').first().text().trim(); const price = sanitizeFiniteNumber(priceText.replace(/[^0-9.-]/g, '')); let marketCapText = $('div[data-attrid="Market cap"]').find('div > div').first().text().trim(); if (!marketCapText) { marketCapText = $('div.P6K39c:contains("Market cap")').prev().text().trim(); } const marketCap = parseFinancialString(marketCapText); if (price === null) { throw new Error('Google scrape failed to find price.'); } tickerLogger.debug(`Google fetched: price=${price}, market_cap=${marketCap}`); return { price, marketCap, tickerUsed: tickerToUse }; }; return retryOperationWithTimeout(operation, 'fetchGoogleVerification', tickerToUse, 1, 500, FETCH_TIMEOUT_MS); }

// --- Exchange Rate & Conversion ---
// ... (loadExchangeRates, getExchangeRate, convertToUSD remain the same) ...
async function loadExchangeRates() { /* ... */ logger.info('Loading exchange rates...'); try { const rates = await dbAll('SELECT from_currency, to_currency, rate FROM exchange_rates', [], logger); exchangeRatesCache=rates.reduce((acc, row)=>{if (!acc[row.from_currency])acc[row.from_currency]={}; acc[row.from_currency][row.to_currency]=row.rate; return acc;},{}); logger.info(`Loaded ${rates.length} rates.`); if(!getExchangeRate('CAD','USD')){logger.warn('CAD->USD rate missing, fallback 0.73');if(!exchangeRatesCache.CAD)exchangeRatesCache.CAD={};exchangeRatesCache.CAD.USD=0.73;} if(!getExchangeRate('USD','CAD')){logger.warn('USD->CAD rate missing, fallback 1.37');if(!exchangeRatesCache.USD)exchangeRatesCache.USD={};exchangeRatesCache.USD.CAD=1.37;} if(!getExchangeRate('AUD','USD')){logger.warn('AUD->USD rate missing, fallback 0.66');if(!exchangeRatesCache.AUD)exchangeRatesCache.AUD={};exchangeRatesCache.AUD.USD=0.66;} } catch (err) { logger.error(`Failed loading rates: ${err.message}`, { stack: err.stack }); exchangeRatesCache={CAD:{USD:0.73},USD:{CAD:1.37},AUD:{USD:0.66}}; logger.warn('Using fallback rates.'); } }
function getExchangeRate(f, t) { if (!f || !t) return null; if (f === t) return 1.0; return exchangeRatesCache[f]?.[t] || null; }
function convertToUSD(v, c, o = 'Conv', l = logger) { const n = sanitizeFiniteNumber(v); if (n === null) return null; if (!c) { l.warn(`No currency for ${n} in ${o}. Assume USD.`); c = 'USD'; } c = c.toUpperCase(); if (c === 'USD') return n; const r = getExchangeRate(c, 'USD'); if (r === null) { l.error(`Can't convert ${c} to USD for ${o}: Rate missing.`); return null; } return n * r; }


// --- Database Update Functions ---
// --- RESTORED REAL DATABASE LOGIC ---
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
            [companyId, priceDateStr],
            tickerLogger
        );
        if (existingToday) {
            tickerLogger.debug(`Stock price for ${priceDateStr} already exists.`);
            return true;
        }

        const latestExisting = await dbGet(
            'SELECT price_value, price_currency, date(price_date) as date FROM stock_prices WHERE company_id = ? ORDER BY price_date DESC LIMIT 1',
            [companyId],
            tickerLogger
        );

        if (latestExisting?.price_value > 0 && latestExisting.date !== priceDateStr) {
             const latestExistingUSD = convertToUSD(latestExisting.price_value, latestExisting.price_currency, 'Threshold Check', tickerLogger);
             const priceUSD = convertToUSD(price, currency, 'Threshold Check', tickerLogger);

             if(latestExistingUSD !== null && priceUSD !== null) {
                // Use relative comparison for small numbers, absolute for larger ones? Or just relative.
                const denominator = Math.max(latestExistingUSD, 0.01); // Avoid division by zero or near-zero
                const variance = Math.abs(priceUSD - latestExistingUSD) / denominator;
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
            [companyId, priceDateStr, price, currency],
            tickerLogger
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
            [companyId],
            tickerLogger
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
                const denominator = Math.max(currentMCapUSD, 1); // Avoid division by zero
                const variance = Math.abs(fetchedMCapUSD - currentMCapUSD) / denominator;
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
            [marketCap, currency, sanitizeFiniteNumber(enterpriseValue), evCurrency, now, companyId],
            tickerLogger
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
        const finData = await dbGet(`SELECT * FROM financials WHERE company_id = ?`, [companyId], tickerLogger);
        const estData = await dbGet(`SELECT * FROM mineral_estimates WHERE company_id = ?`, [companyId], tickerLogger);
        const prodData = await dbGet(`SELECT * FROM production WHERE company_id = ?`, [companyId], tickerLogger);

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

        const safeDivide = (numerator, denominator) => {
            const num = sanitizeFiniteNumber(numerator);
            const den = sanitizeFiniteNumber(denominator);
            // Use a small epsilon to avoid division by tiny near-zero numbers if desired
            // const epsilon = 1e-9;
            // return (num !== null && den !== null && Math.abs(den) > epsilon) ? num / den : null;
             return (num !== null && den !== null && den !== 0) ? num / den : null;
        };

        const metrics = {
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

        await dbRun(sql, values, tickerLogger);
        tickerLogger.info(`Updated valuation_metrics.`);

    } catch (err) {
        tickerLogger.error(`Error recalculating/updating valuation_metrics: ${err.message}`, { stack: err.stack });
    }
}

// --- Main Processing Logic ---
async function processCompanyUpdate(company, index, total) {
    const { company_id: companyId, tsx_code: originalTicker } = company;
    const tickerLogger = createTickerLogger(originalTicker);

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
    let tickerUsed = null;
    let updateFinancialsSuccess = false;

    try {
        // Step 0: Determine Ticker
        tickerLogger.debug("Determining ticker to use...");
        tickerUsed = await getTickerToUse(companyId, originalTicker, tickerLogger);
        if (!tickerUsed) {
            tickerLogger.error(`No valid ticker could be determined. Skipping fetches.`);
            return;
        }
        const effectiveLogger = tickerUsed === originalTicker ? tickerLogger : createTickerLogger(tickerUsed);
        effectiveLogger.debug(`Effective ticker set to: ${tickerUsed}`);


        // Step 1: Fetch Primary (Yahoo)
        effectiveLogger.debug("Starting Yahoo fetch step...");
        yahooQuote = await fetchYahooQuote(tickerUsed); // Pass the determined ticker
        effectiveLogger.debug(`Finished awaiting fetchYahooQuote result: ${yahooQuote ? 'Got data' : 'Got null'}`);

        if (yahooQuote) {
            price = sanitizeFiniteNumber(yahooQuote.regularMarketPrice);
            marketCap = sanitizeFiniteNumber(yahooQuote.marketCap);
            currency = yahooQuote.currency?.toUpperCase() || 'USD';
            primarySource = 'Yahoo Finance';
            effectiveLogger.debug(`Yahoo OK: Price=${price}, MCap=${marketCap}, Cur=${currency}`);
        } else {
            effectiveLogger.warn(`Yahoo fetch failed definitively.`);
        }
         effectiveLogger.debug("Finished Yahoo fetch step processing.");

        // Step 2 & 3: Google/Verification (Skipped for now)
        effectiveLogger.debug("Skipping Google verification step.");

        // Step 4: Check data validity
        if (price === null && marketCap === null) {
            effectiveLogger.error(`Skipping updates. No valid price or market cap found.`);
            return;
        }

        // Step 5: Update DB (REAL UPDATES REINSTATED)
        effectiveLogger.debug("Starting DB updates...");
        let priceUpdateSkipped = false;
        if (price !== null) {
            const priceUpdateSuccess = await updateStockPrice(companyId, tickerUsed, price, currency);
            if (!priceUpdateSuccess) priceUpdateSkipped = true;
        } else {
            effectiveLogger.warn(`Skipping price update as price is null.`);
            priceUpdateSkipped = true;
        }

        if (marketCap !== null) {
            if (!priceUpdateSkipped) {
                 updateFinancialsSuccess = await updateFinancialMarketData(companyId, tickerUsed, marketCap, currency);
            } else {
                effectiveLogger.warn(`Skipping financial market data update because stock price update was skipped or price was null.`);
            }
        } else {
             effectiveLogger.warn(`Skipping financial market data update as marketCap is null.`);
        }
         effectiveLogger.debug("Finished DB updates.");

        // Step 6: Recalculate Valuations
        if (updateFinancialsSuccess) {
             effectiveLogger.debug("Starting valuation metrics recalculation...");
             await recalculateAndUpdateValuationMetrics(companyId, tickerUsed);
             effectiveLogger.debug("Finished valuation metrics recalculation.");
        } else {
             effectiveLogger.debug(`Skipping valuation metrics update as financial market data was not updated.`);
        }

        effectiveLogger.info(`Processing finished using ${primarySource}.`);

    } catch (error) {
        createTickerLogger(originalTicker).error(`Unhandled error during processing company update: ${error.message}`, { stack: error.stack });
    } finally {
        if ((index + 1) % LOG_PROGRESS_INTERVAL === 0 || (index + 1) === total) {
            logger.info(`--- Progress: ${index + 1} / ${total} companies processed ---`);
        }
        createTickerLogger(originalTicker).debug(`=== COMPLETED processing company ID ${companyId} ===`);
    }
}

// Re-enabled p-limit in runDailyUpdates
async function runDailyUpdates() {
    if (isProcessing) {
        logger.warn("Update process already running. Skipping this trigger.");
        return;
    }
    isProcessing = true;
    logger.info('Starting DAILY data update run (CONCURRENT MODE)...'); // Indicate concurrent
    const startTime = Date.now();

    try {
        await connectDb();
        await loadExchangeRates();

        let companies = [];
        try {
            companies = await dbAll(`SELECT company_id, tsx_code FROM companies WHERE status != ? AND tsx_code IS NOT NULL AND tsx_code != '' ORDER BY company_id`, ['delisted'], logger);
            if (!companies.length) {
                logger.error('No active companies found.');
                isProcessing = false;
                return;
            }
            logger.info(`Found ${companies.length} companies. Processing with concurrency limit: ${CONCURRENT_FETCH_LIMIT}`);
        } catch (dbErr) {
             logger.error(`DB Error fetching companies: ${dbErr.message}`, { stack: dbErr.stack });
             isProcessing = false;
             return;
        }

        // Process companies concurrently using p-limit
        const promises = companies.map((company, index) =>
            fetchLimiter(() => processCompanyUpdate(company, index, companies.length))
                .catch(err => logger.error(`[${company.tsx_code}] FATAL error in process limiter: ${err.message}`, { stack: err.stack }))
        );
        await Promise.allSettled(promises); // Wait for all concurrent operations to settle

    } catch (error) {
        logger.error(`Error during daily update setup/run: ${error.message}`, { stack: error.stack });
    } finally {
        const duration = (Date.now() - startTime) / 1000;
        logger.info(`Daily data update run finished in ${duration.toFixed(1)} seconds.`);
        isProcessing = false;
    }
}


// --- Lock File and Execution / Scheduling ---
// ... (main, cleanup, handleShutdown, global error handlers remain the same) ...
async function main(runNow = false) { if (fs.existsSync(LOCK_FILE)) { const lockContent = fs.readFileSync(LOCK_FILE, 'utf8'); logger.warn(`Lock file exists. Running since: ${lockContent.split(': ')[1] || 'unknown'}. Exiting.`); if (db) await connectDb().then(d => d.close()).catch(e => logger.error(`Error closing DB on lock exit: ${e.message}`)); db = null; return; } let lockFd; try { lockFd = fs.openSync(LOCK_FILE, 'wx'); fs.writeSync(lockFd, `Running since: ${new Date().toISOString()} PID: ${process.pid}`); fs.closeSync(lockFd); logger.info('Lock file created.'); if (runNow) { logger.info('Executing initial run...'); await runDailyUpdates(); logger.info('Initial run complete.'); } } catch (err) { if (err.code === 'EEXIST') { logger.warn('Lock file appeared after check. Exiting.'); if (db) await connectDb().then(d => d.close()).catch(e => logger.error(`Error closing DB on lock exit: ${e.message}`)); db = null; } else { logger.error(`Critical error during main setup/lock: ${err.message}`, { stack: err.stack }); await cleanup(); } return; } if (!runOnce && !runNowAndSchedule) { await cleanup(); } }
async function cleanup() { logger.info('Running cleanup...'); try { if (fs.existsSync(LOCK_FILE)) { fs.unlinkSync(LOCK_FILE); logger.info('Lock file removed.'); } } catch (unlinkErr) { logger.error(`Error removing lock file: ${unlinkErr.message}`); } return new Promise((resolve) => { if (db) { logger.debug("Attempting DB close..."); db.close((err) => { if (err) logger.error(`DB close error: ${err.message}`); else logger.info('DB connection closed.'); db = null; resolve(); }); } else { logger.info('DB connection already closed.'); resolve(); } }); }
const runOnce = process.argv.includes('--once'); const runNowAndSchedule = process.argv.includes('--run-now'); let activeCronTask = null;
if (runOnce) { logger.info('Running --once mode.'); main(true).catch(async (e) => { logger.error(`Error in --once: ${e.message}`, { stack: e.stack }); process.exitCode = 1; }).finally(async () => { logger.info('--- Run Once Mode Finished ---'); await cleanup(); }); } else { connectDb().then(() => { logger.info(`Scheduled mode. Cron: "${CRON_SCHEDULE}" (${CRON_TIMEZONE}).`); if (runNowAndSchedule) { logger.info("`--run-now`: Initial run now..."); main(true).catch(e => logger.error(`Error during initial run: ${e.message}`, { stack: e.stack })); } else { logger.info("Waiting for schedule. Use --run-now for immediate exec."); } activeCronTask = cron.schedule(CRON_SCHEDULE, async () => { logger.info(`Cron triggered: ${new Date().toISOString()}`); if (!isProcessing && !fs.existsSync(LOCK_FILE)) { await main(true); } else { logger.warn("Skipping cron run: Lock file exists or still processing."); } }, { scheduled: true, timezone: CRON_TIMEZONE }); logger.info('Cron scheduled. Keep process running (Ctrl+C to exit).'); process.stdin.resume(); }).catch(e => { logger.error(`Initial DB connect failed: ${e.message}`); process.exit(1); }); process.on('SIGINT', () => handleShutdown('SIGINT', activeCronTask)); process.on('SIGTERM', () => handleShutdown('SIGTERM', activeCronTask)); }
async function handleShutdown(signal, task = null) { if (isShuttingDown) return; isShuttingDown = true; logger.info(`Received ${signal}. Shutting down...`); if (task) { task.stop(); logger.info('Stopped cron task.'); } await cleanup(); logger.info('Shutdown complete.'); process.exit(0); }
process.on('uncaughtException', async (err) => { logger.error(`UNCAUGHT EXCEPTION: ${err.message}`, { stack: err.stack }); if (!isShuttingDown) { await handleShutdown('uncaughtException', activeCronTask); } process.exit(1); });
process.on('unhandledRejection', async (reason, promise) => { logger.error('UNHANDLED REJECTION:', { reason: reason?.message || reason, stack: reason?.stack }); if (!isShuttingDown) { await handleShutdown('unhandledRejection', activeCronTask); } process.exit(1); });