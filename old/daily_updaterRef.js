require('dotenv').config();
const yahooFinance = require('yahoo-finance2').default;
const axios = require('axios');
// ... other imports
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const path = require('path');
const fs = require('fs');
const util = require('util');
// const pLimit = require('p-limit').default; // REMOVED for testing
const cron = require('node-cron');
const { URL } = require('url');

// --- Configuration ---
const DB_FILE = path.resolve(__dirname, 'mining_companies.db');
const LOG_DIR = path.resolve(__dirname, 'logs');
const LOCK_FILE = path.resolve(__dirname, 'daily_updater.lock');

// const CONCURRENT_FETCH_LIMIT = 1; // REMOVED for testing
// const fetchLimiter = pLimit(CONCURRENT_FETCH_LIMIT); // REMOVED for testing

const PRICE_CHANGE_WARN_THRESHOLD = 0.25;
// ... other config vars ...
const RETRY_COUNT = 2;
const RETRY_DELAY_MS = 2000;
const FETCH_TIMEOUT_MS = 30000;
const LOG_PROGRESS_INTERVAL = 10;
const ENABLE_GOOGLE_VERIFICATION = process.env.ENABLE_GOOGLE_VERIFICATION === 'false';
const CRON_SCHEDULE = '5 3 * * *';
const CRON_TIMEZONE = "America/Toronto";


let isShuttingDown = false;
let isProcessing = false;
let exchangeRatesCache = {};
let db;

// --- Logger Setup ---
const logger = winston.createLogger({ /* ... as before ... */ level: process.env.LOG_LEVEL || 'debug', /* ... */ format: winston.format.combine( winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }), winston.format.errors({ stack: true }), winston.format.printf(({ timestamp, level, message, stack, ticker }) => `${timestamp} [${level.toUpperCase()}]${ticker ? ` [${ticker}]` : ''}: ${stack || message}` ) ), transports: [ new winston.transports.Console({ format: winston.format.combine(winston.format.colorize(), winston.format.printf(({ timestamp, level, message, ticker }) => `${timestamp} [${level}]${ticker ? ` [${ticker}]` : ''}: ${message}`)) }), new winston.transports.File({ filename: path.join(LOG_DIR, 'daily_update.log'), maxsize: 5242880, maxFiles: 3, tailable: true }), new winston.transports.File({ filename: path.join(LOG_DIR, 'daily_update_errors.log'), level: 'error', maxsize: 5242880, maxFiles: 3, tailable: true }) ], exceptionHandlers: [new winston.transports.File({ filename: path.join(LOG_DIR, 'exceptions.log') })], rejectionHandlers: [new winston.transports.File({ filename: path.join(LOG_DIR, 'rejections.log') })] });
const createTickerLogger = (ticker) => logger.child({ ticker });

// --- Database Setup & Promisification ---
// ... (connectDb, dbRun, dbGet, dbAll remain the same) ...
async function connectDb() { return new Promise((resolve, reject) => { if (db) { logger.debug("Reusing DB connection."); return resolve(db); } logger.debug("Opening DB connection..."); try { if (!fs.existsSync(DB_FILE)) { return reject(new Error(`DB file not found: ${DB_FILE}`)); } const newDb = new sqlite3.Database(DB_FILE, sqlite3.OPEN_READWRITE, (err) => { if (err) { logger.error(`DB connect error: ${err.message}`, { stack: err.stack }); return reject(err); } logger.info(`Connected to DB: ${DB_FILE}`); db = newDb; resolve(db); }); } catch (err) { logger.error(`DB init error: ${err.message}`, { stack: err.stack }); reject(err); } }); }
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
// FetchYahooQuote NO LONGER uses p-limit
async function fetchYahooQuote(tickerToUse) {
    const tickerLogger = createTickerLogger(tickerToUse);
    const operation = async () => { // Keep async for await inside
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
    const quoteResult = await retryOperationWithTimeout(operation, 'fetchYahooQuote', tickerToUse);
    if (!quoteResult) {
        tickerLogger.error(`fetchYahooQuote failed definitively after retries/timeout.`);
    }
    return quoteResult;
}
// ... (fetchGoogleVerification remains the same, might be disabled anyway) ...
async function fetchGoogleVerification(tickerToUse) { if (!ENABLE_GOOGLE_VERIFICATION) { logger.debug(`[${tickerToUse}] Google verification disabled.`); return null; } const tickerLogger = createTickerLogger(tickerToUse); const operation = async () => { const safeTicker = tickerToUse.replace('.', '-'); let exchangeSuffix = ''; if (tickerToUse.endsWith('.TO')) exchangeSuffix = 'TSE'; else if (tickerToUse.endsWith('.V') || tickerToUse.endsWith('.CN') || tickerToUse.endsWith('.NE')) exchangeSuffix = 'CVE'; else if (tickerToUse.endsWith('.L')) exchangeSuffix = 'LON'; else if (tickerToUse.endsWith('.AX')) exchangeSuffix = 'ASX'; const url = `https://www.google.com/finance/quote/${safeTicker.split('.')[0]}${exchangeSuffix ? ':' + exchangeSuffix : ''}`; tickerLogger.debug(`Fetching Google verification from ${url}`); const { data } = await axios.get(url, { timeout: FETCH_TIMEOUT_MS, headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36', 'Accept-Language': 'en-US,en;q=0.9', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8', 'Connection': 'keep-alive' } }); const $ = cheerio.load(data); const priceText = $('div.YMlKec.fxKbKc').first().text().trim(); const price = sanitizeFiniteNumber(priceText.replace(/[^0-9.-]/g, '')); let marketCapText = $('div[data-attrid="Market cap"]').find('div > div').first().text().trim(); if (!marketCapText) { marketCapText = $('div.P6K39c:contains("Market cap")').prev().text().trim(); } const marketCap = parseFinancialString(marketCapText); if (price === null) { throw new Error('Google scrape failed to find price.'); } tickerLogger.debug(`Google fetched: price=${price}, market_cap=${marketCap}`); return { price, marketCap, tickerUsed: tickerToUse }; }; return retryOperationWithTimeout(operation, 'fetchGoogleVerification', tickerToUse, 1, 500, FETCH_TIMEOUT_MS); }

// --- Exchange Rate & Conversion ---
// ... (loadExchangeRates, getExchangeRate, convertToUSD remain the same) ...
async function loadExchangeRates() { /* ... */ logger.info('Loading exchange rates...'); try { const rates = await dbAll('SELECT from_currency, to_currency, rate FROM exchange_rates', [], logger); exchangeRatesCache=rates.reduce((acc, row)=>{if (!acc[row.from_currency])acc[row.from_currency]={}; acc[row.from_currency][row.to_currency]=row.rate; return acc;},{}); logger.info(`Loaded ${rates.length} rates.`); if(!getExchangeRate('CAD','USD')){logger.warn('CAD->USD rate missing, fallback 0.73');if(!exchangeRatesCache.CAD)exchangeRatesCache.CAD={};exchangeRatesCache.CAD.USD=0.73;} if(!getExchangeRate('USD','CAD')){logger.warn('USD->CAD rate missing, fallback 1.37');if(!exchangeRatesCache.USD)exchangeRatesCache.USD={};exchangeRatesCache.USD.CAD=1.37;} if(!getExchangeRate('AUD','USD')){logger.warn('AUD->USD rate missing, fallback 0.66');if(!exchangeRatesCache.AUD)exchangeRatesCache.AUD={};exchangeRatesCache.AUD.USD=0.66;} } catch (err) { logger.error(`Failed loading rates: ${err.message}`, { stack: err.stack }); exchangeRatesCache={CAD:{USD:0.73},USD:{CAD:1.37},AUD:{USD:0.66}}; logger.warn('Using fallback rates.'); } }
function getExchangeRate(f, t) { if (!f || !t) return null; if (f === t) return 1.0; return exchangeRatesCache[f]?.[t] || null; }
function convertToUSD(v, c, o = 'Conv', l = logger) { const n = sanitizeFiniteNumber(v); if (n === null) return null; if (!c) { l.warn(`No currency for ${n} in ${o}. Assume USD.`); c = 'USD'; } c = c.toUpperCase(); if (c === 'USD') return n; const r = getExchangeRate(c, 'USD'); if (r === null) { l.error(`Can't convert ${c} to USD for ${o}: Rate missing.`); return null; } return n * r; }


// --- Database Update Functions (STUBS for Debugging) ---
async function updateStockPrice(companyId, ticker, fetchedPrice, fetchedCurrency) { createTickerLogger(ticker).info(`SIMULATING updateStockPrice: ${fetchedPrice} ${fetchedCurrency}`); await delay(1); return true; }
async function updateFinancialMarketData(companyId, ticker, fetchedMCap, fetchedCurrency) { createTickerLogger(ticker).info(`SIMULATING updateFinancialMarketData: MCap ${fetchedMCap} ${fetchedCurrency}`); await delay(1); return true; }
async function recalculateAndUpdateValuationMetrics(companyId, ticker) { createTickerLogger(ticker).info(`SIMULATING recalculateAndUpdateValuationMetrics`); await delay(2); return true; }

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

        // Step 2 & 3: Google/Verification (Skipped)
        effectiveLogger.debug("Skipping Google verification step.");

        // Step 4: Check data validity
        if (price === null && marketCap === null) {
            effectiveLogger.error(`Skipping updates. No valid price or market cap found.`);
            return; // Skip this company
        }

        // Step 5: Simulate DB Updates
        effectiveLogger.debug("Starting SIMULATED DB updates...");
        // ... (Simulated DB updates using effectiveLogger) ...
        let priceUpdateSkipped = false; if (price !== null) { const priceUpdateSuccess = await updateStockPrice(companyId, tickerUsed, price, currency); if (!priceUpdateSuccess) priceUpdateSkipped = true; } else { effectiveLogger.warn(`Skipping price update simulation as price is null.`); priceUpdateSkipped = true; } if (marketCap !== null) { if (!priceUpdateSkipped) { updateFinancialsSuccess = await updateFinancialMarketData(companyId, tickerUsed, marketCap, currency); } else { effectiveLogger.warn(`Skipping financial market data update simulation.`); } } else { effectiveLogger.warn(`Skipping financial market data update simulation as marketCap is null.`); } effectiveLogger.debug("Finished SIMULATED DB updates.");


        // Step 6: Simulate Valuation Recalculation
        if (updateFinancialsSuccess) {
             effectiveLogger.debug("Starting SIMULATED valuation metrics recalculation...");
             await recalculateAndUpdateValuationMetrics(companyId, tickerUsed);
             effectiveLogger.debug("Finished SIMULATED valuation metrics recalculation.");
        } else {
             effectiveLogger.debug(`Skipping SIMULATED valuation metrics update.`);
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

// Modified runDailyUpdates to run sequentially WITHOUT p-limit
async function runDailyUpdates() {
    if (isProcessing) {
        logger.warn("Update process already running. Skipping this trigger.");
        return;
    }
    isProcessing = true;
    logger.info('Starting DAILY data update run (SEQUENTIAL MODE)...');
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
            logger.info(`Found ${companies.length} companies.`);
        } catch (dbErr) {
             logger.error(`DB Error fetching companies: ${dbErr.message}`, { stack: dbErr.stack });
             isProcessing = false;
             return;
        }

        // Process companies sequentially
        for (let i = 0; i < companies.length; i++) {
            if (isShuttingDown) {
                logger.info("Shutdown signal received, stopping company processing loop.");
                break;
            }
            const company = companies[i];
            try {
                await processCompanyUpdate(company, i, companies.length);
            } catch (err) {
                // Log error from processCompanyUpdate if it somehow bubbles up unexpectedly
                 logger.error(`[${company.tsx_code}] Uncaught error during sequential processCompanyUpdate: ${err.message}`, { stack: err.stack });
            }
        }

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