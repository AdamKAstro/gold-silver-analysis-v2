require('dotenv').config();
const yahooFinance = require('yahoo-finance2').default;
// const axios = require('axios'); // Google verification disabled
// const cheerio = require('cheerio'); // Google verification disabled
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const path = require('path');
const fs = require('fs');
const util = require('util');
// const pLimit = require('p-limit').default; // NO CONCURRENCY FOR THIS VERSION
const cron = require('node-cron');
const { URL } = require('url');

// Usage :  LOG_LEVEL=debug node daily_updater_sequential.js --run-now



// --- Configuration ---
const DB_FILE = path.resolve(__dirname, 'mining_companies.db');
const LOG_DIR = path.resolve(__dirname, 'logs');
const LOCK_FILE = path.resolve(__dirname, 'daily_updater_sequential.lock'); // Use different lock file

try { /* ... ensure logs dir ... */ if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true }); } catch (err) { console.error(`Log dir error: ${LOG_DIR}`, err); process.exit(1); }

const PRICE_CHANGE_WARN_THRESHOLD = 0.25;
const MCAP_CHANGE_WARN_THRESHOLD = 0.20;
const RETRY_COUNT = 2;
const RETRY_DELAY_MS = 2000;
const CRON_SCHEDULE = '15 3 * * *'; // Shifted slightly
const CRON_TIMEZONE = "America/Toronto";
const FETCH_TIMEOUT_MS = 35000; // Increased timeout
const LOG_PROGRESS_INTERVAL = 25;
// const ENABLE_GOOGLE_VERIFICATION = false; // Disabled for simplicity

let isShuttingDown = false;
let isProcessing = false;
let exchangeRatesCache = {};
let db;

// --- Logger Setup ---
const logger = winston.createLogger({ /* ... as before ... */ level: process.env.LOG_LEVEL || 'info', /* ... */ format: winston.format.combine( winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }), winston.format.errors({ stack: true }), winston.format.printf(({ timestamp, level, message, stack, ticker }) => `${timestamp} [${level.toUpperCase()}]${ticker ? ` [${ticker}]` : ''}: ${stack || message}` ) ), transports: [ new winston.transports.Console({ format: winston.format.combine(winston.format.colorize(), winston.format.printf(({ timestamp, level, message, ticker }) => `${timestamp} [${level}]${ticker ? ` [${ticker}]` : ''}: ${message}`)) }), new winston.transports.File({ filename: path.join(LOG_DIR, 'daily_updater_sequential.log'), maxsize: 5242880, maxFiles: 3, tailable: true }), new winston.transports.File({ filename: path.join(LOG_DIR, 'daily_updater_errors_seq.log'), level: 'error', maxsize: 5242880, maxFiles: 3, tailable: true }) ], exceptionHandlers: [new winston.transports.File({ filename: path.join(LOG_DIR, 'exceptions_seq.log') })], rejectionHandlers: [new winston.transports.File({ filename: path.join(LOG_DIR, 'rejections_seq.log') })] });
const createTickerLogger = (ticker) => logger.child({ ticker });

// --- Database Setup & Promisification ---
async function connectDb() { /* ... as before ... */ return new Promise((resolve, reject) => { if (db) { logger.debug("Reusing DB connection."); return resolve(db); } logger.debug("Opening DB connection..."); try { if (!fs.existsSync(DB_FILE)) { return reject(new Error(`DB file not found: ${DB_FILE}`)); } const newDb = new sqlite3.Database(DB_FILE, sqlite3.OPEN_READWRITE, (err) => { if (err) { logger.error(`DB connect error: ${err.message}`, { stack: err.stack }); return reject(err); } logger.info(`Connected to DB: ${DB_FILE}`); db = newDb; resolve(db); }); } catch (err) { logger.error(`DB init error: ${err.message}`, { stack: err.stack }); reject(err); } }); }
async function dbRun(sql, params = [], localLogger = logger) { localLogger.debug(`DB Run: ${sql.substring(0,100)}...`); await connectDb(); return util.promisify(db.run.bind(db))(sql, params); }
async function dbGet(sql, params = [], localLogger = logger) { localLogger.debug(`DB Get: ${sql.substring(0,100)}...`); await connectDb(); return util.promisify(db.get.bind(db))(sql, params); }
async function dbAll(sql, params = [], localLogger = logger) { localLogger.debug(`DB All: ${sql.substring(0,100)}...`); await connectDb(); return util.promisify(db.all.bind(db))(sql, params); }


// --- Utility Functions ---
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function sanitizeFiniteNumber(value) { /* ... as before ... */ if (value === null || value === undefined) return null; let num; if (typeof value === 'number') { num = value; } else if (typeof value === 'string') { const cleaned = value.replace(/[, ]/g, ''); if (cleaned === '' || cleaned === '-' || cleaned === '.') return null; num = parseFloat(cleaned); } else if (typeof value === 'object' && value !== null && 'raw' in value) { return sanitizeFiniteNumber(value.raw); } else { return null; } return Number.isFinite(num) ? num : null; }
// parseFinancialString only needed if Google scraping enabled, removed for now.
// function parseFinancialString(value) { /* ... */ }
// Simplified retry - no timeout logic needed if call itself doesn't hang
async function retryOperationSimple(fn, operationName, ticker, retries = RETRY_COUNT, baseDelay = RETRY_DELAY_MS) {
    const tickerLogger = createTickerLogger(ticker);
    for (let i = 0; i <= retries; i++) {
        if (isShuttingDown) throw new Error(`Operation ${operationName} aborted.`);
        try {
            tickerLogger.debug(`Attempting ${operationName}, try ${i + 1}/${retries + 1}...`);
            const result = await fn(); // Direct await
            tickerLogger.debug(`${operationName} success (try ${i + 1}).`);
            return result;
        } catch (e) {
            const statusCode = e?.response?.status;
            const isHttpClientError = statusCode >= 400 && statusCode < 500;

            if (i === retries || isHttpClientError) {
                const reason = isHttpClientError ? `Client Error ${statusCode}` : `Max retries`;
                tickerLogger.error(`Failed ${operationName} (${reason}): ${e.message}`);
                // Optional stack trace log removed for brevity in simple retry
                return null;
            }
            const delayMs = baseDelay * Math.pow(2, i) + Math.random() * baseDelay;
            tickerLogger.warn(`Error ${operationName}: ${e.message}. Retry ${i + 1}/${retries} in ${Math.round(delayMs)}ms...`);
            await delay(delayMs);
        }
    }
    return null;
}
// --- Data Fetching Functions ---
function extractYahooTickerFromUrl(yahooUrl) { /* ... */ if (!yahooUrl) return null; try { const urlObj = new URL(yahooUrl); const pathParts = urlObj.pathname.split('/').filter(part => part); if (pathParts.length >= 2 && pathParts[0].toLowerCase() === 'quote') { return pathParts[1]; } } catch (e) { logger.error(`Error parsing Yahoo URL "${yahooUrl}": ${e.message}`); } return null; }
// getTickerToUse remains the same
async function getTickerToUse(companyId, fallbackTicker, localLogger = logger) { localLogger.debug(`Getting ticker for ID ${companyId}...`); try { const urlRow = await dbGet( `SELECT url FROM company_urls WHERE company_id = ? AND url_type = 'yahoo_finance' ORDER BY last_validated DESC, url_id DESC LIMIT 1`, [companyId], localLogger); if (urlRow?.url) { const extractedTicker = extractYahooTickerFromUrl(urlRow.url); if (extractedTicker) { localLogger.debug(`Using ticker from DB URL: ${extractedTicker}`); return extractedTicker; } else { localLogger.warn(`Failed extract ticker from DB URL: ${urlRow.url}. Fallback ${fallbackTicker}.`); } } else { localLogger.debug(`No yahoo_finance URL found. Fallback ${fallbackTicker}.`); } } catch (dbErr) { localLogger.error(`Error fetching ticker: ${dbErr.message}`); } return fallbackTicker; }

// fetchYahooQuote - Simplified: No p-limit, uses simple retry
async function fetchYahooQuote(tickerToUse) {
    const tickerLogger = createTickerLogger(tickerToUse);
    const operation = async () => {
        tickerLogger.debug(`Calling yahooFinance.quote...`);
        const startTime = Date.now();
        let result;
        try {
             result = await yahooFinance.quote(tickerToUse, {
                 fields: ['regularMarketPrice', 'currency', 'marketCap']
            }, { validateResult: false });
        } finally {
            const duration = Date.now() - startTime;
            // Log duration only if DEBUG is enabled to reduce noise
            tickerLogger.debug(`yahooFinance.quote call took ${duration}ms.`);
        }
        if (!result || result.regularMarketPrice === undefined || result.marketCap === undefined) {
            throw new Error(`Incomplete data received`);
        }
        result.tickerUsed = tickerToUse;
        return result;
    };
    // Use simple retry without timeout race
    const quoteResult = await retryOperationSimple(operation, 'fetchYahooQuote', tickerToUse);
    if (!quoteResult) {
        tickerLogger.error(`fetchYahooQuote failed definitively.`);
    }
    return quoteResult;
}

// --- Exchange Rate & Conversion ---
// ... (loadExchangeRates, getExchangeRate, convertToUSD remain the same) ...
async function loadExchangeRates() { /* ... */ logger.info('Loading exchange rates...'); try { const rates = await dbAll('SELECT from_currency, to_currency, rate FROM exchange_rates', [], logger); exchangeRatesCache=rates.reduce((acc, row)=>{if (!acc[row.from_currency])acc[row.from_currency]={}; acc[row.from_currency][row.to_currency]=row.rate; return acc;},{}); logger.info(`Loaded ${rates.length} rates.`); if(!getExchangeRate('CAD','USD')){logger.warn('CAD->USD rate missing, fallback 0.73');if(!exchangeRatesCache.CAD)exchangeRatesCache.CAD={};exchangeRatesCache.CAD.USD=0.73;} if(!getExchangeRate('USD','CAD')){logger.warn('USD->CAD rate missing, fallback 1.37');if(!exchangeRatesCache.USD)exchangeRatesCache.USD={};exchangeRatesCache.USD.CAD=1.37;} if(!getExchangeRate('AUD','USD')){logger.warn('AUD->USD rate missing, fallback 0.66');if(!exchangeRatesCache.AUD)exchangeRatesCache.AUD={};exchangeRatesCache.AUD.USD=0.66;} } catch (err) { logger.error(`Failed loading rates: ${err.message}`, { stack: err.stack }); exchangeRatesCache={CAD:{USD:0.73},USD:{CAD:1.37},AUD:{USD:0.66}}; logger.warn('Using fallback rates.'); } }
function getExchangeRate(f, t) { if (!f || !t) return null; if (f === t) return 1.0; return exchangeRatesCache[f]?.[t] || null; }
function convertToUSD(v, c, o = 'Conv', l = logger) { const n = sanitizeFiniteNumber(v); if (n === null) return null; if (!c) { l.warn(`No currency for ${n} in ${o}. Assume USD.`); c = 'USD'; } c = c.toUpperCase(); if (c === 'USD') return n; const r = getExchangeRate(c, 'USD'); if (r === null) { l.error(`Can't convert ${c} to USD for ${o}: Rate missing.`); return null; } return n * r; }


// --- Database Update Functions ---
// --- USING REAL DB LOGIC AGAIN ---
async function updateStockPrice(companyId, ticker, fetchedPrice, fetchedCurrency) {
    const tickerLogger = createTickerLogger(ticker);
    const priceDateStr = new Date().toISOString().split('T')[0];
    const price = sanitizeFiniteNumber(fetchedPrice);
    const currency = fetchedCurrency?.toUpperCase() || 'USD';

    if (price === null || price <= 0) {
        tickerLogger.warn(`Skipping stock price update: invalid price ${fetchedPrice}`);
        return { updated: false, skipped: true };
    }

    tickerLogger.debug(`Attempt update ${priceDateStr}: ${price} ${currency}`);
    try {
        const existingToday = await dbGet(
            'SELECT price_id FROM stock_prices WHERE company_id = ? AND date(price_date) = ?',
            [companyId, priceDateStr],
            tickerLogger
        );
        if (existingToday) {
            tickerLogger.debug(`Price for ${priceDateStr} exists.`);
            return { updated: false, skipped: false }; // Not updated, but not skipped due to error/threshold
        }

        const latestExisting = await dbGet(
            'SELECT price_value, price_currency, date(price_date) as date FROM stock_prices WHERE company_id = ? ORDER BY price_date DESC LIMIT 1',
            [companyId],
            tickerLogger
        );

        if (latestExisting?.price_value > 0 && latestExisting.date !== priceDateStr) {
             const latestExistingUSD = convertToUSD(latestExisting.price_value, latestExisting.price_currency, 'Threshold', tickerLogger);
             const priceUSD = convertToUSD(price, currency, 'Threshold', tickerLogger);

             if(latestExistingUSD !== null && priceUSD !== null) {
                const denominator = Math.max(latestExistingUSD, 0.01);
                const variance = Math.abs(priceUSD - latestExistingUSD) / denominator;
                if (variance > PRICE_CHANGE_WARN_THRESHOLD) {
                    tickerLogger.warn(`Price change>${PRICE_CHANGE_WARN_THRESHOLD*100}% (USD ${latestExistingUSD.toFixed(2)}->${priceUSD.toFixed(2)}). Skipping.`);
                    return { updated: false, skipped: true }; // Skipped due to threshold
                }
             } else { tickerLogger.warn(`Cannot compare price threshold.`); }
        }

        await dbRun(
            'INSERT INTO stock_prices (company_id, price_date, price_value, price_currency, last_updated) VALUES (?,?,?,?,CURRENT_TIMESTAMP)',
            [companyId, priceDateStr, price, currency],
            tickerLogger
        );
        tickerLogger.info(`Inserted price ${priceDateStr}: ${price} ${currency}`);
        return { updated: true, skipped: false };

    } catch (err) {
        tickerLogger.error(`Error update stock_prices: ${err.message}`, { stack: err.stack });
        return { updated: false, skipped: false, error: true }; // Failed due to error
    }
}

async function updateFinancialMarketData(companyId, ticker, fetchedMCap, fetchedCurrency, currentFinancials) {
    const tickerLogger=createTickerLogger(ticker);
    const marketCap=sanitizeFiniteNumber(fetchedMCap);
    const currency=fetchedCurrency?.toUpperCase()||'USD';
    const now=new Date().toISOString();

    if(marketCap===null||marketCap<=0){tickerLogger.warn(`Skipping market data: invalid MCap ${fetchedMCap}`); return false;}
    tickerLogger.debug(`Attempt market update: MCap=${marketCap} ${currency}`);

    try{
        if(!currentFinancials){tickerLogger.warn(`No existing financial record passed.`); return false;}

        // Threshold Check (Compare in USD)
        if (currentFinancials.market_cap_value && currentFinancials.market_cap_value > 0) {
            const currentMCapUSD=convertToUSD(currentFinancials.market_cap_value,currentFinancials.market_cap_currency,'MCap Threshold',tickerLogger);
            const fetchedMCapUSD=convertToUSD(marketCap,currency,'MCap Threshold',tickerLogger);
            if(currentMCapUSD!==null&&fetchedMCapUSD!==null){
                const denominator=Math.max(currentMCapUSD,1);
                const variance=Math.abs(fetchedMCapUSD-currentMCapUSD)/denominator;
                if(variance>MCAP_CHANGE_WARN_THRESHOLD){tickerLogger.warn(`MCap change>${MCAP_CHANGE_WARN_THRESHOLD*100}% (USD ${currentMCapUSD.toFixed(0)}->${fetchedMCapUSD.toFixed(0)}). Skipping.`);return false;}
            }else{tickerLogger.warn(`Cannot compare MCap threshold.`);}
        }

        // Calculate EV in USD
        const cashUSD=convertToUSD(currentFinancials.cash_value,currentFinancials.cash_currency,'EV Calc',tickerLogger)||0;
        const dv=sanitizeFiniteNumber(currentFinancials.debt_value); const lv=sanitizeFiniteNumber(currentFinancials.liabilities);
        const dtu=(dv!==null&&dv>0)?dv:((lv!==null&&lv>0)?lv:0);
        const dc=(dv!==null&&dv>0)?currentFinancials.debt_currency:currentFinancials.liabilities_currency;
        const dUSD=convertToUSD(dtu,dc,'EV Calc',tickerLogger)||0;
        const fmu=convertToUSD(marketCap,currency,'EV Calc',tickerLogger);
        let ev=null; let evc=null;
        if(fmu!==null){ev=fmu+dUSD-cashUSD;evc='USD';} else {tickerLogger.warn(`Cannot calculate EV: MCap USD conversion failed.`);}

        await dbRun(`UPDATE financials SET market_cap_value=?, market_cap_currency=?, enterprise_value_value=?, enterprise_value_currency=?, last_updated=? WHERE company_id=?`,[marketCap,currency,sanitizeFiniteNumber(ev),evc,now,companyId],tickerLogger);
        tickerLogger.info(`Updated market data: MCap=${marketCap} ${currency}, EV=${sanitizeFiniteNumber(ev)} ${evc}`);
        return true;
    } catch (err){tickerLogger.error(`Error update market data: ${err.message}`,{stack:err.stack}); return false;}
}

async function recalculateAndUpdateValuationMetrics(companyId, ticker, updatedFinancials) {
    const tickerLogger = createTickerLogger(ticker);
    tickerLogger.debug(`Recalculating valuation metrics...`);
    try {
        // Use the updated financials passed in, fetch others
        const estData = await dbGet(`SELECT * FROM mineral_estimates WHERE company_id = ?`, [companyId], tickerLogger);
        const prodData = await dbGet(`SELECT * FROM production WHERE company_id = ?`, [companyId], tickerLogger);

        if (!updatedFinancials || updatedFinancials.market_cap_value === null || updatedFinancials.enterprise_value_value === null) {
            tickerLogger.warn(`Skipping metrics: Missing updated MCap/EV.`);
            return;
        }

        const mCapUSD = convertToUSD(updatedFinancials.market_cap_value, updatedFinancials.market_cap_currency, 'Valuation MCap', tickerLogger);
        const evUSD = convertToUSD(updatedFinancials.enterprise_value_value, updatedFinancials.enterprise_value_currency, 'Valuation EV', tickerLogger);

        if (mCapUSD === null || evUSD === null) {
             tickerLogger.warn(`Skipping metrics: Failed MCap/EV USD conversion.`);
             return;
        }

        const safeDivide = (n, d) => { /* ... */ const num=sanitizeFiniteNumber(n);const den=sanitizeFiniteNumber(d);return(num!==null&&den!==null&&den!==0)?num/den:null;};
        const metrics = { company_id: companyId, /* ... all 19 calculations ... */ mkt_cap_per_reserve_oz_precious: safeDivide(mCapUSD, (estData?.reserves_precious_aueq_moz || 0) * 1e6), mkt_cap_per_mi_oz_precious: safeDivide(mCapUSD, (estData?.measured_indicated_precious_aueq_moz || 0) * 1e6), mkt_cap_per_resource_oz_precious: safeDivide(mCapUSD, (estData?.resources_precious_aueq_moz || 0) * 1e6), mkt_cap_per_mineable_oz_precious: safeDivide(mCapUSD, (estData?.mineable_precious_aueq_moz || 0) * 1e6), mkt_cap_per_reserve_oz_all: safeDivide(mCapUSD, (estData?.reserves_total_aueq_moz || 0) * 1e6), mkt_cap_per_mi_oz_all: safeDivide(mCapUSD, (estData?.measured_indicated_total_aueq_moz || 0) * 1e6), mkt_cap_per_resource_oz_all: safeDivide(mCapUSD, (estData?.resources_total_aueq_moz || 0) * 1e6), mkt_cap_per_mineable_oz_all: safeDivide(mCapUSD, (estData?.mineable_total_aueq_moz || 0) * 1e6), ev_per_reserve_oz_precious: safeDivide(evUSD, (estData?.reserves_precious_aueq_moz || 0) * 1e6), ev_per_mi_oz_precious: safeDivide(evUSD, (estData?.measured_indicated_precious_aueq_moz || 0) * 1e6), ev_per_resource_oz_precious: safeDivide(evUSD, (estData?.resources_precious_aueq_moz || 0) * 1e6), ev_per_mineable_oz_precious: safeDivide(evUSD, (estData?.mineable_precious_aueq_moz || 0) * 1e6), ev_per_reserve_oz_all: safeDivide(evUSD, (estData?.reserves_total_aueq_moz || 0) * 1e6), ev_per_mi_oz_all: safeDivide(evUSD, (estData?.measured_indicated_total_aueq_moz || 0) * 1e6), ev_per_resource_oz_all: safeDivide(evUSD, (estData?.resources_total_aueq_moz || 0) * 1e6), ev_per_mineable_oz_all: safeDivide(evUSD, (estData?.mineable_total_aueq_moz || 0) * 1e6), mkt_cap_per_production_oz: safeDivide(mCapUSD, (prodData?.current_production_total_aueq_koz || 0) * 1e3), ev_per_production_oz: safeDivide(evUSD, (prodData?.current_production_total_aueq_koz || 0) * 1e3), last_updated: new Date().toISOString() };
        const columns = Object.keys(metrics);
        const placeholders = columns.map(() => '?').join(',');
        const values = Object.values(metrics);
        const sql = `INSERT OR REPLACE INTO valuation_metrics (${columns.join(', ')}) VALUES (${placeholders})`;
        await dbRun(sql, values, tickerLogger);
        tickerLogger.info(`Updated valuation_metrics.`);
    } catch (err) {
        tickerLogger.error(`Error recalculating valuation_metrics: ${err.message}`, { stack: err.stack });
    }
}


// --- Main Processing Logic ---
async function runDailyUpdates() {
    if (isProcessing) {
        logger.warn("Update process already running. Skipping this trigger.");
        return;
    }
    isProcessing = true;
    logger.info('Starting DAILY data update run (SEQUENTIAL)...');
    const startTime = Date.now();
    let companiesData = {}; // Store pre-fetched data

    try {
        await connectDb();
        await loadExchangeRates();

        // --- Phase 1: Pre-fetch static data ---
        logger.info('Phase 1: Pre-fetching data for all companies...');
        const companies = await dbAll(`SELECT company_id, tsx_code FROM companies WHERE status != ? AND tsx_code IS NOT NULL AND tsx_code != '' ORDER BY company_id`, ['delisted'], logger);
        if (!companies.length) {
            logger.error('No active companies found.');
            isProcessing = false; return;
        }
        logger.info(`Found ${companies.length} companies.`);

        const financialPromises = companies.map(c => dbGet(`SELECT * FROM financials WHERE company_id = ?`, [c.company_id], logger).then(f => ({ id: c.company_id, data: f })));
        const urlPromises = companies.map(c => dbGet(`SELECT url FROM company_urls WHERE company_id = ? AND url_type = 'yahoo_finance' ORDER BY last_validated DESC, url_id DESC LIMIT 1`, [c.company_id], logger).then(u => ({ id: c.company_id, data: u })));

        const [financialResults, urlResults] = await Promise.all([
            Promise.allSettled(financialPromises),
            Promise.allSettled(urlPromises)
        ]);

        companies.forEach(c => {
            const finResult = financialResults.find(r => r.status === 'fulfilled' && r.value.id === c.company_id);
            const urlResult = urlResults.find(r => r.status === 'fulfilled' && r.value.id === c.company_id);
            companiesData[c.company_id] = {
                companyInfo: c,
                currentFinancials: finResult ? finResult.value.data : null,
                yahooUrlRow: urlResult ? urlResult.value.data : null,
                fetchedQuote: null,
                dbUpdateStatus: { price: 'pending', financials: 'pending', valuations: 'pending' }
            };
            if (!companiesData[c.company_id].currentFinancials) {
                logger.warn(`[${c.tsx_code}] Pre-fetch failed for financials.`);
            }
        });
        logger.info('Phase 1: Pre-fetching complete.');

        // --- Phase 2: Fetch Market Data Sequentially ---
        logger.info('Phase 2: Fetching market data sequentially...');
        let fetchedCount = 0;
        let index = 0; // Initialize index manually
        for (const company of companies) { // Using for...of loop
            if (isShuttingDown) break;
            const companyId = company.company_id;
            const originalTicker = company.tsx_code;
            const tickerLogger = createTickerLogger(originalTicker); // Base logger on original

            // Use index here for logging
            tickerLogger.info(`Fetching ${index + 1}/${companies.length}...`);

            // Determine the ticker to use
            let tickerUsed = companiesData[companyId].yahooUrlRow?.url
                ? extractYahooTickerFromUrl(companiesData[companyId].yahooUrlRow.url) || originalTicker
                : originalTicker;

            // NOW define effectiveLogger based on the final tickerUsed
            const effectiveLogger = tickerUsed === originalTicker ? tickerLogger : createTickerLogger(tickerUsed);

            effectiveLogger.debug(`Attempting fetch for ${tickerUsed}...`);
            const quote = await fetchYahooQuote(tickerUsed); // Pass determined ticker
            companiesData[companyId].fetchedQuote = quote; // Store result (or null)
            if (quote) {
                fetchedCount++;
                effectiveLogger.debug(`Fetch successful.`);
            } else {
                 // Log failure using the ticker we *attempted* to use
                 effectiveLogger.error(`Fetch failed.`);
            }

            // Log progress using the manual index
            if ((index + 1) % LOG_PROGRESS_INTERVAL === 0 || (index + 1) === companies.length) {
                logger.info(`--- Fetch Progress: ${index + 1} / ${companies.length} attempts completed ---`);
            }
            index++; // Increment index manually
        }
        logger.info(`Phase 2: Fetching complete. ${fetchedCount} successful fetches.`);


        // --- Phase 3: Update DB Sequentially ---
        logger.info('Phase 3: Updating database sequentially...');
        let priceUpdates = 0, priceSkips = 0, finUpdates = 0, finSkips = 0;
        index = 0; // <--- Reset index for this loop
        for (const company of companies) {
             if (isShuttingDown) break;
            const companyId = company.company_id;
            const originalTicker = company.tsx_code;
            const tickerLogger = createTickerLogger(originalTicker);
            const data = companiesData[companyId];
            const quote = data.fetchedQuote;

            if (!quote) {
                 tickerLogger.warn("Skipping DB update phase - no fetched data.");
                 data.dbUpdateStatus.price = 'skipped (no data)';
                 data.dbUpdateStatus.financials = 'skipped (no data)';
                 index++; // Increment index even if skipped
                 continue;
            }

            const price = sanitizeFiniteNumber(quote.regularMarketPrice);
            const marketCap = sanitizeFiniteNumber(quote.marketCap);
            const currency = quote.currency?.toUpperCase() || 'USD';
            const tickerUsed = quote.tickerUsed; // Use the ticker that actually worked

            // Update Price
             if (price !== null) {
                 const priceResult = await updateStockPrice(companyId, tickerUsed, price, currency);
                 if (priceResult.updated) priceUpdates++;
                 if (priceResult.skipped) priceSkips++;
                 data.dbUpdateStatus.price = priceResult.skipped ? 'skipped (threshold)' : (priceResult.updated ? 'updated' : 'no_change');
             } else {
                 data.dbUpdateStatus.price = 'skipped (invalid price)';
                 priceSkips++;
             }

             // Update Financials
             if (data.dbUpdateStatus.price !== 'skipped (threshold)' && data.dbUpdateStatus.price !== 'skipped (invalid price)' && marketCap !== null) {
                 const finUpdateSuccess = await updateFinancialMarketData(companyId, tickerUsed, marketCap, currency, data.currentFinancials);
                 if (finUpdateSuccess) finUpdates++; else finSkips++;
                  data.dbUpdateStatus.financials = finUpdateSuccess ? 'updated' : 'skipped (threshold/error)';
             } else {
                  data.dbUpdateStatus.financials = 'skipped (dependent)';
                  finSkips++;
             }
             tickerLogger.debug(`DB Update Status: Price=${data.dbUpdateStatus.price}, Financials=${data.dbUpdateStatus.financials}`);
             if ((index + 1) % LOG_PROGRESS_INTERVAL === 0 || (index + 1) === companies.length) {
                logger.info(`--- DB Update Progress: ${index + 1} / ${companies.length} companies processed ---`);
             }
             index++; // Increment index
        }
         logger.info(`Phase 3: DB Update complete. Prices Updated: ${priceUpdates}, Skipped: ${priceSkips}. Financials Updated: ${finUpdates}, Skipped: ${finSkips}.`);


        // --- Phase 4: Recalculate Valuations Sequentially ---
        logger.info('Phase 4: Recalculating valuation metrics sequentially...');
        let valuationUpdates = 0;
        index = 0; // Reset index
        for (const company of companies) {
            if (isShuttingDown) break;
            const companyId = company.company_id;
            const originalTicker = company.tsx_code;
            const tickerLogger = createTickerLogger(originalTicker);
            const data = companiesData[companyId];

            if (data.dbUpdateStatus.financials === 'updated') {
                 // Fetch updated financials (important!)
                 const updatedFinancials = await dbGet(`SELECT * FROM financials WHERE company_id = ?`, [companyId], tickerLogger);
                 if (updatedFinancials) {
                    // Determine tickerUsed again or retrieve from data object if stored reliably
                    const tickerUsed = data.fetchedQuote?.tickerUsed || await getTickerToUse(companyId, originalTicker, tickerLogger);
                    await recalculateAndUpdateValuationMetrics(companyId, tickerUsed, updatedFinancials);
                    valuationUpdates++;
                 } else {
                     tickerLogger.warn("Could not fetch updated financials for valuation recalculation.");
                 }
            } else {
                tickerLogger.debug("Skipping valuation recalculation as financials were not updated.");
            }
            if ((index + 1) % LOG_PROGRESS_INTERVAL === 0 || (index + 1) === companies.length) {
                logger.info(`--- Valuation Recalc Progress: ${index + 1} / ${companies.length} companies processed ---`);
             }
             index++; // Increment index
        }
        logger.info(`Phase 4: Valuation Recalculation complete. Metrics updated for ${valuationUpdates} companies.`);


    } catch (error) {
        logger.error(`Error during sequential daily update run: ${error.message}`, { stack: error.stack });
    } finally {
        const duration = (Date.now() - startTime) / 1000;
        logger.info(`Daily data update run finished in ${duration.toFixed(1)} seconds.`);
        isProcessing = false;
    }
}

// --- Lock File and Execution / Scheduling ---
// ... (main, cleanup, handleShutdown, global error handlers remain the same) ...
async function main(runNow = false) { /* ... */ if (fs.existsSync(LOCK_FILE)) { const lockContent = fs.readFileSync(LOCK_FILE, 'utf8'); logger.warn(`Lock file exists. Running since: ${lockContent.split(': ')[1] || 'unknown'}. Exiting.`); if (db) await connectDb().then(d => d.close()).catch(e => logger.error(`Error closing DB on lock exit: ${e.message}`)); db = null; return; } let lockFd; try { lockFd = fs.openSync(LOCK_FILE, 'wx'); fs.writeSync(lockFd, `Running since: ${new Date().toISOString()} PID: ${process.pid}`); fs.closeSync(lockFd); logger.info('Lock file created.'); if (runNow) { logger.info('Executing initial run...'); await runDailyUpdates(); logger.info('Initial run complete.'); } } catch (err) { if (err.code === 'EEXIST') { logger.warn('Lock file appeared after check. Exiting.'); if (db) await connectDb().then(d => d.close()).catch(e => logger.error(`Error closing DB on lock exit: ${e.message}`)); db = null; } else { logger.error(`Critical error during main setup/lock: ${err.message}`, { stack: err.stack }); await cleanup(); } return; } if (!runOnce && !runNowAndSchedule) { await cleanup(); } }
async function cleanup() { /* ... */ logger.info('Running cleanup...'); try { if (fs.existsSync(LOCK_FILE)) { fs.unlinkSync(LOCK_FILE); logger.info('Lock file removed.'); } } catch (unlinkErr) { logger.error(`Error removing lock file: ${unlinkErr.message}`); } return new Promise((resolve) => { if (db) { logger.debug("Attempting DB close..."); db.close((err) => { if (err) logger.error(`DB close error: ${err.message}`); else logger.info('DB connection closed.'); db = null; resolve(); }); } else { logger.info('DB connection already closed.'); resolve(); } }); }
const runOnce = process.argv.includes('--once'); const runNowAndSchedule = process.argv.includes('--run-now'); let activeCronTask = null;
if (runOnce) { logger.info('Running --once mode.'); main(true).catch(async (e) => { logger.error(`Error in --once: ${e.message}`, { stack: e.stack }); process.exitCode = 1; }).finally(async () => { logger.info('--- Run Once Mode Finished ---'); await cleanup(); }); } else { connectDb().then(() => { logger.info(`Scheduled mode. Cron: "${CRON_SCHEDULE}" (${CRON_TIMEZONE}).`); if (runNowAndSchedule) { logger.info("`--run-now`: Initial run now..."); main(true).catch(e => logger.error(`Error during initial run: ${e.message}`, { stack: e.stack })); } else { logger.info("Waiting for schedule. Use --run-now for immediate exec."); } activeCronTask = cron.schedule(CRON_SCHEDULE, async () => { logger.info(`Cron triggered: ${new Date().toISOString()}`); if (!isProcessing && !fs.existsSync(LOCK_FILE)) { await main(true); } else { logger.warn("Skipping cron run: Lock file exists or still processing."); } }, { scheduled: true, timezone: CRON_TIMEZONE }); logger.info('Cron scheduled. Keep process running (Ctrl+C to exit).'); process.stdin.resume(); }).catch(e => { logger.error(`Initial DB connect failed: ${e.message}`); process.exit(1); }); process.on('SIGINT', () => handleShutdown('SIGINT', activeCronTask)); process.on('SIGTERM', () => handleShutdown('SIGTERM', activeCronTask)); }
async function handleShutdown(signal, task = null) { /* ... */ if (isShuttingDown) return; isShuttingDown = true; logger.info(`Received ${signal}. Shutting down...`); if (task) { task.stop(); logger.info('Stopped cron task.'); } await cleanup(); logger.info('Shutdown complete.'); process.exit(0); }
process.on('uncaughtException', async (err) => { /* ... */ logger.error(`UNCAUGHT EXCEPTION: ${err.message}`, { stack: err.stack }); if (!isShuttingDown) { await handleShutdown('uncaughtException', activeCronTask); } process.exit(1); });
process.on('unhandledRejection', async (reason, promise) => { /* ... */ logger.error('UNHANDLED REJECTION:', { reason: reason?.message || reason, stack: reason?.stack }); if (!isShuttingDown) { await handleShutdown('unhandledRejection', activeCronTask); } process.exit(1); });