const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs').promises;
const fsExtra = require('fs-extra');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const { Mutex } = require('async-mutex');

// Configure Puppeteer with stealth plugin
puppeteer.use(StealthPlugin());

// Configuration
const OUTPUT_DIR = './public/data/PDFs/';
const TEMP_DIR = './public/data/temp/';
const LOG_FILE = 'download_log.txt';
const ERROR_DB = './error_tracking.db';
const MAX_DEPTH = 3;
const DELAY_MS = 2000;
const MAX_RETRIES = 5;
const MAX_PDF_SIZE = 100 * 1024 * 1024;
const TIMEOUT_MS = 90000;
const BATCH_SIZE = 5;
const RETRY_DELAY_MS = 5000;
const PRIORITY_PAGES = ['investors', 'financial', 'agm', 'reports', 'statements'];
const MARKET_CAP_THRESHOLD = 500000000;

// Ensure output and temp directories exist
fsExtra.ensureDirSync(OUTPUT_DIR);
fsExtra.ensureDirSync(TEMP_DIR);

// Logging function with mutex
const logMutex = new Mutex();
async function log(message, level = 'INFO') {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] [${level}] ${message}\n`;
    await logMutex.runExclusive(async () => {
        await fs.appendFile(LOG_FILE, logMessage);
        console.log(logMessage.trim());
    });
}

// Initialize databases
const db = new sqlite3.Database('./mining_companies.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) throw new Error(`Main DB connection failed: ${err.message}`);
    console.log('[INFO] Connected to mining_companies.db.');
});

const errorDb = new sqlite3.Database(ERROR_DB, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE, (err) => {
    if (err) throw new Error(`Error DB connection failed: ${err.message}`);
    errorDb.run(`
        CREATE TABLE IF NOT EXISTS errors (
            error_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            url TEXT,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            last_attempt DATETIME,
            resolved INTEGER DEFAULT 0,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);
    console.log('[INFO] Connected to error_tracking.db.');
});

// Load company URLs with market cap
async function loadCompanyUrls() {
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT c.company_id, c.tsx_code, cu.url, c.description,
                   f.market_cap_value, f.market_cap_currency
            FROM companies c 
            JOIN company_urls cu ON c.company_id = cu.company_id 
            LEFT JOIN financials f ON c.company_id = f.company_id
            WHERE cu.url_type = 'website' AND cu.url IS NOT NULL
        `;
        db.all(sql, [], (err, rows) => {
            if (err) {
                log(`Database query failed: ${err.message}`, 'ERROR');
                reject(err);
            } else {
                resolve(rows.map(row => ({
                    company_id: row.company_id,
                    ticker: row.tsx_code,
                    url: row.url,
                    description: row.description || '',
                    market_cap: row.market_cap_value || 0,
                    currency: row.market_cap_currency || 'CAD',
                    size_proxy: (row.market_cap_value || 0) >= MARKET_CAP_THRESHOLD ? 'large' : 'small'
                })));
            }
        });
    });
}

// Determine page context
function determinePageContext(title, url) {
    const text = (title + ' ' + url).toLowerCase();
    const contextPatterns = [
        { regex: /mineral resource|mineral reserve|resource estimate|reserves/i, context: 'ResourceEstimate', score: 10 },
        { regex: /ni ?43-?101|technical report/i, context: 'NI43101', score: 9 },
        { regex: /projects|operations|mines/i, context: 'Projects', score: 7 },
        { regex: /investors|financial|agm|reports|statements/i, context: 'InvestorReports', score: 6 },
        { regex: /annual report|financials/i, context: 'AnnualReport', score: 5 },
        { regex: /documents/i, context: 'GeneralReports', score: 3 }
    ];
    const matches = contextPatterns.map(p => ({ ...p, match: p.regex.test(text) }));
    const bestMatch = matches.reduce((best, curr) => 
        curr.match && curr.score > best.score ? curr : best, { context: 'Unknown', score: -1 });
    return bestMatch.context;
}

// Infer PDF relevance
function inferPdfRelevance(pdfUrl, linkText, pageContext, sourcePath = '') {
    const text = (pdfUrl + ' ' + linkText + ' ' + sourcePath).toLowerCase();
    const positiveSignals = [
        /ni ?43-?101|technical report/i,
        /mineral (?:reserve|resource)s?|proven|probable|measured|indicated|inferred/i,
        /reserves?|resources?|content|grade/i,
        /tonnes|kt|mt|g\/t|koz|moz/i,
        /gold|silver|au|ag|mn|cu|zn/i,
        /open pit|underground|total/i,
        /interests? of experts|cim definition standards/i,
        /annual information form|year ended december/i,
        /agm|financial statements/i
    ];
    const negativeSignals = [
        /labor|child|sustainability|esg|social|environmental/i,
        /policy|charter|letter|transmittal|plan|reminder/i
    ];

    const positiveCount = positiveSignals.reduce((count, pattern) => count + (pattern.test(text) ? 1 : 0), 0);
    const negativeCount = negativeSignals.reduce((count, pattern) => count + (pattern.test(text) ? 1 : 0), 0);
    const contextWeight = {
        'ResourceEstimate': 3,
        'NI43101': 3,
        'InvestorReports': 3,
        'Projects': 2,
        'AnnualReport': 2,
        'GeneralReports': 1,
        'Unknown': 0
    }[pageContext] || 0;
    const structuralBonus = PRIORITY_PAGES.some(p => text.includes(p)) ? 1 : 0;

    const isRelevant = positiveCount + contextWeight + structuralBonus >= 3 && negativeCount === 0;
    log(`Relevance check for ${pdfUrl}: Positive=${positiveCount}, Context=${pageContext} (Weight=${contextWeight}), Structural=${structuralBonus}, Total=${positiveCount + contextWeight + structuralBonus}, Negative=${negativeCount}, Relevant=${isRelevant}`, 'DEBUG');
    return isRelevant;
}

// Download PDF
async function downloadPdf(url, outputPath, companyId, retries = 0) {
    try {
        const response = await axios({
            url, method: 'GET', responseType: 'stream', timeout: TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'application/pdf' },
            validateStatus: status => status === 200
        });

        if (response.headers['content-length'] && parseInt(response.headers['content-length']) > MAX_PDF_SIZE) {
            throw new Error(`PDF exceeds size limit of ${MAX_PDF_SIZE / (1024 * 1024)} MB`);
        }

        const writer = fsExtra.createWriteStream(outputPath);
        response.data.pipe(writer);
        return await new Promise((resolve, reject) => {
            writer.on('finish', () => resolve(true));
            writer.on('error', (err) => reject(err));
        });
    } catch (error) {
        await logError(companyId, url, error.message, retries + 1);
        if (retries < MAX_RETRIES) {
            await log(`Retrying download for ${url} (${retries + 1}/${MAX_RETRIES}): ${error.message}`, 'WARN');
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * (retries + 1)));
            return downloadPdf(url, outputPath, companyId, retries + 1);
        }
        throw error;
    }
}

// Log errors
async function logError(companyId, url, errorMessage, retryCount = 0) {
    await new Promise((resolve, reject) => {
        errorDb.run(
            `INSERT INTO errors (company_id, url, error_message, retry_count, last_attempt) VALUES (?, ?, ?, ?, ?) 
             ON CONFLICT(url, company_id) DO UPDATE SET error_message = excluded.error_message, retry_count = excluded.retry_count, last_attempt = excluded.last_attempt`,
            [companyId, url, errorMessage, retryCount, new Date().toISOString()],
            (err) => err ? reject(err) : resolve()
        );
    });
}

// Crawl website
async function crawlWebsite(browser, url, companyId, ticker, description, sizeProxy, marketCap, visited = new Set(), depth = 0) {
    if (depth > MAX_DEPTH || visited.has(url)) return;

    visited.add(url);
    let page;
    try {
        page = await browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
        await page.setRequestInterception(true);
        page.on('request', (req) => (req.resourceType() === 'document' ? req.continue() : req.abort()));
        await Promise.race([
            page.goto(url, { waitUntil: 'networkidle2', timeout: TIMEOUT_MS }),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Navigation timeout')), TIMEOUT_MS))
        ]).catch(err => { throw new Error(`Navigation failed: ${err.message}`); });

        const pageTitle = await page.title();
        const pageContext = determinePageContext(pageTitle, url);

        const content = await page.content();
        const $ = cheerio.load(content);
        const linkElements = $('a[href]').toArray();

        const maxDepthForSmall = sizeProxy === 'small' ? 1 : MAX_DEPTH;
        if (depth === 0 || (sizeProxy === 'small' && depth <= maxDepthForSmall)) {
            for (const elem of linkElements) {
                const href = $(elem).attr('href');
                if (href && PRIORITY_PAGES.some(p => href.toLowerCase().includes(p))) {
                    const resolvedHref = new URL(href, url).href;
                    if (!visited.has(resolvedHref)) {
                        await crawlWebsite(browser, resolvedHref, companyId, ticker, description, sizeProxy, marketCap, visited, depth + 1);
                    }
                }
            }
        }

        const maxLinks = sizeProxy === 'large' ? 20 : 50;
        let linkCount = 0;
        for (const elem of linkElements) {
            if (linkCount >= maxLinks && sizeProxy === 'large') break;
            const href = $(elem).attr('href');
            if (!href) continue;

            const resolvedHref = new URL(href, url).href;
            if (resolvedHref.endsWith('.pdf')) {
                const linkText = $(elem).text().trim();
                const sourcePath = url;
                if (!inferPdfRelevance(resolvedHref, linkText, pageContext, sourcePath)) continue;

                const filenameBase = resolvedHref.split('/').pop();
                const tag = PRIORITY_PAGES.some(p => sourcePath.toLowerCase().includes(p)) ? 
                           `_${PRIORITY_PAGES.find(p => sourcePath.toLowerCase().includes(p))}` : '';
                const filename = `${ticker}_${filenameBase}${tag}.pdf`;
                const outputPath = path.join(OUTPUT_DIR, filename);

                if (!fsExtra.existsSync(outputPath)) {
                    try {
                        await downloadPdf(resolvedHref, outputPath, companyId);
                        await log(`Downloaded: ${outputPath}`);
                    } catch (error) {
                        await logError(companyId, resolvedHref, error.message);
                        await log(`Failed to download ${resolvedHref}: ${error.message}`, 'ERROR');
                        continue;
                    }
                }
            } else if (resolvedHref.startsWith('http') && !visited.has(resolvedHref)) {
                await crawlWebsite(browser, resolvedHref, companyId, ticker, description, sizeProxy, marketCap, visited, depth + 1);
            }
            linkCount++;
        }
    } catch (error) {
        await logError(companyId, url, error.message);
        await log(`Failed to crawl ${url}: ${error.message}`, 'ERROR');
    } finally {
        if (page) await page.close().catch(err => log(`Failed to close page: ${err.message}`, 'ERROR'));
    }
}

// Process batches
async function processBatch(browser, companies) {
    const tasks = companies.map(company => async () => {
        const { company_id, ticker, url, description, size_proxy, market_cap } = company;
        await log(`Crawling ${url} for ${ticker} (company_id: ${company_id}, size: ${size_proxy}, market_cap: ${market_cap} ${company.currency})...`);
        try {
            await crawlWebsite(browser, url, company_id, ticker, description, size_proxy, market_cap);
        } catch (error) {
            log(`Crawl failed for ${ticker} (company_id: ${company_id}): ${error.message}`, 'ERROR');
        }

        const failedUrls = await new Promise((resolve) => {
            errorDb.all(`SELECT url, retry_count FROM errors WHERE company_id = ? AND resolved = 0`, [company_id], (err, rows) => {
                resolve(err ? [] : rows.filter(r => r.retry_count < MAX_RETRIES));
            });
        });
        for (const { url: retryUrl } of failedUrls) {
            await log(`Retrying failed URL ${retryUrl} for ${ticker}...`);
            try {
                await crawlWebsite(browser, retryUrl, company_id, ticker, description, size_proxy, market_cap);
            } catch (error) {
                log(`Retry failed for ${retryUrl}: ${error.message}`, 'ERROR');
            }
        }
    });
    await Promise.all(tasks.map(task => task().catch(err => log(`Task failed: ${err.message}`, 'ERROR'))));
}

// Main function
async function main() {
    process.on('unhandledRejection', (reason, promise) => {
        log(`Unhandled Rejection at: ${promise} reason: ${reason.message || reason}`, 'ERROR');
    });

    await log('Starting PDF download script...');
    let browser;

    try {
        const companies = await loadCompanyUrls();
        browser = await puppeteer.launch({
            headless: true,
            timeout: TIMEOUT_MS,
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        });

        const totalCompanies = companies.length;
        log(`Processing ${totalCompanies} companies in batches of ${BATCH_SIZE}...`, 'INFO');
        for (let i = 0; i < companies.length; i += BATCH_SIZE) {
            const batch = companies.slice(i, i + BATCH_SIZE);
            try {
                await processBatch(browser, batch);
            } catch (error) {
                log(`Batch ${Math.floor(i / BATCH_SIZE) + 1} failed: ${error.message}`, 'ERROR');
            }
            const processed = Math.min(i + BATCH_SIZE, totalCompanies);
            log(`Completed batch ${Math.floor(i / BATCH_SIZE) + 1} of ${Math.ceil(totalCompanies / BATCH_SIZE)} (${processed}/${totalCompanies} companies)`, 'INFO');
            await new Promise(resolve => setTimeout(resolve, DELAY_MS * 2));
        }
        log(`Finished processing all ${totalCompanies} companies.`, 'INFO');
    } catch (error) {
        await log(`Main process failed: ${error.message}`, 'ERROR');
    } finally {
        if (browser) await browser.close().catch(err => log(`Failed to close browser: ${err.message}`, 'ERROR'));
        await Promise.all([
            new Promise((r) => db.close((err) => { if (err) log(`Failed to close main DB: ${err.message}`, 'ERROR'); r(); })),
            new Promise((r) => errorDb.close((err) => { if (err) log(`Failed to close error DB: ${err.message}`, 'ERROR'); r(); }))
        ]);
        await log('PDF download script completed.');
    }
}

// Run the script
main().catch(async (error) => {
    await log(`Script failed: ${error.message}`, 'ERROR');
    process.exit(1);
});