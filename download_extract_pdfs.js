const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs').promises;
const fsExtra = require('fs-extra');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
<<<<<<< HEAD
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const pdf2json = require('pdf2json');
const Tesseract = require('tesseract.js');
const { promisify } = require('util');
const { Mutex } = require('async-mutex');
const { PDFDocument } = require('pdf-lib');
const pdfjsLib = require('pdfjs-dist');
const { createCanvas } = require('canvas');

// Configure Puppeteer with stealth plugin
puppeteer.use(StealthPlugin());

// Configuration
const OUTPUT_DIR = './public/data/PDFs/';
const TEMP_DIR = './public/data/temp/';
=======
const RecaptchaPlugin = require('puppeteer-extra-plugin-recaptcha');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const pdf2json = require('pdf2json');
const Tesseract = require('tesseract.js'); // OCR for image-based PDFs
const { promisify } = require('util');
const { Mutex } = require('async-mutex'); // For thread-safe DB writes

// Configure Puppeteer with stealth and recaptcha plugins
puppeteer.use(StealthPlugin());
puppeteer.use(RecaptchaPlugin({ provider: { id: '2captcha', token: 'YOUR_2CAPTCHA_KEY' } }));

// Configuration
const OUTPUT_DIR = './public/data/PDFs/';
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
const LOG_FILE = 'resource_extraction_log.txt';
const ERROR_DB = './error_tracking.db';
const MAX_DEPTH = 3;
const DELAY_MS = 2000;
const MAX_RETRIES = 5;
<<<<<<< HEAD
const MAX_PDF_SIZE = 100 * 1024 * 1024;
const CHUNK_SIZE = 3;
const TIMEOUT_MS = 90000;
const EXTRACT_TIMEOUT_MS = {
    small: 30000,
    large: 360000  // Increased to 360s
};
const BATCH_SIZE = 5;
const RETRY_DELAY_MS = 5000;
const PRIORITY_PAGES = ['investors', 'financial', 'agm', 'reports', 'statements'];
const MARKET_CAP_THRESHOLD = 500000000;
const STANDARD_FONT_DATA_URL = './node_modules/pdfjs-dist/standard_fonts/'; // Local font data

// Ensure output and temp directories exist
fsExtra.ensureDirSync(OUTPUT_DIR);
fsExtra.ensureDirSync(TEMP_DIR);

// Logging function with mutex
=======
const MAX_PDF_SIZE = 100 * 1024 * 1024; // 100 MB
const TIMEOUT_MS = 90000; // 90 seconds timeout
const BATCH_SIZE = 5;
const RETRY_DELAY_MS = 5000; // Delay between retries for failed URLs

// Ensure output directory exists
fsExtra.ensureDirSync(OUTPUT_DIR);

// Logging function with mutex for thread safety
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
const logMutex = new Mutex();
async function log(message, level = 'INFO') {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] [${level}] ${message}\n`;
    await logMutex.runExclusive(async () => {
        await fs.appendFile(LOG_FILE, logMessage);
        console.log(logMessage.trim());
    });
}

<<<<<<< HEAD
// Initialize databases
=======
// Initialize databases with schema validation
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
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

<<<<<<< HEAD
// Load company URLs with market cap
async function loadCompanyUrls() {
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT c.company_id, c.tsx_code, cu.url, c.description,
                   f.market_cap_value, f.market_cap_currency
            FROM companies c 
            JOIN company_urls cu ON c.company_id = cu.company_id 
            LEFT JOIN financials f ON c.company_id = f.company_id
=======
// Load company URLs with validation
async function loadCompanyUrls() {
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT c.company_id, c.tsx_code, cu.url, c.description
            FROM companies c 
            JOIN company_urls cu ON c.company_id = cu.company_id 
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
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
<<<<<<< HEAD
                    description: row.description || '',
                    market_cap: row.market_cap_value || 0,
                    currency: row.market_cap_currency || 'CAD',
                    size_proxy: (row.market_cap_value || 0) >= MARKET_CAP_THRESHOLD ? 'large' : 'small'
=======
                    description: row.description || ''
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
                })));
            }
        });
    });
}

<<<<<<< HEAD
// Determine page context
=======
// Determine page context with weighted scoring
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
function determinePageContext(title, url) {
    const text = (title + ' ' + url).toLowerCase();
    const contextPatterns = [
        { regex: /mineral resource|mineral reserve|resource estimate|reserves/i, context: 'ResourceEstimate', score: 10 },
        { regex: /ni ?43-?101|technical report/i, context: 'NI43101', score: 9 },
<<<<<<< HEAD
        { regex: /projects|operations|mines/i, context: 'Projects', score: 7 },
        { regex: /investors|financial|agm|reports|statements/i, context: 'InvestorReports', score: 6 },
        { regex: /annual report|financials/i, context: 'AnnualReport', score: 5 },
        { regex: /documents/i, context: 'GeneralReports', score: 3 }
=======
        { regex: /projects|operations|mines/i, context: 'Projects', score: 5 },
        { regex: /investor|reports|documents/i, context: 'GeneralReports', score: 9 },
        { regex: /annual report|financials/i, context: 'AnnualReport', score: 10 }
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
    ];
    const matches = contextPatterns.map(p => ({ ...p, match: p.regex.test(text) }));
    const bestMatch = matches.reduce((best, curr) => 
        curr.match && curr.score > best.score ? curr : best, { context: 'Unknown', score: -1 });
    return bestMatch.context;
}

<<<<<<< HEAD
// Infer PDF relevance
function inferPdfRelevance(pdfUrl, linkText, pageContext, sourcePath = '') {
    const text = (pdfUrl + ' ' + linkText + ' ' + sourcePath).toLowerCase();
    const positiveSignals = [
=======
function inferPdfRelevance(pdfUrl, linkText, pageContext) {
    const text = (pdfUrl + ' ' + linkText).toLowerCase();
    const positiveSignals = [
        // High-priority terms from NI 43-101 and annual reports
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
        /ni ?43-?101|technical report/i,
        /mineral (?:reserve|resource)s?|proven|probable|measured|indicated|inferred/i,
        /reserves?|resources?|content|grade/i,
        /tonnes|kt|mt|g\/t|koz|moz/i,
        /gold|silver|au|ag|mn|cu|zn/i,
        /open pit|underground|total/i,
        /interests? of experts|cim definition standards/i,
<<<<<<< HEAD
        /annual information form|year ended december/i,
        /agm|financial statements/i
    ];
    const negativeSignals = [
        /labor|child|sustainability|esg|social|environmental/i,
        /policy|charter|letter|transmittal|plan|reminder/i
=======
        /annual information form|year ended december/i
    ];
    const negativeSignals = [
        /labor|child|sustainability|esg|social|environmental/i
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
    ];

    const positiveCount = positiveSignals.reduce((count, pattern) => count + (pattern.test(text) ? 1 : 0), 0);
    const negativeCount = negativeSignals.reduce((count, pattern) => count + (pattern.test(text) ? 1 : 0), 0);
    const contextWeight = {
        'ResourceEstimate': 3,
        'NI43101': 3,
<<<<<<< HEAD
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

// Split PDF into chunks
async function splitPdf(filePath) {
    const pdfBytes = await fs.readFile(filePath);
    const pdfDoc = await PDFDocument.load(pdfBytes);
    const pageCount = pdfDoc.getPageCount();
    const chunks = [];
    for (let i = 0; i < pageCount; i += CHUNK_SIZE) {
        const chunkDoc = await PDFDocument.create();
        const endPage = Math.min(i + CHUNK_SIZE - 1, pageCount - 1);
        const pages = await chunkDoc.copyPages(pdfDoc, Array.from({ length: endPage - i + 1 }, (_, j) => i + j));
        pages.forEach(page => chunkDoc.addPage(page));
        const chunkBytes = await chunkDoc.save();
        const chunkPath = `${filePath.replace('.pdf', `_chunk_${i}-${endPage}.pdf`)}`;
        await fs.writeFile(chunkPath, chunkBytes);
        chunks.push({ path: chunkPath, startPage: i, endPage });
    }
    return chunks;
}

// Validate PDF structure
async function isValidPdf(filePath) {
    try {
        const dataBuffer = await fs.readFile(filePath);
        await PDFDocument.load(dataBuffer);
        return true;
    } catch (error) {
        log(`Invalid PDF structure for ${filePath}: ${error.message}`, 'WARN');
        return false;
    }
}

// Extract resource data with pdfjs-dist and Tesseract fallback
async function extractResourceData(filePath, sizeProxy, retries = 0) {
    if (!(await isValidPdf(filePath))) return null;

    try {
        const timeoutMs = EXTRACT_TIMEOUT_MS[sizeProxy] || EXTRACT_TIMEOUT_MS.small;
        const stats = await fs.stat(filePath);
        const fileSizeMB = stats.size / (1024 * 1024);
        let chunks = [{ path: filePath, startPage: 0, endPage: null }];

        if (fileSizeMB > 50) {
            log(`Splitting large PDF ${filePath} (${fileSizeMB} MB) into chunks...`, 'INFO');
            chunks = await splitPdf(filePath);
        }
=======
        'Projects': 2,
        'GeneralReports': 1,
        'AnnualReport': 1,
        'Unknown': 0
    }[pageContext] || 0;

    const isRelevant = positiveCount + contextWeight >= 3 && negativeCount === 0; // Increased threshold to 3
    log(`Relevance check for ${pdfUrl}: Positive=${positiveCount}, Context=${pageContext} (Weight=${contextWeight}), Total=${positiveCount + contextWeight}, Negative=${negativeCount}, Relevant=${isRelevant}`, 'DEBUG');
    return isRelevant;
}

// Unit conversion with uncertainty tracking
const unitConversions = {
    moz: { toMoz: v => ({ value: v, uncertainty: 0 }), toKoz: v => ({ value: v * 1000, uncertainty: v * 0.01 }), toTonnes: (v, g) => ({ value: v * 32.1507 / (g || 1), uncertainty: v * 0.05 / (g || 1) }) },
    koz: { toMoz: v => ({ value: v * 0.001, uncertainty: v * 0.00001 }), toKoz: v => ({ value: v, uncertainty: 0 }), toTonnes: (v, g) => ({ value: v * 0.0321507 / (g || 1), uncertainty: v * 0.0005 / (g || 1) }) },
    tonnes: { toMoz: (v, g) => ({ value: v * (g || 1) / 32.1507, uncertainty: v * (g || 1) * 0.05 / 32.1507 }), toKoz: (v, g) => ({ value: v * (g || 1) / 0.0321507, uncertainty: v * (g || 1) * 0.05 / 0.0321507 }), toTonnes: v => ({ value: v, uncertainty: 0 }) }
};

function convertUnits(value, fromUnit, toUnit, grade = null) {
    if (!value || !fromUnit || !toUnit) return null;
    const conversion = unitConversions[fromUnit.toLowerCase()];
    if (!conversion) throw new Error(`Unknown unit: ${fromUnit}`);
    const fn = conversion[`to${toUnit.charAt(0).toUpperCase() + toUnit.slice(1).toLowerCase()}`];
    if (!fn) throw new Error(`Conversion from ${fromUnit} to ${toUnit} not supported`);
    const result = fn(value, grade);
    log(`Converted ${value} ${fromUnit} to ${result.value} ${toUnit} (uncertainty: ±${result.uncertainty}) with grade ${grade || 'default 1 g/t'}`, 'DEBUG');
    return result.value;
}

// Extract resource data with OCR fallback and multi-metal support
async function extractResourceData(filePath) {
    try {
        const pdfParser = new pdf2json();
        const parsePdf = promisify(pdfParser.parseBuffer.bind(pdfParser));
        const dataBuffer = await fs.readFile(filePath);
        let pdfData = await parsePdf(dataBuffer).catch(async (err) => {
            log(`PDF parsing failed, attempting OCR: ${err.message}`, 'WARN');
            const ocrResult = await Tesseract.recognize(filePath, 'eng', { logger: m => log(m, 'DEBUG') });
            return { formImage: { Pages: [{ Texts: ocrResult.data.lines.map(l => ({ R: [{ T: l.text }] })) }] } };
        });

        let textContent = '';
        pdfData.formImage.Pages.forEach(page => {
            page.Texts.forEach(text => {
                textContent += decodeURIComponent(text.R[0].T) + ' ';
            });
        });
        textContent = textContent.toLowerCase();
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)

        const resourceData = {
            gold: { reserves_moz: null, mi_moz: null, resources_moz: null, production_koz: null },
            silver: { reserves_moz: null, mi_moz: null, resources_moz: null, production_koz: null }
        };

<<<<<<< HEAD
        let textContent = '';
        for (const chunk of chunks) {
            log(`Processing chunk ${chunk.path} (pages ${chunk.startPage}-${chunk.endPage || 'end'})`, 'DEBUG');
            const pdfParser = new pdf2json();
            const parsePdf = promisify(pdfParser.parseBuffer.bind(pdfParser));
            const dataBuffer = await fs.readFile(chunk.path);

            let pdfData = await Promise.race([
                parsePdf(dataBuffer),
                new Promise((_, reject) => setTimeout(() => reject(new Error('PDF parsing timeout')), timeoutMs))
            ]).catch(async (err) => {
                log(`pdf2json parsing failed for ${chunk.path}: ${err.message}, attempting pdfjs-dist`, 'WARN');
                const uint8Array = new Uint8Array(dataBuffer);
                const loadingTask = pdfjsLib.getDocument({
                    data: uint8Array,
                    standardFontDataUrl: STANDARD_FONT_DATA_URL
                });
                const pdf = await loadingTask.promise;
                const pageNum = Math.min(chunk.endPage !== null ? chunk.endPage + 1 : pdf.numPages, chunk.startPage + CHUNK_SIZE);
                const textPromises = [];
                for (let pageNumIter = chunk.startPage + 1; pageNumIter <= pageNum; pageNumIter++) {
                    textPromises.push((async () => {
                        try {
                            const page = await pdf.getPage(pageNumIter);
                            const textContentPage = await page.getTextContent();
                            const text = textContentPage.items.map(item => item.str).join(' ');
                            log(`Extracted text from page ${pageNumIter} of ${chunk.path}: ${text.substring(0, 100)}...`, 'DEBUG');
                            return text;
                        } catch (pageErr) {
                            log(`Failed to extract text from page ${pageNumIter} of ${chunk.path}: ${pageErr.message}`, 'ERROR');
                            return '';
                        }
                    })());
                }
                const texts = await Promise.all(textPromises);
                return { formImage: { Pages: [{ Texts: [{ R: [{ T: encodeURIComponent(texts.join(' ')) }] }] }] } };
            }).catch(async (pdfjsErr) => {
                log(`pdfjs-dist failed for ${chunk.path}: ${pdfjsErr.message}, attempting Tesseract OCR`, 'ERROR');
                const pdfDoc = await PDFDocument.load(dataBuffer, { ignoreEncryption: true });
                const page = pdfDoc.getPage(0);
                const viewport = page.getViewport({ scale: 2.0 });
                const canvas = createCanvas(viewport.width, viewport.height);
                const context = canvas.getContext('2d');
                const renderContext = {
                    canvasContext: context,
                    viewport: viewport
                };
                await page.render(renderContext).promise;
                const imageData = canvas.toBuffer('image/png');
                const { data: { text } } = await Tesseract.recognize(imageData, 'eng', {
                    logger: m => log(`Tesseract (${chunk.path}, page ${chunk.startPage + 1}): ${JSON.stringify(m)}`, 'DEBUG')
                });
                return { formImage: { Pages: [{ Texts: [{ R: [{ T: encodeURIComponent(text) }] }] }] } };
            });

            pdfData.formImage.Pages.forEach(page => {
                page.Texts.forEach(text => {
                    textContent += decodeURIComponent(text.R[0].T) + ' ';
                });
            });

            if (chunk.path !== filePath) {
                await fs.unlink(chunk.path).catch(err => log(`Failed to delete chunk ${chunk.path}: ${err.message}`, 'WARN'));
            }
        }

        textContent = textContent.toLowerCase();
=======
        // Dynamic grade detection near resource mentions
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
        const gradeMatch = textContent.match(/grade.*?(\d+\.?\d*)\s*(g\/t)/i);
        const grade = gradeMatch ? parseFloat(gradeMatch[1]) : 1;

        const patterns = [
            { metal: 'gold', type: 'reserve', regex: /(?:reserve[s]?|proven|probable).*?(?:gold|au).*?(\d+\.?\d*)\s*(moz|koz|tonnes)/i, field: 'reserves_moz', targetUnit: 'moz' },
            { metal: 'silver', type: 'reserve', regex: /(?:reserve[s]?|proven|probable).*?(?:silver|ag).*?(\d+\.?\d*)\s*(moz|koz|tonnes)/i, field: 'reserves_moz', targetUnit: 'moz' },
            { metal: 'gold', type: 'mi', regex: /(?:measured|indicated|m\&i).*?(?:gold|au).*?(\d+\.?\d*)\s*(moz|koz|tonnes)/i, field: 'mi_moz', targetUnit: 'moz' },
            { metal: 'silver', type: 'mi', regex: /(?:measured|indicated|m\&i).*?(?:silver|ag).*?(\d+\.?\d*)\s*(moz|koz|tonnes)/i, field: 'mi_moz', targetUnit: 'moz' },
            { metal: 'gold', type: 'resource', regex: /(?:resource[s]?|inferred).*?(?:gold|au).*?(\d+\.?\d*)\s*(moz|koz|tonnes)/i, field: 'resources_moz', targetUnit: 'moz' },
            { metal: 'silver', type: 'resource', regex: /(?:resource[s]?|inferred).*?(?:silver|ag).*?(\d+\.?\d*)\s*(moz|koz|tonnes)/i, field: 'resources_moz', targetUnit: 'moz' },
            { metal: 'gold', type: 'production', regex: /(?:production|produced).*?(?:gold|au).*?(\d+\.?\d*)\s*(moz|koz|tonnes)/i, field: 'production_koz', targetUnit: 'koz' },
            { metal: 'silver', type: 'production', regex: /(?:production|produced).*?(?:silver|ag).*?(\d+\.?\d*)\s*(moz|koz|tonnes)/i, field: 'production_koz', targetUnit: 'koz' }
        ];

        for (const pattern of patterns) {
            const match = textContent.match(pattern.regex);
            if (match) {
                const value = parseFloat(match[1]);
                const unit = match[2].toLowerCase();
                const convertedValue = convertUnits(value, unit, pattern.targetUnit, unit === 'tonnes' ? grade : null);
                resourceData[pattern.metal][pattern.field] = resourceData[pattern.metal][pattern.field] 
                    ? resourceData[pattern.metal][pattern.field] + convertedValue 
                    : convertedValue;
<<<<<<< HEAD
                log(`Matched ${pattern.metal} ${pattern.type} (${pattern.field}) with value ${convertedValue} ${pattern.targetUnit}`, 'DEBUG');
=======
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
            }
        }

        return Object.values(resourceData.gold).some(v => v !== null) || Object.values(resourceData.silver).some(v => v !== null) ? resourceData : null;
    } catch (error) {
<<<<<<< HEAD
        await log(`Failed to extract data from PDF ${filePath}: ${error.message} (Stack: ${error.stack})`, 'ERROR');
        if (retries < MAX_RETRIES) {
            await log(`Retrying extraction for ${filePath} (${retries + 1}/${MAX_RETRIES}) with ${timeoutMs}ms timeout...`, 'WARN');
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * (retries + 1)));
            return extractResourceData(filePath, sizeProxy, retries + 1);
        }
=======
        await log(`Failed to extract data from PDF ${filePath}: ${error.message}`, 'ERROR');
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
        return null;
    }
}

<<<<<<< HEAD
// Unit conversion
const unitConversions = {
    moz: { toMoz: v => v, toKoz: v => v * 1000, toTonnes: (v, g) => v * 32.1507 / (g || 1) },
    koz: { toMoz: v => v * 0.001, toKoz: v => v, toTonnes: (v, g) => v * 0.0321507 / (g || 1) },
    tonnes: { toMoz: (v, g) => v * (g || 1) / 32.1507, toKoz: (v, g) => v * (g || 1) / 0.0321507, toTonnes: v => v }
};

function convertUnits(value, fromUnit, toUnit, grade = null) {
    if (!value || !fromUnit || !toUnit) return null;
    const conversion = unitConversions[fromUnit.toLowerCase()];
    if (!conversion) throw new Error(`Unknown unit: ${fromUnit}`);
    const fn = conversion[`to${toUnit.charAt(0).toUpperCase() + toUnit.slice(1).toLowerCase()}`];
    return fn ? fn(value, grade) : null;
}

// Cross-check and validate
function crossCheckData(resourceData, description) {
    const desc = description.toLowerCase();
    const metalsMentioned = { gold: /gold|au/i.test(desc), silver: /silver|ag/i.test(desc) };
=======
// Cross-check with company description
function crossCheckData(resourceData, description) {
    const desc = description.toLowerCase();
    const metalsMentioned = {
        gold: /gold|au/i.test(desc),
        silver: /silver|ag/i.test(desc)
    };
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
    return (!metalsMentioned.gold || resourceData.gold.reserves_moz || resourceData.gold.mi_moz || resourceData.gold.resources_moz || resourceData.gold.production_koz) &&
           (!metalsMentioned.silver || resourceData.silver.reserves_moz || resourceData.silver.mi_moz || resourceData.silver.resources_moz || resourceData.silver.production_koz);
}

<<<<<<< HEAD
function validateResourceData(data) {
    if (!data) return false;
    const bounds = { moz: 1e5, koz: 1e6 };
=======
// Validate extracted data with bounds
function validateResourceData(data) {
    if (!data) return false;
    const bounds = { moz: 1e5, koz: 1e6 }; // 100k Moz, 1M koz
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
    const checkBounds = (value, unit) => value === null || (value >= 0 && value < bounds[unit]);
    return Object.entries(data).every(([metal, metrics]) =>
        checkBounds(metrics.reserves_moz, 'moz') &&
        checkBounds(metrics.mi_moz, 'moz') &&
        checkBounds(metrics.resources_moz, 'moz') &&
        checkBounds(metrics.production_koz, 'koz')
    );
}

<<<<<<< HEAD
// Update database with check-and-upsert approach
=======
// Update database with atomic transactions
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
async function updateDatabase(companyId, resourceData, description) {
    if (!validateResourceData(resourceData) || !crossCheckData(resourceData, description)) {
        await log(`Invalid or inconsistent resource data for company ${companyId}: ${JSON.stringify(resourceData)}`, 'WARN');
        return;
    }

    const now = new Date().toISOString();
    const dbMutex = new Mutex();
    await dbMutex.runExclusive(async () => {
<<<<<<< HEAD
        await new Promise((resolve, reject) => db.run('BEGIN TRANSACTION', (err) => err ? reject(err) : resolve()));
        try {
            const goldEq = (resourceData.gold.reserves_moz || 0) + (resourceData.silver.reserves_moz || 0) * 0.0125;
=======
        await new Promise((resolve, reject) => {
            db.run('BEGIN TRANSACTION', (err) => err ? reject(err) : resolve());
        });

        try {
            const mineralSql = `
                INSERT INTO mineral_estimates (
                    company_id, reserves_precious_aueq_moz, measured_indicated_precious_aueq_moz,
                    resources_precious_aueq_moz, last_updated
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(company_id) DO UPDATE SET
                    reserves_precious_aueq_moz = COALESCE(excluded.reserves_precious_aueq_moz, reserves_precious_aueq_moz),
                    measured_indicated_precious_aueq_moz = COALESCE(excluded.measured_indicated_precious_aueq_moz, measured_indicated_precious_aueq_moz),
                    resources_precious_aueq_moz = COALESCE(excluded.resources_precious_aueq_moz, resources_precious_aueq_moz),
                    last_updated = excluded.last_updated
            `;
            const productionSql = `
                INSERT INTO production (
                    company_id, current_production_precious_aueq_koz, last_updated
                ) VALUES (?, ?, ?)
                ON CONFLICT(company_id) DO UPDATE SET
                    current_production_precious_aueq_koz = COALESCE(excluded.current_production_precious_aueq_koz, current_production_precious_aueq_koz),
                    last_updated = excluded.last_updated
            `;

            const goldEq = (resourceData.gold.reserves_moz || 0) + (resourceData.silver.reserves_moz || 0) * 0.0125; // Silver:Gold ratio ~80:1
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
            const miEq = (resourceData.gold.mi_moz || 0) + (resourceData.silver.mi_moz || 0) * 0.0125;
            const resourceEq = (resourceData.gold.resources_moz || 0) + (resourceData.silver.resources_moz || 0) * 0.0125;
            const prodEq = (resourceData.gold.production_koz || 0) + (resourceData.silver.production_koz || 0) * 0.0125;

<<<<<<< HEAD
            const mineralExists = await new Promise((resolve, reject) => {
                db.get(`SELECT estimate_id FROM mineral_estimates WHERE company_id = ?`, [companyId], (err, row) => {
                    if (err) reject(err);
                    resolve(!!row);
                });
            });

            if (mineralExists) {
                const updateMineralSql = `
                    UPDATE mineral_estimates
                    SET reserves_precious_aueq_moz = ?,
                        measured_indicated_precious_aueq_moz = ?,
                        resources_precious_aueq_moz = ?,
                        last_updated = ?
                    WHERE company_id = ?
                `;
                await new Promise((resolve, reject) => {
                    db.run(updateMineralSql, [goldEq || null, miEq || null, resourceEq || null, now, companyId], (err) => {
                        if (err) {
                            log(`Mineral update failed for company ${companyId}: ${err.message}`, 'ERROR');
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                });
            } else {
                const insertMineralSql = `
                    INSERT INTO mineral_estimates (
                        company_id, reserves_precious_aueq_moz, measured_indicated_precious_aueq_moz,
                        resources_precious_aueq_moz, last_updated
                    ) VALUES (?, ?, ?, ?, ?)
                `;
                await new Promise((resolve, reject) => {
                    db.run(insertMineralSql, [companyId, goldEq || null, miEq || null, resourceEq || null, now], (err) => {
                        if (err) {
                            log(`Mineral insert failed for company ${companyId}: ${err.message}`, 'ERROR');
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                });
            }

            if (prodEq) {
                const productionExists = await new Promise((resolve, reject) => {
                    db.get(`SELECT production_id FROM production WHERE company_id = ?`, [companyId], (err, row) => {
                        if (err) reject(err);
                        resolve(!!row);
                    });
                });

                if (productionExists) {
                    const updateProductionSql = `
                        UPDATE production
                        SET current_production_precious_aueq_koz = ?,
                            last_updated = ?
                        WHERE company_id = ?
                    `;
                    await new Promise((resolve, reject) => {
                        db.run(updateProductionSql, [prodEq, now, companyId], (err) => {
                            if (err) {
                                log(`Production update failed for company ${companyId}: ${err.message}`, 'ERROR');
                                reject(err);
                            } else {
                                resolve();
                            }
                        });
                    });
                } else {
                    const insertProductionSql = `
                        INSERT INTO production (
                            company_id, current_production_precious_aueq_koz, last_updated
                        ) VALUES (?, ?, ?)
                    `;
                    await new Promise((resolve, reject) => {
                        db.run(insertProductionSql, [companyId, prodEq, now], (err) => {
                            if (err) {
                                log(`Production insert failed for company ${companyId}: ${err.message}`, 'ERROR');
                                reject(err);
                            } else {
                                resolve();
                            }
                        });
                    });
                }
            }

            await new Promise((resolve, reject) => db.run('COMMIT', (err) => {
                if (err) log(`Commit failed: ${err.message}`, 'ERROR');
                resolve();
            }));
            await log(`Updated database for company ${companyId} with ${JSON.stringify({ goldEq, miEq, resourceEq, prodEq })}`);
        } catch (error) {
            await new Promise((resolve) => db.run('ROLLBACK', resolve));
            log(`Database transaction failed for company ${companyId}: ${error.message}`, 'ERROR');
=======
            await new Promise((resolve, reject) => {
                db.run(mineralSql, [companyId, goldEq || null, miEq || null, resourceEq || null, now], (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });

            if (prodEq) {
                await new Promise((resolve, reject) => {
                    db.run(productionSql, [companyId, prodEq, now], (err) => {
                        if (err) reject(err);
                        else resolve();
                    });
                });
            }

            await new Promise((resolve, reject) => {
                db.run('COMMIT', (err) => err ? reject(err) : resolve());
            });
            await log(`Updated database for company ${companyId} with ${JSON.stringify({ goldEq, miEq, resourceEq, prodEq })}`);
        } catch (error) {
            await new Promise((resolve) => db.run('ROLLBACK', resolve));
            throw error;
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
        }
    });
}

<<<<<<< HEAD
// Log errors
async function logError(companyId, url, errorMessage, retryCount = 0) {
    await new Promise((resolve, reject) => {
        errorDb.run(
            `INSERT INTO errors (company_id, url, error_message, retry_count, last_attempt) VALUES (?, ?, ?, ?, ?) 
             ON CONFLICT(url, company_id) DO UPDATE SET error_message = excluded.error_message, retry_count = excluded.retry_count, last_attempt = excluded.last_attempt`,
=======
// Log and manage errors with retry logic
async function logError(companyId, url, errorMessage, retryCount = 0) {
    await new Promise((resolve, reject) => {
        errorDb.run(
            `INSERT INTO errors (company_id, url, error_message, retry_count, last_attempt) 
             VALUES (?, ?, ?, ?, ?) 
             ON CONFLICT(url, company_id) DO UPDATE SET 
                error_message = excluded.error_message, 
                retry_count = excluded.retry_count, 
                last_attempt = excluded.last_attempt`,
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
            [companyId, url, errorMessage, retryCount, new Date().toISOString()],
            (err) => err ? reject(err) : resolve()
        );
    });
}

<<<<<<< HEAD
// Download PDF
async function downloadPdf(url, outputPath, companyId, retries = 0) {
    try {
        const response = await axios({
            url, method: 'GET', responseType: 'stream', timeout: TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'application/pdf' },
=======
// Download PDF with advanced error handling
async function downloadPdf(url, outputPath, companyId, retries = 0) {
    try {
        const response = await axios({
            url,
            method: 'GET',
            responseType: 'stream',
            timeout: TIMEOUT_MS,
            headers: { 
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/pdf'
            },
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
            validateStatus: status => status === 200
        });

        if (response.headers['content-length'] && parseInt(response.headers['content-length']) > MAX_PDF_SIZE) {
            throw new Error(`PDF exceeds size limit of ${MAX_PDF_SIZE / (1024 * 1024)} MB`);
        }

        const writer = fsExtra.createWriteStream(outputPath);
        response.data.pipe(writer);
<<<<<<< HEAD
=======

>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
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

<<<<<<< HEAD
// Crawl website
async function crawlWebsite(browser, url, companyId, ticker, description, sizeProxy, marketCap, visited = new Set(), depth = 0) {
=======
// Crawl website with extreme robustness
// Update crawlWebsite function
async function crawlWebsite(browser, url, companyId, ticker, description, visited = new Set(), depth = 0) {
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
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
<<<<<<< HEAD
=======
        await page.solveRecaptchas().catch(err => log(`Captcha solve failed: ${err.message}`, 'WARN'));
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)

        const pageTitle = await page.title();
        const pageContext = determinePageContext(pageTitle, url);

        const content = await page.content();
        const $ = cheerio.load(content);
        const linkElements = $('a[href]').toArray();

<<<<<<< HEAD
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
=======
        for (const elem of linkElements) {
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
            const href = $(elem).attr('href');
            if (!href) continue;

            const resolvedHref = new URL(href, url).href;
            if (resolvedHref.endsWith('.pdf')) {
                const linkText = $(elem).text().trim();
<<<<<<< HEAD
                const sourcePath = url;
                if (!inferPdfRelevance(resolvedHref, linkText, pageContext, sourcePath)) continue;

                const filenameBase = resolvedHref.split('/').pop();
                const tag = PRIORITY_PAGES.some(p => sourcePath.toLowerCase().includes(p)) ? 
                           `_${PRIORITY_PAGES.find(p => sourcePath.toLowerCase().includes(p))}` : '';
                const filename = `${ticker}_${filenameBase}${tag}.pdf`;
=======
                if (!inferPdfRelevance(resolvedHref, linkText, pageContext)) continue;

                const filename = `${ticker}_${resolvedHref.split('/').pop()}`;
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
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

                const resourceData = await Promise.race([
<<<<<<< HEAD
                    extractResourceData(outputPath, sizeProxy),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('Extraction timeout')), EXTRACT_TIMEOUT_MS[sizeProxy] || EXTRACT_TIMEOUT_MS.small))
=======
                    extractResourceData(outputPath),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('Extraction timeout')), 30000)) // 30s timeout
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
                ]).catch(err => {
                    log(`Extraction failed for ${outputPath}: ${err.message}`, 'ERROR');
                    return null;
                });
                if (resourceData) await updateDatabase(companyId, resourceData, description);
            } else if (resolvedHref.startsWith('http') && !visited.has(resolvedHref)) {
<<<<<<< HEAD
                await crawlWebsite(browser, resolvedHref, companyId, ticker, description, sizeProxy, marketCap, visited, depth + 1);
            }
            linkCount++;
=======
                await crawlWebsite(browser, resolvedHref, companyId, ticker, description, visited, depth + 1);
            }
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
        }
    } catch (error) {
        await logError(companyId, url, error.message);
        await log(`Failed to crawl ${url}: ${error.message}`, 'ERROR');
    } finally {
        if (page) await page.close().catch(err => log(`Failed to close page: ${err.message}`, 'ERROR'));
    }
}

<<<<<<< HEAD
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

=======

// Process companies with retry logic for failed URLs
async function processBatch(browser, companies) {
    const tasks = companies.map(company => async () => {
        const { company_id, ticker, url, description } = company;
        await log(`Crawling ${url} for ${ticker} (company_id: ${company_id})...`);
        await crawlWebsite(browser, url, company_id, ticker, description);

        // Retry failed URLs
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
        const failedUrls = await new Promise((resolve) => {
            errorDb.all(`SELECT url, retry_count FROM errors WHERE company_id = ? AND resolved = 0`, [company_id], (err, rows) => {
                resolve(err ? [] : rows.filter(r => r.retry_count < MAX_RETRIES));
            });
        });
        for (const { url: retryUrl } of failedUrls) {
            await log(`Retrying failed URL ${retryUrl} for ${ticker}...`);
<<<<<<< HEAD
            try {
                await crawlWebsite(browser, retryUrl, company_id, ticker, description, size_proxy, market_cap);
            } catch (error) {
                log(`Retry failed for ${retryUrl}: ${error.message}`, 'ERROR');
            }
=======
            await crawlWebsite(browser, retryUrl, company_id, ticker, description);
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
        }
    });
    await Promise.all(tasks.map(task => task().catch(err => log(`Task failed: ${err.message}`, 'ERROR'))));
}

<<<<<<< HEAD
// Main function with global error handling
async function main() {
    process.on('unhandledRejection', (reason, promise) => {
        log(`Unhandled Rejection at: ${promise} reason: ${reason.message || reason}`, 'ERROR');
    });

=======
// Update main function
async function main() {
>>>>>>> 8d71b98 (Deleted Barrons files, updated package files, added new files)
    await log('Starting resource extraction script...');
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
        await log('Resource extraction script completed.');
    }
}

// Run the script
main().catch(async (error) => {
    await log(`Script failed: ${error.message}`, 'ERROR');
    process.exit(1);
});