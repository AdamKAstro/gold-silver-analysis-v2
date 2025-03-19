const fs = require('fs').promises;
const fsExtra = require('fs-extra');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const pdf2json = require('pdf2json');
const Tesseract = require('tesseract.js');
const { promisify } = require('util');
const { Mutex } = require('async-mutex');
const { PDFDocument } = require('pdf-lib');
const pdfjsLib = require('pdfjs-dist');

// Configuration
const PDF_DIR = './public/data/PDFs/GoodCandidates';
const LOG_FILE = 'extract_log.txt';
const CHUNK_SIZE = 10; // Increased to cover more pages per chunk
const EXTRACT_TIMEOUT_MS = { small: 60000, large: 600000 };
const MAX_RETRIES = 5;
const RETRY_DELAY_MS = 5000;
const THREE_MONTHS_MS = 3 * 30 * 24 * 60 * 60 * 1000;
const STANDARD_FONT_DATA_URL = 'node_modules/pdfjs-dist/standard_fonts/';

// Logging with mutex
const logMutex = new Mutex();
async function log(message, level = 'INFO') {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] [${level}] ${message}\n`;
    await logMutex.runExclusive(async () => {
        await fs.appendFile(LOG_FILE, logMessage);
        console.log(logMessage.trim());
    });
}

// Database connection
const db = new sqlite3.Database('./mining_companies.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) throw new Error(`DB connection failed: ${err.message}`);
    log('Connected to mining_companies.db.');
});

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

async function isValidPdf(filePath) {
    try {
        const dataBuffer = await fs.readFile(filePath);
        await PDFDocument.load(dataBuffer);
        return true;
    } catch (error) {
        await log(`Invalid PDF structure for ${filePath}: ${error.message}`, 'WARN');
        return false;
    }
}

async function extractResourceData(filePath, sizeProxy, retries = 0) {
    if (!(await isValidPdf(filePath))) return null;

    try {
        const timeoutMs = EXTRACT_TIMEOUT_MS[sizeProxy] || EXTRACT_TIMEOUT_MS.small;
        const stats = await fs.stat(filePath);
        const fileSizeMB = stats.size / (1024 * 1024);
        let chunks = [];

        if (fileSizeMB > 50) {
            await log(`Splitting large PDF ${filePath} (${fileSizeMB} MB)...`, 'INFO');
            chunks = await splitPdf(filePath);
        } else {
            chunks = [{ path: filePath, startPage: 0, endPage: null }];
        }

        let textContent = '';
        for (const chunk of chunks) {
            const pdfParser = new pdf2json();
            const parsePdf = promisify(pdfParser.parseBuffer.bind(pdfParser));
            const dataBuffer = await fs.readFile(chunk.path);

            const pdfData = await Promise.race([
                parsePdf(dataBuffer),
                new Promise((_, reject) => setTimeout(() => reject(new Error('PDF parsing timeout')), timeoutMs))
            ]).catch(async (err) => {
                await log(`pdf2json failed for ${chunk.path}: ${err.message}, trying pdfjs`, 'WARN');
                const uint8Array = new Uint8Array(dataBuffer);
                const pdf = await pdfjsLib.getDocument({ data: uint8Array, disableWorker: true, standardFontDataUrl: STANDARD_FONT_DATA_URL }).promise;
                const pageCount = pdf.numPages;
                const startPage = chunk.startPage + 1; // 1-based index
                const endPage = chunk.endPage !== null ? chunk.endPage + 1 : pageCount;
                const texts = await Promise.all(
                    Array.from({ length: endPage - startPage + 1 }, (_, i) => startPage + i).map(async (num) => {
                        const page = await pdf.getPage(num);
                        const textContent = await page.getTextContent();
                        const pageText = textContent.items.map(item => item.str).join(' ');
                        await log(`Extracted page ${num} from ${chunk.path}: ${pageText.substring(0, 200)}...`, 'DEBUG');
                        return pageText;
                    })
                );
                const combinedText = texts.join('\n');
                await log(`pdfjs extracted from ${chunk.path} (pages ${startPage}-${endPage}): ${combinedText.substring(0, 500)}...`, 'DEBUG');
                return { formImage: { Pages: [{ Texts: [{ R: [{ T: encodeURIComponent(combinedText) }] }] }] } };
            }).catch(async (err) => {
                await log(`pdfjs failed for ${chunk.path}: ${err.message}, trying OCR`, 'WARN');
                const pdfDoc = await PDFDocument.load(dataBuffer);
                const pageCount = pdfDoc.getPageCount();
                const startPage = chunk.startPage;
                const endPage = chunk.endPage !== null ? chunk.endPage : pageCount - 1;
                let ocrText = '';
                for (let i = startPage; i <= endPage; i++) {
                    const page = pdfDoc.getPage(i);
                    const viewport = page.getViewport({ scale: 2.0 });
                    const canvas = require('canvas').createCanvas(viewport.width, viewport.height);
                    await page.render({ canvasContext: canvas.getContext('2d'), viewport }).promise;
                    const { data: { text } } = await Tesseract.recognize(canvas.toBuffer('image/png'), 'eng');
                    ocrText += text + '\n';
                }
                await log(`OCR extracted from ${chunk.path} (pages ${startPage + 1}-${endPage + 1}): ${ocrText.substring(0, 500)}...`, 'DEBUG');
                return { formImage: { Pages: [{ Texts: [{ R: [{ T: encodeURIComponent(ocrText) }] }] }] } };
            });

            pdfData.formImage.Pages.forEach(page => {
                page.Texts.forEach(text => {
                    textContent += decodeURIComponent(text.R[0].T) + '\n';
                });
            });

            if (chunk.path !== filePath) await fs.unlink(chunk.path).catch(err => log(`Failed to delete ${chunk.path}: ${err.message}`, 'WARN'));
        }

        await log(`Full text extracted from ${filePath}: ${textContent.substring(0, 2000)}...`, 'DEBUG'); // Even longer snippet
        const docDate = parseDate(textContent) || new Date().toISOString().split('T')[0];
        const projectData = await parseProjectData(textContent, docDate);
        return projectData;
    } catch (error) {
        await log(`Extraction failed for ${filePath}: ${error.message}`, 'ERROR');
        if (retries < MAX_RETRIES) {
            await log(`Retrying ${filePath} (${retries + 1}/${MAX_RETRIES})...`, 'WARN');
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * (retries + 1)));
            return extractResourceData(filePath, sizeProxy, retries + 1);
        }
        return null;
    }
}

function parseDate(text) {
    const dateMatch = text.match(/(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}/i);
    return dateMatch ? new Date(dateMatch[0]).toISOString().split('T')[0] : null;
}

async function parseProjectData(text, docDate) {
    const projectData = {};
    const lines = text.split('\n');
    let currentProject = null;

    const patterns = [
        { type: 'reserves', regex: /(reserve[s]?|proven|probable).*?(\d+\.?\d*)\s*(moz|koz|t)\s*(gold|au|silver|ag|copper|cu|manganese|mn)/i },
        { type: 'measured', regex: /measured.*?(\d+\.?\d*)\s*(moz|koz|t)\s*(gold|au|silver|ag|copper|cu|manganese|mn)/i },
        { type: 'indicated', regex: /indicated.*?(\d+\.?\d*)\s*(moz|koz|t)\s*(gold|au|silver|ag|copper|cu|manganese|mn)/i },
        { type: 'inferred', regex: /inferred.*?(\d+\.?\d*)\s*(moz|koz|t)\s*(gold|au|silver|ag|copper|cu|manganese|mn)/i },
        { type: 'measured', regex: /(measured|meas).*?(\d+\.?\d*)\s*(moz|koz|t)\s*(ag|silver|cu|copper|mn|manganese)/i },
        { type: 'indicated', regex: /(indicated|ind).*?(\d+\.?\d*)\s*(moz|koz|t)\s*(ag|silver|cu|copper|mn|manganese)/i },
        { type: 'inferred', regex: /(inferred|inf).*?(\d+\.?\d*)\s*(moz|koz|t)\s*(ag|silver|cu|copper|mn|manganese)/i },
        { type: 'reserves', regex: /(\d+\.?\d*)\s*(moz|koz|t)\s*(ag|silver|cu|copper|mn|manganese).*?(reserve[s]?|proven|probable)/i },
        { type: 'resources', regex: /(\d+\.?\d*)\s*(moz|koz|t)\s*(ag|silver|cu|copper|mn|manganese).*?(resource[s]?|meas|ind|inf)/i },
    ];

    for (const line of lines) {
        const projectMatch = line.match(/(berenguela|challacollo)/i);
        if (projectMatch) currentProject = projectMatch[1].toLowerCase();

        if (currentProject) {
            projectData[currentProject] = projectData[currentProject] || { reserves: {}, resources: {}, date: docDate };
            for (const pattern of patterns) {
                const match = line.match(pattern.regex);
                if (match) {
                    const value = convertToAuEq(parseFloat(match[1] || match[2]), match[2] || match[3], match[3] || match[4]);
                    const mineral = (match[3] || match[4]).toLowerCase();
                    if (pattern.type === 'reserves') {
                        projectData[currentProject].reserves[mineral] = (projectData[currentProject].reserves[mineral] || 0) + value;
                    } else {
                        projectData[currentProject].resources[pattern.type] = projectData[currentProject].resources[pattern.type] || {};
                        projectData[currentProject].resources[pattern.type][mineral] = (projectData[currentProject].resources[pattern.type][mineral] || 0) + value;
                    }
                    await log(`Matched ${currentProject} ${pattern.type} ${mineral}: ${value} AuEq from line: ${line}`, 'DEBUG');
                }
            }
        }
    }
    return projectData;
}

function convertToAuEq(value, unit, mineral) {
    const conversionRates = { gold: 1, au: 1, silver: 0.0125, ag: 0.0125, copper: 0.004, cu: 0.004, manganese: 0.0001, mn: 0.0001 };
    let mozValue = unit === 'moz' ? value : unit === 'koz' ? value / 1000 : value * 0.0321507 / 1000000;
    return mozValue * (conversionRates[mineral.toLowerCase()] || 1);
}

function aggregateTotals(projectData) {
    const totals = {
        reserves_precious_aueq_moz: 0, measured_indicated_precious_aueq_moz: 0, resources_precious_aueq_moz: 0,
        reserves_non_precious_aueq_moz: 0, measured_indicated_non_precious_aueq_moz: 0, resources_non_precious_aueq_moz: 0
    };

    for (const project of Object.values(projectData)) {
        ['gold', 'au', 'silver', 'ag'].forEach(m => {
            if (project.reserves[m]) totals.reserves_precious_aueq_moz += project.reserves[m];
            if (project.resources.measured?.[m]) totals.measured_indicated_precious_aueq_moz += project.resources.measured[m];
            if (project.resources.indicated?.[m]) totals.measured_indicated_precious_aueq_moz += project.resources.indicated[m];
            if (project.resources.inferred?.[m]) totals.resources_precious_aueq_moz += project.resources.inferred[m];
        });
        ['copper', 'cu', 'manganese', 'mn'].forEach(m => {
            if (project.reserves[m]) totals.reserves_non_precious_aueq_moz += project.reserves[m];
            if (project.resources.measured?.[m]) totals.measured_indicated_non_precious_aueq_moz += project.resources.measured[m];
            if (project.resources.indicated?.[m]) totals.measured_indicated_non_precious_aueq_moz += project.resources.indicated[m];
            if (project.resources.inferred?.[m]) totals.resources_non_precious_aueq_moz += project.resources.inferred[m];
        });
    }

    totals.reserves_total_aueq_moz = totals.reserves_precious_aueq_moz + totals.reserves_non_precious_aueq_moz;
    totals.measured_indicated_total_aueq_moz = totals.measured_indicated_precious_aueq_moz + totals.measured_indicated_non_precious_aueq_moz;
    totals.resources_total_aueq_moz = totals.resources_precious_aueq_moz + totals.resources_non_precious_aueq_moz;

    return totals;
}

async function upsertData(companyId, totals, docDate) {
    const now = new Date().toISOString();
    const existing = await new Promise((resolve, reject) => {
        db.get('SELECT * FROM mineral_estimates WHERE company_id = ?', [companyId], (err, row) => err ? reject(err) : resolve(row));
    });

    if (existing && (Date.now() - new Date(existing.last_updated).getTime()) < THREE_MONTHS_MS) {
        await log(`Skipping update for company_id ${companyId}: Data from ${existing.last_updated} is recent`, 'INFO');
        return;
    }

    if (!Object.values(totals).some(v => v > 0)) {
        await log(`Skipping update for company_id ${companyId}: No valid data - ${JSON.stringify(totals)}`, 'WARN');
        return;
    }

    const sql = `
        INSERT INTO mineral_estimates (
            company_id, reserves_precious_aueq_moz, measured_indicated_precious_aueq_moz, resources_precious_aueq_moz,
            reserves_non_precious_aueq_moz, measured_indicated_non_precious_aueq_moz, resources_non_precious_aueq_moz,
            reserves_total_aueq_moz, measured_indicated_total_aueq_moz, resources_total_aueq_moz, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET
            reserves_precious_aueq_moz = excluded.reserves_precious_aueq_moz,
            measured_indicated_precious_aueq_moz = excluded.measured_indicated_precious_aueq_moz,
            resources_precious_aueq_moz = excluded.resources_precious_aueq_moz,
            reserves_non_precious_aueq_moz = excluded.reserves_non_precious_aueq_moz,
            measured_indicated_non_precious_aueq_moz = excluded.measured_indicated_non_precious_aueq_moz,
            resources_non_precious_aueq_moz = excluded.resources_non_precious_aueq_moz,
            reserves_total_aueq_moz = excluded.reserves_total_aueq_moz,
            measured_indicated_total_aueq_moz = excluded.measured_indicated_total_aueq_moz,
            resources_total_aueq_moz = excluded.resources_total_aueq_moz,
            last_updated = excluded.last_updated
    `;
    await new Promise((resolve, reject) => {
        db.run(sql, [companyId, ...Object.values(totals), now], (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
    await log(`Updated mineral_estimates for company_id ${companyId} with ${JSON.stringify(totals)}`, 'INFO');
}

async function processPdfs() {
    const pdfFiles = await fs.readdir(PDF_DIR);
    for (const pdfFile of pdfFiles.filter(f => f.endsWith('.pdf'))) {
        const filePath = path.join(PDF_DIR, pdfFile);
        const ticker = pdfFile.split('_')[0];
        await log(`Processing file ${pdfFile} with ticker ${ticker}`, 'DEBUG');

        const company = await new Promise((resolve, reject) => {
            db.get('SELECT company_id FROM companies WHERE tsx_code = ?', [ticker], (err, row) => {
                if (err) reject(err);
                else resolve(row);
            });
        });

        if (!company) {
            await log(`No company found for ${ticker} in ${pdfFile}`, 'WARN');
            continue;
        }

        const companyId = company.company_id;
        await log(`Found company_id ${companyId} for ticker ${ticker}`, 'INFO');

        const sizeProxy = 'small';
        const projectData = await extractResourceData(filePath, sizeProxy);
        if (projectData && Object.keys(projectData).length > 0) {
            const totals = aggregateTotals(projectData);
            await upsertData(companyId, totals, projectData[Object.keys(projectData)[0]]?.date);
        } else {
            await log(`No valid project data extracted from ${pdfFile}`, 'WARN');
        }
    }
}

async function main() {
    process.on('unhandledRejection', (reason) => log(`Unhandled Rejection: ${reason.message || reason}`, 'ERROR'));
    await log('Starting PDF extraction...');
    try {
        await processPdfs();
        await log('Finished processing PDFs.', 'INFO');
    } catch (error) {
        await log(`Main process failed: ${error.message}`, 'ERROR');
    } finally {
        db.close((err) => err && log(`DB close failed: ${err.message}`, 'ERROR'));
    }
}

main().catch(async (error) => {
    await log(`Script failed: ${error.message}`, 'ERROR');
    process.exit(1);
});