const sqlite3 = require('sqlite3').verbose();
const fs = require('fs').promises;
const path = require('path');
const util = require('util');


// Usage : node inspect-db-v2.js --startId=61 --endId=70


// --- Configuration ---
const DB_FILE = './mining_companies.db'; // Adjust if needed
const DEFAULT_CHUNK_SIZE = 15;
const TABLES_TO_INSPECT_BY_COMPANY = [
    'companies',
    'financials',
    'capital_structure',
    'mineral_estimates',
    'production',
    'costs',
    'valuation_metrics',
    'company_urls',
    'stock_prices' // We'll limit this one
];
const STOCK_PRICE_LIMIT = 5; // Show latest N stock prices per company

// --- Argument Parsing ---
function parseArgs() {
    const args = process.argv.slice(2);
    let startId = null;
    let endId = null;

    for (let i = 0; i < args.length; i++) {
        if (args[i].startsWith('--startId=')) {
            startId = parseInt(args[i].split('=')[1], 10);
        } else if (args[i].startsWith('--endId=')) {
            endId = parseInt(args[i].split('=')[1], 10);
        }
    }

    if (startId === null || isNaN(startId) || startId < 1) {
        startId = 1;
        console.warn(`Using default startId: ${startId}`);
    }
    if (endId === null || isNaN(endId) || endId < startId) {
        endId = startId + DEFAULT_CHUNK_SIZE - 1;
        console.warn(`Using default endId based on startId and chunk size: ${endId}`);
    }

    console.log(`Inspecting company IDs from ${startId} to ${endId}`);
    return { startId, endId };
}

// --- Utility Functions ---

/** Check if the database file exists */
async function checkDatabaseFile(filePath) {
    try {
        await fs.access(filePath);
        console.log(`Database file found: ${path.resolve(filePath)}`);
    } catch (error) {
        console.error(`Database file not found or inaccessible: ${filePath}`);
        console.error(`Error details: ${error.message}`);
        process.exit(1);
    }
}

// Promisify sqlite3 methods
// Note: db.run needs special handling if you need lastID or changes
const dbAll = (db, sql, params) => util.promisify(db.all.bind(db))(sql, params);
const dbGet = (db, sql, params) => util.promisify(db.get.bind(db))(sql, params);

/** Main Inspection Function */
async function inspectDatabaseChunk(startId, endId) {
    // Step 1: Verify DB file
    await checkDatabaseFile(DB_FILE);

    // Step 2: Connect (Readonly is safer for inspection)
    const db = new sqlite3.Database(DB_FILE, sqlite3.OPEN_READONLY, (err) => {
        if (err) {
            console.error(`Connection error: ${err.message}`);
            process.exit(1);
        }
        console.log('Connected to the database.');
    });

    try {
        // Step 3: Get and Print Schema Info Once
        console.log('\n=== Database Schema Summary ===');
        const tables = await dbAll(db, "SELECT name FROM sqlite_master WHERE type='table';", []);
        if (tables.length === 0) {
            console.log('No tables found in the database.');
            return; // Exit early if no tables
        }

        console.log(`Found ${tables.length} table(s): ${tables.map(t => t.name).join(', ')}`);
        for (const tableRow of tables) {
            const tableName = tableRow.name;
            console.log(`\n--- Schema for Table: ${tableName} ---`);
            const columns = await dbAll(db, `PRAGMA table_info(${tableName});`, []);
            columns.forEach(col => {
                console.log(`  - ${col.name}: ${col.type} (PK: ${col.pk}, NotNull: ${col.notnull})`);
            });
            const rowCountResult = await dbGet(db, `SELECT COUNT(*) as count FROM ${tableName};`, []);
            console.log(`  Total Rows: ${rowCountResult.count}`);
        }
        console.log('=== End Schema Summary ===\n');


        // Step 4: Get Target Company IDs for the Chunk
        console.log(`\n=== Fetching Data for Companies ${startId} to ${endId} ===`);
        const targetCompanies = await dbAll(db,
            `SELECT company_id FROM companies WHERE company_id >= ? AND company_id <= ? ORDER BY company_id;`,
            [startId, endId]
        );

        if (targetCompanies.length === 0) {
            console.log(`No companies found in the ID range ${startId}-${endId}.`);
            return; // Exit if no companies in range
        }

        console.log(`Found ${targetCompanies.length} companies in the specified range.`);

        // Step 5: Iterate through each company and fetch related data
        for (const company of targetCompanies) {
            const currentCompanyId = company.company_id;
            console.log(`\n\n=== START COMPANY DATA | ID: ${currentCompanyId} ===\n`);

            for (const tableName of TABLES_TO_INSPECT_BY_COMPANY) {
                let data = null;
                try {
                     // Check if table exists before querying (optional, but safer)
                     const tableExists = tables.find(t => t.name === tableName);
                     if (!tableExists) {
                         console.log(`--- Table: ${tableName} (Does not exist, skipping) ---`);
                         continue;
                     }

                    console.log(`--- Table: ${tableName} ---`);
                    // Use db.get for tables expected to have 0 or 1 row per company_id
                    if (['companies', 'financials', 'capital_structure', 'mineral_estimates', 'production', 'costs', 'valuation_metrics'].includes(tableName)) {
                        data = await dbGet(db, `SELECT * FROM ${tableName} WHERE company_id = ?;`, [currentCompanyId]);
                    }
                    // Use db.all for tables expected to have 0 or more rows
                    else if (tableName === 'company_urls') {
                        data = await dbAll(db, `SELECT * FROM ${tableName} WHERE company_id = ?;`, [currentCompanyId]);
                    }
                    // Special handling for stock_prices (limit results)
                    else if (tableName === 'stock_prices') {
                         data = await dbAll(db, `SELECT * FROM ${tableName} WHERE company_id = ? ORDER BY price_date DESC LIMIT ?;`, [currentCompanyId, STOCK_PRICE_LIMIT]);
                         console.log(` (Showing latest ${data?.length || 0} of potentially many)`);
                    }

                    // Print the fetched data
                    if (data && (!Array.isArray(data) || data.length > 0)) {
                         console.log(JSON.stringify(data, null, 2)); // Pretty print JSON
                    } else if (Array.isArray(data) && data.length === 0) {
                         console.log("[] // No records found for this company.");
                    }
                     else {
                        console.log("null // No record found for this company.");
                    }
                } catch (tableError) {
                    console.error(`Error fetching data for company ${currentCompanyId} from table ${tableName}: ${tableError.message}`);
                    console.log("null // Error occurred during fetch."); // Indicate error in output
                }
                 console.log(''); // Add a blank line for readability between tables
            }
            console.log(`=== END COMPANY DATA | ID: ${currentCompanyId} ===`);
        }

    } catch (error) {
        console.error(`\nError during inspection: ${error.message}`);
    } finally {
        // Step 6: Close the database connection
        db.close((err) => {
            if (err) console.error(`Error closing database: ${err.message}`);
            else console.log('\nDatabase connection closed.');
        });
    }
}

// --- Run the Inspection ---
console.log('Starting database inspection (v2)...\n');
const { startId, endId } = parseArgs();
inspectDatabaseChunk(startId, endId)
    .catch(err => console.error(`Unexpected error in main execution: ${err.message}`));