const sqlite3 = require('sqlite3').verbose();
const fs = require('fs').promises;
const path = require('path');
const util = require('util');


// Usage:     node inspect-db-v4byId.js --id=132

// Usage:     node inspect-db-v4byId.js --id=58

// Usage:     node inspect-db-v4byId.js --id=1




// --- Configuration ---
const DB_FILE = './mining_companies.db'; // Adjust if needed
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
    let companyId = null;

    for (let i = 0; i < args.length; i++) {
        if (args[i].startsWith('--id=')) {
            const val = parseInt(args[i].split('=')[1], 10);
             if (!isNaN(val) && val > 0) {
                companyId = val;
             }
            break; // Found the ID, no need to check further
        }
    }

    if (companyId === null) {
        console.error("Error: Please provide a company ID using --id=NUMBER");
        console.error("Example: node inspect-db-v4byId.js --id=132");
        process.exit(1);
    }

    console.log(`Inspecting data for Company ID: ${companyId}`);
    return { companyId };
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
const dbAll = (db, sql, params) => util.promisify(db.all.bind(db))(sql, params);
const dbGet = (db, sql, params) => util.promisify(db.get.bind(db))(sql, params);

/** Main Inspection Function */
async function inspectDatabaseById(companyId) {
    // Step 1: Verify DB file
    await checkDatabaseFile(DB_FILE);

    // Step 2: Connect (Readonly)
    const db = new sqlite3.Database(DB_FILE, sqlite3.OPEN_READONLY, (err) => {
        if (err) {
            console.error(`Connection error: ${err.message}`);
            process.exit(1);
        }
        console.log('Connected to the database.');
    });

    try {
        // Step 3: Get Schema Info (Optional, can be commented out if running often)
        console.log('\n=== Database Schema Summary ===');
        const tables = await dbAll(db, "SELECT name FROM sqlite_master WHERE type='table';", []);
        if (tables.length === 0) {
            console.log('No tables found in the database.');
            return; // Exit early
        }
        console.log(`Found ${tables.length} table(s): ${tables.map(t => t.name).join(', ')}`);
        // Optional: Add loop to print detailed schema if desired, like in v2/v3
        console.log('=== End Schema Summary ===\n');


        // Step 4: Verify Target Company ID Exists
        console.log(`\n=== Fetching Data for Company ID: ${companyId} ===`);
        const companyInfo = await dbGet(db,
            `SELECT company_id, company_name, tsx_code FROM companies WHERE company_id = ?;`,
            [companyId]
        );

        if (!companyInfo) {
            console.error(`Error: Company with ID ${companyId} not found in the 'companies' table.`);
            return; // Exit if company ID invalid
        }
        console.log(`Found Company: ${companyInfo.company_name} (${companyInfo.tsx_code})`);


        // Step 5: Fetch and print data for the specific company ID
        console.log(`\n\n=== START COMPANY DATA | ID: ${companyId} ===\n`);

        for (const tableName of TABLES_TO_INSPECT_BY_COMPANY) {
            let data = null;
            try {
                const tableExists = tables.find(t => t.name === tableName);
                if (!tableExists) {
                    console.log(`--- Table: ${tableName} (Does not exist, skipping) ---`);
                    continue;
                }

                console.log(`--- Table: ${tableName} ---`);
                // Use db.get for tables expected to have 0 or 1 row per company_id
                if (['companies', 'financials', 'capital_structure', 'mineral_estimates', 'production', 'costs', 'valuation_metrics'].includes(tableName)) {
                    data = await dbGet(db, `SELECT * FROM ${tableName} WHERE company_id = ?;`, [companyId]);
                }
                // Use db.all for tables expected to have 0 or more rows
                else if (tableName === 'company_urls') {
                    data = await dbAll(db, `SELECT * FROM ${tableName} WHERE company_id = ?;`, [companyId]);
                }
                // Special handling for stock_prices (limit results)
                else if (tableName === 'stock_prices') {
                    data = await dbAll(db, `SELECT * FROM ${tableName} WHERE company_id = ? ORDER BY price_date DESC LIMIT ?;`, [companyId, STOCK_PRICE_LIMIT]);
                    console.log(` (Showing latest ${data?.length || 0} of potentially many)`);
                }

                // Print the fetched data
                if (data && (!Array.isArray(data) || data.length > 0)) {
                    console.log(JSON.stringify(data, null, 2));
                } else if (Array.isArray(data) && data.length === 0) {
                     console.log("[] // No records found for this company.");
                } else {
                    console.log("null // No record found for this company.");
                }
            } catch (tableError) {
                console.error(`Error fetching data for company ${companyId} from table ${tableName}: ${tableError.message}`);
                console.log("null // Error occurred during fetch.");
            }
             console.log(''); // Add blank line
        }
        console.log(`=== END COMPANY DATA | ID: ${companyId} ===`);

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
console.log('Starting database inspection (v4 - By Specific ID)...\n');
const { companyId } = parseArgs();
inspectDatabaseById(companyId)
    .catch(err => console.error(`Unexpected error in main execution: ${err.message}`));