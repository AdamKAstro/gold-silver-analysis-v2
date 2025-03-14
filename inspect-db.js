const sqlite3 = require('sqlite3').verbose();
const fs = require('fs').promises;
const path = require('path');

// Configuration
const DB_FILE = './mining_companies.db'; // Replace with your actual database file path
const SAMPLE_ROWS = 3; // Number of sample rows to display per table

// Utility Functions

/** Check if the database file exists and is accessible */
async function checkDatabaseFile() {
    try {
        await fs.access(DB_FILE);
        console.log(`Database file found: ${path.resolve(DB_FILE)}`);
    } catch (error) {
        console.error(`Database file not found or inaccessible: ${DB_FILE}`);
        console.error(`Error details: ${error.message}`);
        process.exit(1);
    }
}

/** Inspect the database */
async function inspectDatabase() {
    // Step 1: Verify the database file
    await checkDatabaseFile();

    // Step 2: Connect to the database
    const db = new sqlite3.Database(DB_FILE, sqlite3.OPEN_READONLY, (err) => {
        if (err) {
            console.error(`Connection error: ${err.message}`);
            process.exit(1);
        }
        console.log('Connected to the database.');
    });

    try {
        // Step 3: Get all table names
        const tables = await new Promise((resolve, reject) => {
            db.all("SELECT name FROM sqlite_master WHERE type='table';", (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(row => row.name));
            });
        });

        // Step 4: Handle empty database case
        if (tables.length === 0) {
            console.log('\nNo tables found in the database.');
            console.log('Possible reasons:');
            console.log('  - The database is newly created and has no tables yet.');
            console.log('  - The wrong database file is being accessed.');
            console.log(`  - Current file path: ${path.resolve(DB_FILE)}`);
            console.log('Next steps:');
            console.log('  - Verify the file path matches your intended database.');
            console.log('  - Ensure your database has been initialized with tables.');
            return;
        }

        // Step 5: Inspect each table
        console.log(`\nFound ${tables.length} table(s) in the database:`);
        for (const table of tables) {
            console.log(`\n=== Inspecting Table: ${table} ===`);

            // Get column information
            const columns = await new Promise((resolve, reject) => {
                db.all(`PRAGMA table_info(${table});`, (err, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                });
            });
            console.log('Columns:');
            columns.forEach(col => {
                console.log(`  - ${col.name}: ${col.type} (Primary Key: ${col.pk ? 'Yes' : 'No'}, Nullable: ${col.notnull ? 'No' : 'Yes'})`);
            });

            // Get row count
            const rowCount = await new Promise((resolve, reject) => {
                db.get(`SELECT COUNT(*) as count FROM ${table};`, (err, row) => {
                    if (err) reject(err);
                    else resolve(row.count);
                });
            });
            console.log(`\nTotal Rows: ${rowCount}`);

            // Get sample data
            const sampleData = await new Promise((resolve, reject) => {
                db.all(`SELECT * FROM ${table} LIMIT ${SAMPLE_ROWS};`, (err, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                });
            });
            console.log('\nSample Data (First 5 Rows):');
            if (sampleData.length === 0) {
                console.log('  No data found in this table.');
            } else {
                sampleData.forEach((row, index) => {
                    console.log(`  Row ${index + 1}:`, JSON.stringify(row, null, 2));
                });
            }
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

// Run the inspection
console.log('Starting database inspection...\n');
inspectDatabase().catch(err => console.error(`Unexpected error: ${err.message}`));