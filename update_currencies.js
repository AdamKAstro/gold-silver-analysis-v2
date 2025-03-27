const axios = require('axios');
const sqlite3 = require('sqlite3').verbose(); // Use verbose for more detailed errors
const path = require('path');

// --- Configuration ---
const DB_PATH = path.resolve(__dirname, 'mining_companies.db'); // Assumes script is in the project root
// Using Frankfurter API - free, no API key needed for basic pairs
// It gets rates FROM the base currency TO others
const API_BASE_URL = 'https://api.frankfurter.app/latest';
const BASE_CURRENCIES = ['USD', 'CAD']; // Currencies to fetch rates FROM
const TARGET_CURRENCIES = ['USD', 'CAD']; // Currencies to fetch rates TO
// --- End Configuration ---

// Function to fetch exchange rates from the API
async function fetchExchangeRates(baseCurrency, targetCurrencies) {
    const targets = targetCurrencies.filter(c => c !== baseCurrency).join(',');
    if (!targets) {
        console.log(`No target currencies specified for base ${baseCurrency}, other than itself.`);
        return {}; // No rates needed if only targeting itself
    }
    const url = `${API_BASE_URL}?from=${baseCurrency}&to=${targets}`;
    console.log(`Fetching rates from: ${url}`);
    try {
        const response = await axios.get(url);
        if (response.data && response.data.rates) {
            console.log(`Successfully fetched rates from ${baseCurrency}:`, response.data.rates);
            return response.data.rates; // Returns object like { 'CAD': 1.37 }
        } else {
            console.error(`Invalid response format from API for ${baseCurrency}:`, response.data);
            return null;
        }
    } catch (error) {
        console.error(`Error fetching exchange rates for base ${baseCurrency}:`, error.response ? error.response.data : error.message);
        return null;
    }
}

// Function to update the database
async function updateDatabase(ratesToUpdate) {
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(DB_PATH, (err) => {
            if (err) {
                console.error('Error connecting to the database:', err.message);
                return reject(err);
            }
            console.log('Connected to the SQLite database.');
        });

        db.serialize(() => {
            // Prepare statement for insertion/replacement
            // rate_id is AUTOINCREMENT usually, so we don't specify it
            const stmt = db.prepare(`
                INSERT OR REPLACE INTO exchange_rates
                (from_currency, to_currency, rate, fetch_date)
                VALUES (?, ?, ?, ?)
            `);

            const now = new Date().toISOString(); // Use ISO format for DATETIME
            let operations = [];

            // ratesToUpdate is expected to be like: { 'USD': { 'CAD': 1.37 }, 'CAD': { 'USD': 0.73 } }
            for (const fromCurrency in ratesToUpdate) {
                for (const toCurrency in ratesToUpdate[fromCurrency]) {
                    const rate = ratesToUpdate[fromCurrency][toCurrency];
                    if (typeof rate === 'number' && isFinite(rate) && rate > 0) {
                        // Wrap each statement run in a promise
                         operations.push(new Promise((res, rej) => {
                            stmt.run(fromCurrency, toCurrency, rate, now, function(err) {
                                if (err) {
                                    console.error(`Error inserting/replacing ${fromCurrency}->${toCurrency}:`, err.message);
                                    rej(err);
                                } else {
                                    console.log(`Updated rate for ${fromCurrency} to ${toCurrency}: ${rate}`);
                                    res();
                                }
                            });
                        }));
                    } else {
                         console.warn(`Skipping invalid rate for ${fromCurrency}->${toCurrency}: ${rate}`);
                    }
                }
            }

             // Finalize the statement after creating all promise operations
            stmt.finalize((err) => {
                 if (err) {
                    console.error('Error finalizing statement:', err.message);
                    // Note: Promises might still be running, but we should report this error.
                 }
            });

            // Wait for all database operations to complete
            Promise.all(operations)
                .then(() => {
                    console.log('All database updates attempted.');
                    db.close((err) => {
                        if (err) {
                            console.error('Error closing the database:', err.message);
                            return reject(err);
                        }
                        console.log('Database connection closed.');
                        resolve();
                    });
                })
                .catch((err) => {
                    // Error occurred during one of the stmt.run calls
                     console.error('Error during database operations:', err);
                     db.close((closeErr) => {
                         if (closeErr) console.error('Error closing database after error:', closeErr.message);
                         reject(err); // Reject with the original operation error
                     });
                });
        });
    });
}


// Main function to orchestrate fetching and updating
async function runUpdate() {
    console.log('Starting currency update process...');
    let allRates = {}; // Structure: { 'USD': {'CAD': 1.37, ...}, 'CAD': {'USD': 0.73, ...} }

    for (const base of BASE_CURRENCIES) {
        const fetchedRates = await fetchExchangeRates(base, TARGET_CURRENCIES);
        if (fetchedRates) {
             allRates[base] = fetchedRates;
             // Calculate and add inverse rates if needed and not directly fetched
             for (const target in fetchedRates) {
                 if (BASE_CURRENCIES.includes(target) && !allRates[target]) {
                     allRates[target] = {}; // Initialize if not present
                 }
                 // Add inverse rate if the target is also a base and doesn't have the inverse yet
                 if (BASE_CURRENCIES.includes(target) && !allRates[target][base]) {
                     const inverseRate = 1 / fetchedRates[target];
                     if (isFinite(inverseRate) && inverseRate > 0) {
                          allRates[target][base] = inverseRate;
                          console.log(`Calculated inverse rate for ${target}->${base}: ${inverseRate}`);
                     } else {
                         console.warn(`Could not calculate valid inverse rate for ${target}->${base}`);
                     }
                 }
             }
        } else {
            console.error(`Failed to fetch rates for base currency ${base}. Aborting update.`);
            return; // Stop if any critical fetch fails
        }
    }


    if (Object.keys(allRates).length > 0) {
        try {
            await updateDatabase(allRates);
            console.log('Currency update process completed successfully.');
        } catch (error) {
            console.error('Currency update process failed during database update.');
        }
    } else {
        console.log('No rates were fetched, database not updated.');
    }
}

// --- Execute the script ---
runUpdate();