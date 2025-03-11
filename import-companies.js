const fs = require('fs');
const { parse } = require('csv-parse');
const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('mining_companies.db');

// Helper to strip BOM from keys
const stripBom = (str) => str.replace(/^\ufeff/, '');

fs.createReadStream('public/data/companies.csv')
  .pipe(parse({ columns: true, skip_empty_lines: true, trim: true }))
  .on('data', (row) => {
    console.log('Raw row:', row); // Log raw data

    // Normalize column names by stripping BOM
    const normalizedRow = {};
    for (const key in row) {
      normalizedRow[stripBom(key)] = row[key];
    }
    console.log('Normalized row:', normalizedRow); // Log normalized data

    try {
      const ticker = normalizedRow.TICKER?.toUpperCase(); // Use normalized key
      const name = normalizedRow.NAME;
      const nameAlt = normalizedRow.NAMEALT || null;

      if (!ticker || !name) {
        console.error('Missing TICKER or NAME in normalized row:', normalizedRow);
        return;
      }

      db.run(
        `INSERT OR IGNORE INTO companies (tsx_code, company_name, name_alt, status)
         VALUES (?, ?, ?, 'explorer')`,
        [ticker, name, nameAlt],
        (err) => {
          if (err) {
            console.error(`Error inserting ${ticker}: ${err}`);
          } else {
            console.log(`Inserted ${ticker} successfully`);
          }
        }
      );
    } catch (err) {
      console.error('Error processing row:', normalizedRow, err);
    }
  })
  .on('end', () => {
    console.log('CSV import completed');
    db.close();
  })
  .on('error', (err) => {
    console.error('CSV parsing error:', err);
  });