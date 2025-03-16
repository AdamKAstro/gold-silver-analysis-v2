const sqlite3 = require('sqlite3').verbose();

const dbPath = 'C:\\Users\\akiil\\gold-silver-analysis-v2\\mining_companies.db';
const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE, err => {
  if (err) {
    console.error('Database connection failed:', err.message);
    return;
  }
  console.log('Connected to database.');
});

db.run(`ALTER TABLE financials ADD COLUMN free_cash_flow REAL`, function(err) {
  if (err) {
    console.error('Error adding column:', err.message);
  } else {
    console.log('Added free_cash_flow column to financials table.');
  }
  db.close(err => {
    if (err) console.error('Database close failed:', err.message);
    else console.log('Database connection closed.');
  });
});