const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('C:\\Users\\akiil\\gold-silver-analysis-v2\\mining_companies.db');

async function upsertFinancials(companyId, data) {
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO financials (financial_id, company_id, revenue_value, net_income_value, shares_outstanding, last_updated, data_source)
    VALUES ((SELECT financial_id FROM financials WHERE company_id = ?), ?, ?, ?, ?, ?, ?)
  `);
  stmt.run([companyId, companyId, data.revenue_value, data.net_income_value, data.shares_outstanding, new Date().toISOString(), 'Barron\'s']);
  stmt.finalize();
}

module.exports = { upsertFinancials };