const winston = require('winston');
const path = require('path');

const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: path.join(__dirname, 'logs', 'scraper.log') })
  ]
});

async function scrapeNetworkData(page, ticker) {
  const networkData = [];
  page.on('response', async response => {
    const url = response.url();
    if (url.includes('financials') && response.headers()['content-type']?.includes('application/json')) {
      try {
        const json = await response.json();
        networkData.push({ url, data: json });
        logger.debug({ ticker, message: 'Captured network data', url, data: JSON.stringify(json).slice(0, 200) });
      } catch (e) {}
    }
  });
  return networkData;
}

async function extractFinancials(page, ticker, tab) {
  const data = {};
  const rows = await page.evaluate(() => {
    const table = document.querySelector('[data-id="FinancialTables_table"]');
    return table ? Array.from(table.querySelectorAll('.table__Row-sc-1djjifq-2')).map(row => ({
      label: row.querySelector('.table__Cell-sc-1djjifq-5')?.textContent.trim().toLowerCase(),
      value: row.querySelectorAll('.table__Cell-sc-1djjifq-5')[5]?.textContent.trim() || ''
    })) : [];
  });

  rows.forEach(({ label, value }) => {
    if (label.includes('revenue') || label.includes('sales')) data.revenue_value = value;
    else if (label.includes('net income')) data.net_income_value = value;
    else if (label.includes('shares outstanding')) data.shares_outstanding = value;
    // Add more mappings as needed
  });

  logger.debug({ ticker, message: `Scraped ${tab.name}`, data });
  return data;
}

module.exports = { scrapeNetworkData, extractFinancials };