const winston = require('winston');
const path = require('path');

const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: path.join(__dirname, 'logs', 'navigation.log') })
  ]
});

const CONFIG = {
  delays: {
    tabSwitch: { min: 60000, max: 90000 },
    click: { min: 20000, max: 40000 }
  }
};

function delay(min, max) {
  const time = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise(resolve => setTimeout(resolve, time));
}

async function switchTab(page, ticker, tab) {
  const { name, selector } = tab;
  logger.debug({ ticker, message: `Switching to ${name}` });

  await delay(CONFIG.delays.tabSwitch.min, CONFIG.delays.tabSwitch.max);
  const tabButton = await page.$(selector);
  if (!tabButton) {
    logger.warn({ ticker, message: `${name} tab not found` });
    return false;
  }
  await tabButton.click();
  await page.waitForSelector('[data-id="FinancialTables_table"]', { timeout: 180000 });
  
  const htmlSnippet = await page.evaluate(() => document.body.innerHTML.slice(0, 500));
  logger.debug({ ticker, message: `${name} loaded`, html: htmlSnippet });
  return true;
}

module.exports = { switchTab };