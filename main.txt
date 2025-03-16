const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { ensureLoggedIn } = require('./auth');
const { switchTab } = require('./navigation');
const { scrapeNetworkData, extractFinancials } = require('./scraper');
const { upsertFinancials } = require('./db');

puppeteer.use(StealthPlugin());

async function run() {
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();
  const ticker = 'AAB.TO';
  const url = `https://www.barrons.com/market-data/stocks/${ticker.replace('.TO', '').toLowerCase()}/financials?countrycode=ca`;

  await ensureLoggedIn(page, ticker, url);
  const networkData = await scrapeNetworkData(page, ticker);

  const tabs = [
    { name: 'Income Statement', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:nth-child(2)' },
    { name: 'Balance Sheet', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:nth-child(3)' },
    { name: 'Cash Flow Statement', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:nth-child(4)' }
  ];

  const financialData = {};
  for (const tab of tabs) {
    await switchTab(page, ticker, tab);
    const tabData = await extractFinancials(page, ticker, tab);
    Object.assign(financialData, tabData);
  }

  await upsertFinancials(1, financialData); // Replace 1 with dynamic company_id
  await browser.close();
}

run().catch(console.error);