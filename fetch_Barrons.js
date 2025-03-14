const puppeteer = require('puppeteer');
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const fs = require('fs').promises;

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'scraper.log' })
  ]
});

function delay(min, max) {
  const time = Math.floor(Math.random() * (max - min + 1)) + min;
  logger.debug({ message: `Delaying for ${time}ms` });
  return new Promise(resolve => setTimeout(resolve, time));
}

async function loadCookies() {
  try {
    const cookiesData = await fs.readFile('cookies.json', 'utf8');
    return JSON.parse(cookiesData);
  } catch (err) {
    return [];
  }
}

async function saveCookies(page) {
  try {
    const cookies = await page.cookies();
    await fs.writeFile('cookies.json', JSON.stringify(cookies, null, 2));
    logger.debug({ message: 'Cookies saved' });
  } catch (err) {
    logger.warn({ message: 'Failed to save cookies', error: err.message });
  }
}

const userAgents = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.91'
];

async function simulateMinimalBehavior(page) {
  logger.debug({ message: 'Simulating minimal human-like behavior' });
  try {
    const currentUrl = await page.url();
    if (!currentUrl.includes('barrons.com')) {
      throw new Error('Page navigated away unexpectedly');
    }
    await page.evaluate(() => true).catch(() => { throw new Error('Page target closed'); });

    const viewport = await page.evaluate(() => ({
      width: window.innerWidth,
      height: window.innerHeight
    }));

    await page.evaluate(() => { window.scrollBy(0, Math.random() * 500 + 200); }); // Increased scroll range
    await delay(2000, 4000); // Increased delay

    await page.mouse.move(Math.random() * viewport.width, Math.random() * viewport.height); // Mouse move
    await page.mouse.click(Math.random() * viewport.width * 0.7, Math.random() * viewport.height * 0.7);
    await delay(2000, 4000);
  } catch (err) {
    logger.warn({ message: 'Error in simulateMinimalBehavior', error: err.message });
    throw err;
  }
}

async function simulateContinuousBehavior(page, pauseSimulation) {
  logger.debug({ message: 'Starting continuous behavior simulation' });
  const interval = setInterval(async () => {
    if (pauseSimulation()) {
      logger.debug({ message: 'Continuous behavior simulation paused' });
      return;
    }
    try {
      await simulateMinimalBehavior(page);
      if (Math.random() < 0.3) { // Increased pause probability
        logger.debug({ message: 'Simulating reading pause' });
        await delay(10000, 20000); // Increased pause duration
      }
    } catch (err) {
      logger.warn({ message: 'Error in continuous behavior simulation', error: err.message });
      clearInterval(interval);
    }
  }, 15000); // Increased interval to 15s

  return interval;
}

async function fetchWithPuppeteer(company) {
  let browser;
  let page;
  let behaviorInterval;
  let isScraping = false;

  try {
    browser = await puppeteer.launch({
      headless: false,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--window-size=1920x1080',
        '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows',
        '--disable-renderer-backgrounding',
        '--enable-low-end-device-mode',
        '--shm-size=2gb'
      ]
    });
    page = await browser.newPage();

    const randomUserAgent = userAgents[Math.floor(Math.random() * userAgents.length)];
    await page.setUserAgent(randomUserAgent);
    logger.debug({ message: `Using user-agent: ${randomUserAgent}` });

    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
      Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
    });

    await page.setViewport({ width: 1920, height: 1080 });

    await page.setRequestInterception(true);
    page.on('request', request => {
      const resourceType = request.resourceType();
      const url = request.url();
      if (
        resourceType === 'media' ||
        url.includes('doubleclick') ||
        url.includes('linkedin') ||
        url.includes('google-analytics') ||
        url.includes('dianomi') // Block ad-related requests
      ) {
        request.abort();
      } else {
        request.continue();
      }
    });

    let networkData = [];
    page.on('response', async response => {
      const url = response.url();
      const contentType = response.headers()['content-type'] || '';
      if (
        contentType.includes('application/json') &&
        (url.includes('financials') || url.includes('data') || url.includes('api')) &&
        !url.includes('bam.nr-data.net')
      ) {
        try {
          const json = await response.json();
          networkData.push({ url, data: json });
          logger.debug({ message: 'Intercepted network data', url, data: JSON.stringify(json).substring(0, 100) + '...' });
        } catch (e) {
          logger.warn({ message: 'Failed to parse network response', url, error: e.message });
        }
      } else {
        logger.debug({ message: 'Skipped non-JSON or non-financial response', url, contentType });
      }
    });

    const cookies = await loadCookies();
    if (cookies.length > 0) {
      await page.setCookie(...cookies);
      logger.debug({ message: 'Loaded cookies' });
    }

    // Construct URL
    const baseTicker = company.tsx_code.replace(/\.([A-Z]{2})$/, '').toLowerCase();
    const fullTicker = company.tsx_code;
    const financialsUrl = `https://www.barrons.com/market-data/stocks/${baseTicker}/financials?countrycode=ca&mod=searchresults_companyquotes&mod=searchbar&search_keywords=${fullTicker}&search_statement_type=suggested`;

    // Try the financials URL
    let pageLoaded = false;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await page.goto(financialsUrl, { waitUntil: 'networkidle2', timeout: 60000 });
        await delay(10000, 20000); // Increased delay to handle dynamic content
        pageLoaded = await page.evaluate(() => {
          return document.querySelector('div.table__Cell-sc-1djjifq-5') || document.querySelector('button.ModuleSubNav__Tab-sc-n8aem8-2');
        });
        if (pageLoaded) break;
        logger.warn({ message: 'No financial data detected on financials page, retrying', ticker: company.tsx_code, attempt });
        await simulateMinimalBehavior(page); // Add behavior between retries
        await delay(15000, 30000); // Increased retry delay
      } catch (err) {
        logger.warn({ message: 'Financials URL load failed', ticker: company.tsx_code, error: err.message, attempt });
        if (attempt === 2) break;
        await simulateMinimalBehavior(page);
        await delay(15000, 30000);
      }
    }

    if (!pageLoaded) {
      logger.error({ message: 'No financial data available after retries, skipping', ticker: company.tsx_code });
      return null;
    }

    logger.info({ message: 'Page loaded', url: financialsUrl, title: await page.title() });
    await saveCookies(page);

    behaviorInterval = await simulateContinuousBehavior(page, () => isScraping);
    await delay(10000, 20000); // Increased initial delay

    const financialTabs = [
      { name: 'Overview', selector: '' },
      { name: 'Income Statement', label: 'Income Statement', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:-soup-contains("Income Statement")' },
      { name: 'Balance Sheet', label: 'Balance Sheet', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:-soup-contains("Balance Sheet")' },
      { name: 'Cash Flow Statement', label: 'Cash Flow Statement', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:-soup-contains("Cash Flow Statement")' }
    ];

    const shuffledTabs = financialTabs.slice(1).sort(() => Math.random() - 0.5);
    logger.debug({ message: 'Shuffled tab order', order: shuffledTabs.map(tab => tab.name) });

    const financialData = {};

    isScraping = true;
    try {
      await page.waitForSelector('div', { timeout: 15000 }); // Increased timeout
      const overviewData = await page.evaluate(() => {
        const data = {};
        const labels = document.querySelectorAll('div');
        for (const label of labels) {
          const text = label.textContent.trim();
          if (text === 'Market Value') {
            const value = label.nextElementSibling?.textContent.trim();
            if (value) data.market_cap_value = value;
          }
          if (text === 'Shares Outstanding') {
            const value = label.nextElementSibling?.textContent.trim();
            if (value) data.shares_outstanding = value;
          }
        }
        return data;
      });

      if (!overviewData.market_cap_value || !overviewData.shares_outstanding) {
        const fallbackData = await page.evaluate(() => {
          const data = {};
          const lines = document.body.innerText.split('\n').map(line => line.trim()).filter(line => line);
          lines.forEach(line => {
            const parts = line.split(/\s+/);
            const label = parts.slice(0, -1).join(' ').toLowerCase();
            const value = parts[parts.length - 1];
            if (value.match(/[\d.-]+[BKM]?/)) {
              if (label.includes('market value')) data.market_cap_value = value;
              else if (label.includes('shares outstanding')) data.shares_outstanding = value;
            }
          });
          return data;
        });
        Object.assign(overviewData, fallbackData);
      }

      Object.assign(financialData, overviewData);
      logger.debug({ message: 'Scraped Overview tab', data: overviewData });
    } catch (err) {
      logger.warn({ message: 'Failed to scrape Overview tab', error: err.message });
    }
    isScraping = false;

    const processNetworkData = (data) => {
      const tabData = {};
      if (data.blocks) {
        const financialBlock = data.blocks.find(block => block.$type === 'MarketData.FinancialStatementCard');
        if (financialBlock && financialBlock.sections) {
          financialBlock.sections.forEach(section => {
            section.items.forEach(item => {
              const displayName = item.displayName.toLowerCase();
              const latestValue = item.values && item.values.length >= 5 ? item.values[4].formatted : null;

              if (latestValue) {
                if (displayName.includes('sales') || displayName.includes('revenue')) tabData.revenue_value = latestValue;
                else if (displayName.includes('net income')) tabData.net_income_value = latestValue;
                else if (displayName.includes('cash') && displayName.includes('short term')) tabData.cash_value = latestValue;
                else if (displayName.includes('total liabilities')) tabData.liabilities = latestValue;
                else if (displayName.includes('non-convertible debt')) {
                  tabData.debt_value = tabData.debt_value 
                    ? (parseFloat(tabData.debt_value) + parseFloat(latestValue.replace(/[^\d.-]/g, ''))).toString() + 'B' 
                    : latestValue;
                } else if (displayName.includes('capitalized lease')) {
                  tabData.debt_value = tabData.debt_value 
                    ? (parseFloat(tabData.debt_value) + parseFloat(latestValue.replace(/[^\d.-]/g, ''))).toString() + 'B' 
                    : latestValue;
                } else if (displayName.includes('operating income')) tabData.operating_income = latestValue;
                else if (displayName.includes('ebitda')) tabData.ebitda = latestValue;
                else if (displayName.includes('free cash flow')) tabData.free_cash_flow = latestValue;
              }
            });
          });
        }
      }
      return tabData;
    };

    for (const tab of shuffledTabs) {
      if (Math.random() < 0.1) {
        logger.debug({ message: `Skipping tab: ${tab.name}` });
        continue;
      }

      isScraping = true;
      try {
        logger.info({ message: `Switching to tab: ${tab.name}` });
        await delay(15000, 30000); // Increased delay

        const tabButton = await page.evaluateHandle(tabLabel => {
          const buttons = Array.from(document.querySelectorAll('button.ModuleSubNav__Tab-sc-n8aem8-2'));
          return buttons.find(btn => btn.textContent.trim() === tabLabel);
        }, tab.label);

        if (!tabButton.asElement()) {
          logger.warn({ message: `Tab button for "${tab.name}" not found` });
          continue;
        }

        await tabButton.click();
        await page.waitForSelector('div.table__Cell-sc-1djjifq-5', { timeout: 30000 });
        await delay(20000, 40000);

        let tabData = {};
        networkData.forEach(({ data }) => {
          Object.assign(tabData, processNetworkData(data));
        });

        if (Object.keys(tabData).length === 0) {
          for (let attempt = 0; attempt < 3; attempt++) {
            try {
              await page.waitForSelector('div.table__Cell-sc-1djjifq-5', { timeout: 30000 });
              const domData = await page.evaluate(() => {
                const data = {};
                const cells = document.querySelectorAll('div.table__Cell-sc-1djjifq-5');
                let currentLabel = '';
                const rawTableData = Array.from(cells).map(cell => cell.textContent.trim()).join(' | ');

                for (let i = 0; i < cells.length; i++) {
                  const cellText = cells[i].textContent.trim();
                  const isLabelCell = !cells[i].classList.contains('fDHbHR') && !cells[i].classList.contains('fRQdPw');

                  if (isLabelCell) {
                    currentLabel = cellText.toLowerCase();
                  } else if (currentLabel) {
                    const nextCells = Array.from(cells).slice(i, i + 5);
                    if (nextCells.length >= 5) {
                      const latestValue = nextCells[4].textContent.trim();
                      if (currentLabel.includes('sales') || currentLabel.includes('revenue')) data.revenue_value = latestValue;
                      else if (currentLabel.includes('net income')) data.net_income_value = latestValue;
                      else if (currentLabel.includes('cash') && currentLabel.includes('short-term')) data.cash_value = latestValue;
                      else if (currentLabel.includes('total liabilities')) data.liabilities = latestValue;
                      else if (currentLabel.includes('non-convertible debt')) {
                        data.debt_value = data.debt_value ? (parseFloat(data.debt_value) + parseFloat(latestValue.replace(/[^\d.-]/g, ''))).toString() + 'B' : latestValue;
                      } else if (currentLabel.includes('capitalized lease')) {
                        data.debt_value = data.debt_value ? (parseFloat(data.debt_value) + parseFloat(latestValue.replace(/[^\d.-]/g, ''))).toString() + 'B' : latestValue;
                      } else if (currentLabel.includes('operating income')) data.operating_income = latestValue;
                      else if (currentLabel.includes('ebitda')) data.ebitda = latestValue;
                      else if (currentLabel.includes('free cash flow')) data.free_cash_flow = latestValue;
                      i += 4;
                    }
                    currentLabel = '';
                  }
                }
                return { data, rawTableData: rawTableData.substring(0, 200) + '...' };
              });
              Object.assign(tabData, domData.data);
              logger.debug({ message: `Scraped ${tab.name} tab via DOM`, data: tabData, rawTableData: domData.rawTableData });
              break;
            } catch (err) {
              logger.warn({ message: `DOM scraping failed for ${tab.name} (attempt ${attempt + 1})`, error: err.message });
              if (attempt < 2) await delay(10000, 20000); // Increased delay
            }
          }
        }

        Object.assign(financialData, tabData);
        logger.debug({ message: `Scraped ${tab.name} tab`, data: tabData });
      } catch (err) {
        logger.warn({ message: `Failed to process tab: ${tab.name}`, error: err.message });
        if (err.message.includes('Target closed') || err.message.includes('detached')) {
          throw err;
        }
      } finally {
        isScraping = false;
      }
    }

    logger.debug({ message: 'Extracted combined financial data', url: financialsUrl, data: financialData });
    return financialData;
  } catch (err) {
    logger.error({ message: 'Puppeteer fetch failed', company: company.tsx_code, error: err.message });
    throw err;
  } finally {
    if (behaviorInterval) clearInterval(behaviorInterval);
    if (browser) {
      try {
        await browser.close();
      } catch (err) {
        logger.warn({ message: 'Failed to close browser cleanly', error: err.message });
      }
    }
  }
}

function validateFinancialData(data) {
  const validatedData = { ...data };
  const anomalies = [];

  for (const [key, value] of Object.entries(validatedData)) {
    if (key.endsWith('_value') && typeof value === 'string') {
      validatedData[key] = parseFloat(value) || null;
    }
    if (key.endsWith('_value') && validatedData[key] < 0) {
      anomalies.push({ field: key, value: validatedData[key], message: 'Negative value detected' });
    }
  }

  if (anomalies.length > 0) {
    logger.warn({ message: 'Data anomalies detected', anomalies });
  }
  return validatedData;
}

function upsertFinancials(db, companyId, financialData) {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.get('SELECT financial_id, last_updated, data_source FROM financials WHERE company_id = ? AND last_updated > ? AND data_source = ?', 
        [companyId, new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(), 'Yahoo Finance'],
        (err, row) => {
          if (err) return reject(err);

          const shouldInsert = !row || new Date(financialData.last_updated) > new Date(row.last_updated);
          if (!shouldInsert) {
            logger.info({ message: 'Skipping update: Recent Yahoo Finance data exists', companyId, last_updated: row.last_updated });
            return resolve();
          }

          const stmt = db.prepare(`
            INSERT OR REPLACE INTO financials (
              financial_id, company_id, cash_value, cash_currency, liabilities, liabilities_currency,
              market_cap_value, market_cap_currency, revenue_value, revenue_currency, net_income_value,
              net_income_currency, operating_income, ebitda, debt_value, debt_currency, shares_outstanding,
              free_cash_flow, last_updated, data_source
            ) VALUES (
              (SELECT financial_id FROM financials WHERE company_id = ?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
          `);

          const validatedData = validateFinancialData(financialData);
          stmt.run([
            companyId, companyId,
            validatedData.cash_value || null, 'CAD',
            validatedData.liabilities || null, 'CAD',
            validatedData.market_cap_value || null, 'CAD',
            validatedData.revenue_value || null, 'CAD',
            validatedData.net_income_value || null, 'CAD',
            validatedData.operating_income || null, 'CAD',
            validatedData.ebitda || null, 'CAD',
            validatedData.debt_value || null, 'CAD',
            validatedData.shares_outstanding || null, 'CAD',
            validatedData.free_cash_flow || null, 'CAD',
            validatedData.last_updated, 'Barron\'s'
          ], function(err) {
            if (err) return reject(err);
            logger.info({ message: 'Upserted financials', financial_id: this.lastID, companyId });
            resolve();
          });
          stmt.finalize();
        });
    });
  });
}

function upsertCapitalStructure(db, companyId, financialData) {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO capital_structure (
          capital_id, company_id, existing_shares, last_updated
        ) VALUES (
          (SELECT capital_id FROM capital_structure WHERE company_id = ?), ?, ?, ?
        )
      `);

      const validatedData = validateFinancialData(financialData);
      stmt.run([
        companyId, companyId,
        validatedData.shares_outstanding || null,
        validatedData.last_updated
      ], function(err) {
        if (err) return reject(err);
        logger.info({ message: 'Upserted capital structure', capital_id: this.lastID, companyId });
        resolve();
      });
      stmt.finalize();
    });
  });
}

async function processCompany(db, company) {
  let attempts = 0;
  const maxAttempts = 3;
  while (attempts < maxAttempts) {
    try {
      const financialData = await fetchWithPuppeteer({
        tsx_code: company.tsx_code,
        company_id: company.company_id
      });

      if (!financialData) {
        logger.warn({ message: 'No financial data retrieved, skipping company', ticker: company.tsx_code });
        break;
      }

      const validatedData = validateFinancialData({
        ...financialData,
        company_id: company.company_id,
        last_updated: new Date().toISOString(),
        data_source: 'Barron\'s'
      });

      await upsertFinancials(db, company.company_id, validatedData);
      await upsertCapitalStructure(db, company.company_id, validatedData);

      const missingFields = ['revenue_value', 'net_income_value', 'cash_value', 'liabilities', 'debt_value', 'operating_income', 'ebitda', 'free_cash_flow', 'market_cap_value', 'shares_outstanding']
        .filter(key => validatedData[key] === null);
      if (missingFields.length > 0) {
        logger.warn({ message: 'Missing fields', ticker: company.tsx_code, fields: missingFields });
      } else {
        logger.info({ message: 'All financial data captured', ticker: company.tsx_code });
      }
      break;
    } catch (err) {
      attempts++;
      logger.error({ message: `Company processing failed (attempt ${attempts}/${maxAttempts})`, ticker: company.tsx_code, error: err.message });
      if (attempts < maxAttempts) {
        await delay(30000, 60000);
      } else {
        logger.error({ message: `Max attempts reached for ${company.tsx_code}, skipping` });
      }
    }
  }
}

async function updateFinancials() {
  const db = new sqlite3.Database('C:\\Users\\akiil\\gold-silver-analysis-v2\\mining_companies.db');
  try {
    const companies = await new Promise((resolve, reject) => {
      db.all('SELECT company_id, tsx_code FROM companies', [], (err, rows) => {
        if (err) return reject(err);
        resolve(rows);
      });
    });

    for (const company of companies) {
      await processCompany(db, company);
      await delay(60000, 120000); // Increased inter-company delay to avoid rate limits
    }
  } catch (err) {
    logger.error({ message: 'Script execution failed', error: err.message });
  } finally {
    db.close();
    logger.info({ message: 'Execution completed successfully' });
  }
}

updateFinancials();