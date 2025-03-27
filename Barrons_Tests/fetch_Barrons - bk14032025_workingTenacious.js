const puppeteer = require('puppeteer');
const winston = require('winston');
const fs = require('fs').promises;

// Logger setup
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

// Utility to introduce random delays
function delay(min, max) {
  const time = Math.floor(Math.random() * (max - min + 1)) + min;
  logger.debug({ message: `Delaying for ${time}ms` });
  return new Promise(resolve => setTimeout(resolve, time));
}

// Load and save cookies for session persistence
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

// Pool of realistic user-agents
const userAgents = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.91'
];

// Simulate diverse human-like behavior with frame and reload checks
async function simulateDiverseBehavior(page) {
  logger.debug({ message: 'Simulating diverse human-like behavior' });
  try {
    // Check if page is still alive and hasn't reloaded
    const currentUrl = await page.url();
    if (!currentUrl.includes('barrons.com')) {
      throw new Error('Page navigated away unexpectedly');
    }
    await page.evaluate(() => true).catch(() => { throw new Error('Page target closed'); });

    const viewport = await page.evaluate(() => ({
      width: window.innerWidth,
      height: window.innerHeight
    }));

    const startX = Math.floor(Math.random() * viewport.width * 0.2);
    const startY = Math.floor(Math.random() * viewport.height * 0.2);
    const endX = Math.floor(Math.random() * viewport.width * 0.8);
    const endY = Math.floor(Math.random() * viewport.height * 0.8);
    const controlX = (startX + endX) / 2 + Math.random() * 200 - 100;
    const controlY = (startY + endY) / 2 + Math.random() * 200 - 100;

    for (let t = 0; t <= 1; t += 0.05) {
      const x = (1 - t) * (1 - t) * startX + 2 * (1 - t) * t * controlX + t * t * endX;
      const y = (1 - t) * (1 - t) * startY + 2 * (1 - t) * t * controlY + t * t * endY;
      await page.mouse.move(x, y);
      await delay(50, 150);
    }

    await page.evaluate(() => {
      window.scrollBy(0, Math.random() * 500 + 300);
    });
    await delay(3000, 7000);

    for (let attempt = 0; attempt < 5; attempt++) {
      const hoverableElements = await page.$$('a:not([style*="display: none"]):not([style*="pointer-events: none"]), button:not([style*="display: none"]):not([style*="pointer-events: none"]), div[role="button"]:not([style*="display: none"])');
      if (hoverableElements.length === 0) {
        logger.debug({ message: 'No hoverable elements found' });
        break;
      }

      const randomElement = hoverableElements[Math.floor(Math.random() * hoverableElements.length)];
      try {
        const isVisible = await randomElement.evaluate(el => {
          const style = window.getComputedStyle(el);
          return style && style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && style.pointerEvents !== 'none';
        });
        if (isVisible) {
          await page.waitForFunction(() => !document.querySelector('body')?.classList.contains('loading'), { timeout: 20000 });
          await randomElement.hover();
          logger.debug({ message: 'Hovered over element' });
          await delay(1000, 3000);
          break;
        } else {
          logger.debug({ message: `Element not visible (attempt ${attempt + 1}), retrying` });
        }
      } catch (err) {
        logger.warn({ message: `Failed to hover over element (attempt ${attempt + 1})`, error: err.message });
        await delay(2000, 4000);
      }
    }

    const searchBar = await page.$('input[type="search"], input[name="search"]');
    if (searchBar) {
      try {
        await searchBar.focus();
        await page.keyboard.type('stock market', { delay: Math.random() * 200 + 100 });
        await delay(2000, 5000);
        await page.keyboard.press('Backspace', { delay: Math.random() * 200 + 100 });
        await page.keyboard.press('Backspace', { delay: Math.random() * 200 + 100 });
        await delay(1000, 3000);
      } catch (err) {
        logger.warn({ message: 'Failed to type in search bar', error: err.message });
      }
    }

    await page.mouse.click(Math.random() * viewport.width * 0.7, Math.random() * viewport.height * 0.7);
    await delay(2000, 5000);
  } catch (err) {
    logger.warn({ message: 'Error in simulateDiverseBehavior', error: err.message });
    throw err; // Propagate error to stop continuous simulation
  }
}

// Simulate continuous human-like behavior with pause control
async function simulateContinuousBehavior(page, pauseSimulation) {
  logger.debug({ message: 'Starting continuous behavior simulation' });
  const interval = setInterval(async () => {
    if (pauseSimulation()) {
      logger.debug({ message: 'Continuous behavior simulation paused' });
      return;
    }
    try {
      await simulateDiverseBehavior(page);
      if (Math.random() < 0.3) {
        logger.debug({ message: 'Simulating reading pause' });
        await delay(10000, 30000);
      }
    } catch (err) {
      logger.warn({ message: 'Error in continuous behavior simulation', error: err.message });
      clearInterval(interval);
    }
  }, 15000);

  return interval;
}

// Main scraping function with enhanced data extraction
async function fetchWithPuppeteer(url) {
  let browser;
  let page;
  let behaviorInterval;
  let isScraping = false; // Flag to pause continuous simulation during scraping

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
        '--enable-low-end-device-mode'
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
        url.includes('google-analytics')
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
        !url.includes('bam.nr-data.net') // Exclude tracking endpoints
      ) {
        try {
          const json = await response.json();
          if (json && (json.blocks || json.sections || json.items)) {
            networkData.push({ url, data: json });
            logger.debug({ message: 'Intercepted financial network data', url, data: JSON.stringify(json).substring(0, 100) + '...' });
          }
        } catch (e) {
          logger.warn({ message: 'Failed to parse financial network response', url, error: e.message });
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

    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
    logger.info({ message: 'Page loaded', url, title: await page.title() });

    await saveCookies(page);

    behaviorInterval = await simulateContinuousBehavior(page, () => isScraping);

    await delay(15000, 30000);

    const financialTabs = [
      { name: 'Overview', selector: '' },
      { name: 'Income Statement', label: 'Income Statement', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:-soup-contains("Income Statement")' },
      { name: 'Balance Sheet', label: 'Balance Sheet', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:-soup-contains("Balance Sheet")' },
      { name: 'Cash Flow Statement', label: 'Cash Flow Statement', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:-soup-contains("Cash Flow Statement")' }
    ];

    const shuffledTabs = financialTabs.slice(1).sort(() => Math.random() - 0.5);
    logger.debug({ message: 'Shuffled tab order', order: shuffledTabs.map(tab => tab.name) });

    const financialData = {};

    // Scrape Overview tab
    isScraping = true;
    try {
      await page.waitForSelector('div', { timeout: 15000 });
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

    // Scrape financial statement tabs
    for (const tab of shuffledTabs) {
      if (Math.random() < 0.2) {
        logger.debug({ message: `Skipping tab: ${tab.name}` });
        continue;
      }

      isScraping = true;
      try {
        logger.info({ message: `Switching to tab: ${tab.name}` });
        await delay(20000, 40000);

        const tabButton = await page.evaluateHandle(tabLabel => {
          const buttons = Array.from(document.querySelectorAll('button.ModuleSubNav__Tab-sc-n8aem8-2'));
          return buttons.find(btn => btn.textContent.trim() === tabLabel);
        }, tab.label);

        if (!tabButton.asElement()) {
          logger.warn({ message: `Tab button for "${tab.name}" not found` });
          continue;
        }

        await tabButton.click();
        await page.waitForSelector('div.table__Cell-sc-1djjifq-5', { timeout: 60000 }); // Increased wait
        await delay(60000, 90000); // Increased delay for data loading

        if (Math.random() < 0.3) {
          const previousTab = financialTabs[Math.floor(Math.random() * financialTabs.length)];
          logger.debug({ message: `Revisiting tab: ${previousTab.name}` });
          const prevTabButton = await page.evaluateHandle(tabLabel => {
            const buttons = Array.from(document.querySelectorAll('button.ModuleSubNav__Tab-sc-n8aem8-2'));
            return buttons.find(btn => btn.textContent.trim() === tabLabel);
          }, previousTab.label);
          if (prevTabButton.asElement()) {
            await prevTabButton.click();
            await page.waitForSelector('div.table__Cell-sc-1djjifq-5', { timeout: 60000 });
            await delay(15000, 30000);
          }
        }

        const tabNetworkData = networkData.filter(({ url }) => {
          const match = (
            (tab.name === 'Income Statement' && url.includes('income-statement')) ||
            (tab.name === 'Balance Sheet' && url.includes('balance-sheet')) ||
            (tab.name === 'Cash Flow Statement' && url.includes('cash-flow'))
          );
          if (match) logger.debug({ message: `Matched network data for ${tab.name}`, url });
          return match;
        });

        const tabData = {};
        if (tabNetworkData.length > 0) {
          tabNetworkData.forEach(({ url, data }) => {
            if (data.blocks) {
              const financialBlock = data.blocks.find(block => block.$type === 'MarketData.FinancialStatementCard');
              if (financialBlock && financialBlock.sections) {
                financialBlock.sections.forEach(section => {
                  section.items.forEach(item => {
                    const displayName = item.displayName.toLowerCase();
                    const latestValue = item.values && item.values.length >= 5 ? item.values[4].formatted : null;

                    if (latestValue) {
                      if (displayName.includes('sales/revenue')) tabData.revenue_value = latestValue;
                      else if (displayName.includes('net income before extraordinaries')) tabData.net_income_value = latestValue;
                      else if (displayName.includes('cash & short term investments')) tabData.cash_value = latestValue;
                      else if (displayName.includes('total liabilities')) tabData.liabilities = latestValue;
                      else if (displayName.includes('non-convertible debt')) {
                        tabData.debt_value = tabData.debt_value 
                          ? (parseFloat(tabData.debt_value) + parseFloat(latestValue.replace(/[^\d.-]/g, ''))).toString() + 'B' 
                          : latestValue;
                      } else if (displayName.includes('capitalized lease obligations')) {
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
          });
        } else {
          logger.warn({ message: `No network data captured for ${tab.name}` });
        }

        if (Object.keys(tabData).length === 0) {
          for (let attempt = 0; attempt < 3; attempt++) {
            try {
              await page.waitForSelector('div.table__Cell-sc-1djjifq-5', { timeout: 60000 });
              const domData = await page.evaluate(() => {
                const data = {};
                const cells = document.querySelectorAll('div.table__Cell-sc-1djjifq-5');
                let currentLabel = '';
                const htmlSnippet = cells.length > 0 ? cells[0].outerHTML.substring(0, 100) + '...' : 'No elements found';

                for (let i = 0; i < cells.length; i++) {
                  const cellText = cells[i].textContent.trim();
                  const isLabelCell = !cells[i].classList.contains('fDHbHR') && !cells[i].classList.contains('fRQdPw');

                  if (isLabelCell) {
                    currentLabel = cellText.toLowerCase();
                  } else if (currentLabel) {
                    const nextCells = Array.from(cells).slice(i, i + 5);
                    if (nextCells.length >= 5) {
                      const latestValue = nextCells[4].textContent.trim();
                      if (currentLabel.includes('sales/revenue')) data.revenue_value = latestValue;
                      else if (currentLabel.includes('net income')) data.net_income_value = latestValue;
                      else if (currentLabel.includes('cash and short-term investments')) data.cash_value = latestValue;
                      else if (currentLabel.includes('total liabilities')) data.liabilities = latestValue;
                      else if (currentLabel.includes('non-convertible debt')) {
                        data.debt_value = data.debt_value ? (parseFloat(data.debt_value) + parseFloat(latestValue.replace(/[^\d.-]/g, ''))).toString() + 'B' : latestValue;
                      } else if (currentLabel.includes('capitalized lease obligations')) {
                        data.debt_value = data.debt_value ? (parseFloat(data.debt_value) + parseFloat(latestValue.replace(/[^\d.-]/g, ''))).toString() + 'B' : latestValue;
                      } else if (currentLabel.includes('operating income')) data.operating_income = latestValue;
                      else if (currentLabel.includes('ebitda')) data.ebitda = latestValue;
                      else if (currentLabel.includes('free cash flow')) data.free_cash_flow = latestValue;
                      i += 4;
                    }
                    currentLabel = '';
                  }
                }
                return { data, htmlSnippet };
              });
              Object.assign(tabData, domData.data);
              logger.debug({ message: `Scraped ${tab.name} tab via DOM`, data: tabData, htmlSnippet: domData.htmlSnippet });
              break;
            } catch (err) {
              logger.warn({ message: `DOM scraping failed for ${tab.name} (attempt ${attempt + 1})`, error: err.message });
              if (attempt < 2) await delay(10000, 20000);
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

    logger.debug({ message: 'Extracted combined financial data', url, data: financialData });
    return financialData;
  } catch (err) {
    logger.error({ message: 'Puppeteer fetch failed', url, error: err.message });
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

// Process company data with validation
async function processCompany(company, data) {
  const sanitizedData = {};
  for (const key in data) {
    let value = data[key];
    if (typeof value === 'string' && value.match(/[\d.-]+[BKM]?/)) {
      const multiplier = value.endsWith('B') ? 1000000000 : value.endsWith('M') ? 1000000 : value.endsWith('K') ? 1000 : 1;
      const parsed = parseFloat(value.replace(/[^\d.-]/g, '')) * multiplier;
      logger.debug({ message: `Sanitizing ${key}`, value, parsed, multiplier });
      sanitizedData[key] = parsed;
    }
  }

  const finalData = {
    company_id: company.companyId,
    data_source: 'Barron\'s',
    last_updated: new Date().toISOString(),
    market_cap_currency: 'CAD',
    market_cap_value: sanitizedData.market_cap_value || null,
    shares_outstanding: sanitizedData.shares_outstanding || null,
    revenue_currency: 'CAD',
    revenue_value: sanitizedData.revenue_value || null,
    net_income_currency: 'CAD',
    net_income_value: sanitizedData.net_income_value || null,
    cash_currency: 'CAD',
    cash_value: sanitizedData.cash_value || null,
    liabilities_currency: 'CAD',
    liabilities: sanitizedData.liabilities || null,
    debt_currency: 'CAD',
    debt_value: sanitizedData.debt_value || null,
    operating_income: sanitizedData.operating_income || null,
    ebitda: sanitizedData.ebitda || null,
    free_cash_flow: sanitizedData.free_cash_flow || null
  };

  const missingFields = Object.keys(finalData).filter(key => finalData[key] === null && !['company_id', 'data_source', 'last_updated'].includes(key));
  if (missingFields.length > 0) {
    logger.warn({ message: 'Missing fields', ticker: company.fullTicker, fields: missingFields });
  } else {
    logger.info({ message: 'All financial data captured', ticker: company.fullTicker });
  }

  logger.info({ message: 'Inserted financials', ticker: company.fullTicker, data: finalData });
  logger.info({ message: 'Inserted capital structure', ticker: company.fullTicker, data: { company_id: company.companyId, existing_shares: sanitizedData.shares_outstanding || null, last_updated: new Date().toISOString() } });
  logger.info({ message: 'Processed company', ticker: company.fullTicker });
}

// Main execution function with restart and resume
async function updateFinancials() {
  const companies = [
    { fullTicker: 'XOM', baseTicker: 'xom', companyId: 999 },
    { fullTicker: 'AAB.TO', baseTicker: 'aab', companyId: 5 }
  ];

  for (const company of companies) {
    let attempts = 0;
    const maxAttempts = 3;
    let lastProcessedTabIndex = 0;
    while (attempts < maxAttempts) {
      try {
        const url = `https://www.barrons.com/market-data/stocks/${company.baseTicker}/financials?countrycode=ca&mod=searchresults_companyquotes&mod=searchbar&search_keywords=${company.fullTicker}&search_statement_type=suggested`;
        const data = await fetchWithPuppeteer(url);
        await processCompany(company, data);
        lastProcessedTabIndex = 0;
        break;
      } catch (err) {
        attempts++;
        logger.error({ message: `Company processing failed (attempt ${attempts}/${maxAttempts})`, ticker: company.fullTicker, error: err.message });
        if (attempts < maxAttempts) {
          logger.info({ message: `Retrying company ${company.fullTicker} after delay` });
          await delay(60000, 120000);
        } else {
          logger.error({ message: `Max attempts reached for ${company.fullTicker}, skipping` });
        }
      }
    }
    await delay(60000, 120000);
  }

  logger.info({ message: 'Execution completed successfully' });
}

// Run the script
updateFinancials().catch(err => {
  logger.error({ message: 'Script execution failed', error: err.message });
});