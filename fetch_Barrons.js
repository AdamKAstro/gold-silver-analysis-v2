const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const sqlite3 = require('sqlite3').verbose();
const winston = require('winston');
const fs = require('fs-extra');
const path = require('path');

puppeteer.use(StealthPlugin());

// Configuration
const CONFIG = {
  dbPath: 'C:\\Users\\akiil\\gold-silver-analysis-v2\\mining_companies.db',
  cookiePath: path.join(__dirname, 'cookies.json'),
  pwdPath: path.join(__dirname, 'Barrons_pwdjson.json'),
  headless: false,
  maxRetries: 3,
  delays: {
    initial: { min: 10000, max: 20000 },
    tabSwitch: { min: 60000, max: 90000 },
    click: { min: 20000, max: 40000 },
    interCompany: { min: 30000, max: 60000 },
    behavior: { min: 1000, max: 2000 }
  },
  userAgents: [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0'
  ],
  loginUrl: 'https://www.barrons.com/login'
};

// Logger Setup
const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: path.join(__dirname, 'logs', 'scraper.log') })
  ]
});

// Utilities
function delay(min, max, context = 'generic') {
  const time = Math.floor(Math.random() * (max - min + 1)) + min;
  logger.debug({ context, message: `Delaying for ${time}ms` });
  return new Promise(resolve => setTimeout(resolve, time));
}

async function loadCookies(page) {
  try {
    const cookies = await fs.readJson(CONFIG.cookiePath);
    const sanitizedCookies = cookies.map(cookie => ({
      name: cookie.name,
      value: cookie.value,
      domain: cookie.domain,
      path: cookie.path,
      expires: cookie.expires,
      httpOnly: cookie.httpOnly,
      secure: cookie.secure,
      sameSite: cookie.sameSite === 'Lax' || cookie.sameSite === 'Strict' || cookie.sameSite === 'None' ? cookie.sameSite : 'Lax'
    }));
    await page.setCookie(...sanitizedCookies);
    logger.debug({ message: 'Loaded cookies', cookieCount: sanitizedCookies.length });
    return sanitizedCookies;
  } catch (err) {
    logger.warn({ message: 'No cookies found or invalid format', error: err.message });
    return [];
  }
}

async function saveCookies(page) {
  try {
    const cookies = await page.cookies();
    await fs.writeJson(CONFIG.cookiePath, cookies, { spaces: 2 });
    logger.debug({ message: 'Saved cookies', cookieCount: cookies.length });
  } catch (err) {
    logger.error({ message: 'Failed to save cookies', error: err.message });
  }
}

async function simulateMinimalBehavior(page, ticker) {
  logger.debug({ ticker, message: 'Simulating minimal behavior' });
  try {
    const viewport = await page.evaluate(() => ({ width: window.innerWidth, height: window.innerHeight }));
    await page.evaluate(() => window.scrollBy(0, Math.random() * 300 + 200));
    await delay(CONFIG.delays.behavior.min, CONFIG.delays.behavior.max, 'scroll');
    await page.mouse.click(Math.random() * viewport.width * 0.7, Math.random() * viewport.height * 0.7);
    await delay(CONFIG.delays.behavior.min, CONFIG.delays.behavior.max, 'click');
  } catch (err) {
    logger.warn({ ticker, message: 'Behavior simulation failed', error: err.message });
  }
}

async function simulateContinuousBehavior(page, ticker, pauseSimulation) {
  logger.debug({ ticker, message: 'Starting continuous behavior' });
  const interval = setInterval(async () => {
    if (pauseSimulation()) return;
    await simulateMinimalBehavior(page, ticker);
  }, 20000);
  return interval;
}

async function launchBrowser(ticker) {
  const browser = await puppeteer.launch({
    headless: CONFIG.headless,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--start-maximized',
      '--shm-size=2gb',
      '--disable-accelerated-2d-canvas'
    ],
    defaultViewport: null
  });

  const page = await browser.newPage();
  const ua = CONFIG.userAgents[Math.floor(Math.random() * CONFIG.userAgents.length)];
  await page.setUserAgent(ua);
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.barrons.com/'
  });

  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
    Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
  });

  logger.debug({ ticker, message: 'Browser launched', userAgent: ua });
  return { browser, page };
}

async function autoLogin(page, ticker) {
  try {
    const { email, pwd } = await fs.readJson(CONFIG.pwdPath);
    logger.info({ ticker, message: 'Attempting auto-login', email });
    await page.goto(CONFIG.loginUrl, { waitUntil: 'networkidle2', timeout: 180000 });
    await page.waitForSelector('input[name="username"]', { timeout: 60000 });
    await page.type('input[name="username"]', email, { delay: 100 });
    await page.type('input[name="password"]', pwd, { delay: 100 });
    await page.click('button[type="submit"]');
    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 180000 });
    await saveCookies(page);
    logger.info({ ticker, message: 'Auto-login completed, cookies saved' });
  } catch (err) {
    logger.error({ ticker, message: 'Auto-login failed', error: err.message, stack: err.stack });
    throw err;
  }
}

async function ensureLogin(page, ticker, financialsUrl) {
  logger.info({ ticker, message: 'Checking login state' });
  await loadCookies(page);
  await page.goto(financialsUrl, { waitUntil: 'networkidle2', timeout: 180000 });

  const isLoggedIn = await page.evaluate(() => !!document.querySelector('[data-id="FinancialTables_table"]') || !!document.querySelector('.ModuleSubNav__Tab-sc-n8aem8-2'));
  if (!isLoggedIn) {
    logger.warn({ ticker, message: 'Cookies invalid or expired, attempting auto-login' });
    await autoLogin(page, ticker);
    await page.goto(financialsUrl, { waitUntil: 'networkidle2', timeout: 180000 });
  }
  const loginSuccess = await page.evaluate(() => !!document.querySelector('[data-id="FinancialTables_table"]'));
  logger.debug({ ticker, message: 'Login check result', isLoggedIn: loginSuccess });
}

async function fetchWithPuppeteer(company) {
  const { ticker, company_id } = company;
  let browser, page, behaviorInterval;
  let attempt = 0;
  let isScraping = false;

  while (attempt < CONFIG.maxRetries) {
    try {
      const { browser: b, page: p } = await launchBrowser(ticker);
      browser = b;
      page = p;

      const financialsUrl = `https://www.barrons.com/market-data/stocks/${ticker.replace(/\.([A-Z]{2})$/, '').toLowerCase()}/financials?countrycode=ca&mod=searchresults_companyquotes`;
      await ensureLogin(page, ticker, financialsUrl);
      logger.info({ ticker, message: 'Navigating to financials URL', url: financialsUrl });

      await page.setRequestInterception(true);
      let networkData = [];
      page.on('response', async response => {
        const url = response.url();
        const contentType = response.headers()['content-type'] || '';
        if (contentType.includes('application/json') && (url.includes('financials') || url.includes('data'))) {
          try {
            const json = await response.json();
            if (json && (json.keys || json.sections || json.items)) {
              networkData.push({ url, data: json });
              logger.debug({ ticker, message: 'Intercepted JSON', url, data: JSON.stringify(json).slice(0, 200) });
            }
          } catch (e) {
            logger.warn({ ticker, message: 'Failed to parse JSON', url, error: e.message });
          }
        }
      });

      await delay(CONFIG.delays.initial.min, CONFIG.delays.initial.max, 'initial');
      const rawBody = await page.evaluate(() => document.body.innerHTML.slice(0, 500));
      logger.debug({ ticker, message: 'Raw page body', rawBody });

      const checkCaptchaOrBlock = async () => {
        const bodyText = await page.evaluate(() => document.body.innerText.toLowerCase());
        const isBlocked = bodyText.includes('captcha') || bodyText.includes('verify you are not a bot') || bodyText.includes('access blocked');
        if (isBlocked) logger.error({ ticker, message: 'CAPTCHA or block detected', bodyPreview: bodyText.slice(0, 200) });
        return isBlocked;
      };
      if (await checkCaptchaOrBlock()) {
        throw new Error('CAPTCHA detected despite auto-login');
      }

      await saveCookies(page);
      behaviorInterval = await simulateContinuousBehavior(page, ticker, () => isScraping);

      const financialTabs = [
        { name: 'Income Statement', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:nth-child(2)' },
        { name: 'Balance Sheet', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:nth-child(3)' },
        { name: 'Cash Flow Statement', selector: 'button.ModuleSubNav__Tab-sc-n8aem8-2:nth-child(4)' }
      ];
      const shuffledTabs = financialTabs.sort(() => Math.random() - 0.5);
      logger.debug({ ticker, message: 'Tab order shuffled', order: shuffledTabs.map(t => t.name) });

      const financialData = {};
      isScraping = true;
      for (let retry = 0; retry < 3; retry++) {
        try {
          await page.waitForSelector('[data-id="FinancialTables_table"]', { timeout: 180000 });
          const rawRows = await page.evaluate(() => {
            const rows = document.querySelectorAll('[data-id="FinancialTables_table"] .table__Row-sc-1djjifq-2');
            return Array.from(rows).map(row => ({
              html: row.outerHTML.slice(0, 500),
              cells: Array.from(row.querySelectorAll('.table__Cell-sc-1djjifq-5')).map(cell => cell.textContent.trim())
            }));
          });
          logger.debug({ ticker, message: 'Raw Overview rows', rawRows });

          const overviewData = {};
          rawRows.forEach(row => {
            const label = row.cells[0].toLowerCase();
            const latestValue = row.cells.length >= 6 ? row.cells[5] : row.cells[row.cells.length - 2]; // 2024 is 5th value, before chart
            if (label.includes('sales/revenue')) overviewData.revenue_value = latestValue;
            else if (label.includes('net income')) overviewData.net_income_value = latestValue;
            else if (label.includes('shares outstanding')) overviewData.shares_outstanding = latestValue;
            else if (label.includes('market value')) overviewData.market_cap_value = latestValue;
          });
          Object.assign(financialData, overviewData);
          logger.debug({ ticker, message: 'Scraped Overview', data: overviewData });
          break;
        } catch (err) {
          logger.warn({ ticker, message: `Overview scrape failed (retry ${retry + 1}/3)`, error: err.message });
          if (retry < 2) {
            await page.reload({ waitUntil: 'networkidle2', timeout: 180000 });
            await delay(10000, 20000, 'retry');
          } else throw err;
        }
      }
      isScraping = false;

      for (const tab of shuffledTabs) {
        if (Math.random() < 0.1) {
          logger.debug({ ticker, message: `Skipping tab: ${tab.name}` });
          continue;
        }

        isScraping = true;
        await delay(CONFIG.delays.tabSwitch.min, CONFIG.delays.tabSwitch.max, 'tabSwitch');
        const tabButton = await page.$(tab.selector);
        if (!tabButton) {
          logger.warn({ ticker, message: `Tab ${tab.name} not found, falling back` });
          const fallbackButton = await page.evaluateHandle(name => {
            const buttons = Array.from(document.querySelectorAll('.ModuleSubNav__Tab-sc-n8aem8-2'));
            return buttons.find(btn => btn.textContent.trim().toLowerCase().includes(name.toLowerCase()));
          }, tab.name);
          if (fallbackButton.asElement()) await fallbackButton.click();
          else {
            logger.error({ ticker, message: `No button for ${tab.name}` });
            continue;
          }
        } else {
          await tabButton.click();
        }

        for (let retry = 0; retry < 3; retry++) {
          try {
            await page.waitForSelector('[data-id="FinancialTables_table"]', { timeout: 180000 });
            const rawTabRows = await page.evaluate(() => {
              const rows = document.querySelectorAll('[data-id="FinancialTables_table"] .table__Row-sc-1djjifq-2');
              return Array.from(rows).map(row => ({
                html: row.outerHTML.slice(0, 500),
                cells: Array.from(row.querySelectorAll('.table__Cell-sc-1djjifq-5')).map(cell => cell.textContent.trim())
              }));
            });
            logger.debug({ ticker, message: `Raw ${tab.name} rows`, rawTabRows });

            const tabNetworkData = networkData.filter(({ url }) => {
              return (
                (tab.name === 'Income Statement' && url.includes('income-statement')) ||
                (tab.name === 'Balance Sheet' && url.includes('balance-sheet')) ||
                (tab.name === 'Cash Flow Statement' && url.includes('cash-flow'))
              );
            });
            logger.debug({ ticker, message: `Network data for ${tab.name}`, networkData: tabNetworkData.map(d => ({ url: d.url, data: JSON.stringify(d.data).slice(0, 200) })) });

            const tabData = {};
            if (tabNetworkData.length > 0) {
              tabNetworkData.forEach(({ data }) => {
                if (data.keys) {
                  const financialBlock = data.keys.find(block => block.$type === 'MarketData.FinancialStatementCard');
                  if (financialBlock?.sections) {
                    financialBlock.sections.forEach(section => {
                      section.items.forEach(item => {
                        const displayName = item.displayName.toLowerCase();
                        const latestValue = item.values && item.values.length >= 5 ? item.values[4].formatted : null;
                        if (latestValue) {
                          if (displayName.includes('sales/revenue')) tabData.revenue_value = latestValue;
                          else if (displayName.includes('net income')) tabData.net_income_value = latestValue;
                          else if (displayName.includes('cash & short term')) tabData.cash_value = latestValue;
                          else if (displayName.includes('total liabilities')) tabData.liabilities = latestValue;
                          else if (displayName.includes('debt')) tabData.debt_value = latestValue;
                          else if (displayName.includes('operating income')) tabData.operating_income = latestValue;
                          else if (displayName.includes('ebitda')) tabData.ebitda = latestValue;
                          else if (displayName.includes('free cash flow')) tabData.free_cash_flow = latestValue;
                        }
                      });
                    });
                  }
                }
              });
            }

            if (Object.keys(tabData).length === 0) {
              rawTabRows.forEach(row => {
                const label = row.cells[0].toLowerCase();
                const latestValue = row.cells.length >= 6 ? row.cells[5] : row.cells[row.cells.length - 2]; // 2024 or last before chart
                if (label.includes('revenue') || label.includes('sales')) tabData.revenue_value = latestValue;
                else if (label.includes('net income')) tabData.net_income_value = latestValue;
                else if (label.includes('cash') && label.includes('short-term')) tabData.cash_value = latestValue;
                else if (label.includes('liabilities')) tabData.liabilities = latestValue;
                else if (label.includes('debt')) tabData.debt_value = latestValue;
                else if (label.includes('operating income')) tabData.operating_income = latestValue;
                else if (label.includes('ebitda')) tabData.ebitda = latestValue;
                else if (label.includes('free cash flow')) tabData.free_cash_flow = latestValue;
              });
              logger.debug({ ticker, message: `Scraped ${tab.name} from DOM`, data: tabData });
            }

            Object.assign(financialData, tabData);
            logger.debug({ ticker, message: `Combined ${tab.name} data`, data: tabData });
            break;
          } catch (err) {
            logger.warn({ ticker, message: `Tab ${tab.name} failed (retry ${retry + 1}/3)`, error: err.message });
            if (retry < 2) {
              await page.reload({ waitUntil: 'networkidle2', timeout: 180000 });
              await delay(10000, 20000, 'retry');
            } else throw err;
          }
        }
        isScraping = false;
        await delay(CONFIG.delays.click.min, CONFIG.delays.click.max, 'click');
      }

      logger.info({ ticker, message: 'Financial data extracted', data: financialData });
      return financialData;

    } catch (err) {
      attempt++;
      logger.error({ ticker, message: `Attempt ${attempt}/${CONFIG.maxRetries} failed`, error: err.message, stack: err.stack });
      if (err.message.includes('CAPTCHA') || err.message.includes('block')) {
        await saveCookies(page);
        if (attempt === CONFIG.maxRetries) throw new Error(`Max retries for ${ticker}`);
      }
      await delay(CONFIG.delays.interCompany.min / 2, CONFIG.delays.interCompany.max / 2, 'retry');
    } finally {
      if (behaviorInterval) clearInterval(behaviorInterval);
      if (browser) await browser.close().catch(err => logger.warn({ ticker, message: 'Browser close failed', error: err.message }));
    }
  }
}

// Validation and Database Upserts
function validateFinancialData(data) {
  const validated = { ...data };
  const anomalies = [];
  for (const [key, value] of Object.entries(validated)) {
    if (typeof value === 'string') {
      const isNegative = value.includes('(');
      const num = parseFloat(value.replace(/[$,()BMK]/g, '')) * (isNegative ? -1 : 1) * 
        (value.includes('B') ? 1e9 : value.includes('M') ? 1e6 : value.includes('K') ? 1e3 : 1);
      validated[key] = isNaN(num) ? null : num;
      if (num < 0) anomalies.push({ field: key, value: num, message: 'Negative value' });
      if (key === 'shares_outstanding' && num < 1e6) validated[key] = null; // Filter small shares
      if (key === 'revenue_value' && num < 1e5) validated[key] = null; // Revenue >100K
    }
  }
  if (anomalies.length) logger.warn({ message: 'Data anomalies', anomalies });
  return validated;
}

function upsertFinancials(db, companyId, financialData) {
  return new Promise((resolve, reject) => {
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
    const validated = validateFinancialData(financialData);
    stmt.run([
      companyId, companyId,
      validated.cash_value || null, 'CAD', validated.liabilities || null, 'CAD',
      validated.market_cap_value || null, 'CAD', validated.revenue_value || null, 'CAD',
      validated.net_income_value || null, 'CAD', validated.operating_income || null, 'CAD',
      validated.ebitda || null, 'CAD', validated.debt_value || null, 'CAD',
      validated.shares_outstanding || null, 'CAD', validated.free_cash_flow || null, 'CAD',
      new Date().toISOString(), 'Barron\'s'
    ], function(err) {
      if (err) reject(err);
      else {
        logger.info({ companyId, message: 'Financials upserted', financial_id: this.lastID });
        resolve();
      }
    });
    stmt.finalize();
  });
}

function upsertCapitalStructure(db, companyId, financialData) {
  return new Promise((resolve, reject) => {
    const stmt = db.prepare(`
      INSERT OR REPLACE INTO capital_structure (
        capital_id, company_id, existing_shares, last_updated
      ) VALUES (
        (SELECT capital_id FROM capital_structure WHERE company_id = ?), ?, ?, ?
      )
    `);
    const validated = validateFinancialData(financialData);
    stmt.run([companyId, companyId, validated.shares_outstanding || null, new Date().toISOString()], function(err) {
      if (err) reject(err);
      else {
        logger.info({ companyId, message: 'Capital structure upserted', capital_id: this.lastID });
        resolve();
      }
    });
    stmt.finalize();
  });
}

async function updateFinancials() {
  const db = new sqlite3.Database(CONFIG.dbPath, sqlite3.OPEN_READWRITE, err => {
    if (err) logger.error({ message: 'Database connection failed', error: err.message });
  });

  try {
    const companies = await new Promise((resolve, reject) => {
      db.all('SELECT company_id, tsx_code AS ticker FROM companies', [], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });
    logger.info({ message: 'Fetched companies', count: companies.length });

    console.log('Ensure VPN is active. Using credentials from Barrons_pwdjson.json and cookies from cookies.json.');
    await new Promise(resolve => setTimeout(resolve, 10000));

    for (const company of companies) {
      try {
        const financialData = await fetchWithPuppeteer(company);
        if (!financialData || Object.keys(financialData).length === 0) {
          logger.warn({ ticker: company.ticker, message: 'No financial data retrieved' });
          continue;
        }

        const validatedData = validateFinancialData({ ...financialData, company_id: company.company_id });
        await upsertFinancials(db, company.company_id, validatedData);
        await upsertCapitalStructure(db, company.company_id, validatedData);

        const missingFields = Object.entries(validatedData)
          .filter(([k, v]) => v === null && k !== 'company_id')
          .map(([k]) => k);
        if (missingFields.length) logger.warn({ ticker: company.ticker, message: 'Missing fields', fields: missingFields });
        else logger.info({ ticker: company.ticker, message: 'All fields captured' });

        await delay(CONFIG.delays.interCompany.min, CONFIG.delays.interCompany.max, 'interCompany');
      } catch (err) {
        logger.error({ ticker: company.ticker, message: 'Company processing failed', error: err.message });
      }
    }
  } catch (err) {
    logger.error({ message: 'Execution failed', error: err.message });
  } finally {
    db.close(err => err && logger.error({ message: 'Database close failed', error: err.message }));
    logger.info({ message: 'Execution completed' });
  }
}

updateFinancials().catch(err => logger.error({ message: 'Top-level error', error: err.message }));