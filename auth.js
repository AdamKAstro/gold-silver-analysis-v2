const fs = require('fs-extra');
const path = require('path');
const winston = require('winston');

const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: path.join(__dirname, 'logs', 'auth.log') })
  ]
});

const CONFIG = {
  cookiePath: path.join(__dirname, 'cookies.json'),
  pwdPath: path.join(__dirname, 'Barrons_pwdjson.json'),
  loginUrl: 'https://www.barrons.com/login'
};

async function loadCookies(page) {
  try {
    const cookies = await fs.readJson(CONFIG.cookiePath);
    await page.setCookie(...cookies);
    logger.debug({ message: 'Cookies loaded', count: cookies.length });
    return true;
  } catch (err) {
    logger.warn({ message: 'Failed to load cookies', error: err.message });
    return false;
  }
}

async function saveCookies(page) {
  const cookies = await page.cookies();
  await fs.writeJson(CONFIG.cookiePath, cookies, { spaces: 2 });
  logger.debug({ message: 'Cookies saved', count: cookies.length });
}

async function login(page, ticker) {
  const { email, pwd } = await fs.readJson(CONFIG.pwdPath);
  logger.info({ ticker, message: 'Starting login', email });

  await page.goto(CONFIG.loginUrl, { waitUntil: 'networkidle2', timeout: 180000 });
  await page.type('input[name="username"]', email, { delay: 150 });
  await page.type('input[name="password"]', pwd, { delay: 150 });
  await page.click('button[type="submit"]');
  await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 180000 });

  const isLoggedIn = await page.evaluate(() => !document.querySelector('input[name="username"]'));
  if (!isLoggedIn) throw new Error('Login failed');
  
  await saveCookies(page);
  logger.info({ ticker, message: 'Login successful' });
  return true;
}

async function ensureLoggedIn(page, ticker, url) {
  await loadCookies(page);
  await page.goto(url, { waitUntil: 'networkidle2', timeout: 180000 });

  const isLoggedIn = await page.evaluate(() => !!document.querySelector('[data-id="FinancialTables_table"]'));
  if (!isLoggedIn) {
    await login(page, ticker);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 180000 });
  }
  logger.debug({ ticker, message: 'Login state verified' });
}

module.exports = { ensureLoggedIn, saveCookies };