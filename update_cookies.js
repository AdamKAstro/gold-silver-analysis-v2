const puppeteer = require('puppeteer');
const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const winston = require('winston');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'cookie_update.log' })
  ]
});

async function updateCookies() {
  let browser;
  try {
    // Define Chrome executable
    const executablePath = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';

    logger.info({ message: 'Using Chrome executable', path: executablePath });

    // Check if executable path exists
    if (!fs.existsSync(executablePath)) {
      throw new Error(`Chrome executable not found at ${executablePath}`);
    }

    // Launch Puppeteer without userDataDir to avoid conflicts
    browser = await puppeteer.launch({
      headless: false, // Visible for manual interaction
      executablePath: executablePath,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-gpu',
        '--disable-dev-shm-usage',
        '--remote-debugging-port=9222'
      ],
      ignoreDefaultArgs: ['--disable-extensions'],
      dumpio: true // Outputs browser process logs to console
    });

    // Log browser events for debugging
    browser.on('disconnected', () => {
      logger.error({ message: 'Browser disconnected unexpectedly' });
    });

    const page = await browser.newPage();

    // Log page errors
    page.on('error', (err) => {
      logger.error({ message: 'Page error', error: err.message });
    });
    page.on('pageerror', (err) => {
      logger.error({ message: 'Page uncaught exception', error: err.message });
    });

    // Navigate to the page with a longer timeout
    logger.info({ message: 'Navigating to Barron\'s financials page...' });
    await page.goto('https://www.barrons.com/market-data/stocks/xom/financials', {
      waitUntil: 'networkidle2',
      timeout: 120000 // 2-minute timeout to handle CAPTCHA delays
    });

    logger.info({ message: 'Page loaded. Please solve the CAPTCHA, log in, and navigate to the financials page. Press Enter when ready to save cookies...' });

    // Keep the script alive and wait for Enter keypress
    await new Promise((resolve) => {
      process.stdin.setRawMode(true);
      process.stdin.resume();
      process.stdin.on('data', (key) => {
        if (key[0] === 13) { // Enter key (ASCII 13)
          process.stdin.setRawMode(false);
          resolve();
        }
      });
    });

    // Take a screenshot for debugging
    await page.screenshot({ path: 'debug_before_save.png' });

    // Save cookies
    const cookies = await page.cookies();
    await fsp.writeFile(path.join(__dirname, 'cookies.json'), JSON.stringify(cookies, null, 2));
    logger.info({ message: 'Cookies updated and saved to cookies.json', cookieCount: cookies.length });

  } catch (error) {
    logger.error({ message: 'Cookie update failed', error: error.message, stack: error.stack });
    if (browser) {
      // Take a screenshot before closing for debugging
      const page = (await browser.pages())[0];
      if (page) await page.screenshot({ path: 'debug_error.png' });
    }
    process.exit(1);
  } finally {
    if (browser) await browser.close();
  }
}

updateCookies();