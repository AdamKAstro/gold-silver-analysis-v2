# Gold Silver Analysis V2

A robust system for analyzing CANADIAN  gold and silver mining companies.

## Database Schema

### Tables

1. **companies**
   - `company_id`: Primary key
   - `tsx_code`: Unique TSX ticker (e.g., "AAB.TO")
   - `company_name`: Company name
   - `status`: "producer", "developer", or "explorer"
   - `headquarters`: Location (nullable)
   - `minerals_of_interest`: Comma-separated minerals (e.g., "gold,silver")
   - `percent_gold`: % value from gold (nullable)
   - `percent_silver`: % value from silver (nullable)
   - `description`: Projects/capital raisings (nullable)
   - `last_updated`: Data freshness

2. **financials**
   - Financial metrics (cash, liabilities, market cap, etc.) with currency fields.
   - See `inspect-db.js` for full details.

3. **stock_prices**
   - Historical and recent share prices with 1-year % change.

4. **capital_structure**
   - Shares, options, and related revenue.

5. **mineral_estimates**
   - Reserves, resources, and potential in AuEq Moz for precious, non-precious, and total metals.

6. **production**
   - Current and future production in AuEq koz, reserve life.

7. **costs**
   - Construction costs, AISC, AIC, and TCO with currencies.

8. **valuation_metrics**
   - Market cap and EV per ounce for precious and all metals, plus production-based metrics.

9. **company_urls**
   - URLs for company data sources (e.g., homepage, Yahoo Finance).

10. **exchange_rates**
    - Currency conversion rates with fetch dates.

## Setup
1. Run `npm install` to install dependencies.
2. Run `node setup-db.js` to create the database.
3. Run `node import-companies.js` to import `public/data/companies.csv`.  //old
3. Run `node scrape-yahoo-finance.js --force`   to get financials
install dependencies
npm install pdf2json puppeteer-extra-plugin-recaptcha tesseract.js async-mutex axios cheerio fs-extra puppeteer-extra puppeteer-extra-plugin-stealth sqlite3 pdf-parse pdf-lib


## Future Population Scripts
- Stock prices: Use `yahoo-finance2` (see `update-stock-prices.js`).
- Financials: Scrape from company reports or APIs.
- Mining data: Scrape SEDAR+ NI 43-101 reports with Puppeteer (TBD).
- Exchange rates: Fetch from `exchangeratesapi.io`.

## Notes
- All monetary fields have corresponding `_currency` fields.
- Nullable fields support explorers with no production data.
- Indexes on `company_id` improve query performance.