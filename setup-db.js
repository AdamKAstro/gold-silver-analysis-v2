const sqlite3 = require('sqlite3').verbose();
const fs = require('fs').promises;

// Database connection with verbose error handling
const db = new sqlite3.Database('mining_companies.db', (err) => {
  if (err) {
    console.error(`[${new Date().toISOString()}] ERROR: Failed to connect to database: ${err.message}`);
    process.exit(1);
  }
  console.log(`[${new Date().toISOString()}] INFO: Successfully connected to mining_companies.db`);
});

// Log file setup
const LOG_FILE = 'setup-db-log.txt';
async function log(message) {
  const timestampedMessage = `[${new Date().toISOString()}] ${message}\n`;
  console.log(timestampedMessage.trim());
  await fs.appendFile(LOG_FILE, timestampedMessage).catch(err => 
    console.error(`[${new Date().toISOString()}] ERROR: Failed to write to log file: ${err.message}`)
  );
}

// Execute SQL with error handling and logging
function runSql(query, tableName) {
  return new Promise((resolve, reject) => {
    db.run(query, function (err) {
      if (err) {
        log(`ERROR: Failed to create ${tableName}: ${err.message}`);
        reject(err);
      } else {
        log(`INFO: Successfully created or verified ${tableName} (rows affected: ${this.changes})`);
        resolve();
      }
    });
  });
}

// Define table creation queries as an array for sequential execution
const tableQueries = [
  {
    query: `
      CREATE TABLE IF NOT EXISTS companies (
        company_id INTEGER PRIMARY KEY AUTOINCREMENT,
        tsx_code TEXT UNIQUE NOT NULL,              -- Unique TSX ticker (e.g., "AAB.TO")
        company_name TEXT NOT NULL,                 -- Full company name from CSV NAME
        name_alt TEXT,                              -- Alternate name from CSV NAMEALT, nullable
        status TEXT CHECK(status IN ('producer', 'developer', 'explorer')) NOT NULL, -- Company type
        headquarters TEXT,                          -- Headquarters location (e.g., "Ontario, Canada"), nullable
        minerals_of_interest TEXT,                  -- Comma-separated minerals (e.g., "gold,silver,copper"), nullable
        percent_gold REAL,                          -- Percentage of value from gold, nullable
        percent_silver REAL,                        -- Percentage of value from silver, nullable
        description TEXT,                           -- Description of projects, capital raisings, etc., nullable
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP -- Last update timestamp
      );
      CREATE INDEX IF NOT EXISTS idx_companies_tsx_code ON companies(tsx_code);
    `,
    name: 'companies'
  },
  {
    query: `
      CREATE TABLE IF NOT EXISTS financials (
        financial_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL UNIQUE,         -- Links to companies, one record per company
        cash_value REAL,                            -- Cash on hand, nullable
        cash_currency TEXT,                         -- Currency of cash (e.g., "USD"), nullable
        cash_date DATETIME,                         -- Date of cash value, nullable
        investments_json TEXT,                      -- JSON string of investments, nullable
        hedgebook TEXT,                             -- Hedging details (JSON or text), nullable
        liabilities REAL,                           -- Total liabilities, nullable
        liabilities_currency TEXT,                  -- Currency of liabilities (e.g., "USD"), nullable
        other_financial_assets REAL,                -- Other financial assets, nullable
        other_financial_assets_currency TEXT,       -- Currency of other assets, nullable
        market_cap_value REAL,                      -- Market capitalization, nullable
        market_cap_currency TEXT,                   -- Currency of market cap (e.g., "CAD"), nullable
        enterprise_value_value REAL,                -- Enterprise value (market cap + debt - cash), nullable
        enterprise_value_currency TEXT,             -- Currency of EV (e.g., "CAD"), nullable
        net_financial_assets REAL,                  -- Net financial assets (cash - liabilities), nullable
        net_financial_assets_currency TEXT,         -- Currency of net assets, nullable
        trailing_pe REAL,                           -- Trailing price-to-earnings ratio, nullable
        forward_pe REAL,                            -- Forward price-to-earnings ratio, nullable
        peg_ratio REAL,                             -- PEG ratio (5yr expected), nullable
        price_to_sales REAL,                        -- Price-to-sales ratio, nullable
        price_to_book REAL,                         -- Price-to-book ratio, nullable
        enterprise_to_revenue REAL,                 -- Enterprise value to revenue ratio, nullable
        enterprise_to_ebitda REAL,                  -- Enterprise value to EBITDA ratio, nullable
        revenue_value REAL,                         -- Total revenue, nullable
        revenue_currency TEXT,                      -- Currency of revenue, nullable
        cost_of_revenue REAL,                       -- Cost of revenue, nullable
        gross_profit REAL,                          -- Gross profit, nullable
        operating_expense REAL,                     -- Operating expenses, nullable
        operating_income REAL,                      -- Operating income, nullable
        net_income_value REAL,                      -- Net income for common stockholders, nullable
        net_income_currency TEXT,                   -- Currency of net income, nullable
        ebitda REAL,                                -- EBITDA, nullable
        debt_value REAL,                            -- Total debt, nullable
        debt_currency TEXT,                         -- Currency of debt, nullable
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
      );
      CREATE INDEX IF NOT EXISTS idx_financials_company_id ON financials(company_id);
    `,
    name: 'financials'
  },
  {
    query: `
      CREATE TABLE IF NOT EXISTS stock_prices (
        price_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,                -- Links to companies
        price_date DATETIME NOT NULL,               -- Date of price record
        price_value REAL NOT NULL,                  -- Share price value
        price_currency TEXT NOT NULL,               -- Currency of price (e.g., "CAD")
        change_1yr_percent REAL,                    -- 1-year percentage change, nullable
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
      );
      CREATE INDEX IF NOT EXISTS idx_stock_prices_company_id ON stock_prices(company_id);
    `,
    name: 'stock_prices'
  },
  {
    query: `
      CREATE TABLE IF NOT EXISTS capital_structure (
        capital_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL UNIQUE,         -- Links to companies, one record per company
        existing_shares INTEGER,                    -- Current shares outstanding, nullable
        fully_diluted_shares INTEGER,               -- Fully diluted shares, nullable
        in_the_money_options INTEGER,               -- Number of in-the-money options, nullable
        options_revenue REAL,                       -- Revenue from options, nullable
        options_revenue_currency TEXT,              -- Currency of options revenue, nullable
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
      );
      CREATE INDEX IF NOT EXISTS idx_capital_structure_company_id ON capital_structure(company_id);
    `,
    name: 'capital_structure'
  },
  {
    query: `
      CREATE TABLE IF NOT EXISTS mineral_estimates (
        estimate_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL UNIQUE,         -- Links to companies, one record per company
        reserves_precious_aueq_moz REAL,            -- Precious metal reserves in AuEq Moz, nullable
        measured_indicated_precious_aueq_moz REAL,  -- Precious M&I resources in AuEq Moz, nullable
        resources_precious_aueq_moz REAL,           -- Precious total resources in AuEq Moz, nullable
        potential_precious_aueq_moz REAL,           -- Precious potential beyond JORC/NI43-101, nullable
        mineable_precious_aueq_moz REAL,            -- Precious mineable amount in AuEq Moz, nullable
        reserves_non_precious_aueq_moz REAL,        -- Non-precious reserves in AuEq Moz, nullable
        measured_indicated_non_precious_aueq_moz REAL, -- Non-precious M&I in AuEq Moz, nullable
        resources_non_precious_aueq_moz REAL,       -- Non-precious total resources in AuEq Moz, nullable
        potential_non_precious_aueq_moz REAL,       -- Non-precious potential in AuEq Moz, nullable
        mineable_non_precious_aueq_moz REAL,        -- Non-precious mineable in AuEq Moz, nullable
        reserves_total_aueq_moz REAL,               -- Total reserves in AuEq Moz, nullable
        measured_indicated_total_aueq_moz REAL,     -- Total M&I in AuEq Moz, nullable
        resources_total_aueq_moz REAL,              -- Total resources in AuEq Moz, nullable
        potential_total_aueq_moz REAL,              -- Total potential in AuEq Moz, nullable
        mineable_total_aueq_moz REAL,               -- Total mineable in AuEq Moz, nullable
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
      );
      CREATE INDEX IF NOT EXISTS idx_mineral_estimates_company_id ON mineral_estimates(company_id);
    `,
    name: 'mineral_estimates'
  },
  {
    query: `
      CREATE TABLE IF NOT EXISTS production (
        production_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL UNIQUE,         -- Links to companies, one record per company
        current_production_precious_aueq_koz REAL,  -- Current precious production in AuEq koz, nullable
        current_production_non_precious_aueq_koz REAL, -- Current non-precious production in AuEq koz, nullable
        current_production_total_aueq_koz REAL,     -- Total current production in AuEq koz, nullable
        future_production_total_aueq_koz REAL,      -- Projected total production in AuEq koz, nullable
        reserve_life_years REAL,                    -- Reserve life in years, nullable
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
      );
      CREATE INDEX IF NOT EXISTS idx_production_company_id ON production(company_id);
    `,
    name: 'production'
  },
  {
    query: `
      CREATE TABLE IF NOT EXISTS costs (
        cost_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL UNIQUE,         -- Links to companies, one record per company
        construction_costs REAL,                    -- Construction costs, nullable
        construction_costs_currency TEXT,           -- Currency of construction costs, nullable
        aisc_last_quarter REAL,                     -- All-In Sustaining Cost last quarter, nullable
        aisc_last_quarter_currency TEXT,            -- Currency of AISC last quarter, nullable
        aisc_last_year REAL,                        -- AISC last year, nullable
        aisc_last_year_currency TEXT,               -- Currency of AISC last year, nullable
        aisc_future REAL,                           -- Projected future AISC, nullable
        aisc_future_currency TEXT,                  -- Currency of future AISC, nullable
        aic_last_quarter REAL,                      -- All-In Cost last quarter, nullable
        aic_last_quarter_currency TEXT,             -- Currency of AIC last quarter, nullable
        aic_last_year REAL,                         -- AIC last year, nullable
        aic_last_year_currency TEXT,                -- Currency of AIC last year, nullable
        tco_current REAL,                           -- Total Cost of Ownership current, nullable
        tco_current_currency TEXT,                  -- Currency of current TCO, nullable
        tco_future REAL,                            -- Projected future TCO, nullable
        tco_future_currency TEXT,                   -- Currency of future TCO, nullable
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
      );
      CREATE INDEX IF NOT EXISTS idx_costs_company_id ON costs(company_id);
    `,
    name: 'costs'
  },
  {
    query: `
      CREATE TABLE IF NOT EXISTS valuation_metrics (
        valuation_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL UNIQUE,         -- Links to companies, one record per company
        mkt_cap_per_reserve_oz_precious REAL,       -- Market cap per precious reserve oz, nullable
        mkt_cap_per_mi_oz_precious REAL,            -- Market cap per precious M&I oz, nullable
        mkt_cap_per_resource_oz_precious REAL,      -- Market cap per precious resource oz, nullable
        mkt_cap_per_mineable_oz_precious REAL,      -- Market cap per precious mineable oz, nullable
        mkt_cap_per_reserve_oz_all REAL,            -- Market cap per all metals reserve oz, nullable
        mkt_cap_per_mi_oz_all REAL,                 -- Market cap per all metals M&I oz, nullable
        mkt_cap_per_resource_oz_all REAL,           -- Market cap per all metals resource oz, nullable
        mkt_cap_per_mineable_oz_all REAL,           -- Market cap per all metals mineable oz, nullable
        ev_per_reserve_oz_precious REAL,            -- EV per precious reserve oz, nullable
        ev_per_mi_oz_precious REAL,                 -- EV per precious M&I oz, nullable
        ev_per_resource_oz_precious REAL,           -- EV per precious resource oz, nullable
        ev_per_mineable_oz_precious REAL,           -- EV per precious mineable oz, nullable
        ev_per_reserve_oz_all REAL,                 -- EV per all metals reserve oz, nullable
        ev_per_mi_oz_all REAL,                      -- EV per all metals M&I oz, nullable
        ev_per_resource_oz_all REAL,                -- EV per all metals resource oz, nullable
        ev_per_mineable_oz_all REAL,                -- EV per all metals mineable oz, nullable
        mkt_cap_per_production_oz REAL,             -- Market cap per current production oz, nullable
        ev_per_production_oz REAL,                  -- EV per current production oz, nullable
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
      );
      CREATE INDEX IF NOT EXISTS idx_valuation_metrics_company_id ON valuation_metrics(company_id);
    `,
    name: 'valuation_metrics'
  },
  {
    query: `
      CREATE TABLE IF NOT EXISTS company_urls (
        url_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,                -- Links to companies
        url_type TEXT NOT NULL,                     -- Type of URL (e.g., "homepage", "yahoo_finance")
        url TEXT NOT NULL,                          -- Full URL
        last_validated DATETIME DEFAULT CURRENT_TIMESTAMP, -- Last validation timestamp
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
      );
      CREATE INDEX IF NOT EXISTS idx_company_urls_company_id ON company_urls(company_id);
    `,
    name: 'company_urls'
  },
  {
    query: `
      CREATE TABLE IF NOT EXISTS exchange_rates (
        rate_id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_currency TEXT NOT NULL,                -- Source currency (e.g., "USD")
        to_currency TEXT NOT NULL,                  -- Target currency (e.g., "CAD")
        rate REAL NOT NULL,                         -- Exchange rate value
        fetch_date DATETIME DEFAULT CURRENT_TIMESTAMP, -- When rate was fetched
        UNIQUE(from_currency, to_currency, fetch_date) -- Prevent duplicates
      );
    `,
    name: 'exchange_rates'
  }
];

// Main setup function
async function setupDatabase() {
  try {
    await log('INFO: Starting database setup');

    // Execute all table creation queries sequentially
    for (const { query, name } of tableQueries) {
      await runSql(query, name);
    }

    await log('INFO: Database schema creation completed successfully');
  } catch (err) {
    await log(`ERROR: Database setup failed: ${err.message}`);
    throw err;
  } finally {
    db.close((err) => {
      if (err) log(`ERROR: Failed to close database: ${err.message}`);
      else log('INFO: Database connection closed');
    });
  }
}

// Run the setup
setupDatabase().catch(err => {
  console.error('Setup failed:', err);
  process.exit(1);
});