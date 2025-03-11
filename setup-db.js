const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('mining_companies.db');

db.serialize(() => {
  // 1. Companies Table (Central table for company metadata)
  db.run(`
    CREATE TABLE IF NOT EXISTS companies (
      company_id INTEGER PRIMARY KEY AUTOINCREMENT,
      tsx_code TEXT UNIQUE NOT NULL,              -- Unique TSX ticker (e.g., "AAB.TO")
      company_name TEXT NOT NULL,                  -- Full company name
	  name_alt TEXT,                               -- Alternate name (from NAMEALT), nullable
      status TEXT CHECK(status IN ('producer', 'developer', 'explorer')) NOT NULL, -- Company type
      headquarters TEXT,                           -- Location (e.g., "Ontario, Canada"), nullable
      minerals_of_interest TEXT,                   -- Comma-separated list (e.g., "gold,silver,copper")
      percent_gold REAL,                           -- % of value from gold, nullable
      percent_silver REAL,                         -- % of value from silver, nullable
      description TEXT,                            -- Projects, capital raisings, etc., nullable
      last_updated DATETIME DEFAULT CURRENT_TIMESTAMP -- Tracks data freshness
    );
    CREATE INDEX IF NOT EXISTS idx_companies_tsx_code ON companies(tsx_code);
  `);

  // 2. Financials Table (Financial metrics and derived values)
  db.run(`
    CREATE TABLE IF NOT EXISTS financials (
      financial_id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,                 -- Links to companies table
      cash_value REAL,                             -- Cash on hand, nullable
      cash_currency TEXT,                          -- Currency of cash (e.g., "USD")
      cash_date DATETIME,                          -- Date of cash value, nullable
      investments_json TEXT,                       -- JSON list of investments, nullable
      hedgebook TEXT,                              -- Hedging details (JSON or text), nullable
      liabilities REAL,                            -- Total liabilities, nullable
      liabilities_currency TEXT,                   -- Currency of liabilities
      other_financial_assets REAL,                 -- Other assets, nullable
      other_financial_assets_currency TEXT,        -- Currency of other assets
      market_cap_value REAL,                       -- Market capitalization, nullable
      market_cap_currency TEXT,                    -- Currency of market cap
      enterprise_value REAL,                       -- EV (market cap + debt - cash), nullable
      enterprise_value_currency TEXT,              -- Currency of EV
      net_financial_assets REAL,                   -- Cash - liabilities, nullable
      net_financial_assets_currency TEXT,          -- Currency of net assets
      last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (company_id) REFERENCES companies(company_id)
    );
    CREATE INDEX IF NOT EXISTS idx_financials_company_id ON financials(company_id);
  `);

  // 3. Stock Prices Table (Historical and recent share prices)
  db.run(`
    CREATE TABLE IF NOT EXISTS stock_prices (
      price_id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,                 -- Links to companies
      price_date DATETIME NOT NULL,                -- Date of price record
      price_value REAL NOT NULL,                   -- Share price
      price_currency TEXT NOT NULL,                -- Currency of price (e.g., "CAD")
      change_1yr_percent REAL,                     -- 1-year % change, nullable (calculated later)
      last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (company_id) REFERENCES companies(company_id)
    );
    CREATE INDEX IF NOT EXISTS idx_stock_prices_company_id ON stock_prices(company_id);
  `);

  // 4. Capital Structure Table (Share and options data)
  db.run(`
    CREATE TABLE IF NOT EXISTS capital_structure (
      capital_id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,                 -- Links to companies
      existing_shares INTEGER,                     -- Current shares outstanding, nullable
      fully_diluted_shares INTEGER,                -- Fully diluted shares, nullable
      in_the_money_options INTEGER,                -- Number of in-the-money options, nullable
      options_revenue REAL,                        -- Revenue from options, nullable
      options_revenue_currency TEXT,               -- Currency of options revenue
      last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (company_id) REFERENCES companies(company_id)
    );
    CREATE INDEX IF NOT EXISTS idx_capital_structure_company_id ON capital_structure(company_id);
  `);

  // 5. Mineral Estimates Table (Reserves, resources, etc.)
  db.run(`
    CREATE TABLE IF NOT EXISTS mineral_estimates (
      estimate_id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,                 -- Links to companies
      -- Precious metals (gold, silver, etc.) in AuEq Moz
      reserves_precious_aueq_moz REAL,             -- Proven reserves, nullable
      measured_indicated_precious_aueq_moz REAL,   -- M&I resources, nullable
      resources_precious_aueq_moz REAL,            -- Total resources (incl. inferred), nullable
      potential_precious_aueq_moz REAL,            -- Potential beyond JORC/NI43-101, nullable
      mineable_precious_aueq_moz REAL,             -- Mineable amount, nullable
      -- Non-precious metals (e.g., copper) in AuEq Moz
      reserves_non_precious_aueq_moz REAL,         -- Proven reserves, nullable
      measured_indicated_non_precious_aueq_moz REAL, -- M&I resources, nullable
      resources_non_precious_aueq_moz REAL,        -- Total resources, nullable
      potential_non_precious_aueq_moz REAL,        -- Potential beyond JORC/NI43-101, nullable
      mineable_non_precious_aueq_moz REAL,         -- Mineable amount, nullable
      -- Total metals in AuEq Moz (precious + non-precious)
      reserves_total_aueq_moz REAL,                -- Total reserves, nullable
      measured_indicated_total_aueq_moz REAL,      -- Total M&I, nullable
      resources_total_aueq_moz REAL,               -- Total resources, nullable
      potential_total_aueq_moz REAL,               -- Total potential, nullable
      mineable_total_aueq_moz REAL,                -- Total mineable, nullable
      last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (company_id) REFERENCES companies(company_id)
    );
    CREATE INDEX IF NOT EXISTS idx_mineral_estimates_company_id ON mineral_estimates(company_id);
  `);

  // 6. Production Table (Mining production metrics)
  db.run(`
    CREATE TABLE IF NOT EXISTS production (
      production_id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,                 -- Links to companies
      current_production_precious_aueq_koz REAL,   -- Annual production (precious), nullable
      current_production_non_precious_aueq_koz REAL, -- Annual production (non-precious), nullable
      current_production_total_aueq_koz REAL,      -- Total annual production, nullable
      future_production_total_aueq_koz REAL,       -- Projected future production, nullable
      reserve_life_years REAL,                     -- Reserve life in years, nullable
      last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (company_id) REFERENCES companies(company_id)
    );
    CREATE INDEX IF NOT EXISTS idx_production_company_id ON production(company_id);
  `);

  // 7. Costs Table (Cost metrics)
  db.run(`
    CREATE TABLE IF NOT EXISTS costs (
      cost_id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,                 -- Links to companies
      construction_costs REAL,                     -- Construction costs, nullable
      construction_costs_currency TEXT,            -- Currency of construction costs
      aisc_last_quarter REAL,                      -- All-In Sustaining Cost, last quarter, nullable
      aisc_last_quarter_currency TEXT,             -- Currency of AISC
      aisc_last_year REAL,                         -- AISC, last year, nullable
      aisc_last_year_currency TEXT,                -- Currency of AISC
      aisc_future REAL,                            -- Projected future AISC, nullable
      aisc_future_currency TEXT,                   -- Currency of AISC
      aic_last_quarter REAL,                       -- All-In Cost, last quarter, nullable
      aic_last_quarter_currency TEXT,              -- Currency of AIC
      aic_last_year REAL,                          -- AIC, last year, nullable
      aic_last_year_currency TEXT,                 -- Currency of AIC
      tco_current REAL,                            -- Total Cost of Ownership, current, nullable
      tco_current_currency TEXT,                   -- Currency of TCO
      tco_future REAL,                             -- Projected future TCO, nullable
      tco_future_currency TEXT,                    -- Currency of TCO
      last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (company_id) REFERENCES companies(company_id)
    );
    CREATE INDEX IF NOT EXISTS idx_costs_company_id ON costs(company_id);
  `);

  // 8. Valuation Metrics Table (Market cap and EV per ounce)
  db.run(`
    CREATE TABLE IF NOT EXISTS valuation_metrics (
      valuation_id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,                 -- Links to companies
      -- Market Cap per Ounce (Precious)
      mkt_cap_per_reserve_oz_precious REAL,        -- Market cap / reserves, nullable
      mkt_cap_per_mi_oz_precious REAL,             -- Market cap / M&I, nullable
      mkt_cap_per_resource_oz_precious REAL,       -- Market cap / resources, nullable
      mkt_cap_per_mineable_oz_precious REAL,       -- Market cap / mineable, nullable
      -- Market Cap per Ounce (All Metals)
      mkt_cap_per_reserve_oz_all REAL,             -- Market cap / reserves, nullable
      mkt_cap_per_mi_oz_all REAL,                  -- Market cap / M&I, nullable
      mkt_cap_per_resource_oz_all REAL,            -- Market cap / resources, nullable
      mkt_cap_per_mineable_oz_all REAL,            -- Market cap / mineable, nullable
      -- EV per Ounce (Precious)
      ev_per_reserve_oz_precious REAL,             -- EV / reserves, nullable
      ev_per_mi_oz_precious REAL,                  -- EV / M&I, nullable
      ev_per_resource_oz_precious REAL,            -- EV / resources, nullable
      ev_per_mineable_oz_precious REAL,            -- EV / mineable, nullable
      -- EV per Ounce (All Metals)
      ev_per_reserve_oz_all REAL,                  -- EV / reserves, nullable
      ev_per_mi_oz_all REAL,                       -- EV / M&I, nullable
      ev_per_resource_oz_all REAL,                 -- EV / resources, nullable
      ev_per_mineable_oz_all REAL,                 -- EV / mineable, nullable
      -- Production-Based Valuation
      mkt_cap_per_production_oz REAL,              -- Market cap / current production, nullable
      ev_per_production_oz REAL,                   -- EV / current production, nullable
      last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (company_id) REFERENCES companies(company_id)
    );
    CREATE INDEX IF NOT EXISTS idx_valuation_metrics_company_id ON valuation_metrics(company_id);
  `);

  // 9. Company URLs Table (Source URLs)
  db.run(`
    CREATE TABLE IF NOT EXISTS company_urls (
      url_id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,                 -- Links to companies
      url_type TEXT NOT NULL,                      -- e.g., "homepage", "yahoo_finance", "miningfeeds"
      url TEXT NOT NULL,                           -- Full URL
      last_validated DATETIME DEFAULT CURRENT_TIMESTAMP, -- Tracks URL validity
      FOREIGN KEY (company_id) REFERENCES companies(company_id)
    );
    CREATE INDEX IF NOT EXISTS idx_company_urls_company_id ON company_urls(company_id);
  `);

  // 10. Exchange Rates Table (Currency conversions)
  db.run(`
    CREATE TABLE IF NOT EXISTS exchange_rates (
      rate_id INTEGER PRIMARY KEY AUTOINCREMENT,
      from_currency TEXT NOT NULL,                 -- Source currency (e.g., "USD")
      to_currency TEXT NOT NULL,                   -- Target currency (e.g., "CAD")
      rate REAL NOT NULL,                          -- Exchange rate
      fetch_date DATETIME DEFAULT CURRENT_TIMESTAMP, -- When rate was fetched
      UNIQUE(from_currency, to_currency, fetch_date) -- Avoid duplicates
    );
  `);

  console.log('Database schema created successfully');
});

db.close();