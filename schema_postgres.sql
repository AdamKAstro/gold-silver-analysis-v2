-- PostgreSQL Schema Generated on 2025-03-28 08:12:45

CREATE TABLE IF NOT EXISTS companies (
    company_id BIGSERIAL,
    tsx_code TEXT NOT NULL,
    company_name TEXT NOT NULL,
    name_alt TEXT,
    status TEXT NOT NULL,
    headquarters TEXT,
    minerals_of_interest TEXT,
    percent_gold DOUBLE PRECISION,
    percent_silver DOUBLE PRECISION,
    description TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT companies_pkey PRIMARY KEY (company_id)
);

CREATE TABLE IF NOT EXISTS financials (
    financial_id BIGSERIAL,
    company_id BIGINT NOT NULL,
    cash_value DOUBLE PRECISION,
    cash_currency TEXT,
    cash_date TIMESTAMP WITH TIME ZONE,
    investments_json TEXT,
    hedgebook TEXT,
    liabilities DOUBLE PRECISION,
    liabilities_currency TEXT,
    other_financial_assets DOUBLE PRECISION,
    other_financial_assets_currency TEXT,
    market_cap_value DOUBLE PRECISION,
    market_cap_currency TEXT,
    enterprise_value_value DOUBLE PRECISION,
    enterprise_value_currency TEXT,
    net_financial_assets DOUBLE PRECISION,
    net_financial_assets_currency TEXT,
    trailing_pe DOUBLE PRECISION,
    forward_pe DOUBLE PRECISION,
    peg_ratio DOUBLE PRECISION,
    price_to_sales DOUBLE PRECISION,
    price_to_book DOUBLE PRECISION,
    enterprise_to_revenue DOUBLE PRECISION,
    enterprise_to_ebitda DOUBLE PRECISION,
    revenue_value DOUBLE PRECISION,
    revenue_currency TEXT,
    cost_of_revenue DOUBLE PRECISION,
    gross_profit DOUBLE PRECISION,
    operating_expense DOUBLE PRECISION,
    operating_income DOUBLE PRECISION,
    net_income_value DOUBLE PRECISION,
    net_income_currency TEXT,
    ebitda DOUBLE PRECISION,
    debt_value DOUBLE PRECISION,
    debt_currency TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_source TEXT,
    shares_outstanding DOUBLE PRECISION,
    free_cash_flow DOUBLE PRECISION,
    CONSTRAINT financials_pkey PRIMARY KEY (financial_id)
);

CREATE TABLE IF NOT EXISTS capital_structure (
    capital_id BIGSERIAL,
    company_id BIGINT NOT NULL,
    existing_shares BIGINT,
    fully_diluted_shares BIGINT,
    in_the_money_options BIGINT,
    options_revenue DOUBLE PRECISION,
    options_revenue_currency TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT capital_structure_pkey PRIMARY KEY (capital_id)
);

CREATE TABLE IF NOT EXISTS mineral_estimates (
    estimate_id BIGSERIAL,
    company_id BIGINT NOT NULL,
    reserves_precious_aueq_moz DOUBLE PRECISION,
    measured_indicated_precious_aueq_moz DOUBLE PRECISION,
    resources_precious_aueq_moz DOUBLE PRECISION,
    potential_precious_aueq_moz DOUBLE PRECISION,
    mineable_precious_aueq_moz DOUBLE PRECISION,
    reserves_non_precious_aueq_moz DOUBLE PRECISION,
    measured_indicated_non_precious_aueq_moz DOUBLE PRECISION,
    resources_non_precious_aueq_moz DOUBLE PRECISION,
    potential_non_precious_aueq_moz DOUBLE PRECISION,
    mineable_non_precious_aueq_moz DOUBLE PRECISION,
    reserves_total_aueq_moz DOUBLE PRECISION,
    measured_indicated_total_aueq_moz DOUBLE PRECISION,
    resources_total_aueq_moz DOUBLE PRECISION,
    potential_total_aueq_moz DOUBLE PRECISION,
    mineable_total_aueq_moz DOUBLE PRECISION,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT mineral_estimates_pkey PRIMARY KEY (estimate_id)
);

CREATE TABLE IF NOT EXISTS production (
    production_id BIGSERIAL,
    company_id BIGINT NOT NULL,
    current_production_precious_aueq_koz DOUBLE PRECISION,
    current_production_non_precious_aueq_koz DOUBLE PRECISION,
    current_production_total_aueq_koz DOUBLE PRECISION,
    future_production_total_aueq_koz DOUBLE PRECISION,
    reserve_life_years DOUBLE PRECISION,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT production_pkey PRIMARY KEY (production_id)
);

CREATE TABLE IF NOT EXISTS costs (
    cost_id BIGSERIAL,
    company_id BIGINT NOT NULL,
    construction_costs DOUBLE PRECISION,
    construction_costs_currency TEXT,
    aisc_last_quarter DOUBLE PRECISION,
    aisc_last_quarter_currency TEXT,
    aisc_last_year DOUBLE PRECISION,
    aisc_last_year_currency TEXT,
    aisc_future DOUBLE PRECISION,
    aisc_future_currency TEXT,
    aic_last_quarter DOUBLE PRECISION,
    aic_last_quarter_currency TEXT,
    aic_last_year DOUBLE PRECISION,
    aic_last_year_currency TEXT,
    tco_current DOUBLE PRECISION,
    tco_current_currency TEXT,
    tco_future DOUBLE PRECISION,
    tco_future_currency TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT costs_pkey PRIMARY KEY (cost_id)
);

CREATE TABLE IF NOT EXISTS valuation_metrics (
    valuation_id BIGSERIAL,
    company_id BIGINT NOT NULL,
    mkt_cap_per_reserve_oz_precious DOUBLE PRECISION,
    mkt_cap_per_mi_oz_precious DOUBLE PRECISION,
    mkt_cap_per_resource_oz_precious DOUBLE PRECISION,
    mkt_cap_per_mineable_oz_precious DOUBLE PRECISION,
    mkt_cap_per_reserve_oz_all DOUBLE PRECISION,
    mkt_cap_per_mi_oz_all DOUBLE PRECISION,
    mkt_cap_per_resource_oz_all DOUBLE PRECISION,
    mkt_cap_per_mineable_oz_all DOUBLE PRECISION,
    ev_per_reserve_oz_precious DOUBLE PRECISION,
    ev_per_mi_oz_precious DOUBLE PRECISION,
    ev_per_resource_oz_precious DOUBLE PRECISION,
    ev_per_mineable_oz_precious DOUBLE PRECISION,
    ev_per_reserve_oz_all DOUBLE PRECISION,
    ev_per_mi_oz_all DOUBLE PRECISION,
    ev_per_resource_oz_all DOUBLE PRECISION,
    ev_per_mineable_oz_all DOUBLE PRECISION,
    mkt_cap_per_production_oz DOUBLE PRECISION,
    ev_per_production_oz DOUBLE PRECISION,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valuation_metrics_pkey PRIMARY KEY (valuation_id)
);

CREATE TABLE IF NOT EXISTS company_urls (
    url_id BIGSERIAL,
    company_id BIGINT NOT NULL,
    url_type TEXT NOT NULL,
    url TEXT NOT NULL,
    last_validated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT company_urls_pkey PRIMARY KEY (url_id)
);

CREATE TABLE IF NOT EXISTS exchange_rates (
    rate_id BIGSERIAL,
    from_currency TEXT NOT NULL,
    to_currency TEXT NOT NULL,
    rate DOUBLE PRECISION NOT NULL,
    fetch_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT exchange_rates_pkey PRIMARY KEY (rate_id)
);

CREATE TABLE IF NOT EXISTS stock_prices (
    price_id BIGSERIAL,
    company_id BIGINT NOT NULL,
    price_date TIMESTAMP WITH TIME ZONE NOT NULL,
    price_value DOUBLE PRECISION NOT NULL,
    price_currency TEXT NOT NULL,
    change_1yr_percent DOUBLE PRECISION,
    last_updated TIMESTAMP WITH TIME ZONE,
    CONSTRAINT stock_prices_pkey PRIMARY KEY (price_id)
);

-- Foreign Key Constraints
ALTER TABLE financials ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;
ALTER TABLE capital_structure ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;
ALTER TABLE mineral_estimates ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;
ALTER TABLE production ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;
ALTER TABLE costs ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;
ALTER TABLE valuation_metrics ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;
ALTER TABLE company_urls ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;
ALTER TABLE stock_prices ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL;
