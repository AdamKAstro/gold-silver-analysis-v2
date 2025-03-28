CREATE TABLE IF NOT EXISTS staging_companies (LIKE companies);
TRUNCATE staging_companies;
\COPY staging_companies FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\companies.csv' WITH (FORMAT csv, HEADER true, NULL '');

        INSERT INTO companies
        SELECT * FROM staging_companies
        ON CONFLICT (company_id) DO UPDATE SET
        tsx_code = EXCLUDED.tsx_code, company_name = EXCLUDED.company_name, name_alt = EXCLUDED.name_alt, status = EXCLUDED.status, headquarters = EXCLUDED.headquarters, minerals_of_interest = EXCLUDED.minerals_of_interest, percent_gold = EXCLUDED.percent_gold, percent_silver = EXCLUDED.percent_silver, description = EXCLUDED.description, last_updated = EXCLUDED.last_updated;
        
DROP TABLE staging_companies;
CREATE TABLE IF NOT EXISTS staging_financials (LIKE financials);
TRUNCATE staging_financials;
\COPY staging_financials FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\financials.csv' WITH (FORMAT csv, HEADER true, NULL '');

            DELETE FROM staging_financials
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT company_id FROM companies);
            

        INSERT INTO financials
        SELECT * FROM staging_financials
        ON CONFLICT (financial_id) DO UPDATE SET
        company_id = EXCLUDED.company_id, cash_value = EXCLUDED.cash_value, cash_currency = EXCLUDED.cash_currency, cash_date = EXCLUDED.cash_date, investments_json = EXCLUDED.investments_json, hedgebook = EXCLUDED.hedgebook, liabilities = EXCLUDED.liabilities, liabilities_currency = EXCLUDED.liabilities_currency, other_financial_assets = EXCLUDED.other_financial_assets, other_financial_assets_currency = EXCLUDED.other_financial_assets_currency, market_cap_value = EXCLUDED.market_cap_value, market_cap_currency = EXCLUDED.market_cap_currency, enterprise_value_value = EXCLUDED.enterprise_value_value, enterprise_value_currency = EXCLUDED.enterprise_value_currency, net_financial_assets = EXCLUDED.net_financial_assets, net_financial_assets_currency = EXCLUDED.net_financial_assets_currency, trailing_pe = EXCLUDED.trailing_pe, forward_pe = EXCLUDED.forward_pe, peg_ratio = EXCLUDED.peg_ratio, price_to_sales = EXCLUDED.price_to_sales, price_to_book = EXCLUDED.price_to_book, enterprise_to_revenue = EXCLUDED.enterprise_to_revenue, enterprise_to_ebitda = EXCLUDED.enterprise_to_ebitda, revenue_value = EXCLUDED.revenue_value, revenue_currency = EXCLUDED.revenue_currency, cost_of_revenue = EXCLUDED.cost_of_revenue, gross_profit = EXCLUDED.gross_profit, operating_expense = EXCLUDED.operating_expense, operating_income = EXCLUDED.operating_income, net_income_value = EXCLUDED.net_income_value, net_income_currency = EXCLUDED.net_income_currency, ebitda = EXCLUDED.ebitda, debt_value = EXCLUDED.debt_value, debt_currency = EXCLUDED.debt_currency, last_updated = EXCLUDED.last_updated, data_source = EXCLUDED.data_source, shares_outstanding = EXCLUDED.shares_outstanding, free_cash_flow = EXCLUDED.free_cash_flow;
        
DROP TABLE staging_financials;
CREATE TABLE IF NOT EXISTS staging_capital_structure (LIKE capital_structure);
TRUNCATE staging_capital_structure;
\COPY staging_capital_structure FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\capital_structure.csv' WITH (FORMAT csv, HEADER true, NULL '');

            DELETE FROM staging_capital_structure
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT company_id FROM companies);
            

        INSERT INTO capital_structure
        SELECT * FROM staging_capital_structure
        ON CONFLICT (capital_id) DO UPDATE SET
        company_id = EXCLUDED.company_id, existing_shares = EXCLUDED.existing_shares, fully_diluted_shares = EXCLUDED.fully_diluted_shares, in_the_money_options = EXCLUDED.in_the_money_options, options_revenue = EXCLUDED.options_revenue, options_revenue_currency = EXCLUDED.options_revenue_currency, last_updated = EXCLUDED.last_updated;
        
DROP TABLE staging_capital_structure;
CREATE TABLE IF NOT EXISTS staging_mineral_estimates (LIKE mineral_estimates);
TRUNCATE staging_mineral_estimates;
\COPY staging_mineral_estimates FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\mineral_estimates.csv' WITH (FORMAT csv, HEADER true, NULL '');

            DELETE FROM staging_mineral_estimates
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT company_id FROM companies);
            

        INSERT INTO mineral_estimates
        SELECT * FROM staging_mineral_estimates
        ON CONFLICT (estimate_id) DO UPDATE SET
        company_id = EXCLUDED.company_id, reserves_precious_aueq_moz = EXCLUDED.reserves_precious_aueq_moz, measured_indicated_precious_aueq_moz = EXCLUDED.measured_indicated_precious_aueq_moz, resources_precious_aueq_moz = EXCLUDED.resources_precious_aueq_moz, potential_precious_aueq_moz = EXCLUDED.potential_precious_aueq_moz, mineable_precious_aueq_moz = EXCLUDED.mineable_precious_aueq_moz, reserves_non_precious_aueq_moz = EXCLUDED.reserves_non_precious_aueq_moz, measured_indicated_non_precious_aueq_moz = EXCLUDED.measured_indicated_non_precious_aueq_moz, resources_non_precious_aueq_moz = EXCLUDED.resources_non_precious_aueq_moz, potential_non_precious_aueq_moz = EXCLUDED.potential_non_precious_aueq_moz, mineable_non_precious_aueq_moz = EXCLUDED.mineable_non_precious_aueq_moz, reserves_total_aueq_moz = EXCLUDED.reserves_total_aueq_moz, measured_indicated_total_aueq_moz = EXCLUDED.measured_indicated_total_aueq_moz, resources_total_aueq_moz = EXCLUDED.resources_total_aueq_moz, potential_total_aueq_moz = EXCLUDED.potential_total_aueq_moz, mineable_total_aueq_moz = EXCLUDED.mineable_total_aueq_moz, last_updated = EXCLUDED.last_updated;
        
DROP TABLE staging_mineral_estimates;
CREATE TABLE IF NOT EXISTS staging_production (LIKE production);
TRUNCATE staging_production;
\COPY staging_production FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\production.csv' WITH (FORMAT csv, HEADER true, NULL '');

            DELETE FROM staging_production
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT company_id FROM companies);
            

        INSERT INTO production
        SELECT * FROM staging_production
        ON CONFLICT (production_id) DO UPDATE SET
        company_id = EXCLUDED.company_id, current_production_precious_aueq_koz = EXCLUDED.current_production_precious_aueq_koz, current_production_non_precious_aueq_koz = EXCLUDED.current_production_non_precious_aueq_koz, current_production_total_aueq_koz = EXCLUDED.current_production_total_aueq_koz, future_production_total_aueq_koz = EXCLUDED.future_production_total_aueq_koz, reserve_life_years = EXCLUDED.reserve_life_years, last_updated = EXCLUDED.last_updated;
        
DROP TABLE staging_production;
CREATE TABLE IF NOT EXISTS staging_costs (LIKE costs);
TRUNCATE staging_costs;
\COPY staging_costs FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\costs.csv' WITH (FORMAT csv, HEADER true, NULL '');

            DELETE FROM staging_costs
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT company_id FROM companies);
            

        INSERT INTO costs
        SELECT * FROM staging_costs
        ON CONFLICT (cost_id) DO UPDATE SET
        company_id = EXCLUDED.company_id, construction_costs = EXCLUDED.construction_costs, construction_costs_currency = EXCLUDED.construction_costs_currency, aisc_last_quarter = EXCLUDED.aisc_last_quarter, aisc_last_quarter_currency = EXCLUDED.aisc_last_quarter_currency, aisc_last_year = EXCLUDED.aisc_last_year, aisc_last_year_currency = EXCLUDED.aisc_last_year_currency, aisc_future = EXCLUDED.aisc_future, aisc_future_currency = EXCLUDED.aisc_future_currency, aic_last_quarter = EXCLUDED.aic_last_quarter, aic_last_quarter_currency = EXCLUDED.aic_last_quarter_currency, aic_last_year = EXCLUDED.aic_last_year, aic_last_year_currency = EXCLUDED.aic_last_year_currency, tco_current = EXCLUDED.tco_current, tco_current_currency = EXCLUDED.tco_current_currency, tco_future = EXCLUDED.tco_future, tco_future_currency = EXCLUDED.tco_future_currency, last_updated = EXCLUDED.last_updated;
        
DROP TABLE staging_costs;
CREATE TABLE IF NOT EXISTS staging_valuation_metrics (LIKE valuation_metrics);
TRUNCATE staging_valuation_metrics;
\COPY staging_valuation_metrics FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\valuation_metrics.csv' WITH (FORMAT csv, HEADER true, NULL '');

            DELETE FROM staging_valuation_metrics
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT company_id FROM companies);
            

        INSERT INTO valuation_metrics
        SELECT * FROM staging_valuation_metrics
        ON CONFLICT (valuation_id) DO UPDATE SET
        company_id = EXCLUDED.company_id, mkt_cap_per_reserve_oz_precious = EXCLUDED.mkt_cap_per_reserve_oz_precious, mkt_cap_per_mi_oz_precious = EXCLUDED.mkt_cap_per_mi_oz_precious, mkt_cap_per_resource_oz_precious = EXCLUDED.mkt_cap_per_resource_oz_precious, mkt_cap_per_mineable_oz_precious = EXCLUDED.mkt_cap_per_mineable_oz_precious, mkt_cap_per_reserve_oz_all = EXCLUDED.mkt_cap_per_reserve_oz_all, mkt_cap_per_mi_oz_all = EXCLUDED.mkt_cap_per_mi_oz_all, mkt_cap_per_resource_oz_all = EXCLUDED.mkt_cap_per_resource_oz_all, mkt_cap_per_mineable_oz_all = EXCLUDED.mkt_cap_per_mineable_oz_all, ev_per_reserve_oz_precious = EXCLUDED.ev_per_reserve_oz_precious, ev_per_mi_oz_precious = EXCLUDED.ev_per_mi_oz_precious, ev_per_resource_oz_precious = EXCLUDED.ev_per_resource_oz_precious, ev_per_mineable_oz_precious = EXCLUDED.ev_per_mineable_oz_precious, ev_per_reserve_oz_all = EXCLUDED.ev_per_reserve_oz_all, ev_per_mi_oz_all = EXCLUDED.ev_per_mi_oz_all, ev_per_resource_oz_all = EXCLUDED.ev_per_resource_oz_all, ev_per_mineable_oz_all = EXCLUDED.ev_per_mineable_oz_all, mkt_cap_per_production_oz = EXCLUDED.mkt_cap_per_production_oz, ev_per_production_oz = EXCLUDED.ev_per_production_oz, last_updated = EXCLUDED.last_updated;
        
DROP TABLE staging_valuation_metrics;
CREATE TABLE IF NOT EXISTS staging_company_urls (LIKE company_urls);
TRUNCATE staging_company_urls;
\COPY staging_company_urls FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\company_urls.csv' WITH (FORMAT csv, HEADER true, NULL '');

            DELETE FROM staging_company_urls
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT company_id FROM companies);
            

        INSERT INTO company_urls
        SELECT * FROM staging_company_urls
        ON CONFLICT (url_id) DO UPDATE SET
        company_id = EXCLUDED.company_id, url_type = EXCLUDED.url_type, url = EXCLUDED.url, last_validated = EXCLUDED.last_validated;
        
DROP TABLE staging_company_urls;
CREATE TABLE IF NOT EXISTS staging_exchange_rates (LIKE exchange_rates);
TRUNCATE staging_exchange_rates;
\COPY staging_exchange_rates FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\exchange_rates.csv' WITH (FORMAT csv, HEADER true, NULL '');

        INSERT INTO exchange_rates
        SELECT * FROM staging_exchange_rates
        ON CONFLICT (rate_id) DO UPDATE SET
        from_currency = EXCLUDED.from_currency, to_currency = EXCLUDED.to_currency, rate = EXCLUDED.rate, fetch_date = EXCLUDED.fetch_date;
        
DROP TABLE staging_exchange_rates;
CREATE TABLE IF NOT EXISTS staging_stock_prices (LIKE stock_prices);
TRUNCATE staging_stock_prices;
\COPY staging_stock_prices FROM 'C:\Users\akiil\gold-silver-analysis-v2\exported_csvs\stock_prices.csv' WITH (FORMAT csv, HEADER true, NULL '');

            DELETE FROM staging_stock_prices
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT company_id FROM companies);
            

        INSERT INTO stock_prices
        SELECT * FROM staging_stock_prices
        ON CONFLICT (price_id) DO UPDATE SET
        company_id = EXCLUDED.company_id, price_date = EXCLUDED.price_date, price_value = EXCLUDED.price_value, price_currency = EXCLUDED.price_currency, change_1yr_percent = EXCLUDED.change_1yr_percent, last_updated = EXCLUDED.last_updated;
        
DROP TABLE staging_stock_prices;