-- Ensure the exchange_rates table has the necessary rates (e.g., CAD to USD)
-- INSERT INTO exchange_rates (from_currency, to_currency, rate, fetch_date) VALUES ('CAD', 'USD', 0.73, CURRENT_TIMESTAMP);
-- INSERT INTO exchange_rates (from_currency, to_currency, rate, fetch_date) VALUES ('USD', 'CAD', 1.37, CURRENT_TIMESTAMP);

-- Populate the valuation_metrics table, replacing existing rows for the same company_id
INSERT OR REPLACE INTO valuation_metrics (
    company_id,
    mkt_cap_per_reserve_oz_precious,
    mkt_cap_per_mi_oz_precious,
    mkt_cap_per_resource_oz_precious,
    mkt_cap_per_mineable_oz_precious,
    mkt_cap_per_reserve_oz_all,
    mkt_cap_per_mi_oz_all,
    mkt_cap_per_resource_oz_all,
    mkt_cap_per_mineable_oz_all,
    ev_per_reserve_oz_precious,
    ev_per_mi_oz_precious,
    ev_per_resource_oz_precious,
    ev_per_mineable_oz_precious,
    ev_per_reserve_oz_all,
    ev_per_mi_oz_all,
    ev_per_resource_oz_all,
    ev_per_mineable_oz_all,
    mkt_cap_per_production_oz,
    ev_per_production_oz,
    last_updated
)
SELECT
    f.company_id,

    -- Market Cap / Ounce Metrics (Precious Metals, in USD/oz)
    CASE WHEN COALESCE(me.reserves_precious_aueq_moz, 0) > 0 AND f.market_cap_value IS NOT NULL THEN (f.market_cap_value * CASE COALESCE(f.market_cap_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.reserves_precious_aueq_moz * 1000000.0, 0) ELSE NULL END AS mkt_cap_per_reserve_oz_precious,
    CASE WHEN COALESCE(me.measured_indicated_precious_aueq_moz, 0) > 0 AND f.market_cap_value IS NOT NULL THEN (f.market_cap_value * CASE COALESCE(f.market_cap_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.measured_indicated_precious_aueq_moz * 1000000.0, 0) ELSE NULL END AS mkt_cap_per_mi_oz_precious,
    CASE WHEN COALESCE(me.resources_precious_aueq_moz, 0) > 0 AND f.market_cap_value IS NOT NULL THEN (f.market_cap_value * CASE COALESCE(f.market_cap_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.resources_precious_aueq_moz * 1000000.0, 0) ELSE NULL END AS mkt_cap_per_resource_oz_precious,
    CASE WHEN COALESCE(me.mineable_precious_aueq_moz, 0) > 0 AND f.market_cap_value IS NOT NULL THEN (f.market_cap_value * CASE COALESCE(f.market_cap_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.mineable_precious_aueq_moz * 1000000.0, 0) ELSE NULL END AS mkt_cap_per_mineable_oz_precious,

    -- Market Cap / Ounce Metrics (All Metals - Total AuEq, in USD/oz)
    CASE WHEN COALESCE(me.reserves_total_aueq_moz, 0) > 0 AND f.market_cap_value IS NOT NULL THEN (f.market_cap_value * CASE COALESCE(f.market_cap_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.reserves_total_aueq_moz * 1000000.0, 0) ELSE NULL END AS mkt_cap_per_reserve_oz_all,
    CASE WHEN COALESCE(me.measured_indicated_total_aueq_moz, 0) > 0 AND f.market_cap_value IS NOT NULL THEN (f.market_cap_value * CASE COALESCE(f.market_cap_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.measured_indicated_total_aueq_moz * 1000000.0, 0) ELSE NULL END AS mkt_cap_per_mi_oz_all,
    CASE WHEN COALESCE(me.resources_total_aueq_moz, 0) > 0 AND f.market_cap_value IS NOT NULL THEN (f.market_cap_value * CASE COALESCE(f.market_cap_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.resources_total_aueq_moz * 1000000.0, 0) ELSE NULL END AS mkt_cap_per_resource_oz_all,
    CASE WHEN COALESCE(me.mineable_total_aueq_moz, 0) > 0 AND f.market_cap_value IS NOT NULL THEN (f.market_cap_value * CASE COALESCE(f.market_cap_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.mineable_total_aueq_moz * 1000000.0, 0) ELSE NULL END AS mkt_cap_per_mineable_oz_all,

    -- Enterprise Value / Ounce Metrics (Precious Metals, in USD/oz)
    CASE WHEN COALESCE(me.reserves_precious_aueq_moz, 0) > 0 AND f.enterprise_value_value IS NOT NULL THEN (f.enterprise_value_value * CASE COALESCE(f.enterprise_value_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.reserves_precious_aueq_moz * 1000000.0, 0) ELSE NULL END AS ev_per_reserve_oz_precious,
    CASE WHEN COALESCE(me.measured_indicated_precious_aueq_moz, 0) > 0 AND f.enterprise_value_value IS NOT NULL THEN (f.enterprise_value_value * CASE COALESCE(f.enterprise_value_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.measured_indicated_precious_aueq_moz * 1000000.0, 0) ELSE NULL END AS ev_per_mi_oz_precious,
    CASE WHEN COALESCE(me.resources_precious_aueq_moz, 0) > 0 AND f.enterprise_value_value IS NOT NULL THEN (f.enterprise_value_value * CASE COALESCE(f.enterprise_value_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.resources_precious_aueq_moz * 1000000.0, 0) ELSE NULL END AS ev_per_resource_oz_precious,
    CASE WHEN COALESCE(me.mineable_precious_aueq_moz, 0) > 0 AND f.enterprise_value_value IS NOT NULL THEN (f.enterprise_value_value * CASE COALESCE(f.enterprise_value_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.mineable_precious_aueq_moz * 1000000.0, 0) ELSE NULL END AS ev_per_mineable_oz_precious,

    -- Enterprise Value / Ounce Metrics (All Metals - Total AuEq, in USD/oz)
    CASE WHEN COALESCE(me.reserves_total_aueq_moz, 0) > 0 AND f.enterprise_value_value IS NOT NULL THEN (f.enterprise_value_value * CASE COALESCE(f.enterprise_value_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.reserves_total_aueq_moz * 1000000.0, 0) ELSE NULL END AS ev_per_reserve_oz_all,
    CASE WHEN COALESCE(me.measured_indicated_total_aueq_moz, 0) > 0 AND f.enterprise_value_value IS NOT NULL THEN (f.enterprise_value_value * CASE COALESCE(f.enterprise_value_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.measured_indicated_total_aueq_moz * 1000000.0, 0) ELSE NULL END AS ev_per_mi_oz_all,
    CASE WHEN COALESCE(me.resources_total_aueq_moz, 0) > 0 AND f.enterprise_value_value IS NOT NULL THEN (f.enterprise_value_value * CASE COALESCE(f.enterprise_value_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.resources_total_aueq_moz * 1000000.0, 0) ELSE NULL END AS ev_per_resource_oz_all,
    CASE WHEN COALESCE(me.mineable_total_aueq_moz, 0) > 0 AND f.enterprise_value_value IS NOT NULL THEN (f.enterprise_value_value * CASE COALESCE(f.enterprise_value_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(me.mineable_total_aueq_moz * 1000000.0, 0) ELSE NULL END AS ev_per_mineable_oz_all,

    -- Production Metrics (in USD / Annual Ounce)
    CASE WHEN COALESCE(p.current_production_total_aueq_koz, 0) > 0 AND f.market_cap_value IS NOT NULL THEN (f.market_cap_value * CASE COALESCE(f.market_cap_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(p.current_production_total_aueq_koz * 1000.0, 0) ELSE NULL END AS mkt_cap_per_production_oz,
    CASE WHEN COALESCE(p.current_production_total_aueq_koz, 0) > 0 AND f.enterprise_value_value IS NOT NULL THEN (f.enterprise_value_value * CASE COALESCE(f.enterprise_value_currency, 'USD') WHEN 'USD' THEN 1.0 WHEN 'CAD' THEN COALESCE((SELECT rate FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'), 0.73) ELSE NULL END) / NULLIF(p.current_production_total_aueq_koz * 1000.0, 0) ELSE NULL END AS ev_per_production_oz,

    -- Timestamp
    CURRENT_TIMESTAMP AS last_updated
FROM
    financials f
LEFT JOIN
    mineral_estimates me ON f.company_id = me.company_id
LEFT JOIN
    production p ON f.company_id = p.company_id;

SELECT 'Valuation metrics calculated and table populated/updated.';