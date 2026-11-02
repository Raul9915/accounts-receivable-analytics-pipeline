/* =====================================================================
   04 - Analysis queries (T-SQL)
   Ad-hoc questions answered directly in SQL Server.
   Run against: ARAnalytics, after loading the Gold layer.
   ===================================================================== */

/* Q1. How has portfolio quality evolved? */
SELECT
    snapshot_date,
    total_balance_lc,
    overdue_balance_lc,
    overdue_ratio,
    over_90_ratio,
    overdue_change_lc
FROM gold.vw_kpi_monthly
ORDER BY snapshot_date;


/* Q2. Which segments concentrate the risk in the latest month? */
DECLARE @latest DATE = (SELECT MAX(snapshot_date) FROM gold.fact_ar_open_items);

SELECT
    dc.segment,
    COUNT(DISTINCT f.customer_id)                                                    AS customers,
    SUM(f.amount_lc)                                                                 AS balance_lc,
    CAST(SUM(CASE WHEN f.is_overdue = 1 THEN f.amount_lc ELSE 0 END)
         / NULLIF(SUM(f.amount_lc), 0) AS DECIMAL(9, 4))                             AS overdue_ratio,
    CAST(SUM(CASE WHEN f.aging_bucket_id = 5 THEN f.amount_lc ELSE 0 END)
         / NULLIF(SUM(f.amount_lc), 0) AS DECIMAL(9, 4))                             AS over_90_ratio
FROM gold.fact_ar_open_items AS f
JOIN gold.dim_customer       AS dc ON dc.customer_id = f.customer_id
WHERE f.snapshot_date = @latest
GROUP BY dc.segment
ORDER BY over_90_ratio DESC;


/* Q3. Pareto: how many customers hold 80% of the overdue balance? */
WITH ranked AS (
    SELECT
        customer_id,
        overdue_balance_lc,
        SUM(overdue_balance_lc) OVER (ORDER BY overdue_balance_lc DESC, customer_id
                                      ROWS UNBOUNDED PRECEDING)
            / SUM(overdue_balance_lc) OVER ()                                        AS cumulative_share
    FROM gold.vw_customer_risk_latest
    WHERE overdue_balance_lc > 0
)
SELECT
    COUNT(*)                                                       AS overdue_customers,
    SUM(CASE WHEN cumulative_share <= 0.80 THEN 1 ELSE 0 END) + 1  AS customers_holding_80_pct
FROM ranked;


/* Q4. Roll-rate matrix: share of customers moving between worst buckets one month later */
WITH totals AS (
    SELECT
        from_bucket_id,
        to_bucket_id,
        SUM(customers) AS customers
    FROM gold.bucket_transitions
    GROUP BY from_bucket_id, to_bucket_id
),
shares AS (
    SELECT
        from_bucket_id,
        to_bucket_id,
        CAST(customers * 1.0 / SUM(customers) OVER (PARTITION BY from_bucket_id) AS DECIMAL(5, 3)) AS share
    FROM totals
)
SELECT
    ab.aging_bucket AS from_bucket,
    [0] AS settled, [1] AS not_due, [2] AS d0_30, [3] AS d31_60, [4] AS d61_90, [5] AS over_90
FROM shares
PIVOT (MAX(share) FOR to_bucket_id IN ([0], [1], [2], [3], [4], [5])) AS p
JOIN gold.dim_aging_bucket AS ab ON ab.aging_bucket_id = p.from_bucket_id
ORDER BY p.from_bucket_id;


/* Q5. Collector ranking by overdue share in the latest month */
SELECT
    collector,
    COUNT(*)                                                         AS customers,
    SUM(overdue_balance_lc)                                          AS overdue_balance_lc,
    CAST(SUM(overdue_balance_lc) / NULLIF(SUM(total_balance_lc), 0) AS DECIMAL(9, 4)) AS overdue_ratio,
    SUM(CASE WHEN trend_status = 'Deteriorated' THEN 1 ELSE 0 END)   AS deteriorated_customers,
    RANK() OVER (ORDER BY SUM(overdue_balance_lc) / NULLIF(SUM(total_balance_lc), 0) DESC) AS risk_rank
FROM gold.vw_customer_risk_latest
GROUP BY collector
ORDER BY risk_rank;


/* Q6. Focus list: deteriorating customers still in the recoverable 31-90 day window */
SELECT TOP (15)
    customer_name,
    segment,
    collector,
    worst_bucket,
    overdue_balance_lc,
    max_days_in_arrears
FROM gold.vw_customer_risk_latest
WHERE worst_bucket_id IN (3, 4)
  AND trend_status = 'Deteriorated'
ORDER BY overdue_balance_lc DESC;
