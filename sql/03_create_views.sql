/* =====================================================================
   03 - Reporting views
   Business-friendly views on top of the Gold star schema.
   Run against: ARAnalytics
   ===================================================================== */

/* Portfolio by month, company, business area and aging bucket, with readable names */
CREATE OR ALTER VIEW gold.vw_portfolio_monthly
AS
SELECT
    d.snapshot_date,
    d.year_month,
    c.company_name,
    ba.business_area_name,
    ab.aging_bucket_id,
    ab.aging_bucket,
    ab.is_overdue_bucket,
    a.balance_lc,
    a.documents,
    a.customers
FROM gold.agg_portfolio_monthly AS a
JOIN gold.dim_date          AS d  ON d.snapshot_date    = a.snapshot_date
JOIN gold.dim_company       AS c  ON c.company_code     = a.company_code
JOIN gold.dim_business_area AS ba ON ba.business_area   = a.business_area
JOIN gold.dim_aging_bucket  AS ab ON ab.aging_bucket_id = a.aging_bucket_id;
GO

/* Headline KPIs per month, with month-over-month change in overdue balance */
CREATE OR ALTER VIEW gold.vw_kpi_monthly
AS
WITH monthly AS (
    SELECT
        f.snapshot_date,
        SUM(f.amount_lc)                                                        AS total_balance_lc,
        SUM(CASE WHEN f.is_overdue = 1 THEN f.amount_lc ELSE 0 END)             AS overdue_balance_lc,
        SUM(CASE WHEN f.aging_bucket_id = 5 THEN f.amount_lc ELSE 0 END)        AS over_90_balance_lc,
        COUNT(DISTINCT f.customer_id)                                           AS customers
    FROM gold.fact_ar_open_items AS f
    GROUP BY f.snapshot_date
)
SELECT
    snapshot_date,
    total_balance_lc,
    overdue_balance_lc,
    over_90_balance_lc,
    customers,
    CAST(overdue_balance_lc / NULLIF(total_balance_lc, 0) AS DECIMAL(9, 4))   AS overdue_ratio,
    CAST(over_90_balance_lc / NULLIF(total_balance_lc, 0) AS DECIMAL(9, 4))   AS over_90_ratio,
    overdue_balance_lc - LAG(overdue_balance_lc) OVER (ORDER BY snapshot_date) AS overdue_change_lc
FROM monthly;
GO

/* Latest-month customer risk list, ready for the collections team */
CREATE OR ALTER VIEW gold.vw_customer_risk_latest
AS
SELECT
    cm.snapshot_date,
    cm.customer_id,
    dc.customer_name,
    dc.segment,
    dc.collector,
    ab.aging_bucket                                     AS worst_bucket,
    cm.worst_bucket_id,
    cm.total_balance_lc,
    cm.overdue_balance_lc,
    cm.overdue_ratio,
    cm.max_days_in_arrears,
    cm.trend_status,
    RANK() OVER (ORDER BY cm.overdue_balance_lc DESC)   AS overdue_rank
FROM gold.customer_monthly AS cm
JOIN gold.dim_customer     AS dc ON dc.customer_id     = cm.customer_id
JOIN gold.dim_aging_bucket AS ab ON ab.aging_bucket_id = cm.worst_bucket_id
WHERE cm.snapshot_date = (SELECT MAX(snapshot_date) FROM gold.customer_monthly);
GO
