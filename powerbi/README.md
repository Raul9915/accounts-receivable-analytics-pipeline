# Power BI Dashboard - Build Guide

The Power BI report sits on top of the **Gold layer served from SQL Server** (database `ARAnalytics`,
schema `gold`). Run the pipeline and the load first:

```powershell
python -m ar_pipeline.run
python -m ar_pipeline.load_sqlserver --init
```

## 1. Load the data

**Option A — SQL Server (recommended)**

1. Power BI Desktop → **Get data → SQL Server database**.
2. Server: `localhost` (or `localhost\SQLEXPRESS`), Database: `ARAnalytics`, mode **Import**.
3. Select the tables `gold.fact_ar_open_items`, `gold.dim_date`, `gold.dim_company`, `gold.dim_customer`,
   `gold.dim_business_area`, `gold.dim_aging_bucket`, `gold.customer_monthly`, `gold.bucket_transitions`.
4. Power BI detects most relationships automatically from the foreign keys; check them against the table below.

**Option B — Parquet files (no SQL Server)**

Power BI Desktop → **Get data → Parquet**, one file per table from `data/gold/`. Set dates as *Date* and
`amount_lc` and balance columns as *Fixed decimal number*.

## 2. Relationships (star schema)

| From (many) | To (one) | Key |
|---|---|---|
| `fact_ar_open_items` | `dim_date` | `snapshot_date` |
| `fact_ar_open_items` | `dim_company` | `company_code` |
| `fact_ar_open_items` | `dim_customer` | `customer_id` |
| `fact_ar_open_items` | `dim_business_area` | `business_area` |
| `fact_ar_open_items` | `dim_aging_bucket` | `aging_bucket_id` |
| `customer_monthly` | `dim_date` | `snapshot_date` |
| `customer_monthly` | `dim_customer` | `customer_id` |

All relationships single-direction (dimension → fact). `bucket_transitions` stays disconnected; it is
used only by the roll-rate matrix.

Sort `dim_aging_bucket[aging_bucket]` by `aging_bucket_id`, and `dim_date[month_label]` by `snapshot_date`.

## 3. Measures

1. Create an empty table: **Home → Enter data**, name it `_Measures`, load it.
2. Open **TMDL view**, paste [`measures.tmdl`](measures.tmdl) and click **Apply**. This creates all 25 measures
   with formats, descriptions and display folders in one step.
3. Delete the placeholder column of `_Measures`; the table becomes a measure table.

The same measures are listed in readable form in [`measures.dax`](measures.dax). They are **semi-additive**:
a balance is a month-end photo, so it is never summed across months.

## 4. Report pages

| Page | Visuals | Question it answers |
|---|---|---|
| **Executive overview** | KPI cards (Total Balance, Overdue %, Over 90 %, Customers Over 90, each with its month-over-month change); stacked column of balance by aging bucket per month; line of Overdue % by company; slicers for company, business area, segment | How big is the portfolio and is it getting worse? |
| **Customer risk** | Table of customers with Overdue Balance, worst bucket and trend status (conditional formatting on bucket); bar of Overdue % by segment; Top 10 Overdue Concentration % card | Who concentrates the risk? |
| **Roll rates** | Matrix with `from_bucket_id` in rows, `to_bucket_id` in columns and `Roll Rate %` as values, background color scale | Once a customer falls behind, does it recover? |
| **Collectors** | Bar of Overdue Balance by collector; table with Customers Deteriorated / Improved per collector | Where should the collections team focus? |

Design tips: one accent color for overdue, a light-to-dark single-hue scale for aging buckets, no 3D or
pie charts, titles phrased as the question each visual answers.

## 5. Add it to the repository

1. Save the report as `powerbi/ar_portfolio_dashboard.pbix`.
2. Export one screenshot per page to `images/powerbi_overview.png`, `images/powerbi_customer_risk.png`, etc.
3. Add the screenshots to the main README under **Dashboard**.
