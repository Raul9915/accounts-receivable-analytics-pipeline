/* =====================================================================
   02 - Gold star schema
   Serving layer for Power BI. Tables are fully reloaded by
   `python -m ar_pipeline.load_sqlserver` inside one transaction.
   Run against: ARAnalytics
   Re-runnable: drops and recreates the gold tables (data is reloaded
   from the Parquet files in data/gold, which are the source of truth).
   ===================================================================== */

IF SCHEMA_ID(N'gold') IS NULL
    EXEC (N'CREATE SCHEMA gold AUTHORIZATION dbo;');
GO

-- Drop in dependency order (views first, then facts, then dimensions)
DROP VIEW  IF EXISTS gold.vw_kpi_monthly;
DROP VIEW  IF EXISTS gold.vw_customer_risk_latest;
DROP VIEW  IF EXISTS gold.vw_portfolio_monthly;
DROP TABLE IF EXISTS gold.bucket_transitions;
DROP TABLE IF EXISTS gold.customer_monthly;
DROP TABLE IF EXISTS gold.agg_portfolio_monthly;
DROP TABLE IF EXISTS gold.fact_ar_open_items;
DROP TABLE IF EXISTS gold.dim_customer;
DROP TABLE IF EXISTS gold.dim_date;
DROP TABLE IF EXISTS gold.dim_aging_bucket;
DROP TABLE IF EXISTS gold.dim_business_area;
DROP TABLE IF EXISTS gold.dim_company;
GO

/* ---------------------------- Dimensions ---------------------------- */

CREATE TABLE gold.dim_company (
    company_code        VARCHAR(4)      NOT NULL,
    company_name        NVARCHAR(100)   NOT NULL,
    CONSTRAINT PK_dim_company PRIMARY KEY (company_code)
);

CREATE TABLE gold.dim_business_area (
    business_area       VARCHAR(3)      NOT NULL,
    business_area_name  NVARCHAR(50)    NOT NULL,
    CONSTRAINT PK_dim_business_area PRIMARY KEY (business_area)
);

CREATE TABLE gold.dim_aging_bucket (
    aging_bucket_id     TINYINT         NOT NULL,
    aging_bucket        VARCHAR(10)     NOT NULL,
    is_overdue_bucket   BIT             NOT NULL,
    CONSTRAINT PK_dim_aging_bucket PRIMARY KEY (aging_bucket_id)
);

CREATE TABLE gold.dim_date (
    snapshot_date       DATE            NOT NULL,
    [year]              SMALLINT        NOT NULL,
    [month]             TINYINT         NOT NULL,
    year_month          CHAR(7)         NOT NULL,
    month_label         VARCHAR(10)     NOT NULL,
    CONSTRAINT PK_dim_date PRIMARY KEY (snapshot_date)
);

CREATE TABLE gold.dim_customer (
    customer_id         VARCHAR(10)     NOT NULL,
    customer_name       NVARCHAR(150)   NOT NULL,
    segment             VARCHAR(20)     NOT NULL,
    collector           VARCHAR(10)     NOT NULL,
    first_snapshot      DATE            NOT NULL,
    last_snapshot       DATE            NOT NULL,
    CONSTRAINT PK_dim_customer PRIMARY KEY (customer_id)
);
GO

/* ------------------------------- Fact ------------------------------- */
-- Grain: one open document per company and month-end snapshot.

CREATE TABLE gold.fact_ar_open_items (
    snapshot_date       DATE            NOT NULL,
    company_code        VARCHAR(4)      NOT NULL,
    customer_id         VARCHAR(10)     NOT NULL,
    business_area       VARCHAR(3)      NOT NULL,
    aging_bucket_id     TINYINT         NOT NULL,
    document_number     VARCHAR(12)     NOT NULL,
    document_type       VARCHAR(2)      NOT NULL,
    posting_date        DATE            NOT NULL,
    due_date            DATE            NOT NULL,
    amount_lc           DECIMAL(18, 2)  NOT NULL,
    days_in_arrears     INT             NOT NULL,
    is_overdue          BIT             NOT NULL,
    CONSTRAINT PK_fact_ar_open_items PRIMARY KEY (snapshot_date, company_code, document_number),
    CONSTRAINT FK_fact_date      FOREIGN KEY (snapshot_date)   REFERENCES gold.dim_date (snapshot_date),
    CONSTRAINT FK_fact_company   FOREIGN KEY (company_code)    REFERENCES gold.dim_company (company_code),
    CONSTRAINT FK_fact_customer  FOREIGN KEY (customer_id)     REFERENCES gold.dim_customer (customer_id),
    CONSTRAINT FK_fact_area      FOREIGN KEY (business_area)   REFERENCES gold.dim_business_area (business_area),
    CONSTRAINT FK_fact_bucket    FOREIGN KEY (aging_bucket_id) REFERENCES gold.dim_aging_bucket (aging_bucket_id),
    CONSTRAINT CK_fact_amount_not_zero CHECK (amount_lc <> 0),
    CONSTRAINT CK_fact_due_after_posting CHECK (due_date >= posting_date)
);

-- Supports customer-level analysis and slicing by customer in Power BI
CREATE NONCLUSTERED INDEX IX_fact_customer_snapshot
    ON gold.fact_ar_open_items (customer_id, snapshot_date)
    INCLUDE (amount_lc, aging_bucket_id, is_overdue);
GO

/* ------------------------- Analytical tables ------------------------ */

CREATE TABLE gold.agg_portfolio_monthly (
    snapshot_date       DATE            NOT NULL,
    company_code        VARCHAR(4)      NOT NULL,
    business_area       VARCHAR(3)      NOT NULL,
    aging_bucket_id     TINYINT         NOT NULL,
    balance_lc          DECIMAL(18, 2)  NOT NULL,
    documents           INT             NOT NULL,
    customers           INT             NOT NULL,
    CONSTRAINT PK_agg_portfolio_monthly PRIMARY KEY (snapshot_date, company_code, business_area, aging_bucket_id),
    CONSTRAINT FK_agg_date    FOREIGN KEY (snapshot_date)   REFERENCES gold.dim_date (snapshot_date),
    CONSTRAINT FK_agg_company FOREIGN KEY (company_code)    REFERENCES gold.dim_company (company_code),
    CONSTRAINT FK_agg_area    FOREIGN KEY (business_area)   REFERENCES gold.dim_business_area (business_area),
    CONSTRAINT FK_agg_bucket  FOREIGN KEY (aging_bucket_id) REFERENCES gold.dim_aging_bucket (aging_bucket_id)
);

CREATE TABLE gold.customer_monthly (
    snapshot_date                DATE            NOT NULL,
    customer_id                  VARCHAR(10)     NOT NULL,
    total_balance_lc             DECIMAL(18, 2)  NOT NULL,
    overdue_balance_lc           DECIMAL(18, 2)  NOT NULL,
    overdue_ratio                DECIMAL(9, 4)   NULL,
    worst_bucket_id              TINYINT         NOT NULL,
    max_days_in_arrears          INT             NOT NULL,
    open_documents               INT             NOT NULL,
    previous_worst_bucket_id     TINYINT         NULL,
    previous_overdue_balance_lc  DECIMAL(18, 2)  NOT NULL,
    trend_status                 VARCHAR(15)     NOT NULL,
    CONSTRAINT PK_customer_monthly PRIMARY KEY (snapshot_date, customer_id),
    CONSTRAINT FK_cm_date     FOREIGN KEY (snapshot_date)   REFERENCES gold.dim_date (snapshot_date),
    CONSTRAINT FK_cm_customer FOREIGN KEY (customer_id)     REFERENCES gold.dim_customer (customer_id),
    CONSTRAINT FK_cm_bucket   FOREIGN KEY (worst_bucket_id) REFERENCES gold.dim_aging_bucket (aging_bucket_id),
    CONSTRAINT CK_cm_trend CHECK (trend_status IN ('Baseline', 'New', 'Improved', 'Stable', 'Deteriorated'))
);

-- to_bucket_id = 0 means "settled" (no open items next month), so it has no FK to dim_aging_bucket
CREATE TABLE gold.bucket_transitions (
    to_snapshot         DATE            NOT NULL,
    from_snapshot       DATE            NOT NULL,
    from_bucket_id      TINYINT         NOT NULL,
    to_bucket_id        TINYINT         NOT NULL,
    customers           INT             NOT NULL,
    from_balance_lc     DECIMAL(18, 2)  NOT NULL,
    CONSTRAINT PK_bucket_transitions PRIMARY KEY (to_snapshot, from_bucket_id, to_bucket_id),
    CONSTRAINT CK_bt_to_bucket CHECK (to_bucket_id BETWEEN 0 AND 5)
);
GO
