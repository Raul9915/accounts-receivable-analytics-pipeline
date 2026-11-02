"""Builds notebooks/01_portfolio_analysis.ipynb from code so it stays reviewable in Git diffs.

Run:  python notebooks/build_notebook.py && jupyter nbconvert --to notebook --execute --inplace notebooks/01_portfolio_analysis.ipynb
"""

from pathlib import Path

import nbformat as nbf

md = nbf.v4.new_markdown_cell
code = nbf.v4.new_code_cell

cells = [
    md(
        "# Accounts Receivable Portfolio Analysis\n\n"
        "Exploratory analysis on the **Gold layer** produced by the pipeline (`python -m ar_pipeline.run`).\n\n"
        "> All data is **synthetic**. Company names are fictional; no real customer or company information is used.\n\n"
        "**Questions answered**\n"
        "1. How large is the portfolio and how much of it is overdue?\n"
        "2. Is portfolio quality improving or deteriorating?\n"
        "3. Which segments, companies and collectors concentrate the risk?\n"
        "4. Once a customer falls behind, how likely is it to recover?\n"
        "5. Where should the collections team focus next month?"
    ),
    code(
        "from pathlib import Path\n\n"
        "import duckdb\n"
        "import pandas as pd\n\n"
        "pd.options.display.float_format = '{:,.2f}'.format\n"
        "GOLD = Path('..') / 'data' / 'gold'\n"
        "con = duckdb.connect()\n"
        "for table in GOLD.glob('*.parquet'):\n"
        "    con.execute(f\"CREATE VIEW {table.stem} AS SELECT * FROM read_parquet('{table.as_posix()}')\")\n"
        "con.sql('SHOW TABLES').df()"
    ),
    md("## 1. Portfolio size and overdue share"),
    code(
        "monthly = con.sql('''\n"
        "    SELECT snapshot_date,\n"
        "           round(sum(amount_lc) / 1e6, 1)                                     AS total_m,\n"
        "           round(sum(amount_lc) FILTER (WHERE is_overdue) / 1e6, 1)           AS overdue_m,\n"
        "           round(sum(amount_lc) FILTER (WHERE is_overdue) / sum(amount_lc), 3) AS overdue_ratio,\n"
        "           round(sum(amount_lc) FILTER (WHERE aging_bucket_id = 5) / sum(amount_lc), 3) AS over_90_ratio,\n"
        "           count(DISTINCT customer_id)                                        AS customers\n"
        "    FROM fact_ar_open_items GROUP BY 1 ORDER BY 1\n"
        "''').df()\n"
        "monthly"
    ),
    md(
        "**Reading:** the open balance grows moderately during the year, but the **overdue share rises much faster** "
        "and the >90-day bucket roughly quadruples its weight. Growth in receivables is coming from late payment, "
        "not from more sales."
    ),
    md("## 2. Risk by customer segment (latest month)"),
    code(
        "con.sql('''\n"
        "    SELECT d.segment,\n"
        "           count(DISTINCT f.customer_id)                                        AS customers,\n"
        "           round(sum(f.amount_lc) / 1e6, 1)                                     AS balance_m,\n"
        "           round(sum(f.amount_lc) FILTER (WHERE f.is_overdue) / sum(f.amount_lc), 3) AS overdue_ratio,\n"
        "           round(sum(f.amount_lc) FILTER (WHERE f.aging_bucket_id = 5) / sum(f.amount_lc), 3) AS over_90_ratio\n"
        "    FROM fact_ar_open_items f JOIN dim_customer d USING (customer_id)\n"
        "    WHERE f.snapshot_date = (SELECT max(snapshot_date) FROM fact_ar_open_items)\n"
        "    GROUP BY 1 ORDER BY over_90_ratio DESC\n"
        "''').df()"
    ),
    md("## 3. Concentration: how much of the overdue balance sits with a few customers?"),
    code(
        "pareto = con.sql('''\n"
        "    WITH latest AS (\n"
        "        SELECT c.customer_id, d.customer_name, d.segment, c.overdue_balance_lc, c.worst_bucket_id, c.trend_status\n"
        "        FROM customer_monthly c JOIN dim_customer d USING (customer_id)\n"
        "        WHERE c.snapshot_date = (SELECT max(snapshot_date) FROM customer_monthly) AND c.overdue_balance_lc > 0\n"
        "    )\n"
        "    SELECT *,\n"
        "           sum(overdue_balance_lc) OVER (ORDER BY overdue_balance_lc DESC) / sum(overdue_balance_lc) OVER () AS cumulative_share,\n"
        "           row_number() OVER (ORDER BY overdue_balance_lc DESC) AS rank\n"
        "    FROM latest ORDER BY overdue_balance_lc DESC\n"
        "''').df()\n"
        "customers_for_80 = int((pareto['cumulative_share'] <= 0.80).sum()) + 1\n"
        "print(f'{customers_for_80} of {len(pareto)} overdue customers ({customers_for_80/len(pareto):.0%}) hold 80% of the overdue balance')\n"
        "pareto.head(10)"
    ),
    md("## 4. Collector workload and portfolio quality"),
    code(
        "con.sql('''\n"
        "    SELECT d.collector,\n"
        "           count(DISTINCT f.customer_id)                                         AS customers,\n"
        "           round(sum(f.amount_lc) FILTER (WHERE f.is_overdue) / 1e6, 2)          AS overdue_m,\n"
        "           round(sum(f.amount_lc) FILTER (WHERE f.is_overdue) / sum(f.amount_lc), 3) AS overdue_ratio\n"
        "    FROM fact_ar_open_items f JOIN dim_customer d USING (customer_id)\n"
        "    WHERE f.snapshot_date = (SELECT max(snapshot_date) FROM fact_ar_open_items)\n"
        "    GROUP BY 1 ORDER BY overdue_m DESC\n"
        "''').df()"
    ),
    md("## 5. Roll rates: what happens one month after a customer falls behind?"),
    code(
        "transitions = con.sql('''\n"
        "    SELECT from_bucket_id, to_bucket_id, sum(customers) AS customers\n"
        "    FROM bucket_transitions GROUP BY ALL\n"
        "''').df()\n"
        "labels = dict(con.sql('SELECT aging_bucket_id, aging_bucket FROM dim_aging_bucket').fetchall())\n"
        "labels[0] = 'Settled'\n"
        "matrix = transitions.pivot_table(index='from_bucket_id', columns='to_bucket_id', values='customers', fill_value=0)\n"
        "matrix = matrix.div(matrix.sum(axis=1), axis=0).rename(index=labels, columns=labels)\n"
        "matrix.style.format('{:.0%}').background_gradient(cmap='Blues', axis=None)"
    ),
    md(
        "**Reading:** customers in 0-30 days still recover often, but the probability of recovery falls sharply with "
        "each bucket. Once a customer reaches **>90 days it almost never comes back** within a month, so the cheapest "
        "moment to act is the 31-60 bucket."
    ),
    md("## 6. Focus list for next month"),
    code(
        "con.sql('''\n"
        "    SELECT d.customer_name, d.segment, d.collector, c.worst_bucket_id,\n"
        "           round(c.overdue_balance_lc, 0) AS overdue_balance_lc, c.trend_status\n"
        "    FROM customer_monthly c JOIN dim_customer d USING (customer_id)\n"
        "    WHERE c.snapshot_date = (SELECT max(snapshot_date) FROM customer_monthly)\n"
        "      AND c.worst_bucket_id IN (3, 4)          -- 31-90 days: still recoverable\n"
        "      AND c.trend_status = 'Deteriorated'\n"
        "    ORDER BY c.overdue_balance_lc DESC\n"
        "    LIMIT 15\n"
        "''').df()"
    ),
    md(
        "## Conclusions\n\n"
        "* Portfolio growth is driven by **slower payment**, not by sales: the overdue share and the >90-day share rise "
        "throughout the year in all three companies.\n"
        "* Risk is **concentrated**: a minority of customers holds most of the overdue balance, so a short focus list "
        "covers most of the money at risk.\n"
        "* **Timing matters more than volume**: recovery probability collapses after 90 days. Prioritising "
        "deteriorating customers in the 31-90 day range gives the best return on collection effort.\n\n"
        "*Synthetic data: the patterns are realistic by design, but the figures do not describe any real company.*"
    ),
]

nb = nbf.v4.new_notebook(cells=cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}})
path = Path(__file__).with_name("01_portfolio_analysis.ipynb")
nbf.write(nb, path)
print(f"Wrote {path}")
