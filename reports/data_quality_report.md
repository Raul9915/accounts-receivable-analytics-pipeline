# Data Quality Report

> Generated automatically by the pipeline. All data is synthetic.

## Row flow

| Layer | Rows |
|---|---:|
| Bronze (raw rows ingested) | 37,251 |
| Silver - accepted | 36,995 |
| Silver - rejected | 256 (0.69%) |
| Gold - fact_ar_open_items | 36,995 |

## Rejected rows by rule

| Rule | Rows |
|---|---:|
| `duplicate_document` | 148 |
| `missing_customer` | 45 |
| `due_date_before_posting_date` | 32 |
| `zero_amount` | 31 |

## Control: SAP days in arrears vs. recalculated from dates

117 accepted rows have a different value when days in arrears is recalculated from the due date. SAP remains the official value; these rows are flagged (`days_in_arrears_mismatch`) for follow-up with the source system owner.

## Reconciliation checks

| Check | Result |
|---|---|
| bronze rows equal accepted plus rejected | PASS |
| fact rows equal silver accepted | PASS |
| fact amount equal silver amount | PASS |
| no duplicate documents in fact | PASS |
| every fact row has customer | PASS |
| every fact row has aging bucket | PASS |
| customer monthly reconciles to fact | PASS |
