# E-commerce User Behavior Analytics Platform

This project uses Kaggle E-commerce Clickstream data and a self-defined Product Master dataset to build a high-quality user behavior log (Silver Layer) for data-driven decision-making, and designs a Gold Layer mart for conversion funnel analysis.

Data quality validation is automated using dbt and Airflow, with a dbt tests + Failure Layer structure to detect integrity issues early.

---

## Project Purpose & Scope

### Goals

- Build a high-quality user behavior log (Silver Layer) usable for funnel analysis
- Develop a Gold mart (`product_funnel`) for conversion analysis based on defined KPIs
- Build a data quality validation framework using dbt tests + Failure Layer
- Configure an automated pipeline with Airflow

### Scope

- Design and implement Medallion Architecture (Raw → Bronze → Silver → Gold)
- Build `slv_clickstream` model (core user behavior log)
- Build `product_funnel` Gold fact table (weekly, user-level)
- Build `mart_aarrr` Gold mart (weekly AARRR aggregate — primary analytical surface)
- Design `dq_failures` storage structure for dbt test failures
- Build Airflow DAG
- Define KPI Dictionary and data quality rules

---

## Medallion Architecture

### Raw Layer (`my_dbt_project_raw`)

- Loaded as-is from source
- Two tables: `clickstream` / `product_master`

#### [clickstream](https://www.kaggle.com/datasets/waqi786/e-commerce-clickstream-and-transaction-dataset)

| Column Name | Details |
| ----------- | ------- |
| UserID | Unique identifier for each user |
| SessionID | Unique identifier for each session |
| Timestamp | Date and time of the interaction |
| EventType | Type of event (page_view, product_view, add_to_cart, purchase) |
| ProductID | Unique identifier for products involved in interactions |
| Amount | Amount of the transaction (for purchases) |
| Outcome | Target event (e.g., purchase) |

#### product_master

| Column Name | Details |
| ---------------- | ---------------------------------- |
| product_id | Unique identifier for each product |
| manufacturer | Name of the manufacturer |
| management_group | A category for managing products |

---

### Bronze Layer (`my_dbt_project_bronze`)

- Lightweight transformation only: column name standardization, type casting
- No business logic applied (simple staging)

---

### Silver Layer (`my_dbt_project_silver.slv_clickstream`)

- Standardized event table with quality validation applied for funnel analysis
- Includes event timestamp ordering, product FK validation, and type refinement
- Single source for the Gold mart (`product_funnel`)
- Partitioned by `event_timestamp` (day), clustered by `(user_id, session_id)`

| Column Name | Details |
| ---------------------- | ---------------------------------------------------------------------- |
| user_id | Unique identifier for each user |
| session_id | Session identifier |
| event_timestamp | Time the event occurred |
| event_type | Event type |
| product_id | Product identifier linked to the event (nullable for page_view) |
| amount | Transaction amount for purchase events (nullable) |
| is_valid_event | Data quality validation pass flag (1 = valid; only valid events flow to Gold) |
| event_order_in_session | Sequential order of the event within a session |
| funnel_step | Funnel stage mapping: page_view(1) → product_view(2) → add_to_cart(3) → purchase(4) |
| is_complete_funnel | 1 if the user's daily funnel steps are sequential with no skipped stages; 0 otherwise |

**dbt Tests (all with `store_failures: true`):**

| Test | Column | Notes |
| ---- | ------ | ----- |
| `not_null` | user_id, session_id, event_timestamp, event_type | Required field checks |
| `accepted_values` | event_type | Validates against the four known event types |
| `relationships` | product_id | FK check against `brz_product_master` (where product_id IS NOT NULL) |
| `column_length` | product_id | Length must be between 1–4 characters (where product_id IS NOT NULL) |
| `unique_combination_of_columns` | (user_id, session_id, event_timestamp) | Deduplication check |

The `column_length` generic test is defined in `macros/tests/column_length.sql`. Only records with `is_valid_event = 1` are passed to the Gold fact table.

---

### DQ Failure Layer (`my_dbt_project_dq_failures`)

- Layer that automatically stores records failing dbt tests
- Configured via `tests.+store_failures: true` in `dbt_project.yml`

---

### Gold Layer

#### `product_funnel` — Fact Table (user-week level)

Weekly user-level funnel activity. One row per user per week. Built exclusively from valid events (`is_valid_event = 1`).

| Column Name | Details |
| ----------------------- | ---------------------------------------------------- |
| week_start | Week start date (Monday), via `DATE_TRUNC(..., WEEK(MONDAY))` |
| user_id | User identifier |
| page_views | Count of page_view events for this user this week |
| product_views | Count of product_view events for this user this week |
| add_to_carts | Count of add_to_cart events for this user this week |
| purchases | Count of purchase events for this user this week |
| total_revenue | Sum of purchase amounts for this user this week |
| is_activated | 1 if first purchase occurred within 7 days of first-ever page_view (lifetime flag) |
| is_retained | 1 if user re-purchased within 30 days of a prior purchase this week |
| first_page_view_date | Date of the user's first ever page_view (lifetime) |
| first_purchase_date | Date of the user's first ever purchase (lifetime) |

#### `mart_aarrr` — MART (weekly aggregate)

Aggregated from `product_funnel`. One row per week. **Primary surface for growth marketing analysis and cohort studies.**

Based on the **AARRR Growth Marketing Framework** (Referral excluded — not available in this dataset).

| Column Name | Details |
| ----------------------- | ---------------------------------------------------- |
| week_start | Start of the analysis week (Monday) |
| acquisition_users | Users with ≥1 page_view this week — **Acquisition** |
| activated_users | Users whose first purchase was within 7 days of first visit — **Activation** |
| retained_users | Users who re-purchased within 30 days of a prior purchase — **Retention** |
| total_revenue | Total purchase revenue for the week — **Revenue** |
| paying_users | Distinct users with ≥1 purchase this week |
| total_purchases | Total number of purchase events |
| page_view_users | Distinct users with ≥1 page_view (funnel step 1) |
| product_view_users | Distinct users with ≥1 product_view (funnel step 2) |
| add_to_cart_users | Distinct users with ≥1 add_to_cart (funnel step 3) |
| purchase_users | Distinct users with ≥1 purchase (funnel step 4) |
| activation_rate | activated_users / acquisition_users |
| retention_rate | retained_users / paying_users |
| avg_revenue_per_paying_user | total_revenue / paying_users (ARPPU) |
| pv_to_product_view_cvr | product_view_users / page_view_users |
| product_view_to_atc_cvr | add_to_cart_users / product_view_users |
| atc_to_purchase_cvr | purchase_users / add_to_cart_users |
| overall_funnel_cvr | purchase_users / page_view_users (end-to-end conversion) |

---

## Data Quality Targets

| Metric | Target | Definition |
| ------------ | ------- | ----------------------------- |
| Completeness | 98% | Required column fill rate |
| Validity | 95% | Accepted values & FK match rate |
| Timeliness | 99% | ETL success rate & freshness |
| MTTA | 1 hour | Time to detect a dbt test failure |
| MTTR | 24 hours | Time to resolve a detected issue |

---

## Functional Requirements

- **Silver Layer** (`my_dbt_project_silver.slv_clickstream`)
  - Build a standardized user behavior log from Raw/Bronze clickstream data
  - Generate session-level event sequence (`event_order_in_session`) sorted by event timestamp
  - Map `event_type` to funnel steps (`funnel_step`): 1(page_view) → 2(product_view) → 3(add_to_cart) → 4(purchase)
  - Set up FK join structure using `product_master` on `product_id`
  - Calculate `is_valid_event` quality flag per event
  - Calculate `is_complete_funnel` flag per user per day — identifies users who skipped funnel steps (e.g., jumped from step 1 to step 4); use this to include or exclude users from strict funnel conversion analysis
- **Data Quality Validation** (dbt tests on `slv_clickstream`)
  - `not_null` (user_id, session_id, event_type, event_timestamp)
  - `accepted_values` (event_type)
  - FK validation via `relationships` test on product_id (where not null)
  - Custom `column_length` test on product_id (1–4 chars, where not null) — defined in `macros/tests/column_length.sql`; only records passing this test flow to the Gold fact table
  - `unique_combination_of_columns` on (user_id, session_id, event_timestamp)
  - Failed rows stored to `my_dbt_project_dq_failures` schema
- **Gold Fact Table** (`my_dbt_project_gold.product_funnel`)
  - Weekly user-level aggregation from valid Silver events (`is_valid_event = 1`)
  - Retention flag: user re-purchased within 30 days of a prior purchase
  - Activation flag: user completed first purchase within 7 days of first page_view
- **Gold MART** (`my_dbt_project_gold.mart_aarrr`)
  - Aggregated weekly AARRR metrics from the `product_funnel` fact table
  - Funnel conversion rates, activation rate, retention rate, ARPPU
  - Designed for AARRR-based cohort analysis in BigQuery (e.g., Python + BigQuery growth marketing analysis)

---

## Non-functional Requirements

- Gold mart ready every day before 7:00 AM
- Airflow DAG success rate ≥ 99%
- Track test failure counts and run quality improvement metrics
- Silver/Gold schema must be extensible when new event types are added

---

## Pipeline · Airflow DAG

**DAG ID:** `dbt_pipeline_via_container`
**Schedule:** Daily at 5:00 AM (Gold mart ready before 7:00 AM SLA)

```
dbt_debug
    ↓
dbt_run_bronze
    ↓
dbt_run_silver
    ↓
dbt_test_silver  ←── failures stored to dq_failures schema
    ↓
dbt_run_gold     (product_funnel + mart_aarrr)
    ↓
dbt_test_gold
```

---

## Tech Stack

| Component | Technology |
| --------- | ---------- |
| Data Warehouse | Google BigQuery |
| Transformation | dbt-bigquery |
| Orchestration | Apache Airflow 2.8.1 (CeleryExecutor) |
| Containerization | Docker + Docker Compose |
| Message Broker | Redis |
| Metadata DB | PostgreSQL 13 |
| GCP Auth | Service Account (JSON key) |
