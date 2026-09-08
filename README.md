# CPG Supply Chain Data Warehouse
## Snowflake Medallion Pipeline with Automated Orchestration & Power BI Analytics
A production-style, end-to-end data engineering project simulating a Consumer Packaged Goods (CPG) supply chain analytics platform — built on Snowflake using medallion architecture (Bronze → Silver → Gold), automated with native Snowflake Task orchestration, and surfaced through a 3-page Power BI report covering profitability, discounting, and delivery performance.

## Overview
This project models a realistic supply chain analytics pipeline for a CPG-style organization the same category of business as Procter & Gamble, Healthy Options, or any product based company. It ingests raw order and logistics data, refines it through a three-layer medallion architecture, and exposes curated, business-ready data to both SQL analysts and BI tooling.

### What this project demonstrates:
- Cloud data warehouse design using Snowflake (staging, file formats, schemas, RBAC)
- Medallion architecture (Bronze/Silver/Gold) with deliberate grain and data-quality decisions at each layer
- Dimensional modeling (star schema: fact + conformed dimensions)
- Change Data Capture via Snowflake Streams, feeding an incremental MERGE-based transformation
- Automated orchestration via a scheduled Snowflake Task DAG with conditional execution and run logging
- Finance-oriented data marts translating operational data into business metrics (margin, discount effectiveness, delivery risk exposure)
- A 3-page Power BI report built on the curated Gold layer only

## Pipeline Architecture
<img src="img/Project Visualization.png" alt="Pipeline architecture" width="800">
## Design Decisions
 
- **Why `TRY_TO_...()` instead of hard casts?**
A single malformed value in a 180K-row batch shouldn't fail the entire load. `TRY_...` functions convert bad values to `NULL`, which are then caught by explicit data-quality checks — a loud failure is deferred to a controlled validation step, not a silent pipeline crash.
 
- **Why full-refresh for Gold instead of incremental `MERGE`?**
The expensive part of incremental processing is protecting against reprocessing *large* raw data — that saving already happens at Bronze → Silver. Gold tables are derived from an already-clean, much smaller Silver table, so a full rebuild is cheap, simple to reason about, and avoids matching-key/upsert edge cases. Added pipeline complexity should be justified by an actual performance problem — here, it isn't.
 
- **Why is `ORDER_ITEM_ID` the chosen grain, not `ORDER_ID`?**
An order can contain multiple line items; deduplicating or aggregating at the wrong grain silently produces incorrect totals. `ORDER_ITEM_ID` is the true unique identifier of a row in this dataset and is used consistently as the dedup/merge key from Silver onward.
 
- **Why does Bronze retain duplicate rows rather than deduplicating on load?**
Bronze's purpose is raw lineage preservation, not correctness — Silver is where deduplication logic lives. This was validated directly.
---

## Validated Resilience
 
Rather than assuming the incremental pipeline worked correctly, it was deliberately stress-tested:
 
- **Overlapping file reload test:** a new batch file containing rows that overlapped with previously loaded data was introduced into the stage. Result: Bronze correctly retained both raw copies (duplicates present, as expected for a raw layer), while Silver's `ROW_NUMBER()`-based deduplication logic automatically resolved the duplication with zero manual intervention — confirmed via direct duplicate-count queries before and after.
- **End-to-end trace test:** a synthetic order row was injected into a new staged file and traced through all three layers (Bronze → Silver → Gold) after a scheduled task run, confirming the full chain — file detection, incremental Bronze load, stream-triggered Silver merge, and Gold rebuild — functions correctly end-to-end, not just in isolated steps.

## Dataset
Source: DataCo Smart Supply Chain Dataset — ~180,000 real-world-style order and shipping records, 53 columns, covering product categories, customer segments, order/shipping dates, shipping modes, discounts, and profit.
```Note on "real-world" data: this is a publicly available dataset representing realistic supply chain operations — not literal proprietary data from any named company. The CPG/P&G framing describes the industry context and use case this pipeline was designed to serve, not a claim about the data's origin.```
Order date range in the source data: January 2015 – January 2018.

## Data Model
 
**Gold layer - star schema:**
 
| Table | Grain | Description |
|---|---|---|
| `FACT_ORDERS` | One row per order line item | Core transactional fact table — sales, profit, discount, shipping performance |
| `DIM_PRODUCT` | One row per product | Product name, category, department, price |
| `DIM_CUSTOMER` | One row per customer | Segment, city, state, country |
| `DIM_DATE` | One row per calendar day | Generated calendar dimension (`GENERATOR` + `SEQ4`), 2015–2024 |
| `DIM_SHIPPING_MODE` | One row per shipping mode | Reference dimension |
 
**Finance marts:**
 
| Mart | Answers |
|---|---|
| `MART_PROFIT_BY_CATEGORY_REGION` | Which categories/regions are actually profitable, not just high-selling? |
| `MART_LATE_DELIVERY_IMPACT` | Which shipping modes/regions carry the most late-delivery risk, and what revenue is exposed? |
| `MART_DISCOUNT_EFFECTIVENESS` | Which categories are discounted heavily without the margin to justify it? |
 
---

## Orchestration
 
A three-task DAG, chained via `AFTER` dependencies:
 
```sql
TASK_LOAD_BRONZE (root, CRON-scheduled)
   └─► TASK_TRANSFORM_SILVER (WHEN stream has data → MERGE)
          └─► TASK_REFRESH_GOLD (WHEN stream has data → full rebuild)
```
 
- **`TASK_LOAD_BRONZE`** — scheduled via CRON, runs `COPY INTO`. Snowflake's built-in load-history tracking makes this idempotent — a previously loaded file is never reprocessed.
- **`TASK_TRANSFORM_SILVER`** — triggered only when `SYSTEM$STREAM_HAS_DATA()` is true on the Bronze stream. Uses `MERGE` to upsert only new/changed rows into Silver — avoiding a full reprocess of the entire table on every run.
- **`TASK_REFRESH_GOLD`** — triggered only when the Silver stream has data. Rebuilds the star schema and marts.
- Every task run is logged to `UTILS.PIPELINE_RUN_LOG` (task name, layer, status, row count, timestamp) — basic pipeline observability.

---

## Dashboards
 
A 3-page Power BI report, connected to the **Gold schema only**
 
1. **Executive Summary** — 5 headline KPIs (Total Sales, Total Profit, Overall Margin %, Late Delivery Rate %, Sales at Risk) + Top 5 Categories by Sales + Sales Trend Over Time. Scoped deliberately to a "5-second glance" — no more than 5–7 total visual elements.
2. **Profitability** — margin by category and region, discount rate vs. margin scatter analysis, discount cost breakdown, and a profit-by-shipping-mode donut.
3. **Delivery Performance** — late delivery rate and sales-at-risk by shipping mode and region, delivery status breakdown, and average days late.
---
<img src="placeholder" alt="star_schema" width="300">
<img src="placeholder" alt="star_schema" width="300">
<img src="placeholder" alt="star_schema" width="300">

### Setup
-- 1. Warehouse
```sql
CREATE WAREHOUSE IF NOT EXISTS CPG_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE CPG_WH;
```
-- 2. Database + medallion schemas
```sql
CREATE DATABASE IF NOT EXISTS CPG_SUPPLY_CHAIN;
USE DATABASE CPG_SUPPLY_CHAIN;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS UTILS;
```
-- 3. File format + stage
```sql
CREATE FILE FORMAT IF NOT EXISTS UTILS.CSV_FF
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  ENCODING = 'ISO-8859-1';

CREATE STAGE IF NOT EXISTS UTILS.SUPPLY_CHAIN_STAGE
  FILE_FORMAT = UTILS.CSV_FF;
```
## Bronze
### Creation of Bronze Table
```sql
DROP TABLE IF EXISTS BRONZE_ORDERS;
CREATE TABLE IF NOT EXISTS BRONZE_ORDERS (
    *COLUMN NAMES STRING DTYPE,
    _LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), --exact time this row was inserted
    _SOURCE_FILE STRING -- for data lineage if we ever load multiple files into this table
```

### Ingesting the Data to the Bronze_Orders table
```sql
COPY INTO BRONZE_ORDERS (
    *COLUMN NAMES
    _SOURCE_FILE-- destination column that receives METADATA$FILENAME below
FROM (
    -- $1 through $53 = source CSV columns, read by position (matches header order)
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
           $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,
           $39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,
           METADATA$FILENAME --- the 54th value: which file this row came from
   reading directly from a stage
    FROM @UTILS.SUPPLY_CHAIN_STAGE
    )
FILE_FORMAT = (FORMAT_NAME = UTILS.CSV_FF)
ON_ERROR = 'CONTINUE'
PATTERN = '.*\.csv';
)
```

## Data Validation and Exploration
### Ingestion Validation Checking

```sql
SELECT COUNT(*) FROM BRONZE_ORDERS;
SELECT * FROM BRONZE_ORDERS LIMIT 10;
DESCRIBE TABLE BRONZE_ORDERS;
```
### Finding the grain
- Checking the which is the grain
- ORDER_ID repeats across rows, but ORDER_ITEM_ID is unique per row. That tells me the table's grain is 'one order line item' and every single row in this table represents exactly one item within an order, not the whole order.

```sql
SELECT ORDER_ITEM_ID, COUNT(*) 
FROM BRONZE_ORDERS
GROUP BY ORDER_ITEM_ID
HAVING COUNT(*) > 1

SELECT ORDER_ID, COUNT(*) 
FROM BRONZE_ORDERS
GROUP BY ORDER_ID
HAVING COUNT(*) > 1
```

## Silver
### Objectives
- TRY_TO_...() everywhere instead of :: — bad values become NULL, not a crashed load
- Renamed Columns for clarity
- Dropped CUSTOMER_EMAIL, CUSTOMER_FNAME/LNAME, CUSTOMER_PASSWORD, CUSTOMER_STREET, PRODUCT_IMAGE, PRODUCT_DESCRIPTION — not needed for our analytics and it is only a generated synthetic PII data
- WHERE ORDER_ID IS NOT NULL — remove nulls since we can't identify orders that's null
- QUALIFY ROW_NUMBER() ... = 1 — this is the dedup, enforcing our grain (ORDER_ITEM_ID)

```sql
USE DATABASE CPG_SUPPLY_CHAIN;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE SILVER_ORDERS AS
SELECT
    -- Order & item identifiers (grain)
    TRY_TO_NUMBER(ORDER_ID) AS ORDER_ID,
    TRY_TO_NUMBER(ORDER_ITEM_ID) AS ORDER_ITEM_ID,

    -- Customer info 
    TRY_TO_NUMBER(CUSTOMER_ID) AS CUSTOMER_ID,
    CUSTOMER_SEGMENT AS CUSTOMER_SEGMENT,
    CUSTOMER_CITY AS CUSTOMER_CITY,
    CUSTOMER_STATE AS CUSTOMER_STATE,
    CUSTOMER_COUNTRY AS CUSTOMER_COUNTRY,

    -- Product info
    TRY_TO_NUMBER(PRODUCT_CARD_ID) AS PRODUCT_ID,
    PRODUCT_NAME AS PRODUCT_NAME,
    CATEGORY_NAME AS CATEGORY_NAME,
    DEPARTMENT_NAME AS DEPARTMENT_NAME,
    TRY_TO_DECIMAL(PRODUCT_PRICE, 12, 2) AS PRODUCT_PRICE,

    -- Order location/status
    MARKET AS MARKET,
    ORDER_REGION AS ORDER_REGION,
    ORDER_COUNTRY AS ORDER_COUNTRY,
    ORDER_STATE AS ORDER_STATE,
    ORDER_CITY AS ORDER_CITY,
    ORDER_STATUS AS ORDER_STATUS,
    
    -- Dates
    TRY_TO_TIMESTAMP_NTZ(ORDER_DATE_DATEORDERS, 'MM/DD/YYYY HH24:MI') AS ORDER_TS,
    TRY_TO_TIMESTAMP_NTZ(SHIPPING_DATE_DATEORDERS, 'MM/DD/YYYY HH24:MI') AS SHIPPING_TS,

    -- Shipping performance
    SHIPPING_MODE AS SHIPPING_MODE,
    TRY_TO_NUMBER(DAYS_FOR_SHIPPING_REAL) AS DAYS_SHIPPING_ACTUAL,
    TRY_TO_NUMBER(DAYS_FOR_SHIPMENT_SCHEDULED) AS DAYS_SHIPPING_SCHEDULED,
    IFF(TRY_TO_NUMBER(LATE_DELIVERY_RISK) = 1, TRUE, FALSE)AS IS_LATE_DELIVERY_RISK,
    DELIVERY_STATUS AS DELIVERY_STATUS,

    -- Financials (renamed for clarity)
    TRY_TO_NUMBER(ORDER_ITEM_QUANTITY) AS ORDER_ITEM_QUANTITY,
    TRY_TO_DECIMAL(SALES, 12, 2) AS SALES_AMOUNT,
    TRY_TO_DECIMAL(ORDER_ITEM_TOTAL, 12, 2) AS ORDER_ITEM_TOTAL,
    TRY_TO_DECIMAL(ORDER_ITEM_DISCOUNT, 12, 2) AS ORDER_ITEM_DISCOUNT,
    TRY_TO_DECIMAL(ORDER_ITEM_DISCOUNT_RATE, 6, 4) AS ORDER_ITEM_DISCOUNT_RATE,
    TRY_TO_DECIMAL(BENEFIT_PER_ORDER, 12, 2) AS PROFIT_PER_ORDER,
    TRY_TO_DECIMAL(ORDER_ITEM_PROFIT_RATIO, 6, 4) AS PROFIT_RATIO,

    -- Lineage (carried through from Bronze)
    _LOAD_TS,
    _SOURCE_FILE
    FROM BRONZE.BRONZE_ORDERS
    WHERE ORDER_ID IS NOT NULL          -- remove nulls since we can't identify orders that's null
    QUALIFY ROW_NUMBER() OVER(
    PARTITION BY ORDER_ITEM_ID      -- partition by grain
    ORDER BY _LOAD_TS DESC
    ) = 1;                          -- dedup'
```
```sql
  --this lets us later put a Stream on Silver too, so Gold can process only new/changed rows instead of full-refreshing.
    ALTER TABLE SILVER_ORDERS SET CHANGE_TRACKING = TRUE;
```
### Transformation Quality Check
```sql
    -- Silver table changes validation
    SELECT COUNT(*) FROM SILVER_ORDERS;

    -- Should return 0 rows if our grain/dedup logic is correct
    SELECT ORDER_ITEM_ID, COUNT(*) 
    FROM SILVER_ORDERS
    GROUP BY ORDER_ITEM_ID
    HAVING COUNT(*) > 1;

  -- Value spot check a few rows
    SELECT * FROM SILVER_ORDERS LIMIT 10;
```

## Gold Layer
```sql
  -- Creates DIM_DATE dimension table with date attributes for CPG supply chain analytics
  USE DATABASE CPG_SUPPLY_CHAIN;
  USE SCHEMA GOLD;

  CREATE OR REPLACE TABLE DIM_DATE AS
  SELECT 
      DATE_KEY,
      YEAR(DATE_KEY) AS YEAR,
      QUARTER(DATE_KEY) AS QUARTER,
      MONTH(DATE_KEY) AS MONTH,
      DAY(DATE_KEY) AS DAY_OF_MONTH,
      DAYOFWEEK(DATE_KEY) AS DAY_OF_WEEK,
      DAYNAME(DATE_KEY) AS DAY_NAME,
      IFF(DAYOFWEEK(DATE_KEY) IN (0, 6), TRUE, FALSE) AS IS_WEEKEND
  FROM(
      SELECT DATEADD(DAY, SEQ4(), '2015-01-01')::DATE AS DATE_KEY
      FROM TABLE(GENERATOR(ROWCOUNT => 3653))
      ) AS GENERATED_DATES -- ~10 years of datas from 2015-01-01
```
### DIM_DATE dimension validation
```sql
  
  SELECT COUNT(*) FROM DIM_DATE; 
  -- expect 3653

  SELECT MIN(DATE_KEY), MAX(DATE_KEY) FROM DIM_DATE;
  -- expect 2015-01-01 to roughly 2024-12-31

  SELECT * FROM DIM_DATE LIMIT 10; 
  -- column spotting
```
### Creates DIM_PRODUCT dimension table
```sql
CREATE OR REPLACE TABLE DIM_PRODUCT AS 
SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY_NAME,
    DEPARTMENT_NAME,
    PRODUCT_PRICE
FROM (
    SELECT 
        PRODUCT_ID,
        PRODUCT_NAME,
        CATEGORY_NAME,
        DEPARTMENT_NAME,
        PRODUCT_PRICE,
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCT_ID
            ORDER BY _LOAD_TS DESC
        ) AS RN
    FROM SILVER.SILVER_ORDERS
) AS DEDUPED
WHERE RN = 1;
```
### Validation for DIM_PRODUCT
```sql
SELECT COUNT(*) FROM DIM_PRODUCT;
SELECT PRODUCT_ID, COUNT(*) FROM DIM_PRODUCT GROUP BY PRODUCT_ID HAVING COUNT(*) > 1;
SELECT * FROM DIM_PRODUCT LIMIT 10;
```
### Creates DIM_CUSTOMER dimension table
```sql
CREATE OR REPLACE TABLE DIM_CUSTOMER AS
SELECT
    CUSTOMER_ID,
    CUSTOMER_SEGMENT,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    CUSTOMER_COUNTRY
FROM (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_SEGMENT,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
        CUSTOMER_COUNTRY,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY _LOAD_TS DESC
        ) AS RN
    FROM SILVER.SILVER_ORDERS
) AS DEDUPED
WHERE RN = 1;

-- DIM_CUSTOMER validation
SELECT COUNT(*) FROM DIM_CUSTOMER;
SELECT * FROM DIM_CUSTOMER ORDER BY CUSTOMER_ID DESC LIMIT 20;
SELECT CUSTOMER_ID, COUNT(*) FROM DIM_CUSTOMER GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1;
```
### DIM_SHIPPING_MODE dimension table
```sql
-- Creation of DIM_SHIPPING_MODE dimension table
CREATE OR REPLACE TABLE DIM_SHIPPING_MODE AS
SELECT DISTINCT
    SHIPPING_MODE
FROM SILVER.SILVER_ORDERS
WHERE SHIPPING_MODE IS NOT NULL;
-- Validation for DIM_SHIPPING_MODE
SELECT COUNT(*) FROM DIM_SHIPPING_MODE
SELECT * FROM DIM_SHIPPING_MODE
```
## Creation of FACT_ORDERS dimension table to complete the star schema.
```sql
CREATE OR REPLACE TABLE FACT_ORDERS AS
SELECT 
-- one row per order line item
    S.ORDER_ID,
    S.ORDER_ITEM_ID,

    -- Foreign keys for each dimension
    S.CUSTOMER_ID,
    S.PRODUCT_ID,
    S.SHIPPING_MODE,
    DATE(S.ORDER_TS) AS ORDER_DATE_KEY,
    DATE(S.SHIPPING_TS) AS SHIPPING_DATE_KEY,

    -- Order attributes (low cardinality)
    S.MARKET,
    S.ORDER_REGION,
    S.ORDER_STATUS,
    S.DELIVERY_STATUS,

    --Measures
    S.ORDER_ITEM_QUANTITY,
    S.SALES_AMOUNT,
    S.ORDER_ITEM_TOTAL,
    S.ORDER_ITEM_DISCOUNT,
    S.ORDER_ITEM_DISCOUNT_RATE,
    S.PROFIT_PER_ORDER,
    S.PROFIT_RATIO,
    S.DAYS_SHIPPING_ACTUAL,
    S.DAYS_SHIPPING_SCHEDULED,
    S.IS_LATE_DELIVERY_RISK
FROM SILVER.SILVER_ORDERS S;

-- Fact Table validation
-- should match with the quantity of silver orders (180519)
SELECT
    (SELECT COUNT(*) FROM SILVER.SILVER_ORDERS) AS SILVER_COUNT,
    (SELECT COUNT(*) FROM FACT_ORDERS) AS FACT_COUNT;

--checking if joining the dimensions are good
SELECT F.ORDER_ITEM_ID, P.PRODUCT_NAME, C.CUSTOMER_SEGMENT, D.YEAR, D.MONTH
FROM FACT_ORDERS F
JOIN DIM_PRODUCT P ON F.PRODUCT_ID = P.PRODUCT_ID
JOIN DIM_CUSTOMER C ON F.CUSTOMER_ID = C.CUSTOMER_ID
JOIN DIM_DATE D ON F.ORDER_DATE_KEY = D.DATE_KEY
LIMIT 10;

-- Null checking expected is 0 for all columns
SELECT
    SUM(IFF(SALES_AMOUNT IS NULL, 1, 0))       AS NULL_SALES,
    SUM(IFF(PROFIT_PER_ORDER IS NULL, 1, 0))   AS NULL_PROFIT,
    SUM(IFF(ORDER_ITEM_QUANTITY IS NULL, 1, 0)) AS NULL_QUANTITY
FROM FACT_ORDERS;

-- Test sample Aggregation
SELECT
    D.YEAR,
    COUNT(*) AS NUM_ORDER_ITEMS,
    SUM(F.SALES_AMOUNT) AS TOTAL_SALES,
    SUM(F.PROFIT_PER_ORDER) AS TOTAL_PROFIT
FROM FACT_ORDERS F
JOIN DIM_DATE D ON F.ORDER_DATE_KEY = D.DATE_KEY
GROUP BY D.YEAR
ORDER BY D.YEAR;
```
## Creation of Data marts
### Profit per Region
```sql
CREATE OR REPLACE TABLE MART_PROFIT_BY_CATEGORY_REGION AS
SELECT
    P.CATEGORY_NAME,
    F.ORDER_REGION,
    COUNT(*)                        AS NUM_ORDER_ITEMS,
    SUM(F.SALES_AMOUNT)             AS TOTAL_SALES,
    SUM(F.PROFIT_PER_ORDER)         AS TOTAL_PROFIT,
    ROUND(SUM(F.PROFIT_PER_ORDER) / NULLIF(SUM(F.SALES_AMOUNT), 0) * 100, 2) AS PROFIT_MARGIN_PCT
FROM FACT_ORDERS F
JOIN DIM_PRODUCT P ON F.PRODUCT_ID = P.PRODUCT_ID
GROUP BY P.CATEGORY_NAME, F.ORDER_REGION
ORDER BY TOTAL_SALES DESC;

-- Mart testing/spotting
SELECT * FROM MART_PROFIT_BY_CATEGORY_REGION ORDER BY PROFIT_MARGIN_PCT DESC LIMIT 10;
SELECT * FROM MART_PROFIT_BY_CATEGORY_REGION ORDER BY PROFIT_MARGIN_PCT ASC LIMIT 10;
```

### Cost impact per shipping mode and region
```sql
CREATE OR REPLACE TABLE MART_LATE_DELIVERY_IMPACT AS
SELECT
    F.SHIPPING_MODE,
    F.ORDER_REGION,
    COUNT(*) AS TOTAL_ORDER_ITEMS,
    SUM(IFF(F.IS_LATE_DELIVERY_RISK, 1, 0)) AS LATE_ORDERS_ITEMS,
    ROUND(SUM(IFF(F.IS_LATE_DELIVERY_RISK, 1, 0)) / NULLIF(COUNT(*), 0) * 100, 2) AS LATE_DELIVERY_RATE_PCT,
    SUM(IFF(F.IS_LATE_DELIVERY_RISK, F.SALES_AMOUNT, 0)) AS SALES_AT_RISK,
    ROUND(AVG(F.DAYS_SHIPPING_ACTUAL - F.DAYS_SHIPPING_SCHEDULED), 2) AS AVG_DAYS_LATE
FROM FACT_ORDERS F
GROUP BY F.SHIPPING_MODE, F.ORDER_REGION
ORDER BY LATE_DELIVERY_RATE_PCT DESC;

-- Mart test
SELECT * FROM MART_LATE_DELIVERY_IMPACT ORDER BY SHIPPING_MODE DESC;
```

### Discount effectiveness per category
```sql
CREATE OR REPLACE TABLE MART_DISCOUNT_EFFECTIVENESS AS
SELECT
    P.CATEGORY_NAME,
    COUNT(*) AS NUM_ORDER_ITEMS,
    SUM(F.ORDER_ITEM_QUANTITY) AS TOTAL_UNITS_SOLD,
    SUM(F.SALES_AMOUNT) AS TOTAL_SALES,
    SUM(F.ORDER_ITEM_DISCOUNT) AS TOTAL_DISCOUNT_GIVEN,
    ROUND(AVG(F.ORDER_ITEM_DISCOUNT_RATE) * 100, 2) AS AVG_DISCOUNT_RATE_PCT,
    ROUND(SUM(F.PROFIT_PER_ORDER)/NULLIF(SUM(F.SALES_AMOUNT), 0) * 100, 2) AS PROFIT_MARGIN_PCT
FROM FACT_ORDERS F
JOIN DIM_PRODUCT P ON F.PRODUCT_ID = P.PRODUCT_ID
GROUP BY P.CATEGORY_NAME
ORDER BY TOTAL_DISCOUNT_GIVEN DESC;

-- TEST
SELECT * FROM MART_DISCOUNT_EFFECTIVENESS ORDER BY TOTAL_DISCOUNT_GIVEN DESC;
```

## Creation of Stream and Orchaestration
```sql
USE DATABASE CPG_SUPPLY_CHAIN;
USE SCHEMA UTILS;

-- Stream Creation 
CREATE STREAM IF NOT EXISTS BRONZE_ORDERS_STREAM
ON TABLE BRONZE.BRONZE_ORDERS;

CREATE STREAM IF NOT EXISTS SILVER_ORDERS_STREAM
ON TABLE SILVER.SILVER_ORDERS;

SHOW STREAMS;

-- test if there's any change it should be false
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE_ORDERS_STREAM');
SELECT SYSTEM$STREAM_HAS_DATA('SILVER_ORDERS_STREAM');

-- Orchaestration
-- be sure the pipeline_log was created in setup
SELECT * FROM CPG_SUPPLY_CHAIN.UTILS.PIPELINE_RUN_LOG LIMIT 5;
```
### Task Creation
#### Gold Task
```sql
-- creation of task
-- note: it should be created from parent to child
-- Bronze
CREATE OR REPLACE TASK TASK_LOAD_BRONZE
  WAREHOUSE = CPG_WH
  SCHEDULE = 'USING CRON 0 4 * * * Asia/Manila'  -- runs daily at 4am PHT
AS
BEGIN
    COPY INTO BRONZE.BRONZE_ORDERS (
        TYPE, DAYS_FOR_SHIPPING_REAL, DAYS_FOR_SHIPMENT_SCHEDULED, BENEFIT_PER_ORDER,
        SALES_PER_CUSTOMER, DELIVERY_STATUS, LATE_DELIVERY_RISK, CATEGORY_ID, CATEGORY_NAME,
        CUSTOMER_CITY, CUSTOMER_COUNTRY, CUSTOMER_EMAIL, CUSTOMER_FNAME, CUSTOMER_ID,
        CUSTOMER_LNAME, CUSTOMER_PASSWORD, CUSTOMER_SEGMENT, CUSTOMER_STATE, CUSTOMER_STREET,
        CUSTOMER_ZIPCODE, DEPARTMENT_ID, DEPARTMENT_NAME, LATITUDE, LONGITUDE, MARKET,
        ORDER_CITY, ORDER_COUNTRY, ORDER_CUSTOMER_ID, ORDER_DATE_DATEORDERS, ORDER_ID,
        ORDER_ITEM_CARDPROD_ID, ORDER_ITEM_DISCOUNT, ORDER_ITEM_DISCOUNT_RATE, ORDER_ITEM_ID,
        ORDER_ITEM_PRODUCT_PRICE, ORDER_ITEM_PROFIT_RATIO, ORDER_ITEM_QUANTITY, SALES,
        ORDER_ITEM_TOTAL, ORDER_PROFIT_PER_ORDER, ORDER_REGION, ORDER_STATE, ORDER_STATUS,
        ORDER_ZIPCODE, PRODUCT_CARD_ID, PRODUCT_CATEGORY_ID, PRODUCT_DESCRIPTION, PRODUCT_IMAGE,
        PRODUCT_NAME, PRODUCT_PRICE, PRODUCT_STATUS, SHIPPING_DATE_DATEORDERS, SHIPPING_MODE,
        _SOURCE_FILE
    )
    FROM (
        SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
               $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,
               $39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,
               METADATA$FILENAME
        FROM @UTILS.SUPPLY_CHAIN_STAGE
    )
    FILE_FORMAT = (FORMAT_NAME = UTILS.CSV_FF)
    ON_ERROR = 'CONTINUE'
    PATTERN = '.*\.csv';

    INSERT INTO UTILS.PIPELINE_RUN_LOG (TASK_NAME, LAYER, STATUS, ROW_COUNT, STARTED_AT)
    SELECT 'TASK_LOAD_BRONZE', 'BRONZE', 'SUCCESS', COUNT(*), CURRENT_TIMESTAMP()
    FROM BRONZE.BRONZE_ORDERS;
END;

-- Silver
CREATE OR REPLACE TASK TASK_TRANSFORM_SILVER
  WAREHOUSE = CPG_WH
  AFTER TASK_LOAD_BRONZE
  WHEN SYSTEM$STREAM_HAS_DATA('BRONZE_ORDERS_STREAM')
AS
BEGIN
    MERGE INTO SILVER.SILVER_ORDERS AS TGT
    USING (
        SELECT
            TRY_TO_NUMBER(ORDER_ID)                                AS ORDER_ID,
            TRY_TO_NUMBER(ORDER_ITEM_ID)                           AS ORDER_ITEM_ID,
            TRY_TO_NUMBER(CUSTOMER_ID)                             AS CUSTOMER_ID,
            CUSTOMER_SEGMENT, CUSTOMER_CITY, CUSTOMER_STATE, CUSTOMER_COUNTRY,
            TRY_TO_NUMBER(PRODUCT_CARD_ID)                         AS PRODUCT_ID,
            PRODUCT_NAME, CATEGORY_NAME, DEPARTMENT_NAME,
            TRY_TO_DECIMAL(PRODUCT_PRICE, 12, 2)                   AS PRODUCT_PRICE,
            MARKET, ORDER_REGION, ORDER_COUNTRY, ORDER_STATE, ORDER_CITY, ORDER_STATUS,
            TRY_TO_TIMESTAMP_NTZ(ORDER_DATE_DATEORDERS, 'MM/DD/YYYY HH24:MI')    AS ORDER_TS,
            TRY_TO_TIMESTAMP_NTZ(SHIPPING_DATE_DATEORDERS, 'MM/DD/YYYY HH24:MI') AS SHIPPING_TS,
            SHIPPING_MODE,
            TRY_TO_NUMBER(DAYS_FOR_SHIPPING_REAL)                  AS DAYS_SHIPPING_ACTUAL,
            TRY_TO_NUMBER(DAYS_FOR_SHIPMENT_SCHEDULED)             AS DAYS_SHIPPING_SCHEDULED,
            IFF(TRY_TO_NUMBER(LATE_DELIVERY_RISK) = 1, TRUE, FALSE) AS IS_LATE_DELIVERY_RISK,
            DELIVERY_STATUS,
            TRY_TO_NUMBER(ORDER_ITEM_QUANTITY)                     AS ORDER_ITEM_QUANTITY,
            TRY_TO_DECIMAL(SALES, 12, 2)                           AS SALES_AMOUNT,
            TRY_TO_DECIMAL(ORDER_ITEM_TOTAL, 12, 2)                AS ORDER_ITEM_TOTAL,
            TRY_TO_DECIMAL(ORDER_ITEM_DISCOUNT, 12, 2)             AS ORDER_ITEM_DISCOUNT,
            TRY_TO_DECIMAL(ORDER_ITEM_DISCOUNT_RATE, 6, 4)         AS ORDER_ITEM_DISCOUNT_RATE,
            TRY_TO_DECIMAL(BENEFIT_PER_ORDER, 12, 2)               AS PROFIT_PER_ORDER,
            TRY_TO_DECIMAL(ORDER_ITEM_PROFIT_RATIO, 6, 4)          AS PROFIT_RATIO,
            _LOAD_TS, _SOURCE_FILE
        FROM BRONZE_ORDERS_STREAM
        WHERE ORDER_ID IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ITEM_ID ORDER BY _LOAD_TS DESC) = 1
    ) AS SRC
    ON TGT.ORDER_ITEM_ID = SRC.ORDER_ITEM_ID
    WHEN MATCHED THEN UPDATE SET
        TGT.SALES_AMOUNT = SRC.SALES_AMOUNT, TGT.PROFIT_PER_ORDER = SRC.PROFIT_PER_ORDER,
        TGT._LOAD_TS = SRC._LOAD_TS
    WHEN NOT MATCHED THEN INSERT VALUES (
        SRC.ORDER_ID, SRC.ORDER_ITEM_ID, SRC.CUSTOMER_ID, SRC.CUSTOMER_SEGMENT,
        SRC.CUSTOMER_CITY, SRC.CUSTOMER_STATE, SRC.CUSTOMER_COUNTRY, SRC.PRODUCT_ID,
        SRC.PRODUCT_NAME, SRC.CATEGORY_NAME, SRC.DEPARTMENT_NAME, SRC.PRODUCT_PRICE,
        SRC.MARKET, SRC.ORDER_REGION, SRC.ORDER_COUNTRY, SRC.ORDER_STATE, SRC.ORDER_CITY,
        SRC.ORDER_STATUS, SRC.ORDER_TS, SRC.SHIPPING_TS, SRC.SHIPPING_MODE,
        SRC.DAYS_SHIPPING_ACTUAL, SRC.DAYS_SHIPPING_SCHEDULED, SRC.IS_LATE_DELIVERY_RISK,
        SRC.DELIVERY_STATUS, SRC.ORDER_ITEM_QUANTITY, SRC.SALES_AMOUNT, SRC.ORDER_ITEM_TOTAL,
        SRC.ORDER_ITEM_DISCOUNT, SRC.ORDER_ITEM_DISCOUNT_RATE, SRC.PROFIT_PER_ORDER,
        SRC.PROFIT_RATIO, SRC._LOAD_TS, SRC._SOURCE_FILE
    );

    INSERT INTO UTILS.PIPELINE_RUN_LOG (TASK_NAME, LAYER, STATUS, ROW_COUNT, STARTED_AT)
    SELECT 'TASK_TRANSFORM_SILVER', 'SILVER', 'SUCCESS', COUNT(*), CURRENT_TIMESTAMP()
    FROM SILVER.SILVER_ORDERS;
END;

-- Gold Task
CREATE OR REPLACE TASK TASK_REFRESH_GOLD
    WAREHOUSE = CPG_WH
    AFTER TASK_TRANSFORM_SILVER
    -- to check if there's new data
    WHEN SYSTEM$STREAM_HAS_DATA('SILVER_ORDERS_STREAM')
AS 
-- Procedure
BEGIN
    CREATE OR REPLACE TABLE GOLD.FACT_ORDERS AS
    SELECT
        ORDER_ID, ORDER_ITEM_ID, CUSTOMER_ID, PRODUCT_ID, SHIPPING_MODE,
        DATE(ORDER_TS) AS ORDER_DATE_KEY, DATE(SHIPPING_TS) AS SHIPPING_DATE_KEY,
        MARKET, ORDER_REGION, ORDER_STATUS, DELIVERY_STATUS,
        ORDER_ITEM_QUANTITY, SALES_AMOUNT, ORDER_ITEM_TOTAL, ORDER_ITEM_DISCOUNT,
        ORDER_ITEM_DISCOUNT_RATE, PROFIT_PER_ORDER, PROFIT_RATIO,
        DAYS_SHIPPING_ACTUAL, DAYS_SHIPPING_SCHEDULED, IS_LATE_DELIVERY_RISK
    FROM SILVER.SILVER_ORDERS;
-- historical log
    INSERT INTO UTILS.PIPELINE_RUN_LOG (TASK_NAME, LAYER, STATUS, ROW_COUNT, STARTED_AT)
    SELECT 'TASK_REFRESH_GOLD','GOLD','SUCCESS', COUNT(*), CURRENT_TIMESTAMP()
    FROM GOLD.FACT_ORDERS;
END;
```

## Task Validation and Manual run
```sql
-- to show tasks all should be suspended
SHOW TASKS;

ALTER TASK TASK_REFRESH_GOLD RESUME;
ALTER TASK TASK_TRANSFORM_SILVER RESUME;
ALTER TASK TASK_LOAD_BRONZE RESUME;

-- Manual trigger the pipeline
EXECUTE TASK TASK_LOAD_BRONZE;
```

### Post Pipeline run validation
```sql
-- Checking the if newly uploaded file is good
SELECT FILE_NAME, LAST_LOAD_TIME, ROW_COUNT
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CPG_SUPPLY_CHAIN.BRONZE.BRONZE_ORDERS',
    START_TIME => DATEADD(DAY, -30, CURRENT_TIMESTAMP())
));
```

*Built as an end-to-end portfolio project to demonstrate production-style data engineering practices: layered data quality, dimensional modeling, automated orchestration, and BI delivery — grounded in a real CPG/finance business use case.*
