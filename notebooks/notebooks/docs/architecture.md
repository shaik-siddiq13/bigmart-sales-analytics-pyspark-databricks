# BigMart Sales Analytics – Data Architecture

## 1. Architecture Overview

This project follows the Medallion Architecture to organize data processing into three layers: Bronze, Silver, and Gold.

## 2. Data Flow

### Source
- Input: BigMart Sales CSV
- Records: 8,523
- Columns: 12

### Bronze Layer – Raw Data
- Reads the source CSV using PySpark.
- Stores the original ingested dataset as a managed Delta table.
- Table: `bronze_bigmart_sales`

### Silver Layer – Cleaned Data
- Standardizes inconsistent categorical values.
- Handles missing Item Weight values using median imputation.
- Replaces missing Outlet Size with `Unknown`.
- Performs NULL and duplicate checks.
- Saves the cleaned dataset as a managed Delta table.
- Table: `silver_bigmart_sales`

### Gold Layer – Business Aggregations
Creates five analytical Delta tables:

1. Sales by Item Type
2. Sales by Outlet
3. Average Sales by Outlet Type
4. Sales by Location
5. Top 10 Products by Sales

## 3. Databricks Organization

- Catalog: `bigmart_catalog`
- Schema: `bigmark_schema`
- Storage: Delta Lake
- Governance: Unity Catalog

## 4. Pipeline Summary

```text
BigMart Sales CSV
       |
       v
Bronze Delta Table
       |
       v
Silver Delta Table
       |
       v
Gold Analytical Tables
       |
       v
Business Insights
```

## 5. Validation Results

| Validation | Result |
|---|---:|
| Bronze Records | 8,523 |
| Silver Records | 8,523 |
| Silver NULL Values | 0 |
| Duplicate Rows | 0 |
| Gold Tables | 5 |

## 6. Future Improvements

- Databricks Workflows for scheduling
- Incremental data processing
- Advanced data quality monitoring
- SQL-based reporting and dashboards
