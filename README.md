# Wolt Snack Store Analytics (dbt)

---

## Overview

This project models and analyzes transactional data from the **Wolt Snack Store** using **dbt** and **BigQuery**.  
Its purpose is to transform raw operational data into analytics‑ready tables and reporting layers that answer concrete business questions around category performance, promotions, pricing, and customer behavior.

The project follows a realistic analytics workflow:

- Raw → **staging** → **analytics** (facts & dimensions)  
- **Reporting layers** designed for business consumption  
- Clear **validation**, **testing**, and **documentation** throughout  

---

## Business Questions Addressed

This project focuses on answering questions such as:

- Which product categories and items drive the most revenue and volume  
- How dependent categories and products are on promotions  
- Whether promotions amplify demand or mainly support weaker products  
- How category performance evolves over time (trend, momentum, seasonality)  
- Which products are “stars” vs niche or fragile  
- What products are frequently purchased together  

---

## Data Model Overview

### Core Layers

#### Staging (`stg_`)
- Cleans and standardizes raw source data  
- Minimal transformations, no business logic  

#### Analytics

**Dimensions**
- `dim_customer`  
- `dim_item_history`  
- `dim_item`  
- `dim_promo`  

**Facts**
- `fct_purchases`  
- `fct_purchases_items`  

#### Reporting
- `rep_category_performance`  
- `rep_item_performance`  

---

## Key Reporting Models

### `rep_category_performance`

Daily category‑level performance metrics including:

- Revenue, units sold, orders  
- Promotion indicators  
- Derived KPIs  

Used for:

- Category ranking  
- Promotion dependency analysis  
- Trend and seasonality analysis  
- Identifying “star” vs fragile categories  

### `rep_item_performance`

Item‑level reporting with:

- Revenue and volume  
- Promotion dependency  
- Price positioning  
- Inputs for co‑purchase analysis  

---

## Analytical Approach

- Reporting models are **purpose‑built**  
- No new business logic in reporting  
- All metrics derived from facts and dimensions  
- Time‑aware analysis downstream  
- Promotion logic validated against basket totals  

---

## Data Quality & Validation

Validation includes:

- Duplicate detection  
- Referential integrity checks  
- Price validity checks  
- Basket value reconciliation  

Supporting queries in:

- `analyses/`

---

## dbt Features Used

- Incremental models  
- Schema tests  
- Model tags  
- `dbt docs generate`  

---

## How to Run the Project

```bash
dbt deps
dbt run
dbt test
dbt docs generate
dbt docs serve
```

---

## Project Structure

```
models/
  staging/
  analytics/
  reporting/

analyses/
  data_validation.md
  business_analysis.md

macros/
tests/
snapshots/
```

---

## Notes & Assumptions

- All customers are returning customers  
- Revenue excludes courier/service fees  
- Promotions are percentage‑based  
- Seasonality analysis is directional  

---

## Author

**Nitin**  
Analytics Engineer
