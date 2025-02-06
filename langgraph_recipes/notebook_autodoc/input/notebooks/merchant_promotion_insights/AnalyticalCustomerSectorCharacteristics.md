# Functional Specification Document

## 1. Input Parameters
| Parameter Name | Description |
|:---------------|:------------|
| `catalog` | Catalog containing the target tables. |
| `parym_range` | Date range for the parameters used in the query. |
| `pardt1` | Start date in integer format. |
| `pardt2` | End date in integer format. |
| `parym_end` | End date formatted as 'yyyyMM' from `pardt2`. |

## 2. Source Tables
### 2.1. customer_sector_volumes
| Column Name      | Column Data Type    |
|:-----------------|:--------------------|
| customer_id      | STRING              |
| sector           | STRING              |
| hyper_category   | STRING              |
| monthly_amount   | DECIMAL(15,2)      |
| frequency        | INT                 |
| gr_ind           | BOOLEAN             |
| total_wallet     | DECIMAL(15,2)      |
| ecommerce_ind    | BOOLEAN             |
| par_ym          | INT                 |

## 3. Intermediate Tables
### 3.1. all_comb
| Target Table | Source Table | Row Selection Logic                                                            |
|:-------------|:-------------|:------------------------------------------------------------------------------|
| all_comb     | None         | Generates date sequences over the given date range.                          |

#### Intermediate Columns Details
| Target Column          | Data Type | Transformation Logic                               | Source Table | Source Column | Group By |
|:-----------------------|:----------|:--------------------------------------------------|:-------------|:--------------|:---------|
| par_ym                 | INT       | cast(date_format(ts, 'yyyyMM') as int)          | N/A          | N/A           | N/A      |
| par_ym_semester_before | INT       | cast(date_format(ts - interval 6 months, 'yyyyMM') as int) | N/A          | N/A           | N/A      |
| par_ym_year_before     | INT       | cast(date_format(ts - interval 12 months, 'yyyyMM') as int) | N/A          | N/A           | N/A      |

### 3.2. perc_rank
| Target Table | Source Table | Row Selection Logic |
|:-------------|:-------------|:--------------------|
| perc_rank    | customer_sector_volumes | v.par_ym between ac.par_ym_year_before and ac.par_ym and v.hyper_category is not null  |

#### Intermediate Columns Details
| Target Column      | Data Type | Transformation Logic                                      | Source Table                           | Source Column      | Group By          |
|:-------------------|:----------|:---------------------------------------------------------|:--------------------------------------|:-------------------|:------------------|
| par_ym             | INT       | Direct Copy                                             | perc_rank                             | par_ym             | par_ym, customer_id, hyper_category |
| customer_id        | STRING    | Direct Copy                                             | perc_rank                             | customer_id        | par_ym, customer_id, hyper_category |
| hyper_category      | STRING    | Direct Copy                                             | perc_rank                             | hyper_category      | par_ym, customer_id, hyper_category |
| spending_profile    | STRING    | case when percent_rank() over (...) >= 0.8 then "high_spenders" end | v                                   | spending_profile    | par_ym, customer_id, hyper_category |

### 3.3. channels
| Target Table | Source Table | Row Selection Logic |
|:-------------|:-------------|:--------------------|
| channels     | customer_sector_volumes | v.par_ym between ac.par_ym_semester_before and ac.par_ym and v.hyper_category is not null |

#### Intermediate Columns Details
| Target Column        | Data Type | Transformation Logic                                      | Source Table                         | Source Column      | Group By          |
|:---------------------|:----------|:---------------------------------------------------------|:------------------------------------|:-------------------|:------------------|
| par_ym               | INT       | Direct Copy                                             | channels                             | par_ym             | par_ym, hyper_category, customer_id |
| customer_id          | STRING    | Direct Copy                                             | channels                             | customer_id        | par_ym, hyper_category, customer_id |
| hyper_category        | STRING    | Direct Copy                                             | channels                             | hyper_category      | par_ym, hyper_category, customer_id |
| channel_preference    | STRING    | case when ecommerce_ind = true and round(sum(... > 66.66) then 'digital_consumers' when ecommerce_ind = false ... then 'in_store_consumers' end | v                                   | channel_preference   | par_ym, hyper_category, customer_id |

## 4. Target Tables
### 4.1. customer_sector_characteristics
| Target Table | Source Table | Row Selection Logic                                                          |
|:-------------|:-------------|:----------------------------------------------------------------------------|
| customer_sector_characteristics | perc_rank and channels | ca.customer_id=c.customer_id and cA.hyper_category = c.hyper_category and ca.par_ym = c.par_ym |

#### Target Columns Details
| Target Column          | Data Type    | Transformation Logic                       | Source Table                                     | Source Column      |
|:-----------------------|:-------------|:--------------------------------------------|:------------------------------------------------|:-------------------|
| par_ym                 | INT          | ifnull(ca.par_ym, c.par_ym)              | perc_rank and channels                          | par_ym             |
| customer_id            | STRING       | ifnull(ca.customer_id, c.customer_id)     | perc_rank and channels                          | customer_id        |
| hyper_category         | STRING       | ifnull(ca.hyper_category, c.hyper_category)| perc_rank and channels                          | hyper_category      |
| spending_profile       | STRING       | ca.spending_profile                         | perc_rank                                      | spending_profile    |
| channel_preference     | STRING       | c.channel_preference                       | channels                                       | channel_preference  | 

This functional specification provides a detailed overview of the inputs, sources, intermediates, and targets for the data pipeline in the Databricks notebook for analytical computations on customer sector characteristics.