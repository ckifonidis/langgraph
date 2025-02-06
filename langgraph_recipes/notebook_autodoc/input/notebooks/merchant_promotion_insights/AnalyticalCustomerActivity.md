# Functional Specification Document

## 1. Input Parameters
The following input parameters are defined in the notebook:
| Parameter Name | Description |
|:---------------|:-----------|
| `catalog`      | Catalog name obtained from the `get_catalog()` function. |
| `pardt1`      | Start date parameter obtained from the `get_pardt_integer_range()` function. |
| `pardt2`      | End date parameter obtained from the `get_pardt_integer_range()` function. |

## 2. Source Tables
The following source tables are utilized in the query:

### 2.1. `merchant_all_statistics`
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| `merchant_id`         | STRING           |
| `customer_id`         | STRING           |
| `number_of_transactions` | INT           |
| `par_dt`              | INT              |

### 2.2. `merchant`
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| `merchantid`          | STRING           |
| `is_merchant_user`    | BOOLEAN          |
| `merchant_user_id`    | STRING           |
| `merchant_mcc`        | STRING           |

### 2.3. `merchant_all_statistics_interests`
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| `customer_id`         | STRING           |
| `shopping_interests`  | STRING           |
| `par_dt`              | INT              |

### 2.4. `mcc_categories`
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| `mcc`                 | STRING           |
| `sector`              | STRING           |

### 2.5. `merchant_insights_customer_interests`
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| `merchant_user_id`    | STRING           |
| `customer_id`         | STRING           |
| `par_dt`              | INT              |

## 3. Intermediate Tables

### 3.1. `customer_freq`
| Target Table    | Source Table                                           | Row Selection Logic                                                                                                                                 |
|:----------------|:------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `customer_freq` | `merchant_all_statistics`, `merchant`                   | `users.is_merchant_user = true AND par_dt >= cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 6 months, 'yyyyMMdd') as int)` |

| Target Column       | Data Type | Transformation Logic                                                                         | Source Table                | Source Column            | Group By             |
|:--------------------|:----------|:--------------------------------------------------------------------------------------------|:----------------------------|:-------------------------|:---------------------|
| `merchant_user_id`  | STRING    | Direct Copy                                                                                 | `merchant`                  | `merchant_user_id`       | `customer_id`        |
| `customer_id`       | STRING    | Direct Copy                                                                                 | `merchant_all_statistics`   | `customer_id`            | `users.merchant_user_id` |
| `number_of_transactions` | INT     | SUM                                                                                         | `merchant_all_statistics`   | `number_of_transactions` |                     |

### 3.2. `merchant_avg`
| Target Table   | Source Table    | Row Selection Logic                                                        |
|:---------------|:----------------|:-------------------------------------------------------------------------|
| `merchant_avg` | `customer_freq` | Grouped by `merchant_user_id`.                                           |

| Target Column     | Data Type | Transformation Logic                                     | Source Table    | Source Column          | Group By             |
|:------------------|:----------|:--------------------------------------------------------|:----------------|:-----------------------|:---------------------|
| `merchant_user_id`| STRING    | Direct Copy                                             | `customer_freq`  | `merchant_user_id`     |                     |
| `threshold`       | DECIMAL   | SUM(number_of_transactions) / COUNT(DISTINCT customer_id) | `customer_freq`  | `number_of_transactions` | `merchant_user_id` |

### 3.3. `cust_interests`
| Target Table   | Source Table                                           | Row Selection Logic                                                                                                         |
|:---------------|:------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------|
| `cust_interests` | `merchant_all_statistics_interests`                    | `dt.par_dt >= cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 12 months, 'yyyyMMdd') as int)` |

| Target Column     | Data Type | Transformation Logic                                     | Source Table                     | Source Column     | Group By             |
|:------------------|:----------|:--------------------------------------------------------|:---------------------------------|:------------------|:---------------------|
| `customer_id`     | STRING    | Direct Copy                                             | `merchant_all_statistics_interests` | `customer_id`    |                     |
| `shopping_interests` | STRING  | Direct Copy                                             | `merchant_all_statistics_interests` | `shopping_interests` |                     |

### 3.4. `merchants`
| Target Table    | Source Table    | Row Selection Logic                                                                                                                                               |
|:----------------|:----------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `merchants`     | `merchant`, `mcc_categories` | `array_contains(merchant_platforms, 'Go4More') AND is_merchant_user = true`                                                                                 |

| Target Column        | Data Type | Transformation Logic                                  | Source Table           | Source Column           | Group By             |
|:---------------------|:----------|:-----------------------------------------------------|:-----------------------|:------------------------|:---------------------|
| `merchant_user_id`   | STRING    | Direct Copy                                          | `merchant`             | `merchant_user_id`      |                     |
| `sector`             | STRING    | Direct Copy                                          | `mcc_categories`       | `sector`                |                     |

### 3.5. `m_all`
| Target Table    | Source Table    | Row Selection Logic                                                                                                         |
|:----------------|:----------------|:---------------------------------------------------------------------------------------------------------------------------|
| `m_all`         | `merchants`, `cust_interests` | `m.sector = c.shopping_interests`                                                                                     |

| Target Column      | Data Type | Transformation Logic   | Source Table | Source Column       | Group By             |
|:-------------------|:----------|:-----------------------|:-------------|:--------------------|:---------------------|
| `merchant_user_id` | STRING    | Direct Copy            | `merchants`  | `merchant_user_id`   |                     |
| `sector`           | STRING    | Direct Copy            | `merchants`  | `sector`            |                     |
| `customer_id`      | STRING    | Direct Copy            | `cust_interests` | `customer_id`     |                     |
| `shopping_interests`| STRING   | Direct Copy            | `cust_interests` | `shopping_interests` |                     |

### 3.6. `merch_cust`
| Target Table    | Source Table                                           | Row Selection Logic                                                                                                        |
|:----------------|:------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------|
| `merch_cust`    | `merchant_insights_customer_interests`                | `where merchant_user_id in (select distinct merchant_user_id from merchants) AND par_dt >= cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 12 months, 'yyyyMMdd') as int)` |

| Target Column     | Data Type | Transformation Logic                                     | Source Table                       | Source Column       | Group By             |
|:------------------|:----------|:--------------------------------------------------------|:-----------------------------------|:-------------------|:---------------------|
| `customer_id`     | STRING    | Direct Copy                                             | `merchant_insights_customer_interests` | `customer_id`      |                     |
| `merchant_user_id`| STRING    | Direct Copy                                             | `merchant_insights_customer_interests` | `merchant_user_id`  |                      |

## 4. Target Tables
The following target tables are updated or generated by the query:

### 4.1. `customer_activity`
| Target Table                     | Source Table                   | Row Selection Logic                                                             |
|:---------------------------------|:-------------------------------|:--------------------------------------------------------------------------------|
| `customer_activity`              | `customer_freq`, `merchant_avg` | `join customer_freq on customer_freq.merchant_user_id = merchant_avg.merchant_user_id` |
| `customer_activity`              | `m_all`, `merch_cust`          | `left join merch_cust on m_all.merchant_user_id = merch_cust.merchant_user_id and m_all.customer_id = merch_cust.customer_id where merch_cust.customer_id is null` |

| Target Column        | Data Type | Transformation Logic                                     | Source Table                   | Source Column          |
|:---------------------|:----------|:--------------------------------------------------------|:-------------------------------|:-----------------------|
| `merchant_user_id`   | STRING    | Direct Copy                                            | `customer_freq`                | `merchant_user_id`      |
| `customer_id`        | STRING    | Direct Copy                                            | `customer_freq`                | `customer_id`           |
| `activity`           | STRING    | CASE WHEN number_of_transactions > threshold THEN 'frequent' WHEN number_of_transactions <= threshold THEN 'less_frequent' END | `customer_freq`, `merchant_avg` | `number_of_transactions`, `threshold` |
| `activity`           | STRING    | Direct Copy                                            | `m_all`                        | 'prospective'           |

This document serves to provide a comprehensive overview of the data pipeline process, the tables involved, and the transformations taking place for the card payment analysis.