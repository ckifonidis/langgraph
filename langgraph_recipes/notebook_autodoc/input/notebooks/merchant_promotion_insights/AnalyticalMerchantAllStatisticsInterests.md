# Functional Specification Document

## 1. Input Parameters
The following input parameters are defined in the notebook:

| Parameter Name | Description |
|:---------------|:-----------|
| catalog        | Catalog name, derived from `get_catalog()` function. |
| parym_range    | Range condition for partition year-month, derived from `get_parym_range_condition()` function. |
| pardt_range    | Range condition for partition date, derived from `get_pardt_range_condition()` function. |

## 2. Source Tables
The following source tables are utilized in the query:

### 2.1. `bdprod.merchant_promotion_core.customer_daily_trns`

| Column Name      | Column Data Type |
|:-----------------|:-----------------|
| merchant_id      | STRING           |
| customer_id      | STRING           |
| par_dt           | DATE             |

### 2.2. `bdprod.merchant_promotion_analytical.cards_customer_characteristics`

| Column Name         | Column Data Type |
|:--------------------|:-----------------|
| par_ym              | INT              |
| customer_id         | STRING           |
| shopping_interests  | STRING           |

### 2.3. `bdprod.merchant_promotion_core.merchant`

| Column Name      | Column Data Type |
|:-----------------|:-----------------|
| merchantid       | STRING           |
| mcc              | STRING           |

### 2.4. `bdprod.merchant_promotion_core.retailer_info`

| Column Name | Column Data Type |
|:------------|:-----------------|
| uuid        | STRING           |
| retailer    | STRING           |
| brand       | STRING           |

### 2.5. `bdprod.merchant_promotion_core.mcc_categories`

| Column Name      | Column Data Type |
|:-----------------|:-----------------|
| mcc              | STRING           |
| hyper_category   | STRING           |

### 2.6. `bdprod.merchant_promotion_analytical.customer_sector_characteristics`

| Column Name      | Column Data Type |
|:-----------------|:-----------------|
| par_ym           | INT              |
| customer_id      | STRING           |
| hyper_category    | STRING           |

## 3. Intermediate Tables
The query uses intermediate tables generated from CTEs.

### Intermediate Table 1: Transactions CTE
- **Source Table**: `bdprod.merchant_promotion_core.customer_daily_trns`
- **Row Selection Logic**: 
  ```sql
  WHERE {pardt_range}
  ```
  
| Target Table               | Source Table                                          | Row Selection Logic               |
|:---------------------------|:-----------------------------------------------------|:----------------------------------|
| Transactions CTE           | bdprod.merchant_promotion_core.customer_daily_trns  | {pardt_range}                     |

| Target Column | Data Type | Transformation Logic | Source Table                                          | Source Column  |
|:--------------|:----------|:--------------------|:-----------------------------------------------------|:---------------|
| merchant_id   | STRING    | Direct Copy         | bdprod.merchant_promotion_core.customer_daily_trns   | merchant_id    |
| customer_id   | STRING    | Direct Copy         | bdprod.merchant_promotion_core.customer_daily_trns   | customer_id    |
| par_dt        | DATE      | Direct Copy         | bdprod.merchant_promotion_core.customer_daily_trns   | par_dt         |

### Intermediate Table 2: Customer Characteristics CTE
- **Source Table**: `bdprod.merchant_promotion_analytical.cards_customer_characteristics`
- **Row Selection Logic**: 
  ```sql
  WHERE {parym_range}
  ```

| Target Table                  | Source Table                                         | Row Selection Logic                 |
|:------------------------------|:----------------------------------------------------|:------------------------------------|
| Customer Characteristics CTE   | bdprod.merchant_promotion_analytical.cards_customer_characteristics | {parym_range}                      |

| Target Column      | Data Type | Transformation Logic | Source Table                                         | Source Column        |
|:-------------------|:----------|:--------------------|:-----------------------------------------------------|:---------------------|
| shopping_interests | STRING    | IFNULL(shopping_interests, 'other_category') | bdprod.merchant_promotion_analytical.cards_customer_characteristics | shopping_interests   |

### Intermediate Table 3: Merchant Data CTE
- **Source Tables**: 
  - `bdprod.merchant_promotion_core.merchant`
  - `bdprod.merchant_promotion_core.retailer_info`
  - `bdprod.merchant_promotion_core.mcc_categories`

| Target Table        | Source Table                                              | Row Selection Logic               |
|:--------------------|:---------------------------------------------------------|:----------------------------------|
| Merchant Data CTE   | bdprod.merchant_promotion_core.merchant                  | N/A                               |
|                     | bdprod.merchant_promotion_core.retailer_info             | N/A                               |
|                     | bdprod.merchant_promotion_core.mcc_categories            | N/A                               |

| Target Column | Data Type | Transformation Logic | Source Table                                          | Source Column      |
|:--------------|:----------|:--------------------|:-----------------------------------------------------|:-------------------|
| mid           | STRING    | Direct Copy         | bdprod.merchant_promotion_core.merchant              | merchantid         |
| retailer      | STRING    | Direct Copy         | bdprod.merchant_promotion_core.retailer_info         | retailer           |
| brand         | STRING    | Direct Copy         | bdprod.merchant_promotion_core.retailer_info         | brand              |
| mcc           | STRING    | Direct Copy         | bdprod.merchant_promotion_core.mcc_categories        | mcc                |
| hyper_category | STRING   | Direct Copy         | bdprod.merchant_promotion_core.mcc_categories        | hyper_category      |

### Intermediate Table 4: Customer Sector Characteristics CTE
- **Source Table**: `bdprod.merchant_promotion_analytical.customer_sector_characteristics`

| Target Table                      | Source Table                                        | Row Selection Logic                    |
|:----------------------------------|:---------------------------------------------------|:---------------------------------------|
| Customer Sector Characteristics CTE | bdprod.merchant_promotion_analytical.customer_sector_characteristics | N/A                                    |

| Target Column      | Data Type | Transformation Logic | Source Table                                         | Source Column  |
|:-------------------|:----------|:--------------------|:-----------------------------------------------------|:---------------|
| par_ym             | INT       | Direct Copy         | bdprod.merchant_promotion_analytical.customer_sector_characteristics | par_ym        |
| customer_id        | STRING    | Direct Copy         | bdprod.merchant_promotion_analytical.customer_sector_characteristics | customer_id   |
| hyper_category     | STRING    | Direct Copy         | bdprod.merchant_promotion_analytical.customer_sector_characteristics | hyper_category |

## 4. Target Tables
The target table for the query is:

### Target Table: `bdprod.merchant_promotion_analytical.merchant_all_statistics_interests`

| Target Table                                            | Source Table                                         | Row Selection Logic |
|:-------------------------------------------------------|:----------------------------------------------------|:---------------------|
| bdprod.merchant_promotion_analytical.merchant_all_statistics_interests | Transactions CTE, Customer Characteristics CTE, Merchant Data CTE, Customer Sector Characteristics CTE | N/A                 |

| Target Column      | Data Type | Transformation Logic | Source Table                                         | Source Column        |
|:-------------------|:----------|:--------------------|:-----------------------------------------------------|:---------------------|
| merchant_id        | STRING    | Direct Copy         | Transactions CTE                                     | merchant_id          |
| customer_id        | STRING    | Direct Copy         | Transactions CTE                                     | customer_id          |
| mcc                | STRING    | Direct Copy         | Merchant Data CTE                                    | mcc                  |
| shopping_interests | STRING    | IFNULL(c.shopping_interests, 'other_category') | Customer Characteristics CTE                         | shopping_interests    |
| hyper_category      | STRING   | Direct Copy         | Merchant Data CTE                                    | hyper_category       |
| spending_profile   | STRING    | Direct Copy         | Customer Sector Characteristics CTE                  | spending_profile     |
| channel_preference | STRING    | Direct Copy         | Customer Sector Characteristics CTE                  | channel_preference   |
| retailer           | STRING    | Direct Copy         | Merchant Data CTE                                    | retailer             |
| brand              | STRING    | Direct Copy         | Merchant Data CTE                                    | brand                |
| par_dt            | DATE      | Direct Copy         | Transactions CTE                                     | par_dt               |