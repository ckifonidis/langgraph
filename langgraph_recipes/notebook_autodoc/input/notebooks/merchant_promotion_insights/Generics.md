# Functional Specification Document

## 1. Input Parameters
The following input parameters are used in the notebook:

| Parameter Name       | Type   | Description                             |
|:---------------------|:-------|:----------------------------------------|
| pardt                | INT    | Partition date in format yyyyMMdd.    |
| initial_pardt        | INT    | Initial partition date in format yyyyMMdd. |
| merchant_user_id     | STRING | Merchant user ID for filtering.        |
| force_update         | STRING | Flag to force update the records.     |
| catalog              | STRING | Catalog identifier.                    |
| analytical_only      | STRING | Flag to run only analytical computations. |
| insights_only        | STRING | Flag to run only insights computations. |

## 2. Source Tables
The following source tables are expected to be present when the notebook gets executed:

### 2.1 bdprod.merchant_promotion_core.mcc_categories

| Column Name      | Column Data Type |
|:-----------------|:-----------------|
| mcc              | STRING           |
| category         | STRING           |
| description      | STRING           |
| hyper_category   | STRING           |
| sector           | STRING           |

### 2.2 bdprod.merchant_promotion_core.postal_codes

| Column Name      | Column Data Type |
|:-----------------|:-----------------|
| postal           | STRING           |
| region           | STRING           |
| regional_unit    | STRING           |
| municipality     | STRING           |
| territory        | STRING           |

### 2.3 bdprod.merchant_promotion_analytical.cards_customer_characteristics

| Column Name                  | Column Data Type   |
|:-----------------------------|:-------------------|
| par_ym                       | INT                 |
| customer_id                  | STRING              |
| age                          | INT                 |
| home_location                | STRING              |
| home_municipality            | STRING              |
| home_regional_unit           | STRING              |
| home_region                  | STRING              |
| work_location                | STRING              |
| work_municipality            | STRING              |
| work_regional_unit           | STRING              |
| work_region                  | STRING              |
| gender                       | STRING              |
| occupation                   | STRING              |
| annual_income                | DECIMAL(15,2)      |
| nbg_segment                  | STRING              |
| sms_ind                      | BOOLEAN             |
| email_ind                    | BOOLEAN             |
| addr_ind                     | BOOLEAN             |
| memberships                  | ARRAY<STRING>       |
| ibank_ind                    | BOOLEAN             |
| shopping_interests           | STRING              |
| age_range_cat                | INT                 |

### 2.4 bdprod.merchant_promotion_analytical.customer_sector_volumes

| Column Name         | Column Data Type   |
|:--------------------|:-------------------|
| customer_id         | STRING              |
| sector              | STRING              |
| hyper_category      | STRING              |
| monthly_amount      | DECIMAL(15,2)      |
| frequency           | INT                 |
| gr_ind              | BOOLEAN             |
| total_wallet        | DECIMAL(15,2)      |
| ecommerce_ind       | BOOLEAN             |
| par_ym             | INT                 |

### 2.5 bdprod.merchant_promotion_core.customer

| Column Name          | Column Data Type   |
|:---------------------|:-------------------|
| customer_id          | STRING              |
| age                  | INT                 |
| home_location        | STRING              |
| work_location        | STRING              |
| gender               | STRING              |
| occupation           | STRING              |
| annual_income        | DECIMAL(15,2)      |
| nbg_segment          | STRING              |
| sms_ind              | BOOLEAN             |
| email_ind            | BOOLEAN             |
| addr_ind             | BOOLEAN             |
| memberships          | ARRAY<STRING>       |
| ibank_ind            | BOOLEAN             |
| age_range_cat        | INT                 |

## 3. Intermediate Tables
The notebook utilizes several functions to generate filter conditions and transformations but does not explicitly define CTEs or intermediate tables in the provided context. Therefore, there are no intermediate tables documented.

## 4. Target Tables
The target tables are implied based on the operations likely performed with the data processed through the notebook. The following entries capture the expected output operations for the relevant target tables. 

### 4.1 Target Table: bdprod.merchant_promotion_analytical.customer_sector_volumes

| Target Table                                   | Source Table                                           | Row Selection Logic                                                          |
|:------------------------------------------------|:------------------------------------------------------|:---------------------------------------------------------------------------|
| bdprod.merchant_promotion_analytical.customer_sector_volumes | bdprod.merchant_promotion_analytical.cards_customer_characteristics | par_dt >= {pardt1} and par_dt <= {pardt2} and par_ym >= {parym1} and par_ym <= {parym2}    |

| Target Column     | Data Type      | Transformation Logic   | Source Table                                           | Source Column         | Group By |
|:------------------|:---------------|:------------------------|:------------------------------------------------------|:----------------------|:---------|
| customer_id       | STRING         | Direct Copy             | bdprod.merchant_promotion_analytical.cards_customer_characteristics | customer_id           |         |
| sector            | STRING         | Derived based on mcc    | bdprod.merchant_promotion_core.mcc_categories        | category              |         |
| monthly_amount    | DECIMAL(15,2)  | SUM(amount)             | bdprod.merchant_promotion_analytical.cards_customer_characteristics | monthly_amount        | sector   |
| frequency         | INT            | COUNT(transaction_id)   | bdprod.merchant_promotion_analytical.cards_customer_characteristics | transaction_id        | sector   |
| gr_ind            | BOOLEAN        | Direct Copy             | bdprod.merchant_promotion_core.customer               | gr_ind                |         |
| total_wallet      | DECIMAL(15,2)  | SUM(wallet)             | bdprod.merchant_promotion_core.customer               | total_wallet          |         |
| ecommerce_ind     | BOOLEAN        | Direct Copy             | bdprod.merchant_promotion_core.customer               | ecommerce_ind         |         |
| par_ym           | INT             | Direct Copy             | bdprod.merchant_promotion_analytical.cards_customer_characteristics | par_ym               |         |

This concludes the functional specification document based on the provided notebook content and source table descriptions.