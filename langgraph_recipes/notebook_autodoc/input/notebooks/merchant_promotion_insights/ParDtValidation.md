# Functional Specification Document

## 1. Input Parameters

| Parameter Name  | Description                                                  |
|:----------------|:-------------------------------------------------------------|
| pardt           | The date for which the MerchantPromotionInsightsUpdate is executed, in YYYYMMDD format. This parameter is mandatory. |
| initial_pardt   | The initial date for the range of data to process, in YYYYMMDD format. A value of -1 indicates no initial date. |

## 2. Source Tables

### Source Table Descriptions

#### 2.1 `bdprod.merchant_promotion_core.mcc_categories`

| Column Name      | Column Data Type |
|:-----------------|:-----------------|
| mcc              | STRING           |
| category         | STRING           |
| description      | STRING           |
| hyper_category   | STRING           |
| sector           | STRING           |

#### 2.2 `bdprod.merchant_promotion_core.postal_codes`

| Column Name      | Column Data Type |
|:-----------------|:-----------------|
| postal           | STRING           |
| region           | STRING           |
| regional_unit    | STRING           |
| municipality     | STRING           |
| territory        | STRING           |

#### 2.3 `bdprod.merchant_promotion_analytical.cards_customer_characteristics`

| Column Name            | Column Data Type     |
|:-----------------------|:---------------------|
| par_ym                 | INT                   |
| customer_id            | STRING                |
| age                    | INT                   |
| home_location          | STRING                |
| home_municipality      | STRING                |
| home_regional_unit     | STRING                |
| home_region            | STRING                |
| work_location          | STRING                |
| work_municipality      | STRING                |
| work_regional_unit     | STRING                |
| work_region            | STRING                |
| gender                 | STRING                |
| occupation             | STRING                |
| annual_income          | DECIMAL(15,2)        |
| nbg_segment            | STRING                |
| sms_ind                | BOOLEAN              |
| email_ind              | BOOLEAN              |
| addr_ind               | BOOLEAN              |
| memberships            | ARRAY<STRING>        |
| ibank_ind              | BOOLEAN              |
| shopping_interests     | STRING                |
| age_range_cat          | INT                   |

#### 2.4 `bdprod.merchant_promotion_analytical.customer_sector_volumes`

| Column Name            | Column Data Type     |
|:-----------------------|:---------------------|
| customer_id            | STRING                |
| sector                 | STRING                |
| hyper_category         | STRING                |
| monthly_amount         | DECIMAL(15,2)        |
| frequency              | INT                   |
| gr_ind                 | BOOLEAN              |
| total_wallet           | DECIMAL(15,2)        |
| ecommerce_ind          | BOOLEAN              |
| par_ym                 | INT                   |

#### 2.5 `bdprod.merchant_promotion_core.customer`

| Column Name            | Column Data Type     |
|:-----------------------|:---------------------|
| customer_id            | STRING                |
| age                    | INT                   |
| home_location          | STRING                |
| work_location          | STRING                |
| gender                 | STRING                |
| occupation             | STRING                |
| annual_income          | DECIMAL(15,2)        |
| nbg_segment            | STRING                |
| sms_ind                | BOOLEAN              |
| email_ind              | BOOLEAN              |
| addr_ind               | BOOLEAN              |
| memberships            | ARRAY<STRING>        |
| ibank_ind              | BOOLEAN              |
| age_range_cat          | INT                   |

## 3. Intermediate Tables

*Note: No Intermediate Tables (CTEs) are explicitly mentioned in the provided notebook. Thus, there are no intermediate tables to document.*

## 4. Target Tables

*Note: There are no explicit target tables or transformations from the query provided in the notebook.* 

### Target Table Logic

* Due to the lack of specified target tables in the notebook queries, there are no additional transformation logs to provide at this time.*