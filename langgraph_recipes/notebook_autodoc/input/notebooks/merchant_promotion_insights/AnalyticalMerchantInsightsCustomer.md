# Functional Specification Document

## 1. Input Parameters
| Parameter Name                     | Description                                                        |
|:-----------------------------------|:-------------------------------------------------------------------|
| catalog                            | Catalog name for tables                                            |
| parym_range                       | Condition for selecting specific year-month range for filtering   |
| pardt_range                       | Condition for selecting specific date range for filtering         |
| merchant_user_id_condition        | Condition for filtering by merchant user ID                       |
| (pardt1, pardt2)                 | Integer range for filtering transaction dates                     |

## 2. Source Tables
### Table: `customer_daily_trns`
| Column Name               | Column Data Type |
|:--------------------------|:-----------------|
| merchant_id               | STRING           |
| customer_id               | STRING           |
| number_of_transactions     | INT              |
| amount                    | DECIMAL(15,2)    |
| mcc                       | STRING           |
| ecommerce_ind             | BOOLEAN          |
| payment_type              | STRING           |
| par_dt                    | INT              |

### Table: `merchant`
| Column Name               | Column Data Type |
|:--------------------------|:-----------------|
| merchantid                | STRING           |
| merchant_user_id          | STRING           |
| merchant_mcc              | STRING           |
| is_merchant_user          | BOOLEAN          |
| country                   | STRING           |

### Table: `customer_activity_insights`
| Column Name               | Column Data Type |
|:--------------------------|:-----------------|
| merchant_user_id          | STRING           |
| customer_id               | STRING           |
| activity                  | STRING           |
| par_dt                    | INT              |

### Table: `cards_customer_characteristics`
| Column Name               | Column Data Type |
|:--------------------------|:-----------------|
| par_ym                    | INT              |
| customer_id               | STRING           |
| age                       | INT              |
| home_location             | STRING           |
| home_municipality         | STRING           |
| home_regional_unit        | STRING           |
| home_region               | STRING           |
| work_location             | STRING           |
| work_municipality         | STRING           |
| work_regional_unit        | STRING           |
| work_region               | STRING           |
| gender                    | STRING           |
| occupation                | STRING           |
| annual_income             | DECIMAL(15,2)    |
| nbg_segment               | STRING           |
| sms_ind                   | BOOLEAN          |
| memberships               | ARRAY<STRING>     |
| ibank_ind                 | BOOLEAN          |
| shopping_interests        | STRING           |

### Table: `loyalty_business_transactions`
| Column Name               | Column Data Type |
|:--------------------------|:-----------------|
| transactiondate           | DATE             |
| filler_2                 | STRING           |
| customerkey               | STRING           |
| cardnumbermasked          | STRING           |
| actualcollectedpoints     | DECIMAL(15,2)    |
| amount                    | DECIMAL(15,2)    |
| redeemedamount            | DECIMAL(15,2)    |
| redeempoints              | INT              |

### Table: `mpe_promotions`
| Column Name               | Column Data Type |
|:--------------------------|:-----------------|
| promotion_id              | STRING           |
| customercode              | STRING           |
| merchant_id               | STRING           |
| startdate                 | DATE             |
| enddate                   | DATE             |

### Table: `mpe_promotion_customers`
| Column Name               | Column Data Type |
|:--------------------------|:-----------------|
| promotion_id              | STRING           |
| customercode              | STRING           |
| merchant_id               | STRING           |


## 3. Intermediate Tables
### Intermediate Table: `trns`
| Target Table             | Source Table                           | Row Selection Logic                                                    |
|:-------------------------|:---------------------------------------|:----------------------------------------------------------------------|
| trns                     | `database_name.merchant_promotion_core.customer_daily_trns` | WHERE {pardt_range} AND country = "GRC"                             | 

| Target Column            | Data Type      | Transformation Logic       | Source Table                                                   | Source Column            | Group By |
|:-------------------------|:---------------|:---------------------------|:---------------------------------------------------------------|:-------------------------|:---------|
| merchant_id              | STRING         | Direct Copy                | `database_name.merchant_promotion_core.customer_daily_trns`   | merchant_id              |          |
| customer_id              | STRING         | Direct Copy                | `database_name.merchant_promotion_core.customer_daily_trns`   | customer_id              |          |
| number_of_transactions    | INT            | Direct Copy                | `database_name.merchant_promotion_core.customer_daily_trns`   | number_of_transactions   |          |
| amount                   | DECIMAL(15,2)  | Direct Copy                | `database_name.merchant_promotion_core.customer_daily_trns`   | amount                   |          |
| mcc                      | STRING         | Direct Copy                | `database_name.merchant_promotion_core.customer_daily_trns`   | mcc                      |          |
| ecommerce_ind            | BOOLEAN        | Direct Copy                | `database_name.merchant_promotion_core.customer_daily_trns`   | ecommerce_ind            |          |
| payment_type             | STRING         | Direct Copy                | `database_name.merchant_promotion_core.customer_daily_trns`   | payment_type             |          |
| par_dt                   | INT            | Direct Copy                | `database_name.merchant_promotion_core.customer_daily_trns`   | par_dt                   |          |

### Intermediate Table: `muser`
| Target Table             | Source Table                         | Row Selection Logic                                                    |
|:-------------------------|:-------------------------------------|:----------------------------------------------------------------------|
| muser                    | `database_name.merchant_promotion_core.merchant` | WHERE is_merchant_user = True AND country = "GRC" {merchant_user_id_condition} | 

| Target Column            | Data Type      | Transformation Logic       | Source Table                                                   | Source Column            | Group By |
|:-------------------------|:---------------|:---------------------------|:---------------------------------------------------------------|:-------------------------|:---------|
| merchant_id              | STRING         | Direct Copy                | `database_name.merchant_promotion_core.merchant`              | merchantid               |          |
| merchant_user_id         | STRING         | Direct Copy                | `database_name.merchant_promotion_core.merchant`              | merchant_user_id         |          |
| mcc                      | STRING         | Direct Copy                | `database_name.merchant_promotion_core.merchant`              | merchant_mcc             |          |

### Intermediate Table: `g4m`
| Target Table             | Source Table                             | Row Selection Logic                                                                        |
|:-------------------------|:-----------------------------------------|:------------------------------------------------------------------------------------------|
| g4m                      | `database_name.trlog_card.loyalty_business_transactions` | WHERE filler_2 IN (SELECT distinct merchantid FROM `database_name.merchant_promotion_core.merchant` WHERE is_merchant_user = True) AND CAST(replace(cast(to_date(transactiondate) as string), '-','') as int) >= {pardt1} AND CAST(replace(cast(to_date(transactiondate) as string), '-','') as int) <= {pardt2} | 

| Target Column            | Data Type      | Transformation Logic       | Source Table                                                   | Source Column            | Group By   |
|:-------------------------|:---------------|:---------------------------|:---------------------------------------------------------------|:-------------------------|:-----------|
| par_dt                   | INT            | CAST(replace(cast(to_date(transactiondate) as string), '-','') as int) | `database_name.trlog_card.loyalty_business_transactions`      | transactiondate          |          |
| merchantid               | STRING         | Direct Copy                | `database_name.trlog_card.loyalty_business_transactions`      | filler_2                 |          |
| customerkey              | STRING         | cast(customerkey as string)| `database_name.trlog_card.loyalty_business_transactions`      | customerkey              |          |
| card_type                | STRING         | CASE WHEN ... END          | `database_name.trlog_card.loyalty_business_transactions`      | CASE WHEN substr(cardnumbermasked,...) END |          |
| actualcollectedpoints    | DECIMAL(15,2)  | sum(actualcollectedpoints) | `database_name.trlog_card.loyalty_business_transactions`      | actualcollectedpoints    | par_dt, merchantid, customerkey, card_type |
| rewarded_amount          | DECIMAL(15,2)  | sum(amount)                | `database_name.trlog_card.loyalty_business_transactions`      | amount                   | par_dt, merchantid, customerkey, card_type |
| redeemedamount           | DECIMAL(15,2)  | sum(redeemedamount)        | `database_name.trlog_card.loyalty_business_transactions`      | redeemedamount           | par_dt, merchantid, customerkey, card_type |
| redeempoints             | INT            | sum(redeempoints)          | `database_name.trlog_card.loyalty_business_transactions`      | redeempoints             | par_dt, merchantid, customerkey, card_type |

### Intermediate Table: `cust`
| Target Table             | Source Table                             | Row Selection Logic                                                            |
|:-------------------------|:-----------------------------------------|:------------------------------------------------------------------------------|
| cust                     | `database_name.merchant_promotion_analytical.cards_customer_characteristics` | WHERE {parym_range} GROUP BY...                                              | 

| Target Column            | Data Type      | Transformation Logic       | Source Table                                                   | Source Column            | Group By   |
|:-------------------------|:---------------|:---------------------------|:---------------------------------------------------------------|:-------------------------|:-----------|
| customer_id              | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | customer_id              |          |
| age                      | INT            | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | age                      |          |
| home_location            | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | home_location            |          |
| home_municipality        | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | home_municipality        |          |
| home_regional_unit       | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | home_regional_unit       |          |
| home_region              | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | home_region              |          |
| work_location            | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | work_location            |          |
| work_municipality        | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | work_municipality        |          |
| work_regional_unit       | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | work_regional_unit       |          |
| work_region              | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | work_region              |          |
| gender                   | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | gender                   |          |
| occupation               | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | occupation               |          |
| annual_income            | DECIMAL(15,2)  | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | annual_income            |          |
| nbg_segment              | STRING         | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | nbg_segment              |          |
| sms_ind                  | BOOLEAN        | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | sms_ind                  |          |
| go4more_ind              | BOOLEAN        | case when array_contains(memberships, "Go4More") then True else False end | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | memberships              |          |
| ibank_ind                | BOOLEAN        | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | ibank_ind                |          |
| shopping_interests       | STRING         | array_join(collect_list(shopping_interests), ',') | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | shopping_interests       |          |
| par_ym                   | INT            | Direct Copy                | `database_name.merchant_promotion_analytical.cards_customer_characteristics`  | par_ym                   |          |

## 4. Target Tables
### Target Table: `merchant_insights_customer`
| Target Table                             | Source Table                    | Row Selection Logic                                                        |
|:-----------------------------------------|:---------------------------------|:--------------------------------------------------------------------------|
| `database_name.merchant_promotion_analytical.merchant_insights_customer` | ALL source tables through joins | REPLACE WHERE {pardt_range} {merchant_user_id_condition}                 | 

| Target Column            | Data Type      | Transformation Logic       | Source Table                                                   | Source Column            |
|:-------------------------|:---------------|:---------------------------|:---------------------------------------------------------------|:-------------------------|
| merchant_user_id         | STRING         | Direct Copy                | muser                                                         | merchant_user_id         |
| merchant_id              | STRING         | Direct Copy                | trns                                                          | merchant_id              |
| customer_id              | STRING         | Direct Copy                | trns                                                          | customer_id              |
| number_of_transactions    | INT            | Direct Copy                | trns                                                          | number_of_transactions   |
| amount                   | DECIMAL(15,2)  | Direct Copy                | trns                                                          | amount                   |
| mcc                      | STRING         | Direct Copy                | muser                                                         | mcc                      |
| ecommerce_ind            | BOOLEAN        | Direct Copy                | trns                                                          | ecommerce_ind            |
| card_type                | STRING         | Direct Copy                | trns                                                          | payment_type             |
| age                      | INT            | Direct Copy                | cust                                                          | age                      |
| age_group                | STRING         | CASE...                     | CASE WHEN age <=24 THEN '18-24' WHEN age <=40 THEN...   | age                      |
| home_location            | STRING         | Direct Copy                | cust                                                          | home_location            |
| home_municipality        | STRING         | Direct Copy                | cust                                                          | home_municipality        |
| home_regional_unit       | STRING         | Direct Copy                | cust                                                          | home_regional_unit       |
| home_region              | STRING         | Direct Copy                | cust                                                          | home_region              |
| work_location            | STRING         | Direct Copy                | cust                                                          | work_location            |
| work_municipality        | STRING         | Direct Copy                | cust                                                          | work_municipality        |
| work_regional_unit       | STRING         | Direct Copy                | cust                                                          | work_regional_unit       |
| work_region              | STRING         | Direct Copy                | cust                                                          | work_region              |
| gender                   | STRING         | Direct Copy                | cust                                                          | gender                   |
| occupation               | STRING         | Direct Copy                | cust                                                          | occupation               |
| annual_income            | DECIMAL(15,2)  | Direct Copy                | cust                                                          | annual_income            |
| nbg_segment              | STRING         | Direct Copy                | cust                                                          | nbg_segment              |
| sms_ind                  | BOOLEAN        | Direct Copy                | cust                                                          | sms_ind                  |
| go4more_ind              | BOOLEAN        | Direct Copy                | cust                                                          | go4more_ind              |
| ibank_ind                | BOOLEAN        | Direct Copy                | cust                                                          | ibank_ind                |
| shopping_interests       | STRING         | Direct Copy                | cust                                                          | shopping_interests       |
| activity                 | STRING         | CASE WHEN ...              | act                                                           | activity                 |
| promotion_id             | STRING         | Direct Copy                | promo                                                         | promotion_id             |
| rewarded_points          | DECIMAL(15,2)  | Direct Copy                | g4m                                                           | actualcollectedpoints    |
| rewarded_amount          | DECIMAL(15,2)  | Direct Copy                | g4m                                                           | rewarded_amount          |
| redeemed_points          | INT            | Direct Copy                | g4m                                                           | redeempoints             |
| redeemed_amount          | DECIMAL(15,2)  | Direct Copy                | g4m                                                           | redeemedamount           |
| par_dt                   | INT            | Direct Copy                | trns                                                          | par_dt                   |

This concludes the functional specification document based on the provided Databricks notebook analysis.