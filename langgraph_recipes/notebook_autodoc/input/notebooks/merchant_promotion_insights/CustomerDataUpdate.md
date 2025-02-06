# Functional Specification Document

## 1. Input Parameters

| Parameter Name | Data Type | Description |
|:---------------|:---------|:------------|
| catalog        | STRING   | The name of the database catalog to use. |
| pardt          | INT      | Partition date as an integer in YYYYMMDD format. |
| parym          | INT      | Partition year and month derived from `pardt`. |

## 2. Source Tables

### 2.1 Source Table Descriptions

| Column Name | Column Data Type |
|:------------|:-----------------|
| mcc         | STRING           |
| category    | STRING           |
| description | STRING           |
| hyper_category | STRING         |
| sector      | STRING           |

**Table: bdprod.merchant_promotion_core.mcc_categories**

| Column Name | Column Data Type |
|:------------|:-----------------|
| postal      | STRING           |
| region      | STRING           |
| regional_unit | STRING         |
| municipality | STRING          |
| territory   | STRING           |

**Table: bdprod.merchant_promotion_core.postal_codes**

| Column Name | Column Data Type |
|:------------|:-----------------|
| par_ym     | INT              |
| customer_id | STRING          |
| age        | INT              |
| home_location | STRING        |
| home_municipality | STRING     |
| home_regional_unit | STRING     |
| home_region | STRING          |
| work_location | STRING        |
| work_municipality | STRING     |
| work_regional_unit | STRING     |
| work_region | STRING          |
| gender     | STRING           |
| occupation | STRING           |
| annual_income | DECIMAL(15,2) |
| nbg_segment | STRING          |
| sms_ind    | BOOLEAN          |
| email_ind  | BOOLEAN          |
| addr_ind   | BOOLEAN          |
| memberships | ARRAY<STRING>   |
| ibank_ind  | BOOLEAN          |
| shopping_interests | STRING    |
| age_range_cat | INT           |

**Table: bdprod.merchant_promotion_analytical.cards_customer_characteristics**

| Column Name | Column Data Type |
|:------------|:-----------------|
| customer_id | STRING           |
| sector      | STRING           |
| hyper_category | STRING         |
| monthly_amount | DECIMAL(15,2) |
| frequency   | INT              |
| gr_ind      | BOOLEAN          |
| total_wallet | DECIMAL(15,2)   |
| ecommerce_ind | BOOLEAN        |
| par_ym      | INT              |

**Table: bdprod.merchant_promotion_analytical.customer_sector_volumes**

| Column Name | Column Data Type |
|:------------|:-----------------|
| customer_id | STRING           |
| age         | INT              |
| home_location | STRING         |
| work_location | STRING         |
| gender      | STRING           |
| occupation  | STRING           |
| annual_income | DECIMAL(15,2)  |
| nbg_segment | STRING           |
| sms_ind     | BOOLEAN          |
| email_ind   | BOOLEAN          |
| addr_ind    | BOOLEAN          |
| memberships | ARRAY<STRING>    |
| ibank_ind   | BOOLEAN          |
| age_range_cat | INT            |

**Table: bdprod.merchant_promotion_core.customer** 

## 3. Intermediate Tables

### 3.1 Intermediate Table Descriptions

| Target Table             | Source Table                                             | Row Selection Logic                                              |
|:------------------------|:-------------------------------------------------------|:----------------------------------------------------------------|
| customer_data           | bdprod.merchant_promotion_analytical.customer_activity | SELECT distinct * FROM customer_activity WHERE customer_id = csc.customer_id |
| customer_data           | bdprod.merchant_promotion_analytical.customer_sector_characteristics | SELECT * FROM customer_sector_characteristics WHERE par_ym = {parym} AND hyper_category IN (SELECT distinct hyper_category FROM bdprod.merchant_promotion_core.merchant WHERE is_merchant_user = true) |
| customer_data           | bdprod.merchant_promotion_analytical.cards_customer_characteristics | SELECT * FROM cards_customer_characteristics WHERE par_ym = {parym} AND ibank_ind = true AND sms_ind = true AND array_contains(memberships, "Go4More") |

### 3.2 Intermediate Table Column Transformations

| Target Column           | Data Type      | Transformation Logic | Source Table                                          | Source Column           | Group By |
|:------------------------|:---------------|:---------------------|:-----------------------------------------------------|:------------------------|:---------|
| merchant_user_id        | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.customer_activity | merchant_user_id       |          |
| customer_id             | STRING         | COALESCE              | bdprod.merchant_promotion_analytical.customer_activity | customer_id             |          |
| channel_preference      | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.customer_sector_characteristics | channel_preference      |          |
| spending_profile        | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.customer_sector_characteristics | spending_profile        |          |
| age                     | INT            | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | age                     |          |
| age_range_cat           | INT            | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | age_range_cat           |          |
| gender                  | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | gender                  |          |
| home_municipality       | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | home_municipality       |          |
| work_municipality       | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | work_municipality       |          |
| nbg_segment             | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | nbg_segment             |          |
| occupation              | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | occupation              |          |
| shopping_interests      | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | shopping_interests      |          |
| activity                | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.customer_activity | activity                |          |

## 4. Target Tables

### 4.1 Target Table Descriptions

| Target Table                                       | Source Table                                             | Row Selection Logic                                              |
|:--------------------------------------------------|:-------------------------------------------------------|:----------------------------------------------------------------|
| bdprod.merchant_promotion_insights.customer_data   | bdprod.merchant_promotion_analytical.customer_activity | Combined results matching criteria from customer_activity and customer_sector_characteristics and cards_customer_characteristics |

### 4.2 Target Table Column Transformations

| Target Column          | Data Type      | Transformation Logic | Source Table                                          | Source Column           |
|:-----------------------|:---------------|:---------------------|:-----------------------------------------------------|:------------------------|
| merchant_user_id       | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.customer_activity | merchant_user_id       |
| customer_id            | STRING         | COALESCE             | bdprod.merchant_promotion_analytical.customer_activity | customer_id             |
| channel_preference     | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.customer_sector_characteristics | channel_preference      |
| spending_profile       | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.customer_sector_characteristics | spending_profile        |
| age                    | INT            | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | age                     |
| age_range_cat          | INT            | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | age_range_cat           |
| gender                 | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | gender                  |
| home_municipality      | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | home_municipality       |
| work_municipality      | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | work_municipality       |
| nbg_segment            | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | nbg_segment             |
| occupation             | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | occupation              |
| shopping_interests     | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | shopping_interests      |
| activity               | STRING         | Direct Copy          | bdprod.merchant_promotion_analytical.customer_activity | activity                | 

This functional specification document outlines the necessary components for the data pipeline concerning customer data. Each section details the input parameters, source tables, intermediate logic, and target outcomes as defined in the Databricks notebook.