# Functional Specification Document

## 1. Input Parameters
| Parameter Name | Description |
|----------------|-------------|
| catalog        | Database catalog to be used in the query. |
| parym_range    | Date range condition for partitioned year-month. |
| pardt1        | Start date in 'yyyyMMdd' format for the range. |
| pardt2        | End date in 'yyyyMMdd' format for the range. |
| parym_end      | End year-month derived from end date. |

## 2. Source Tables
### 2.1 Source Table Descriptions

| Column Name             | Column Data Type |
|:------------------------|:-----------------|
| mcc                     | STRING           |
| category                | STRING           |
| description             | STRING           |
| hyper_category          | STRING           |
| sector                  | STRING           |

| Column Name             | Column Data Type |
|:------------------------|:-----------------|
| postal                  | STRING           |
| region                  | STRING           |
| regional_unit           | STRING           |
| municipality            | STRING           |
| territory               | STRING           |

| Column Name             | Column Data Type |
|:------------------------|:-----------------|
| par_ym                  | INT              |
| customer_id             | STRING           |
| age                     | INT              |
| home_location           | STRING           |
| home_municipality       | STRING           |
| home_regional_unit      | STRING           |
| home_region             | STRING           |
| work_location           | STRING           |
| work_municipality       | STRING           |
| work_regional_unit      | STRING           |
| work_region             | STRING           |
| gender                  | STRING           |
| occupation              | STRING           |
| annual_income           | DECIMAL(15,2)    |
| nbg_segment             | STRING           |
| sms_ind                 | BOOLEAN          |
| email_ind               | BOOLEAN          |
| addr_ind                | BOOLEAN          |
| memberships             | ARRAY<STRING>     |
| ibank_ind               | BOOLEAN          |
| shopping_interests      | STRING           |
| age_range_cat           | INT              |

| Column Name             | Column Data Type |
|:------------------------|:-----------------|
| customer_id             | STRING           |
| sector                  | STRING           |
| hyper_category          | STRING           |
| monthly_amount          | DECIMAL(15,2)    |
| frequency               | INT              |
| gr_ind                  | BOOLEAN          |
| total_wallet            | DECIMAL(15,2)    |
| ecommerce_ind           | BOOLEAN          |
| par_ym                  | INT              |

| Column Name             | Column Data Type |
|:------------------------|:-----------------|
| customer_id             | STRING           |
| age                     | INT              |
| home_location           | STRING           |
| work_location           | STRING           |
| gender                  | STRING           |
| occupation              | STRING           |
| annual_income           | DECIMAL(15,2)    |
| nbg_segment             | STRING           |
| sms_ind                 | BOOLEAN          |
| email_ind               | BOOLEAN          |
| addr_ind                | BOOLEAN          |
| memberships             | ARRAY<STRING>     |
| ibank_ind               | BOOLEAN          |
| age_range_cat           | INT              |

## 3. Intermediate Tables
### 3.1 Intermediate Tables Descriptions

| Target Table   | Source Table                                   | Row Selection Logic                                                                                       |
|:---------------|:-----------------------------------------------|:----------------------------------------------------------------------------------------------------------|
| all_comb       | None (Generated using sequence)               | Generated for all the month sequences between pardt1 and pardt2.                                        |
| customer_data  | bdprod.merchant_promotion_analytical.customer_sector_volumes | v.par_ym between ac.par_ym_year_before and ac.par_ym AND v.par_ym between cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 12 months, 'yyyyMM') as int) and {parym_end}      |
| customer_sector_share | Intermediate: customer_data                | Direct Copy of customer_id, hyper_category, par_ym from customer_data, and calculation of perc_share.   |
| hypercategory_mapping | bdprod.merchant_promotion_core.mcc_categories | sector IS NOT NULL                                                                                       |
| interests      | Intermediate: customer_sector_share, hypercategory_mapping | c.perc_share > (ROUND(AVG(perc_share) over (partition by c.par_ym, c.hyper_category), 2))                |

### 3.2 Intermediate Table Logic 

#### 3.2.1 all_comb Table
| Target Column | Data Type | Transformation Logic                           | Source Table | Source Column | Group By |
|:--------------|:----------|:----------------------------------------------|:-------------|:--------------|:---------|
| par_ym        | INT       | CAST(date_format(ts, 'yyyyMM') AS INT)      | None         | None          | None     |
| par_ym_year_before | INT | CAST(date_format(ts - interval 12 months, 'yyyyMM') AS INT) | None         | None          | None     |

#### 3.2.2 customer_data Table
| Target Column | Data Type | Transformation Logic                           | Source Table | Source Column | Group By        |
|:--------------|:----------|:----------------------------------------------|:-------------|:--------------|:----------------|
| par_ym        | INT       | Direct Copy                                   | bdprod.merchant_promotion_analytical.customer_sector_volumes | par_ym  | ac.par_ym, customer_id, hyper_category |
| customer_id   | STRING    | Direct Copy                                   | bdprod.merchant_promotion_analytical.customer_sector_volumes | customer_id | ac.par_ym, customer_id, hyper_category |
| hyper_category | STRING    | Direct Copy                                   | bdprod.merchant_promotion_analytical.customer_sector_volumes | hyper_category | ac.par_ym, customer_id, hyper_category |
| customer_amount| DECIMAL(15,2) | SUM(monthly_amount)                       | bdprod.merchant_promotion_analytical.customer_sector_volumes | monthly_amount | ac.par_ym, customer_id, hyper_category |

#### 3.2.3 customer_sector_share Table
| Target Column | Data Type | Transformation Logic                           | Source Table | Source Column | Group By        |
|:--------------|:----------|:----------------------------------------------|:-------------|:--------------|:----------------|
| par_ym        | INT       | Direct Copy                                   | Intermediate: customer_data  | par_ym | None            |
| customer_id   | STRING    | Direct Copy                                   | Intermediate: customer_data  | customer_id | None          |
| hyper_category | STRING    | Direct Copy                                   | Intermediate: customer_data  | hyper_category | None          |
| perc_share    | DECIMAL(15,2) | ROUND((customer_amount/(SUM(customer_amount) over (partition by par_ym, customer_id))) *100, 2) | Intermediate: customer_data | customer_amount | None |

#### 3.2.4 interests Table
| Target Column | Data Type | Transformation Logic                           | Source Table | Source Column | Group By        |
|:--------------|:----------|:----------------------------------------------|:-------------|:--------------|:----------------|
| par_ym        | INT       | Direct Copy                                   | Intermediate: customer_sector_share | par_ym | None      |
| customer_id   | STRING    | Direct Copy                                   | Intermediate: customer_sector_share | customer_id | None    |
| shopping_interests | STRING | Direct Copy                                 | intermediate: customer_sector_share, bdprod.merchant_promotion_core.mcc_categories | sector | None |

## 4. Target Tables
### 4.1 Target Table Descriptions

| Target Table | Source Table                                   | Row Selection Logic                                                                                       |
|:-------------|:-----------------------------------------------|:----------------------------------------------------------------------------------------------------------|
| bdprod.merchant_promotion_analytical.cards_customer_characteristics | Intermediate: interests, bdprod.merchant_promotion_core.customer, bdprod.merchant_promotion_core.postal_codes (home) & bdprod.merchant_promotion_core.postal_codes (work) | LEFT JOIN on customer_id and postal fields. Including derived columns and direct copy of other fields. |

### 4.2 Target Table Logic 

| Target Column             | Data Type         | Transformation Logic                          | Source Table                                   | Source Column                      |
|:--------------------------|:------------------|:---------------------------------------------|:----------------------------------------------|:-----------------------------------|
| par_ym                    | INT                | Direct Copy                                  | Intermediate: interests                       | par_ym                             |
| customer_id               | STRING             | Direct Copy                                  | Intermediate: interests                       | customer_id                        |
| age                       | INT                | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | age                                |
| home_location             | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | home_location                      |
| home_municipality         | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.postal_codes  | municipality                       |
| home_regional_unit        | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.postal_codes  | regional_unit                      |
| home_region               | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.postal_codes  | region                              |
| work_location             | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | work_location                      |
| work_municipality         | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.postal_codes  | municipality                       |
| work_regional_unit        | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.postal_codes  | regional_unit                      |
| work_region               | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.postal_codes  | region                              |
| gender                    | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | gender                              |
| occupation                | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | occupation                          |
| annual_income             | DECIMAL(15,2)     | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | annual_income                       |
| nbg_segment               | STRING             | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | nbg_segment                        |
| sms_ind                   | BOOLEAN            | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | sms_ind                            |
| email_ind                 | BOOLEAN            | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | email_ind                          |
| addr_ind                  | BOOLEAN            | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | addr_ind                           |
| memberships               | ARRAY<STRING>      | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | memberships                        |
| ibank_ind                 | BOOLEAN            | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | ibank_ind                          |
| shopping_interests        | STRING             | Direct Copy                                  | Intermediate: interests                       | shopping_interests                 |
| age_range_cat             | INT                | Direct Copy                                  | bdprod.merchant_promotion_core.customer      | age_range_cat                      |