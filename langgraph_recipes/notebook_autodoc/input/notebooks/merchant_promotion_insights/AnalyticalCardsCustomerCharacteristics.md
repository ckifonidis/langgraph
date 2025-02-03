# Functional Specification Document

## 1. Input Parameters
The parameters used in the notebook are:
- `catalog`: The catalog/schema where the tables are located.
- `parym_range`: The condition range for the year-month values.
- `(pardt1, pardt2)`: The integer range for the start and end of the date period.
- `parym_end`: The end year-month value extracted from `pardt2`.

## 2. Source Tables
The following source tables are referenced in the query:

### bdprod.merchant_promotion_analytical.customer_sector_volumes

| Column Name       | Column Data Type      |
|:------------------|:----------------------|
| customer_id       | STRING                |
| sector            | STRING                |
| hyper_category     | STRING                |
| monthly_amount    | DECIMAL(15,2)        |
| frequency         | INT                   |
| gr_ind            | BOOLEAN               |
| total_wallet      | DECIMAL(15,2)        |
| ecommerce_ind     | BOOLEAN               |
| par_ym           | INT                   |

### bdprod.merchant_promotion_core.mcc_categories

| Column Name       | Column Data Type      |
|:------------------|:----------------------|
| mcc               | STRING                |
| category          | STRING                |
| description       | STRING                |
| hyper_category     | STRING                |
| sector            | STRING                |

### bdprod.merchant_promotion_core.customer

| Column Name       | Column Data Type      |
|:------------------|:----------------------|
| customer_id       | STRING                |
| age               | INT                   |
| home_location     | STRING                |
| work_location     | STRING                |
| gender            | STRING                |
| occupation        | STRING                |
| annual_income     | DECIMAL(15,2)        |
| nbg_segment       | STRING                |
| sms_ind           | BOOLEAN               |
| email_ind         | BOOLEAN               |
| addr_ind          | BOOLEAN               |
| memberships       | ARRAY<STRING>         |
| ibank_ind         | BOOLEAN               |
| age_range_cat     | INT                   |

### bdprod.merchant_promotion_core.postal_codes

| Column Name       | Column Data Type      |
|:------------------|:----------------------|
| postal            | STRING                |
| region            | STRING                |
| regional_unit     | STRING                |
| municipality      | STRING                |
| territory         | STRING                |

## 3. Intermediate Tables
The following intermediate tables (CTEs) are generated from the source tables:

### 1. all_comb

| Target Table   | Source Table   | Row Selection Logic                                                              |
|:---------------|:---------------|:---------------------------------------------------------------------------------|
| all_comb       | None           | Generates a sequence of year-month values over a specified date range using `explode`. |

| Target Column   | Data Type | Transformation Logic       | Source Table | Source Column |
|:----------------|:----------|:---------------------------|:-------------|:--------------|
| par_ym          | INT       | `cast (date_format(ts, 'yyyyMM') as int)` | None        | ts           |
| par_ym_year_before | INT    | `cast(date_format(ts - interval 12 months, 'yyyyMM') as int)` | None         | ts           |

### 2. customer_data

| Target Table   | Source Table   | Row Selection Logic                                    |
|:---------------|:---------------|:-----------------------------------------------------|
| customer_data  | bdprod.merchant_promotion_analytical.customer_sector_volumes | v.par_ym between ac.par_ym_year_before and ac.par_ym where v.par_ym between `date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 12 months, 'yyyyMM')` and `{parym_end}`. |

| Target Column   | Data Type   | Transformation Logic                 | Source Table                                         | Source Column       | Group By         |
|:----------------|:------------|:-------------------------------------|:----------------------------------------------------|:--------------------|:-----------------|
| par_ym          | INT         | Direct Copy                         | bdprod.merchant_promotion_analytical.customer_sector_volumes | par_ym             | ac.par_ym       |
| customer_id     | STRING      | Direct Copy                         | bdprod.merchant_promotion_analytical.customer_sector_volumes | customer_id        | ac.par_ym       |
| hyper_category   | STRING      | Direct Copy                         | bdprod.merchant_promotion_analytical.customer_sector_volumes | hyper_category     | ac.par_ym       |
| customer_amount | DECIMAL(15,2) | `SUM(monthly_amount)`              | bdprod.merchant_promotion_analytical.customer_sector_volumes | monthly_amount     | ac.par_ym, customer_id, hyper_category |

### 3. customer_sector_share

| Target Table   | Source Table   | Row Selection Logic                                   |
|:---------------|:---------------|:-----------------------------------------------------|
| customer_sector_share | customer_data  | No additional filtering here                          |

| Target Column | Data Type   | Transformation Logic                                      | Source Table             | Source Column       | Group By                          |
|:--------------|:-----------|:--------------------------------------------------------|:-------------------------|:--------------------|:----------------------------------|
| par_ym        | INT        | Direct Copy                                             | customer_data            | par_ym              | none                              |
| customer_id   | STRING     | Direct Copy                                             | customer_data            | customer_id         | none                              |
| hyper_category | STRING     | Direct Copy                                             | customer_data            | hyper_category      | none                              |
| perc_share    | DECIMAL(15,2) | `(customer_amount/(SUM(customer_amount) over (partition by par_ym, customer_id))) *100` | customer_data | customer_amount    | par_ym, customer_id              |

### 4. hypercategory_mapping

| Target Table   | Source Table   | Row Selection Logic                                   |
|:---------------|:---------------|:-----------------------------------------------------|
| hypercategory_mapping | bdprod.merchant_promotion_core.mcc_categories | `sector is not null` filter applied. |

| Target Column   | Data Type | Transformation Logic       | Source Table                                                  | Source Column       |
|:----------------|:----------|:---------------------------|:-------------------------------------------------------------|:--------------------|
| hyper_category   | STRING    | Direct Copy                | bdprod.merchant_promotion_core.mcc_categories                 | hyper_category       |
| sector          | STRING    | Direct Copy                | bdprod.merchant_promotion_core.mcc_categories                 | sector               |

### 5. interests

| Target Table   | Source Table   | Row Selection Logic                                   |
|:---------------|:---------------|:-----------------------------------------------------|
| interests      | customer_sector_share, hypercategory_mapping | `QUALIFY c.perc_share > (ROUND(AVG(perc_share) over (partition by c.par_ym, c.hyper_category), 2))` |

| Target Column   | Data Type | Transformation Logic                      | Source Table                            | Source Column           |
|:----------------|:----------|:------------------------------------------|:---------------------------------------|:------------------------|
| par_ym          | INT       | Direct Copy                               | customer_sector_share                  | par_ym                  |
| customer_id     | STRING    | Direct Copy                               | customer_sector_share                  | customer_id             |
| shopping_interests | STRING    | Direct Copy                               | hypercategory_mapping                  | sector                  |

## 4. Target Tables
The following target table is updated by the query:

### bdprod.merchant_promotion_analytical.cards_customer_characteristics

| Target Table                                       | Source Table                                   | Row Selection Logic                                         |
|:--------------------------------------------------|:-----------------------------------------------|:-----------------------------------------------------------|
| bdprod.merchant_promotion_analytical.cards_customer_characteristics | customer_sector_share, interests, bdprod.merchant_promotion_core.customer, bdprod.merchant_promotion_core.postal_codes | Data is inserted with `REPLACE WHERE {parym_range}` condition.  |

| Target Column        | Data Type       | Transformation Logic         | Source Table                                   | Source Column           |
|:---------------------|:----------------|:-----------------------------|:-----------------------------------------------|:------------------------|
| par_ym               | INT              | `{parym_end}`               | None                                           | None                    |
| customer_id          | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.customer       | customer_id             |
| age                  | INT              | Direct Copy                  | bdprod.merchant_promotion_core.customer       | age                     |
| home_location        | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.customer       | home_location           |
| home_municipality    | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.postal_codes   | municipality            |
| home_regional_unit   | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.postal_codes   | regional_unit           |
| home_region          | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.postal_codes   | region                  |
| work_location        | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.customer       | work_location           |
| work_municipality    | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.postal_codes   | municipality            |
| work_regional_unit   | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.postal_codes   | regional_unit           |
| work_region          | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.postal_codes   | region                  |
| gender               | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.customer       | gender                  |
| occupation           | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.customer       | occupation              |
| annual_income        | DECIMAL(15,2)   | Direct Copy                  | bdprod.merchant_promotion_core.customer       | annual_income           |
| nbg_segment          | STRING           | Direct Copy                  | bdprod.merchant_promotion_core.customer       | nbg_segment             |
| sms_ind              | BOOLEAN          | Direct Copy                  | bdprod.merchant_promotion_core.customer       | sms_ind                 |
| email_ind            | BOOLEAN          | Direct Copy                  | bdprod.merchant_promotion_core.customer       | email_ind               |
| addr_ind             | BOOLEAN          | Direct Copy                  | bdprod.merchant_promotion_core.customer       | addr_ind                |
| memberships          | ARRAY<STRING>    | Direct Copy                  | bdprod.merchant_promotion_core.customer       | memberships             |
| ibank_ind            | BOOLEAN          | Direct Copy                  | bdprod.merchant_promotion_core.customer       | ibank_ind               |
| shopping_interests   | STRING           | Direct Copy                  | interests                                     | shopping_interests      |
| age_range_cat        | INT              | Direct Copy                  | bdprod.merchant_promotion_core.customer       | age_range_cat           |