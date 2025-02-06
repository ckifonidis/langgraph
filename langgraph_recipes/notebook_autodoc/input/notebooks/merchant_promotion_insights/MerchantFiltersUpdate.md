# Functional Specification Document

## 1. Input Parameters
- `catalog`: The catalog database selector for the tables used in the query.

## 2. Source Tables
The following source tables are used in the query:

### 2.1 Table: bdprod.merchant_promotion_core.postal_codes
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| postal                | STRING           |
| region                | STRING           |
| regional_unit         | STRING           |
| municipality          | STRING           |
| territory             | STRING           |

### 2.2 Table: bdprod.merchant_promotion_engine.mpe_merchant_stores
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| store_id              | STRING           |
| merchant_id           | STRING           |
| status                | STRING           |
| name                  | STRING           |

### 2.3 Table: bdprod.merchant_promotion_engine.mpe_merchants
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| merchant_id           | STRING           |
| internal_code         | STRING           |

### 2.4 Table: bdprod.merchant_promotion_engine.mpe_promotions
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| promotion_id          | STRING           |
| name                  | STRING           |
| startdate             | DATE             |
| status                | STRING           |

### 2.5 Table: bdprod.merchant_promotion_core.mcc_categories
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| mcc                   | STRING           |
| category              | STRING           |
| description           | STRING           |
| hyper_category        | STRING           |
| sector                | STRING           |

## 3. Intermediate Tables
No explicit CTEs are created in the query; rather, multiple `UNION ALL` statements are used to generate a single result set.

## 4. Target Tables
The following target table is updated/generated from the query:

### 4.1 Target Table: bdprod.merchant_promotion_insights.merchant_filters
#### Insert/Update Logic

| Target Table                                      | Source Table                                       | Row Selection Logic                                      |
|:--------------------------------------------------|:---------------------------------------------------|:--------------------------------------------------------|
| bdprod.merchant_promotion_insights.merchant_filters | bdprod.merchant_promotion_core.postal_codes       | distinct values selected from postal_codes.             |
| bdprod.merchant_promotion_insights.merchant_filters | bdprod.merchant_promotion_engine.mpe_merchant_stores | distinct values selected with status='Active'.          |
| bdprod.merchant_promotion_insights.merchant_filters | bdprod.merchant_promotion_engine.mpe_promotions   | distinct values selected with status='Completed'.       |
| bdprod.merchant_promotion_insights.merchant_filters | bdprod.merchant_promotion_core.mcc_categories      | distinct values from mcc_categories.                    |

#### Target Column Transformation Logic

| Target Column                 | Data Type | Transformation Logic                  | Source Table                                       | Source Column                 |
|:------------------------------|:----------|:-------------------------------------|:---------------------------------------------------|:------------------------------|
| filter_id                     | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.postal_codes       | location_tree                 |
| filter_value_id               | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.postal_codes       | municipality                  |
| filter_category_description    | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.postal_codes       | "home or work location"       |
| filter_value_description       | STRING    | case when municipality='ΝΕΟ ΗΡΑΚΛΕΙΟ' then 'ΗΡΑΚΛΕΙΟ' else municipality end | bdprod.merchant_promotion_core.postal_codes       | municipality                  |
| parent_filter_id              | STRING    | concat('ΠΕ ',regional_unit)         | bdprod.merchant_promotion_core.postal_codes       | regional_unit                 |
| greek_ind                     | BOOLEAN   | true                                 | N/A                                                | N/A                           |
| merchant_user_id              | STRING    | null                                 | N/A                                                | N/A                           |
| c_timestamp                   | TIMESTAMP | now()                                | N/A                                                | N/A                           |
| filter_id                     | STRING    | Direct Copy                          | bdprod.merchant_promotion_engine.mpe_merchant_stores | "store"                       |
| filter_value_id               | STRING    | Direct Copy                          | bdprod.merchant_promotion_engine.mpe_merchant_stores | ms.store_id                   |
| filter_category_description    | STRING    | Direct Copy                          | bdprod.merchant_promotion_engine.mpe_merchant_stores | "stores"                      |
| filter_value_description       | STRING    | trim(ms.name)                       | bdprod.merchant_promotion_engine.mpe_merchant_stores | name                          |
| filter_value_id               | STRING    | Direct Copy                          | bdprod.merchant_promotion_engine.mpe_promotions   | promotion_id                  |
| filter_value_description       | STRING    | concat(name,',',startdate)         | bdprod.merchant_promotion_engine.mpe_promotions   | name, startdate              |
| filter_value_id               | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.mcc_categories      | sector                        |
| filter_value_description       | STRING    | case when hyper_category = 'Automotive & Fuel Products' then 'Αυτοκίνητο & Καύσιμα' ... | bdprod.merchant_promotion_core.mcc_categories      | hyper_category                |
| filter_value_id               | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.postal_codes       | municipality                  |
| filter_value_description       | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.postal_codes       | municipality                  |
| filter_value_id               | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.postal_codes       | regional_unit                 |
| filter_value_description       | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.postal_codes       | regional_unit                 |
| filter_value_id               | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.postal_codes       | region                        |
| filter_value_description       | STRING    | Direct Copy                          | bdprod.merchant_promotion_core.postal_codes       | region                        |

```
