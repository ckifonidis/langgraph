# Functional Specification Document

## 1. Input Parameters
| Parameter Name                  | Description                                 |
|:--------------------------------|:--------------------------------------------|
| catalog                         | Catalog name for table references.         |
| parym_range                     | Condition for filtering based on parym.    |
| pardt_range                     | Condition for filtering based on pardt.    |
| merchant_user_id_condition      | Condition for filtering based on merchant user ID. |

## 2. Source Tables

### Source Tables Used in the Query

#### 1. bdprod.merchant_promotion_core.customer_daily_trns
| Column Name    | Column Data Type |
|:---------------|:-----------------|
| merchant_id    | STRING           |
| customer_id    | STRING           |
| par_dt         | INT              |

#### 2. bdprod.merchant_promotion_core.merchant
| Column Name          | Column Data Type |
|:---------------------|:-----------------|
| merchantid           | STRING           |
| merchant_user_id     | STRING           |
| merchant_mcc        | STRING           |
| country             | STRING           |

#### 3. bdprod.merchant_promotion_core.mcc_categories
| Column Name    | Column Data Type |
|:---------------|:-----------------|
| mcc            | STRING           |
| hyper_category  | STRING           |

#### 4. bdprod.merchant_promotion_analytical.cards_customer_characteristics
| Column Name       | Column Data Type |
|:------------------|:-----------------|
| par_ym            | INT              |
| customer_id       | STRING           |
| shopping_interests | STRING          |

#### 5. bdprod.merchant_promotion_engine.mpe_promotions
| Column Name     | Column Data Type |
|:----------------|:-----------------|
| promotion_id    | STRING           |
| startdate       | STRING           |
| enddate         | STRING           |

#### 6. bdprod.merchant_promotion_engine.mpe_promotion_customers
| Column Name     | Column Data Type |
|:----------------|:-----------------|
| promotion_id    | STRING           |
| customercode    | STRING           |
| merchant_id     | STRING           |

#### 7. bdprod.merchant_promotion_analytical.customer_sector_characteristics
| Column Name         | Column Data Type |
|:--------------------|:-----------------|
| par_ym              | INT              |
| customer_id         | STRING           |
| hyper_category       | STRING          |
| spending_profile     | STRING          |
| channel_preference   | STRING          |

## 3. Intermediate Tables

### Intermediate Tables Generated in the Query

#### 1. trns
| Target Table              | Source Table                                                          | Row Selection Logic                                            |
|:--------------------------|:---------------------------------------------------------------------|:--------------------------------------------------------------|
| trns                      | bdprod.merchant_promotion_core.customer_daily_trns                  | where {pardt_range} and country = 'GRC'                     |

#### 2. muser
| Target Table              | Source Table                                                          | Row Selection Logic                                            |
|:--------------------------|:---------------------------------------------------------------------|:--------------------------------------------------------------|
| muser                     | bdprod.merchant_promotion_core.merchant                              | left join bdprod.merchant_promotion_core.mcc_categories          |
|                          | bdprod.merchant_promotion_core.mcc_categories                       | where is_merchant_user = true and m.country = 'GRC' {merchant_user_id_condition} |

#### 3. c
| Target Table              | Source Table                                                          | Row Selection Logic                                            |
|:--------------------------|:---------------------------------------------------------------------|:--------------------------------------------------------------|
| c                         | bdprod.merchant_promotion_analytical.cards_customer_characteristics  | where {parym_range}                                          |

#### 4. promo
| Target Table              | Source Table                                                          | Row Selection Logic                                            |
|:--------------------------|:---------------------------------------------------------------------|:--------------------------------------------------------------|
| promo                     | bdprod.merchant_promotion_engine.mpe_promotions                     | join bdprod.merchant_promotion_engine.mpe_promotion_customers      | 
|                          | bdprod.merchant_promotion_engine.mpe_promotion_customers               | where 1=1 {parse_merchant_condition("merchant_id")}         |

#### 5. s
| Target Table              | Source Table                                                          | Row Selection Logic                                            |
|:--------------------------|:---------------------------------------------------------------------|:--------------------------------------------------------------|
| s                         | bdprod.merchant_promotion_analytical.customer_sector_characteristics  | where {parym_range}                                          |

## Insert/Update Logic for Intermediate Tables

### Intermediate Logic for Overall Output

| Target Column      | Data Type | Transformation Logic                                        | Source Table                                                          | Source Column                    | Group By |
|:-------------------|:----------|:-----------------------------------------------------------|:---------------------------------------------------------------------|:---------------------------------|:---------|
| promotion_id       | STRING    | Direct Copy                                               | promo                                                                | promotion_id                     |          |
| merchant_user_id    | STRING    | Direct Copy                                               | muser                                                                | merchant_user_id                 |          |
| customer_id        | STRING    | Direct Copy                                               | trns                                                                | customer_id                     |          |
| shopping_interests | STRING    | CASE WHEN c.shopping_interests IS NULL OR c.shopping_interests='' THEN 'other_category' ELSE c.shopping_interests END | c                                                                    | shopping_interests               |          |
| hyper_category     | STRING    | Direct Copy                                               | muser                                                                | hyper_category                   |          |
| spending_profile    | STRING    | Direct Copy                                               | s                                                                    | spending_profile                  |          |
| channel_preference   | STRING    | Direct Copy                                               | s                                                                    | channel_preference               |          |
| par_dt             | INT      | Direct Copy                                               | trns                                                                | par_dt                           |          |

## 4. Target Tables

### Target Table

#### 1. bdprod.merchant_promotion_analytical.merchant_insights_customer_interests

| Target Table                                             | Source Table                                             | Row Selection Logic                                            |
|:--------------------------------------------------------|:--------------------------------------------------------|:--------------------------------------------------------------|
| bdprod.merchant_promotion_analytical.merchant_insights_customer_interests | trns, muser, c, promo, s                               | REPLACE WHERE {pardt_range} {merchant_user_id_condition} |

### Insert/Update Logic

#### Columns in Target Table

| Target Column      | Data Type | Transformation Logic                                        | Source Table                                                          | Source Column                    |
|:-------------------|:----------|:-----------------------------------------------------------|:---------------------------------------------------------------------|:---------------------------------|
| promotion_id       | STRING    | Direct Copy                                               | promo                                                                | promotion_id                     |
| merchant_user_id    | STRING    | Direct Copy                                               | muser                                                                | merchant_user_id                 |
| customer_id        | STRING    | Direct Copy                                               | trns                                                                | customer_id                     |
| shopping_interests | STRING    | CASE WHEN c.shopping_interests IS NULL OR c.shopping_interests='' THEN 'other_category' ELSE c.shopping_interests END | c                                                                    | shopping_interests               |
| hyper_category     | STRING    | Direct Copy                                               | muser                                                                | hyper_category                   |
| spending_profile    | STRING    | Direct Copy                                               | s                                                                    | spending_profile                  |
| channel_preference   | STRING    | Direct Copy                                               | s                                                                    | channel_preference               |
| par_dt             | INT      | Direct Copy                                               | trns                                                                | par_dt                           |