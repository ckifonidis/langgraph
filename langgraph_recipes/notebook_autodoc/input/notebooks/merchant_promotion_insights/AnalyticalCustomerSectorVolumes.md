# Functional Specification Document

## 1. Input Parameters
- `catalog`: Retrieved from the function `get_catalog()`.
- `parym_range`: Retrieved from the function `get_parym_range_condition()`.
- `pardt_range`: Retrieved from the function `get_monthly_pardt_range_condition()`.

## 2. Source Tables 
The following source tables are utilized in the query:

### Source Table Descriptions

#### 1. `bdprod.merchant_promotion_core.customer_daily_trns`
| Column Name           | Column Data Type |
|:----------------------|:-----------------|
| customer_id           | STRING           |
| merchant_id           | STRING           |
| amount                | DECIMAL(15,2)    |
| number_of_transactions | INT              |
| par_dt                | INT              |

#### 2. `bdprod.merchant_promotion_core.merchant`
| Column Name       | Column Data Type |
|:------------------|:-----------------|
| merchantid        | STRING           |
| mcc               | STRING           |

#### 3. `bdprod.merchant_promotion_core.mcc_categories`
| Column Name | Column Data Type |
|:------------|:-----------------|
| mcc        | STRING           |
| category   | STRING           |
| description| STRING           |
| hyper_category| STRING        |
| sector     | STRING           |

### Source Table Details
- `bdprod.merchant_promotion_core.customer_daily_trns`
- `bdprod.merchant_promotion_core.merchant`
- `bdprod.merchant_promotion_core.mcc_categories`

## 3. Intermediate Tables

The query utilizes the intermediate results generated through common table expressions (CTEs).

### CTE: Derived table within the main query
| Target Table | Source Table                                  | Row Selection Logic          |
|:-------------|:----------------------------------------------|:-----------------------------|
| Derived CTE  | `bdprod.merchant_promotion_core.customer_daily_trns` | WHERE {pardt_range}          |
|              | `bdprod.merchant_promotion_core.merchant`                 | JOIN on m.merchantid = dt.merchant_id |

### Column Transformations
| Target Column       | Data Type      | Transformation Logic                          | Source Table                                                    | Source Column        | Group By                      |
|:--------------------|:---------------|:----------------------------------------------|:--------------------------------------------------------------|:---------------------|:------------------------------|
| customer_id         | STRING         | Direct Copy                                   | `bdprod.merchant_promotion_core.customer_daily_trns`         | customer_id          | customer_id                   |
| sector              | STRING         | manual_category                               | `bdprod.merchant_promotion_core.mcc_categories`              | category             | customer_id                   |
| hyper_category      | STRING         | hyper_category                                | `bdprod.merchant_promotion_core.mcc_categories`              | hyper_category       | customer_id                   |
| monthly_amount      | DECIMAL(15,2)  | SUM(amount)                                   | `bdprod.merchant_promotion_core.customer_daily_trns`         | amount               | customer_id                   |
| frequency           | INT            | SUM(number_of_transactions)                   | `bdprod.merchant_promotion_core.customer_daily_trns`         | number_of_transactions | customer_id                   |
| gr_ind              | BOOLEAN        | COALESCE(country = 'GRC', False)            | `bdprod.merchant_promotion_core.customer_daily_trns`         | country              | customer_id                   |
| ecommerce_ind       | BOOLEAN        | Direct Copy                                   | `bdprod.merchant_promotion_core.customer_daily_trns`         | ecommerce_ind        | customer_id                   |
| par_ym             | INT            | FLOOR(par_dt / 100)                          | `bdprod.merchant_promotion_core.customer_daily_trns`         | par_dt               | customer_id                   |

## 4. Target Tables

The target table that the derived results will be inserted into:

### Target Table Description
#### `bdprod.merchant_promotion_analytical.customer_sector_volumes`
| Column Name        | Column Data Type |
|:-------------------|:-----------------|
| customer_id        | STRING           |
| sector             | STRING           |
| hyper_category     | STRING           |
| monthly_amount     | DECIMAL(15,2)    |
| frequency          | INT              |
| gr_ind             | BOOLEAN          |
| total_wallet       | DECIMAL(15,2)    |
| ecommerce_ind      | BOOLEAN          |
| par_ym             | INT              |

### Target Table Insert Logic
| Target Table                                       | Source Table                                      | Row Selection Logic          |
|:--------------------------------------------------|:--------------------------------------------------|:-----------------------------|
| `bdprod.merchant_promotion_analytical.customer_sector_volumes` | Derived CTE                                       | Directly from derived results |

### Target Column Transformations
| Target Column      | Data Type      | Transformation Logic                          | Source Table                                                    | Source Column        |
|:-------------------|:---------------|:----------------------------------------------|:--------------------------------------------------------------|:---------------------|
| customer_id        | STRING         | Direct Copy                                   | Derived CTE                                                   | customer_id          |
| sector             | STRING         | Direct Copy                                   | Derived CTE                                                   | sector               |
| hyper_category     | STRING         | Direct Copy                                   | Derived CTE                                                   | hyper_category       |
| monthly_amount     | DECIMAL(15,2)  | Direct Copy                                   | Derived CTE                                                   | monthly_amount       |
| frequency          | INT            | Direct Copy                                   | Derived CTE                                                   | frequency            |
| gr_ind             | BOOLEAN        | Direct Copy                                   | Derived CTE                                                   | gr_ind               |
| total_wallet       | DECIMAL(15,2)  | Direct Copy                                   | Derived CTE                                                   | total_wallet         |
| ecommerce_ind      | BOOLEAN        | Direct Copy                                   | Derived CTE                                                   | ecommerce_ind        |
| par_ym             | INT            | Direct Copy                                   | Derived CTE                                                   | par_ym               |

This concludes the functional specification document based on the provided notebook and the associated table descriptions.