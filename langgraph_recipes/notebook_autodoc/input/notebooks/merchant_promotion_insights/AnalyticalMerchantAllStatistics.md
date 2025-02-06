# Functional Specification Document

## 1. Input Parameters

| Parameter Name | Description |
|:---------------|:------------|
| catalog        | The catalog where target tables are located. |
| parym_range    | Date range condition based on year and month for filtering customer characteristics. |
| pardt_range    | Date range condition for filtering transaction data. |

## 2. Source Tables

### 2.1. Source Table: `customer_daily_trns`

| Column Name              | Column Data Type |
|:-------------------------|:-----------------|
| merchant_id              | STRING           |
| customer_id              | STRING           |
| number_of_transactions    | INT              |
| amount                   | DECIMAL(15,2)   |
| ecommerce_ind            | BOOLEAN          |
| country                  | STRING           |
| payment_type             | STRING           |
| par_dt                   | INT              |

### 2.2. Source Table: `cards_customer_characteristics`

| Column Name              | Column Data Type |
|:-------------------------|:-----------------|
| par_ym                   | INT              |
| customer_id              | STRING           |
| age                      | INT              |
| home_location            | STRING           |
| home_municipality        | STRING           |
| home_regional_unit       | STRING           |
| home_region              | STRING           |
| work_location            | STRING           |
| work_municipality        | STRING           |
| work_regional_unit       | STRING           |
| work_region              | STRING           |
| gender                   | STRING           |
| occupation               | STRING           |
| annual_income            | DECIMAL(15,2)   |
| nbg_segment              | STRING           |
| shopping_interests       | STRING           |

### 2.3. Source Table: `merchant`

| Column Name              | Column Data Type |
|:-------------------------|:-----------------|
| merchantid               | STRING           |
| retailer                 | STRING           |
| brand                    | STRING           |
| mcc                      | STRING           |

### 2.4. Source Table: `retailer_info`

| Column Name              | Column Data Type |
|:-------------------------|:-----------------|
| uuid                     | STRING           |
| retailer                 | STRING           |
| brand                    | STRING           |

## 3. Intermediate Tables

### 3.1. Intermediate Table: `trns`

| Target Table | Source Table                                     | Row Selection Logic          |
|:-------------|:------------------------------------------------|:------------------------------|
| trns         | bdprod.merchant_promotion_core.customer_daily_trns | WHERE {pardt_range}          |

| Target Column          | Data Type     | Transformation Logic | Source Table                                | Source Column                     |
|:-----------------------|:--------------|:---------------------|:-------------------------------------------|:----------------------------------|
| merchant_id            | STRING        | Direct Copy          | bdprod.merchant_promotion_core.customer_daily_trns | merchant_id                      |
| customer_id            | STRING        | Direct Copy          | bdprod.merchant_promotion_core.customer_daily_trns | customer_id                      |
| number_of_transactions  | INT           | Direct Copy          | bdprod.merchant_promotion_core.customer_daily_trns | number_of_transactions            |
| amount                 | DECIMAL(15,2) | Direct Copy          | bdprod.merchant_promotion_core.customer_daily_trns | amount                           |
| ecommerce_ind          | BOOLEAN       | Direct Copy          | bdprod.merchant_promotion_core.customer_daily_trns | ecommerce_ind                    |
| country                | STRING        | Direct Copy          | bdprod.merchant_promotion_core.customer_daily_trns | country                          |
| payment_type           | STRING        | Direct Copy          | bdprod.merchant_promotion_core.customer_daily_trns | payment_type                     |
| par_dt                 | INT           | Direct Copy          | bdprod.merchant_promotion_core.customer_daily_trns | par_dt                           |

### 3.2. Intermediate Table: `cust`

| Target Table | Source Table                                     | Row Selection Logic          |
|:-------------|:------------------------------------------------|:------------------------------|
| cust         | bdprod.merchant_promotion_analytical.cards_customer_characteristics | WHERE {parym_range}          |

| Target Column          | Data Type     | Transformation Logic | Source Table                                   | Source Column                     | Group By            |
|:-----------------------|:--------------|:---------------------|:----------------------------------------------|:----------------------------------|:--------------------|
| par_ym                 | INT           | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics| par_ym                           | group by par_ym     |
| customer_id            | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | customer_id                      | group by customer_id |
| age                    | INT           | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | age                              | group by age        |
| home_location          | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | home_location                    | group by home_location|
| home_municipality      | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | home_municipality                | group by home_municipality|
| home_regional_unit     | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | home_regional_unit               | group by home_regional_unit|
| home_region            | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | home_region                      | group by home_region |
| work_location          | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | work_location                    | group by work_location|
| work_municipality      | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | work_municipality                | group by work_municipality|
| work_regional_unit     | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | work_regional_unit               | group by work_regional_unit|
| work_region            | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | work_region                      | group by work_region |
| gender                 | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | gender                           | group by gender      |
| occupation             | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | occupation                       | group by occupation  |
| annual_income          | DECIMAL(15,2) | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | annual_income                    | group by annual_income|
| nbg_segment            | STRING        | Direct Copy          | bdprod.merchant_promotion_analytical.cards_customer_characteristics | nbg_segment                      | group by nbg_segment |
| shopping_interests     | STRING        | array_join(collect_list(shopping_interests), ',') | bdprod.merchant_promotion_analytical.cards_customer_characteristics | shopping_interests                | group by customer_id  |

### 3.3. Intermediate Table: `md`

| Target Table | Source Table                                     | Row Selection Logic          |
|:-------------|:------------------------------------------------|:------------------------------|
| md           | bdprod.merchant_promotion_core.merchant and bdprod.merchant_promotion_core.retailer_info | left join for unique merchants|

| Target Column          | Data Type     | Transformation Logic | Source Table                               | Source Column           |
|:-----------------------|:--------------|:---------------------|:------------------------------------------|:------------------------|
| mid                    | STRING        | Direct Copy          | bdprod.merchant_promotion_core.merchant   | merchantid              |
| retailer                | STRING        | Direct Copy          | bdprod.merchant_promotion_core.retailer_info | retailer                |
| brand                   | STRING        | Direct Copy          | bdprod.merchant_promotion_core.retailer_info | brand                   |
| mcc                     | STRING        | Direct Copy          | bdprod.merchant_promotion_core.merchant   | mcc                     |

## 4. Target Tables

### Target Table: `merchant_all_statistics`

| Target Table                             | Source Table | Row Selection Logic              |
|:------------------------------------------|:-------------|:----------------------------------|
| bdprod.merchant_promotion_analytical.merchant_all_statistics | Combined result of the intermediate tables: `trns`, `cust`, and `md` | INSERT statement using the result of the main query |

| Target Column          | Data Type      | Transformation Logic | Source Table                                  | Source Column              |
|:-----------------------|:---------------|:---------------------|:---------------------------------------------|:---------------------------|
| merchant_id            | STRING         | Direct Copy          | combined from trns and md                   | merchant_id                |
| customer_id            | STRING         | Direct Copy          | result from the join between trns and cust  | customer_id                |
| number_of_transactions  | INT            | Direct Copy          | trns                                       | number_of_transactions    |
| amount                 | DECIMAL(15,2)  | Direct Copy          | trns                                       | amount                     |
| mcc                    | STRING         | Direct Copy          | md                                          | mcc                        |
| ecommerce_ind          | BOOLEAN        | Direct Copy          | trns                                       | ecommerce_ind              |
| country                | STRING         | Direct Copy          | trns                                       | country                    |
| card_type              | STRING         | Direct Copy          | trns                                       | payment_type               |
| age                    | INT             | Direct Copy          | cust                                       | age                        |
| age_group              | STRING         | CASE statement        | Direct computation                          | CASE on age                |
| home_location          | STRING         | Direct Copy          | cust                                       | home_location              |
| home_municipality      | STRING         | Direct Copy          | cust                                       | home_municipality          |
| home_regional_unit     | STRING         | Direct Copy          | cust                                       | home_regional_unit         |
| home_region            | STRING         | Direct Copy          | cust                                       | home_region                |
| work_location          | STRING         | Direct Copy          | cust                                       | work_location              |
| work_municipality      | STRING         | Direct Copy          | cust                                       | work_municipality          |
| work_regional_unit     | STRING         | Direct Copy          | cust                                       | work_regional_unit         |
| work_region            | STRING         | Direct Copy          | cust                                       | work_region                |
| gender                 | STRING         | Direct Copy          | cust                                       | gender                     |
| occupation             | STRING         | Direct Copy          | cust                                       | occupation                 |
| annual_income          | DECIMAL(15,2)  | Direct Copy          | cust                                       | annual_income              |
| nbg_segment            | STRING         | Direct Copy          | cust                                       | nbg_segment                |
| shopping_interests     | STRING         | IF condition          | cust                                       | shopping_interests         |
| retailer                | STRING         | Direct Copy          | md                                         | retailer                   |
| brand                   | STRING         | Direct Copy          | md                                         | brand                      |
| par_dt                 | INT             | Direct Copy          | trns                                       | par_dt                     |