# Functional Specification Document

## 1. Input Parameters
| Parameter Name                | Description                                                         |
|-------------------------------|---------------------------------------------------------------------|
| catalog                       | The catalog name where the tables are located.                     |
| pardt_conditions              | Conditions for filtering by partition date.                        |
| year                          | The year for which competitors are being analyzed.                 |
| merchant_user_id_condition    | The condition related to the merchant user ID for filtering.       |

## 2. Source Tables
### Source Tables and their Descriptions
#### 2.1 `bdprod.merchant_promotion_core.merchant`
| Column Name         | Column Data Type |
|:--------------------|:-----------------|
| merchant_user_id    | STRING           |
| retailer_info_id    | STRING           |
| merchant_mcc        | STRING           |
| merchant_name       | STRING           |
| is_merchant_user     | BOOLEAN          |

#### 2.2 `bdprod.merchant_promotion_core.retailer_info`
| Column Name         | Column Data Type |
|:--------------------|:-----------------|
| uuid                | STRING           |

#### 2.3 `bdprod.merchant_promotion_analytical.mcc_top_competitors`
| Column Name         | Column Data Type |
|:--------------------|:-----------------|
| mcc                 | STRING           |
| top_rank            | INT              |
| brand               | STRING           |
| year                | INT              |

#### 2.4 `bdprod.merchant_promotion_core.merchant_competitors`
| Column Name         | Column Data Type |
|:--------------------|:-----------------|
| merchant_user_id    | STRING           |
| brand               | STRING           |

#### 2.5 `bdprod.merchant_promotion_analytical.merchant_all_statistics`
| Column Name         | Column Data Type |
|:--------------------|:-----------------|
| par_dt              | STRING           |
| mcc                 | STRING           |
| retailer            | STRING           |
| merchant_id         | STRING           |
| customer_id         | STRING           |
| gender              | STRING           |
| home_location       | STRING           |
| home_municipality   | STRING           |
| home_regional_unit  | STRING           |
| home_region         | STRING           |
| work_location       | STRING           |
| work_municipality   | STRING           |
| work_regional_unit  | STRING           |
| work_region         | STRING           |
| age_group           | STRING           |
| age                 | INT              |
| occupation          | STRING           |
| nbg_segment         | STRING           |
| shopping_interests  | STRING           |
| ecommerce_ind       | BOOLEAN          |
| card_type           | STRING           |
| number_of_transactions | INT           |
| amount              | DECIMAL(38,2)    |

#### 2.6 `bdprod.merchant_promotion_analytical.merchant_all_statistics_interests`
| Column Name         | Column Data Type |
|:--------------------|:-----------------|
| par_dt              | STRING           |
| merchant_id         | STRING           |
| customer_id         | STRING           |
| spending_profile     | STRING          |
| channel_preference   | STRING          |

## 3. Intermediate Tables
### 3.1 `merchant_data`
#### Source Tables and their Row Selection Logic
| Target Table | Source Table                                                           | Row Selection Logic                                        |
|:-------------|:---------------------------------------------------------------|:--------------------------------------------------------|
| merchant_data | `bdprod.merchant_promotion_core.merchant`                      | where is_merchant_user = True and {merchant_user_id_condition} |
|              | `bdprod.merchant_promotion_core.retailer_info`                   | Inner join on retailer_info_id with uuid in merchant   |

#### Insert/Update Logic
| Target Column              | Data Type | Transformation Logic                     | Source Table                                          | Source Column           | Group By |
|:---------------------------|:----------|:----------------------------------------|:-----------------------------------------------------|:------------------------|:---------|
| merchant_user_id           | STRING    | Direct Copy                             | `bdprod.merchant_promotion_core.merchant`           | merchant_user_id        |          |
| retailer                   | STRING    | Direct Copy                             | `bdprod.merchant_promotion_core.merchant`           | retailer                |          |
| mcc                        | STRING    | Direct Copy                             | `bdprod.merchant_promotion_core.merchant`           | merchant_mcc            |          |
| merchant_name              | STRING    | Case Statement for name mapping        | `bdprod.merchant_promotion_core.merchant`           | merchant_name           |          |

### 3.2 `mcc_top_comp`
#### Source Tables and their Row Selection Logic
| Target Table  | Source Table                               | Row Selection Logic                 |
|:--------------|:-------------------------------------------|:------------------------------------|
| mcc_top_comp  | `bdprod.merchant_promotion_analytical.mcc_top_competitors` | where year = {year}               |

### 3.3 `total_competitors`
#### Source Tables and their Row Selection Logic
| Target Table  | Source Table                               | Row Selection Logic                  |
|:--------------|:-------------------------------------------|:-------------------------------------|
| total_competitors | `mcc_top_comp`                           | where mcc in (select mcc from merchant_data) |

#### Insert/Update Logic
| Target Column | Data Type | Transformation Logic | Source Table  | Source Column | Group By |
|:--------------|:----------|:--------------------|:--------------|:--------------|:---------|
| mcc           | STRING    | Direct Copy         | `total_competitors`  | mcc          |          |
| tc            | INT       | Count top_rank      | `total_competitors`  | top_rank     | mcc      |

### 3.4 `merchants_position`
#### Source Tables and their Row Selection Logic
| Target Table     | Source Table    | Row Selection Logic                                   |
|:------------------|:----------------|:----------------------------------------------------|
| merchants_position | `mcc_top_comp` | Inner join on total_competitors, merchant_data      |

#### Insert/Update Logic
| Target Column       | Data Type | Transformation Logic                         | Source Table          | Source Column    | Group By |
|:--------------------|:----------|:----------------------------------------------|:----------------------|:------------------|:---------|
| mcc                  | STRING    | Direct Copy                                   | `merchants_position`  | mcc              |          |
| retailer             | STRING    | Direct Copy                                   | `merchants_position`  | brand            |          |
| place                | STRING    | Case Statement for rank classification        | `merchants_position`  | top_rank         |          |
| position_rank        | INT       | Direct Copy                                   | `merchants_position`  | top_rank         |          |

### 3.5 `competitors_from_mcc`
#### Source Tables and their Row Selection Logic
| Target Table           | Source Table               | Row Selection Logic                                       |
|:-----------------------|:---------------------------|:--------------------------------------------------------|
| competitors_from_mcc   | `merchant_data`           | Inner join with `merchants_position` on mcc and retailer |
|                       | `mcc_top_comp`           | Matching mcc, filtering by rank                         |

### 3.6 `competitors`
#### Source Tables and their Row Selection Logic
| Target Table  | Source Table                                         | Row Selection Logic                                     |
|:--------------|:-----------------------------------------------------|:------------------------------------------------------|
| competitors    | `bdprod.merchant_promotion_core.merchant_competitors` | Direct selection for involved merchants                |
|              | `competitors_from_mcc`                           | only including those not in merchant competitors        |

### 3.7 `competitors_with_mcc`
#### Source Tables and their Row Selection Logic
| Target Table         | Source Table   | Row Selection Logic                                |
|:---------------------|:---------------|:--------------------------------------------------|
| competitors_with_mcc | `competitors`  | Inner join on `merchant_data`                     |

### 3.8 `competitor_stores`
#### Source Tables and their Row Selection Logic
| Target Table     | Source Table     | Row Selection Logic                                     |
|:------------------|:----------------|:------------------------------------------------------|
| competitor_stores  | `competitors_with_mcc` | Inner join with `merchant_all_statistics`           |

## 4. Target Tables
### Target Table and Row Selection Logic
| Target Table                                   | Source Table                                      | Row Selection Logic                                        |
|:-----------------------------------------------|:--------------------------------------------------|:----------------------------------------------------------|
| `bdprod.merchant_promotion_insights.statistics_raw` | `competitor_stores` and `merchant_all_statistics` | Inserted with relevant columns and aggregate information   |

### Target Table Columns and their Insert/Update Logic
| Target Column                | Data Type        | Transformation Logic                  | Source Table                                   | Source Column          |
|:-----------------------------|:-----------------|:-------------------------------------|:-----------------------------------------------|:-----------------------|
| par_dt                       | STRING           | ifnull(mas.par_dt, masi.par_dt)    | `merchant_all_statistics`                    | par_dt                 |
| merchant_user_id             | STRING           | Direct Copy                          | `merchant_all_statistics`                    | merchant_user_id       |
| statistics_type              | STRING           | "competition"                       | Direct Copy                                  | N/A                    |
| customer_id                  | STRING           | ifnull(mas.customer_id, masi.customer_id) | `merchant_all_statistics`               | customer_id            |
| merchant_id                  | STRING           | cast(mas.total as string)           | `merchant_all_statistics`                    | merchant_id            |
| gender                       | STRING           | Direct Copy                          | `merchant_all_statistics`                    | gender                 |
| home_location                | STRING           | Direct Copy                          | `merchant_all_statistics`                    | home_location          |
| home_municipality            | STRING           | Direct Copy                          | `merchant_all_statistics`                    | home_municipality      |
| home_regional_unit           | STRING           | Direct Copy                          | `merchant_all_statistics`                    | home_regional_unit     |
| home_region                  | STRING           | Direct Copy                          | `merchant_all_statistics`                    | home_region            |
| work_location                | STRING           | Direct Copy                          | `merchant_all_statistics`                    | work_location          |
| work_municipality            | STRING           | Direct Copy                          | `merchant_all_statistics`                    | work_municipality      |
| work_regional_unit           | STRING           | Direct Copy                          | `merchant_all_statistics`                    | work_regional_unit     |
| work_region                  | STRING           | Direct Copy                          | `merchant_all_statistics`                    | work_region            |
| age_group                    | STRING           | Direct Copy                          | `merchant_all_statistics`                    | age_group              |
| age                          | INT              | Direct Copy                          | `merchant_all_statistics`                    | age                    |
| occupation                  | STRING           | Direct Copy                          | `merchant_all_statistics`                    | occupation             |
| nbg_segment                  | STRING           | Direct Copy                          | `merchant_all_statistics`                    | nbg_segment            |
| promotion_id                 | STRING           | cast(null as string)                 | N/A                                           | N/A                    |
| shopping_interests           | STRING           | Direct Copy                          | `merchant_all_statistics`                    | shopping_interests     |
| activity                     | STRING           | cast(null as string)                 | N/A                                           | N/A                    |
| spending_profile             | STRING           | Direct Copy                          | `merchant_all_statistics_interests`          | spending_profile       |
| channel_preference           | STRING           | Direct Copy                          | `merchant_all_statistics_interests`          | channel_preference     |
| ecommerce_ind                | BOOLEAN          | Direct Copy                          | `merchant_all_statistics`                    | ecommerce_ind          |
| card_type                    | STRING           | Direct Copy                          | `merchant_all_statistics`                    | card_type              |
| sms_ind                      | BOOLEAN          | cast(null as boolean)                | N/A                                           | N/A                    |
| go4more_ind                 | BOOLEAN          | cast(null as boolean)                | N/A                                           | N/A                    |
| ibank_ind                    | BOOLEAN          | cast(null as boolean)                | N/A                                           | N/A                    |
| number_of_transactions       | INT              | sum(mas.number_of_transactions)      | `merchant_all_statistics`                    | number_of_transactions |
| amount                       | DECIMAL(38,2)   | sum(cast(mas.amount as decimal(38,2))) | `merchant_all_statistics`                    | amount                 |
| rewarded_points              | BIGINT          | cast(null as bigint)                  | N/A                                           | N/A                    |
| redeemed_points              | BIGINT          | cast(null as bigint)                 | N/A                                           | N/A                    |
| rewarded_amount              | DECIMAL(38,2)  | cast(null as decimal(38,2))          | N/A                                           | N/A                    |
| redeemed_amount              | DECIMAL(38,2)  | cast(null as decimal(38,2))          | N/A                                           | N/A                    |
