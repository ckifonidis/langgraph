# Functional Specification Document

## 1. Input Parameters
| Parameter Name                        | Description                                      |
|:-------------------------------------|:------------------------------------------------|
| catalog                               | The catalog name used for the tables.          |
| partition_conditions                  | Conditions for partition selection.              |
| merchant_user_id_condition            | Condition related to merchant user ID.          |

## 2. Source Tables
### 2.1. Source Tables Documentation
| Column Name                          | Column Data Type       |
|:-------------------------------------|:-----------------------|
| par_dt                               | DATE                   |
| merchant_user_id                     | STRING                 |
| customer_id                          | STRING                 |
| merchant_id                          | STRING                 |
| gender                               | STRING                 |
| home_location                        | STRING                 |
| home_municipality                    | STRING                 |
| home_regional_unit                   | STRING                 |
| home_region                          | STRING                 |
| work_location                        | STRING                 |
| work_municipality                    | STRING                 |
| work_regional_unit                   | STRING                 |
| work_region                          | STRING                 |
| age_group                            | STRING                 |
| age                                  | INT                    |
| occupation                           | STRING                 |
| nbg_segment                          | STRING                 |
| promotion_id                         | STRING                 |
| shopping_interests                   | STRING                 |
| activity                             | STRING                 |
| spending_profile                     | STRING                 |
| channel_preference                   | STRING                 |
| ecommerce_ind                        | BOOLEAN                |
| card_type                            | STRING                 |
| sms_ind                              | BOOLEAN                |
| go4more_ind                          | BOOLEAN                |
| ibank_ind                            | BOOLEAN                |
| number_of_transactions               | INT                    |
| amount                               | DECIMAL(38,2)         |
| rewarded_points                      | INT                    |
| redeemed_points                      | INT                    |
| rewarded_amount                      | DECIMAL(38,2)         |
| redeemed_amount                      | DECIMAL(38,2)         |
| -------------------------------------|------------------------|
| Source Tables                       |                         |
| bdprod.merchant_promotion_analytical.merchant_insights_customer         |                         |
| bdprod.merchant_promotion_analytical.merchant_insights_customer_interests |                         |

## 3. Intermediate Tables
### 3.1. Intermediate Tables Documentation
| Target Table                         | Source Table                                                 | Row Selection Logic                                            |
|:-------------------------------------|:------------------------------------------------------------|:-------------------------------------------------------------|
| Temporary result set                 | bdprod.merchant_promotion_analytical.merchant_insights_customer        | {partition_conditions} {merchant_user_id_condition}         |
| Temporary result set                 | bdprod.merchant_promotion_analytical.merchant_insights_customer_interests | {partition_conditions} {merchant_user_id_condition}         |

### 3.2. Data Transformation Logic
#### For Intermediate Tables
| Target Column                         | Data Type             | Transformation Logic | Source Table                                                 | Source Column                                           | Group By             |
|:-------------------------------------|:---------------------|:---------------------|:------------------------------------------------------------|:------------------------------------------------------|:---------------------|
| par_dt                               | DATE                 | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | par_dt                                                 |                       |
| merchant_user_id                     | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | merchant_user_id                                       |                       |
| statistics_type                      | STRING               | Static value "analytics" | bdprod.merchant_promotion_analytical.merchant_insights_customer |                                                       |                       |
| customer_id                          | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | customer_id                                            |                       |
| merchant_id                          | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | merchant_id                                            |                       |
| gender                               | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | gender                                                 |                       |
| home_location                        | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | home_location                                          |                       |
| home_municipality                    | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | home_municipality                                      |                       |
| home_regional_unit                   | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | home_regional_unit                                     |                       |
| home_region                          | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | home_region                                            |                       |
| work_location                        | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | work_location                                          |                       |
| work_municipality                    | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | work_municipality                                      |                       |
| work_regional_unit                   | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | work_regional_unit                                     |                       |
| work_region                          | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | work_region                                            |                       |
| age_group                            | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | age_group                                              |                       |
| age                                  | INT                  | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | age                                                    |                       |
| occupation                           | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | occupation                                             |                       |
| nbg_segment                          | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | nbg_segment                                            |                       |
| promotion_id                         | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | promotion_id                                           |                       |
| shopping_interests                   | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | shopping_interests                                     |                       |
| activity                             | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | activity                                               |                       |
| spending_profile                     | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer_interests | spending_profile                                       |                       |
| channel_preference                   | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer_interests | channel_preference                                     |                       |
| ecommerce_ind                        | BOOLEAN              | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | ecommerce_ind                                          |                       |
| card_type                            | STRING               | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | card_type                                              |                       |
| sms_ind                              | BOOLEAN              | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | sms_ind                                                |                       |
| go4more_ind                          | BOOLEAN              | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | go4more_ind                                            |                       |
| ibank_ind                            | BOOLEAN              | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | ibank_ind                                              |                       |
| number_of_transactions               | INT                  | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | number_of_transactions                                 |                       |
| amount                               | DECIMAL(38,2)       | cast(mic.amount as decimal(38,2)) | bdprod.merchant_promotion_analytical.merchant_insights_customer | amount                                                |                       |
| rewarded_points                      | INT                  | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | rewarded_points                                        |                       |
| redeemed_points                      | INT                  | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | redeemed_points                                        |                       |
| rewarded_amount                      | DECIMAL(38,2)       | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | rewarded_amount                                        |                       |
| redeemed_amount                      | DECIMAL(38,2)       | Direct Copy          | bdprod.merchant_promotion_analytical.merchant_insights_customer | redeemed_amount                                        |                       |

## 4. Target Tables
### 4.1. Target Tables Documentation
| Target Table                                       | Source Table                                                  | Row Selection Logic                                                |
|:---------------------------------------------------|:-------------------------------------------------------------|:------------------------------------------------------------------|
| bdprod.merchant_promotion_insights.statistics_raw | bdprod.merchant_promotion_analytical.merchant_insights_customer, bdprod.merchant_promotion_analytical.merchant_insights_customer_interests | REPLACE WHERE statistics_type = "analytics" and {partition_conditions} {merchant_user_id_condition} |

### 4.2. Data Transformation Logic for Target Tables
| Target Column                                   | Data Type         | Transformation Logic                           | Source Table                                                  | Source Column                                   |
|:------------------------------------------------|:------------------|:------------------------------------------------|:-------------------------------------------------------------|:------------------------------------------------|
| par_dt                                         | DATE              | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | par_dt                                          |
| merchant_user_id                               | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | merchant_user_id                                |
| statistics_type                                | STRING            | Static value "analytics"                        |                                                             |                                                 |
| customer_id                                    | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | customer_id                                     |
| merchant_id                                    | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | merchant_id                                     |
| gender                                         | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | gender                                          |
| home_location                                  | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | home_location                                   |
| home_municipality                              | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | home_municipality                               |
| home_regional_unit                             | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | home_regional_unit                              |
| home_region                                    | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | home_region                                     |
| work_location                                  | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | work_location                                   |
| work_municipality                              | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | work_municipality                               |
| work_regional_unit                             | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | work_regional_unit                              |
| work_region                                    | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | work_region                                     |
| age_group                                      | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | age_group                                       |
| age                                            | INT               | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | age                                             |
| occupation                                     | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | occupation                                      |
| nbg_segment                                    | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | nbg_segment                                     |
| promotion_id                                   | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | promotion_id                                    |
| shopping_interests                             | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | shopping_interests                              |
| activity                                       | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | activity                                        |
| spending_profile                               | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer_interests | spending_profile                                |
| channel_preference                             | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer_interests | channel_preference                              |
| ecommerce_ind                                  | BOOLEAN           | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | ecommerce_ind                                   |
| card_type                                      | STRING            | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | card_type                                       |
| sms_ind                                        | BOOLEAN           | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | sms_ind                                         |
| go4more_ind                                    | BOOLEAN           | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | go4more_ind                                     |
| ibank_ind                                      | BOOLEAN           | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | ibank_ind                                       |
| number_of_transactions                         | INT               | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | number_of_transactions                          |
| amount                                         | DECIMAL(38,2)    | cast(mic.amount as decimal(38,2))              | bdprod.merchant_promotion_analytical.merchant_insights_customer | amount                                          |
| rewarded_points                                | INT               | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | rewarded_points                                  |
| redeemed_points                                | INT               | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | redeemed_points                                  |
| rewarded_amount                                | DECIMAL(38,2)    | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | rewarded_amount                                  |
| redeemed_amount                                | DECIMAL(38,2)    | Direct Copy                                     | bdprod.merchant_promotion_analytical.merchant_insights_customer | redeemed_amount                                  |

This concludes the functional specification document for the specified notebook.