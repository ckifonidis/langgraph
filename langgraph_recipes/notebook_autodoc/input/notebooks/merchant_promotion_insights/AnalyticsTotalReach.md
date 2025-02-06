# Functional Specification Document

## Input Parameters
- `catalog`: The catalog name to be used during query execution.

## Source Tables
| Column Name     | Column Data Type |
|:----------------|:-----------------|
| promotion_id    | STRING           |
| customer_id     | STRING           |
| merchant_id     | STRING           |

Source Tables Used:
1. `bdprod.merchant_promotion_engine.mpe_promotion_customers`
2. `bdprod.merchant_promotion_engine.mpe_promotions`

## Intermediate Tables
N/A - No CTEs were created in the provided query.

## Target Tables
| Target Table                                             | Source Table                                                        | Row Selection Logic                      |
|:--------------------------------------------------------|:-------------------------------------------------------------------|:-----------------------------------------|
| bdprod.merchant_promotion_insights.analytics_total_reach| bdprod.merchant_promotion_engine.mpe_promotion_customers (pc)    | pc.promotion_id = p.promotion_id        |
| bdprod.merchant_promotion_insights.analytics_total_reach| bdprod.merchant_promotion_engine.mpe_promotions (p)              | pc.promotion_id = p.promotion_id        |

### Target Table Insert/Update Logic

| Target Column        | Data Type | Transformation Logic | Source Table                                                | Source Column         |
|:---------------------|:----------|:---------------------|:------------------------------------------------------------|:----------------------|
| merchant_user_id     | STRING    | Direct Copy          | bdprod.merchant_promotion_engine.mpe_promotions            | merchant_id           |
| customer_id          | STRING    | Direct Copy          | bdprod.merchant_promotion_engine.mpe_promotion_customers    | customercode          |
| promotion_id         | STRING    | Direct Copy          | bdprod.merchant_promotion_engine.mpe_promotions            | promotion_id          |
