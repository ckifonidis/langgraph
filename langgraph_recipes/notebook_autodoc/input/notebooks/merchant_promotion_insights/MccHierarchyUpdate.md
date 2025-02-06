# Functional Specification Document

## 1. Input Parameters
- `catalog`: A variable that holds the database catalog name used in the SQL queries.

## 2. Source Tables
| Column Name       | Column Data Type |
|:------------------|:-----------------|
| mcc               | STRING           |
| category          | STRING           |
| description       | STRING           |
| hyper_category    | STRING           |
| sector            | STRING           |

### Source Table Reference
- **Table**: `bdprod.merchant_promotion_core.mcc_categories`

## 3. Intermediate Tables
### Intermediate Table Descriptions
- **Intermediate Table 1 (temp1)** 
  - Source Table: `bdprod.merchant_promotion_core.mcc_categories`
  - Row Selection Logic: No filter applied, select distinct values.
  
| Target Table | Source Table                                | Row Selection Logic        |
|:-------------|:---------------------------------------------|:---------------------------|
| temp1       | bdprod.merchant_promotion_core.mcc_categories | DISTINCT                   |

| Target Column | Data Type | Transformation Logic                          | Source Table                                | Source Column   | Group By |
|:--------------|:----------|:---------------------------------------------|:--------------------------------------------|:----------------|:---------|
| ID            | STRING    | Direct Copy                                  | bdprod.merchant_promotion_core.mcc_categories | mcc             |          |
| DisplayName   | STRING    | concat_ws(' - ', mcc, description)         | bdprod.merchant_promotion_core.mcc_categories | mcc, description |          |
| ParentId      | STRING    | Direct Copy                                  | bdprod.merchant_promotion_core.mcc_categories | category        |          |

- **Intermediate Table 2 (temp2)**
  - Source Table: `bdprod.merchant_promotion_core.mcc_categories`
  - Row Selection Logic: No filter applied, select distinct values.

| Target Table | Source Table                                | Row Selection Logic        |
|:-------------|:---------------------------------------------|:---------------------------|
| temp2       | bdprod.merchant_promotion_core.mcc_categories | DISTINCT                   |

| Target Column | Data Type | Transformation Logic                          | Source Table                                | Source Column   | Group By |
|:--------------|:----------|:---------------------------------------------|:--------------------------------------------|:----------------|:---------|
| ID            | STRING    | Direct Copy                                  | bdprod.merchant_promotion_core.mcc_categories | category        |          |
| DisplayName   | STRING    | Direct Copy                                  | bdprod.merchant_promotion_core.mcc_categories | category        |          |
| ParentId      | STRING    | Direct Copy                                  | bdprod.merchant_promotion_core.mcc_categories | hyper_category |          |

- **Intermediate Table 3 (temp3)**
  - Source Table: `bdprod.merchant_promotion_core.mcc_categories`
  - Row Selection Logic: No filter applied, select distinct values.

| Target Table | Source Table                                | Row Selection Logic        |
|:-------------|:---------------------------------------------|:---------------------------|
| temp3       | bdprod.merchant_promotion_core.mcc_categories | DISTINCT                   |

| Target Column | Data Type | Transformation Logic                          | Source Table                                | Source Column   | Group By |
|:--------------|:----------|:---------------------------------------------|:--------------------------------------------|:----------------|:---------|
| ID            | STRING    | Direct Copy                                  | bdprod.merchant_promotion_core.mcc_categories | hyper_category |          |
| DisplayName   | STRING    | Direct Copy                                  | bdprod.merchant_promotion_core.mcc_categories | hyper_category |          |
| ParentId      | STRING    | Direct Copy                                  | bdprod.merchant_promotion_core.mcc_categories | null          |          |

## 4. Target Tables
| Target Table                                         | Source Table                                | Row Selection Logic        |
|:-----------------------------------------------------|:---------------------------------------------|:---------------------------|
| bdprod.merchant_promotion_insights.mcc_hierarchy   | temp1, temp2, temp3                        | UNION ALL temp1, temp2, temp3 |

| Target Column | Data Type   | Transformation Logic               | Source Table                   | Source Column | 
|:--------------|:------------|:-----------------------------------|:-------------------------------|:--------------|
| ID            | STRING      | Direct Copy                        | temp1                          | ID            |
| ID            | STRING      | Direct Copy                        | temp2                          | ID            |
| ID            | STRING      | Direct Copy                        | temp3                          | ID            |
| DisplayName   | STRING      | Direct Copy                        | temp1                          | DisplayName   |
| DisplayName   | STRING      | Direct Copy                        | temp2                          | DisplayName   |
| DisplayName   | STRING      | Direct Copy                        | temp3                          | DisplayName   |
| ParentId      | STRING      | Direct Copy                        | temp1                          | ParentId      |
| ParentId      | STRING      | Direct Copy                        | temp2                          | ParentId      |
| ParentId      | STRING      | Direct Copy                        | temp3                          | ParentId      |

This completes the functional specification document based on the provided notebook content. Each table and transformation is detailed in a structured format, following the specified requirements.