# Functional Specification Document

## 1. Input Parameters
- `catalog`: The catalog name is dynamically set using the `get_catalog()` function. 

## 2. Source Tables

| Column Name | Column Data Type |
|:------------|:-----------------|
| merchantkey | STRING           |
| enddate     | DATE             |

Source Table: `bdprod.trlog_card.loyalty_merchants`

## 3. Intermediate Tables
There are no Common Table Expressions (CTEs) defined in the provided query, thus there are no intermediate tables derived from the source tables.

## 4. Target Tables

| Target Table | Source Table | Row Selection Logic                       |
|:-------------|:-------------|:------------------------------------------|
| bdprod.merchant_promotion_insights.mid_details | bdprod.trlog_card.loyalty_merchants | All rows |

### Target Table Columns

| Target Column | Data Type | Transformation Logic | Source Table | Source Column |
|:--------------|:----------|:---------------------|:-------------|:--------------|
| mid           | STRING    | cast(merchantkey as string) | bdprod.trlog_card.loyalty_merchants | merchantkey |
| isGo4More     | BOOLEAN   | case when enddate is null then true else false end | bdprod.trlog_card.loyalty_merchants | enddate |
```
