# Functional Specification Document

## 1. Input Parameters

| Parameter Name | Description                                           |
|:---------------|:-----------------------------------------------------|
| catalog        | The catalog or database name used for table reference. |
| year           | The year for which the competitor data is being computed. |

## 2. Source Tables 

### 2.1. Source Tables Documentation

| Column Name   | Column Data Type |
|:--------------|:-----------------|
| mcc           | STRING           |
| category      | STRING           |
| description   | STRING           |
| hyper_category| STRING           |
| sector        | STRING           |

Table: `bdprod.merchant_promotion_core.mcc_categories`

| Column Name        | Column Data Type |
|:-------------------|:-----------------|
| uuid                | STRING           |
| retailer           | STRING           |

Table: `bdprod.merchant_promotion_core.retailer_info`

| Column Name   | Column Data Type |
|:--------------|:-----------------|
| merchant_id   | STRING           |
| amount        | DECIMAL(15,2)    |
| par_dt        | INT              |

Table: `bdprod.merchant_promotion_core.customer_daily_trns`

| Column Name   | Column Data Type |
|:--------------|:-----------------|
| customer_id   | STRING           |
| age           | INT              |
| home_location | STRING           |
| work_location | STRING           |
| gender        | STRING           |
| occupation    | STRING           |
| annual_income | DECIMAL(15,2)    |
| nbg_segment   | STRING           |
| sms_ind       | BOOLEAN          |
| email_ind     | BOOLEAN          |
| addr_ind      | BOOLEAN          |
| memberships    | ARRAY<STRING>    |
| ibank_ind     | BOOLEAN          |
| age_range_cat | INT              |

Table: `bdprod.merchant_promotion_core.customer`

## 3. Intermediate Tables 

### 3.1. Intermediate Tables Documentation

| Target Table   | Source Table                                   | Row Selection Logic                                                                           |
|:---------------|:-----------------------------------------------|:----------------------------------------------------------------------------------------------|
| md             | bdprod.merchant_promotion_core.merchant       | `where m.country = 'GRC'`                                                                    |
|                | bdprod.merchant_promotion_core.retailer_info  | `m.retailer_info_id = r.uuid` (LEFT JOIN)                                                   |
|                | bdprod.merchant_promotion_core.mcc_categories  | `m.mcc = cat.mcc` (LEFT JOIN)                                                                |

#### md Intermediate Table Transformation Logic 

| Target Column | Data Type | Transformation Logic | Source Table                                           | Source Column           |
|:--------------|:----------|:---------------------|:------------------------------------------------------|:-------------------------|
| mid           | STRING    | Direct Copy          | bdprod.merchant_promotion_core.merchant              | merchantid               |
| brand         | STRING    | Direct Copy          | bdprod.merchant_promotion_core.retailer_info         | retailer                 |
| mcc           | STRING    | Direct Copy          | bdprod.merchant_promotion_core.merchant              | mcc                      |
| sector        | STRING    | Direct Copy          | bdprod.merchant_promotion_core.mcc_categories        | category                 |
| hyper_category | STRING   | Direct Copy          | bdprod.merchant_promotion_core.mcc_categories        | hyper_category           |

| Target Table | Source Table                                   | Row Selection Logic                                                                    |
|:-------------|:-----------------------------------------------|:---------------------------------------------------------------------------------------|
| trns         | md                                             | `mid=trns.merchant_id` (JOIN with md)                                               |
|              | bdprod.merchant_promotion_core.customer_daily_trns | `where par_dt >= cast(concat(cast({year} as string), "0101") as int) and par_dt <= cast(concat(cast({year} as string), "1231") as int)` |

#### trns Intermediate Table Transformation Logic 

| Target Column | Data Type | Transformation Logic | Source Table                                           | Source Column               | Group By     |
|:--------------|:----------|:---------------------|:------------------------------------------------------|:-----------------------------|:-------------|
| brand         | STRING    | Direct Copy          | md                                                    | brand                         | brand        |
| mcc           | STRING    | Direct Copy          | md                                                    | mcc                           | mcc          |
| sector        | STRING    | Direct Copy          | md                                                    | sector                        | sector       |
| hyper_category| STRING    | Direct Copy          | md                                                    | hyper_category                | hyper_category|
| tot_amount    | DECIMAL(38,2)| sum(num)          | bdprod.merchant_promotion_core.customer_daily_trns   | amount                        |              |

## 4. Target Tables

### 4.1. Target Tables Documentation

| Target Table                                                  | Source Table                                   | Row Selection Logic                           |
|:--------------------------------------------------------------|:-----------------------------------------------|:----------------------------------------------|
| bdprod.merchant_promotion_analytical.mcc_top_competitors     | trns                                          | `sum(CAST(tot_amount as decimal(38,2)))`    |

### 4.2. Target Table Transformation Logic 

| Target Column       | Data Type           | Transformation Logic                         | Source Table                                   | Source Column                     |
|:--------------------|:--------------------|:--------------------------------------------|:-----------------------------------------------|:-----------------------------------|
| year                 | INT                  | Direct Copy                                 | trns                                          | {year}                             |
| mcc                  | STRING               | Direct Copy                                 | trns                                          | mcc                                |
| sector               | STRING               | Direct Copy                                 | trns                                          | sector                             |
| hyper_category       | STRING               | Direct Copy                                 | trns                                          | hyper_category                     |
| brand                | STRING               | Direct Copy                                 | trns                                          | brand                              |
| yearly_amount        | DECIMAL(38,2)       | sum(CAST(tot_amount as decimal(38,2)))    | trns                                          | tot_amount                         |
| top_rank             | INT                  | rank() over (partition by mcc order by sum)... | trns                                          | tot_amount                         |