# Functional Specification Document

## 1. Input Parameters

The following input parameters are defined for the notebook execution:

| Parameter Name              | Description                                                                                  |
|-----------------------------|----------------------------------------------------------------------------------------------|
| catalog                     | The catalog name where target tables are located.                                          |
| pardt_range                 | Date range condition for the relevant time period.                                         |
| merchant_user_id_condition   | Condition to filter specific merchant user IDs.                                            |
| pardt1                     | Start date in 'yyyyMMdd' format for the date range condition.                             |
| pardt2                     | End date in 'yyyyMMdd' format for the date range condition.                               |

## 2. Source Tables

The source tables that the query accesses are:

### `merchant_all_statistics`
| Column Name            | Column Data Type |
|:-----------------------|:-----------------|
| par_dt                 | INT               |
| merchant_id            | STRING            |
| customer_id            | STRING            |
| number_of_transactions  | INT               |

### `merchant`
| Column Name            | Column Data Type |
|:-----------------------|:-----------------|
| merchant_user_id      | STRING            |
| merchantid            | STRING            |
| is_merchant_user      | BOOLEAN          |

## 3. Intermediate Tables

### Table: `all_comb`

The `all_comb` table generates timestamps based on the specified date range.

| Target Table | Source Table | Row Selection Logic                                                |
|:-------------|:-------------|:------------------------------------------------------------------|
| all_comb     | None         | Generate a sequence from `pardt1` to `pardt2` inclusive as `ts`. |

Target Columns:
| Target Column    | Data Type | Transformation Logic                   | Source Table | Source Column |
|:-----------------|:----------|:---------------------------------------|:-------------|:--------------|
| par_dt           | INT       | `cast (date_format(ts, 'yyyyMMdd') as int)` | None         | N/A          |
| par_dt_semester_before | INT | `cast(date_format(ts - interval 6 months, 'yyyyMMdd') as int)` | None         | N/A          |

### Table: `customer_freq`

The `customer_freq` table aggregates customer transaction data for a specified date range.

| Target Table   | Source Table                             | Row Selection Logic                         |
|:----------------|:------------------------------------------|:--------------------------------------------|
| customer_freq   | `merchant_all_statistics`                | `where par_dt between cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 6 months, 'yyyyMMdd') as int) and {pardt2}` |
|                 | `all_comb`                               | `on dt.par_dt between ac.par_dt_semester_before and ac.par_dt` |
|                 | `merchant`                               | `where is_merchant_user = True {merchant_user_id_condition}` |

Target Columns:
| Target Column         | Data Type | Transformation Logic            | Source Table                             | Source Column              | Group By                |
|:----------------------|:----------|:--------------------------------|:-----------------------------------------|:--------------------------|:-----------------------|
| merchant_user_id      | STRING    | Direct Copy                     | `merchant`                               | `merchant_user_id`        | `customer_id`          |
| customer_id           | STRING    | Direct Copy                     | `merchant_all_statistics`                | `customer_id`             |                        |
| number_of_transactions | INT       | `SUM(number_of_transactions)`   | `merchant_all_statistics`                | `number_of_transactions`     | `customer_id`, `merchant_user_id`, `ac.par_dt` |
| par_dt                | INT       | Direct Copy                     | `all_comb`                               | `par_dt`                  |                        |

### Table: `merchant_avg`

The `merchant_avg` table calculates average transactions per merchant.

| Target Table | Source Table   | Row Selection Logic                        |
|:-------------|:---------------|:-------------------------------------------|
| merchant_avg | `customer_freq`| None                                       |

Target Columns:
| Target Column  | Data Type | Transformation Logic                     | Source Table   | Source Column              | Group By             |
|:---------------|:----------|:-----------------------------------------|:---------------|:--------------------------|:---------------------|
| merchant_user_id| STRING   | Direct Copy                             | `customer_freq`| `merchant_user_id`       | `par_dt`             |
| par_dt         | INT       | Direct Copy                             | `customer_freq`| `par_dt`                 |                      |
| threshold      | DECIMAL   | `SUM(number_of_transactions)/COUNT(DISTINCT customer_id)` | `customer_freq`| `number_of_transactions` |                       |

## 4. Target Tables

### Target Table: `customer_activity_insights`

The target table that is being updated with insights data.

| Target Table                      | Source Table                   | Row Selection Logic                                               |
|:-----------------------------------|:-------------------------------|:-----------------------------------------------------------------|
| `merchant_promotion_analytical.customer_activity_insights` | `customer_freq`              | `where customer_freq.customer_id is not null`                   |
| `merchant_promotion_analytical.customer_activity_insights` | `merchant_avg`               | `on customer_freq.par_dt = merchant_avg.par_dt and customer_freq.merchant_user_id = merchant_avg.merchant_user_id` |

Target Columns:
| Target Column      | Data Type | Transformation Logic                                        | Source Table           | Source Column                |
|:-------------------|:----------|:----------------------------------------------------------|:-----------------------|:------------------------------|
| merchant_user_id   | STRING    | Direct Copy                                               | `customer_freq`        | `merchant_user_id`           |
| customer_id        | STRING    | Direct Copy                                               | `customer_freq`        | `customer_id`                |
| activity           | STRING    | `case when number_of_transactions > threshold then "frequent" when number_of_transactions <= threshold then "less_frequent" end` | `customer_freq`        | `number_of_transactions`     |
| par_dt             | INT       | Direct Copy                                               | `customer_freq`        | `par_dt`                     |