# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics

# COMMAND ----------
# DBTITLE 1,Parse Workflow Parameters
catalog = get_catalog()
pardt_range = get_pardt_range_condition()
merchant_user_id_condition = parse_merchant_condition("merchant_user_id")
(pardt1, pardt2) = get_pardt_integer_range()

# COMMAND ----------
# DBTITLE 1,Build Query

query = f"""
INSERT INTO {catalog}.merchant_promotion_analytical.customer_activity_insights
REPLACE WHERE {pardt_range} {merchant_user_id_condition}
WITH all_comb as (
  select
    cast (date_format(ts, 'yyyyMMdd') as int) as par_dt,
    cast(date_format(ts - interval 6 months, 'yyyyMMdd') as int) as par_dt_semester_before
  from (
    select
      explode(sequence(to_timestamp(cast({pardt1} as string), 'yyyyMMdd'), to_timestamp(cast({pardt2} as string), 'yyyyMMdd'), interval 1 day)) as ts
  )
),
customer_freq AS (
    SELECT /*+ RANGE_JOIN(dt, 10) */
        users.merchant_user_id,
        customer_id,
        SUM(number_of_transactions) AS number_of_transactions,
        ac.par_dt
    FROM
        (
          select par_dt, merchant_id, customer_id, number_of_transactions
          from {catalog}.merchant_promotion_analytical.merchant_all_statistics
          where par_dt between cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 6 months, 'yyyyMMdd') as int) and {pardt2}
        ) dt
        join all_comb ac
        on dt.par_dt between ac.par_dt_semester_before and ac.par_dt
        JOIN (
          select merchant_user_id, merchantid
          from {catalog}.merchant_promotion_core.merchant
          where is_merchant_user = True {merchant_user_id_condition}
        ) users
        ON dt.merchant_id = users.merchantid
    GROUP BY customer_id, users.merchant_user_id, ac.par_dt
),
merchant_avg AS (
    SELECT
        merchant_user_id,
        par_dt,
        SUM(number_of_transactions)/COUNT(DISTINCT customer_id) AS threshold
    FROM customer_freq
    group by merchant_user_id, par_dt
)
SELECT DISTINCT
    customer_freq.merchant_user_id,
    customer_freq.customer_id,
    case
        when number_of_transactions > threshold then "frequent"
        when number_of_transactions <= threshold then "less_frequent"
    end as activity,
    customer_freq.par_dt
FROM
    customer_freq
    join
    merchant_avg
    on customer_freq.par_dt = merchant_avg.par_dt and customer_freq.merchant_user_id = merchant_avg.merchant_user_id
where customer_freq.customer_id is not null;

  """

# COMMAND ----------
# DBTITLE 1,Print update query
print(query)

# COMMAND ----------
# DBTITLE 1,Update the table with insights data
if not exclude_analytical_computations():
  display(spark.sql(query))


# COMMAND ----------
# DBTITLE 1,Optimize the table
optimizeQuery = f"""
OPTIMIZE {catalog}.merchant_promotion_analytical.customer_activity_insights 
WHERE {pardt_range} {merchant_user_id_condition}
zorder by (customer_id)
"""

print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if not exclude_analytical_computations():
  display(spark.sql(optimizeQuery))


