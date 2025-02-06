# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics

# COMMAND ----------
# DBTITLE 1,Parse Workflow Parameters
catalog = get_catalog()
parym_range = get_parym_range_condition()
pardt_range = get_monthly_pardt_range_condition()


# COMMAND ----------
# DBTITLE 1,Build Query

query = f"""
INSERT INTO {catalog}.merchant_promotion_analytical.customer_sector_volumes 
REPLACE WHERE {parym_range}
select 
    customer_id, sector, hyper_category, monthly_amount, frequency, gr_ind, 
    sum(monthly_amount) over (partition by par_ym, customer_id),
    ecommerce_ind, par_ym
FROM (
    SELECT 
            customer_id,
            manual_category AS sector,
            hyper_category,
            cast(SUM(amount) as decimal(15,2)) AS monthly_amount,
            SUM(number_of_transactions) AS frequency,
            coalesce(country = 'GRC', False) AS gr_ind,
            ecommerce_ind,
            CAST(floor(par_dt / 100) AS INT) AS par_ym
        FROM
            {catalog}.merchant_promotion_core.customer_daily_trns dt
            LEFT JOIN (
                SELECT /*+ BROADCAST(mc) */ DISTINCT
                    m.merchantid AS m_id, mc.category as manual_category, mc.hyper_category
                    FROM {catalog}.merchant_promotion_core.merchant m
                    left join {catalog}.merchant_promotion_core.mcc_categories mc
                    on m.mcc = mc.mcc
            ) md
            ON md.m_id = dt.merchant_id
        WHERE {pardt_range}
        GROUP BY
            customer_id,
            sector,
            hyper_category,
            gr_ind,
            ecommerce_ind,
            par_ym
)
  """

# COMMAND ----------
# DBTITLE 1,Print  update query
print(query)

# COMMAND ----------
# DBTITLE 1,Update the table with insights data
if (not exclude_analytical_computations()) and generic_update():
    display(spark.sql(query))


# COMMAND ----------
# DBTITLE 1,Optimize the table
optimizeQuery = f"""
OPTIMIZE {catalog}.merchant_promotion_analytical.customer_sector_volumes 
WHERE {parym_range}
zorder by (customer_id, hyper_category)
"""

print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if (not exclude_analytical_computations()) and generic_update():
    display(spark.sql(optimizeQuery))


