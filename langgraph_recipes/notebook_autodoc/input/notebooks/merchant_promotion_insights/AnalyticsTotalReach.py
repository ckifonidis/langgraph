# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics
# COMMAND ----------

query = """
INSERT OVERWRITE {catalog}.merchant_promotion_insights.analytics_total_reach
select 
  p.merchant_id as merchant_user_id, 
  pc.customercode as customer_id,
  p.promotion_id as promotion_id
from 
    {catalog}.merchant_promotion_engine.mpe_promotion_customers pc 
  inner join 
    {catalog}.merchant_promotion_engine.mpe_promotions p 
on pc.promotion_id=p.promotion_id
  """

# COMMAND ----------

# DBTITLE 1,Check if run refers to historical data update and set flag
catalog = get_catalog()

# COMMAND ----------

if is_daily_flow() or force_update():
    print(query.format(catalog=catalog))

# COMMAND ----------
# DBTITLE 1,If daily run, update the table
if is_daily_flow() or force_update():
    display(spark.sql(query.format(catalog=catalog)))


# COMMAND ----------
# DBTITLE 1,If daily run, optimize the table
if is_daily_flow() or force_update():
    optimizeQuery = "OPTIMIZE {catalog}.merchant_promotion_insights.analytics_total_reach zorder by (merchant_user_id, promotion_id)"
    display(spark.sql(optimizeQuery.format(catalog = catalog)))


