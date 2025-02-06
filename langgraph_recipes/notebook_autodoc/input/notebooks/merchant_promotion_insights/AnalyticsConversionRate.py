# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics
# COMMAND ----------

query_insights = """
INSERT INTO {catalog}.merchant_promotion_insights.analytics_conversion_rate 
REPLACE WHERE {partition_conditions} {merchant_user_id_condition}
    select distinct 
      "insights" as tbl, 
      merchant_user_id, 
      par_dt, 
      customer_id, 
      lower(promotion_id) as promotion_id
    from {catalog}.merchant_promotion_analytical.merchant_insights_customer
    where {partition_conditions} and promotion_id is not null {merchant_user_id_condition}
  """

# COMMAND ----------

query_promotions = """
INSERT INTO {catalog}.merchant_promotion_insights.analytics_conversion_rate 
REPLACE WHERE par_dt = -1 {merchant_user_id_condition}
    select distinct 
      "promotions" as tbl, 
      p.merchant_id as merchant_user_id, 
      -1 as par_dt, 
      cp.customercode as customer_id, 
      lower(p.promotion_id) as promotion_id
    from 
        {catalog}.merchant_promotion_engine.mpe_promotion_customers cp 
      join
        {catalog}.merchant_promotion_engine.mpe_promotions p 
      on 
        cp.promotion_id=p.promotion_id
    {merchant_user_id_condition}
  """

# COMMAND ----------

# DBTITLE 1,Check if run refers to historical data update and set flag
from datetime import datetime, timedelta
import pytz

catalog = get_catalog()
partition_conditions = get_pardt_range_condition()
merchant_user_id_condition = parse_merchant_condition("merchant_user_id")
merchant_user_id_condition_promotions = parse_merchant_condition("p.merchant_id")

# COMMAND ----------
# DBTITLE 1,Print insights data query
if not exclude_insights_computations():
    print(query_insights.format(catalog=catalog, partition_conditions=partition_conditions, merchant_user_id_condition=merchant_user_id_condition))

# COMMAND ----------
# DBTITLE 1,Update the table with insights data
if not exclude_insights_computations():
    display(spark.sql(query_insights.format(catalog=catalog, partition_conditions=partition_conditions, merchant_user_id_condition=merchant_user_id_condition)))


# COMMAND ----------
# DBTITLE 1,Print insights data query
if is_daily_flow() or force_update():
    print(query_promotions.format(catalog=catalog, merchant_user_id_condition=merchant_user_id_condition_promotions))

# COMMAND ----------
# DBTITLE 1,Update the table with insights data
if is_daily_flow() or force_update():
    display(spark.sql(query_promotions.format(catalog=catalog,  merchant_user_id_condition=merchant_user_id_condition_promotions)))

# COMMAND ----------
# DBTITLE 1,Optimize the table
optimizeQuery = f"""
OPTIMIZE {catalog}.merchant_promotion_insights.analytics_conversion_rate 
WHERE ((par_dt = -1) or ({partition_conditions})) {merchant_user_id_condition} 
zorder by (promotion_id)
"""
print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
display(spark.sql(optimizeQuery))


