# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics
# COMMAND ----------

query = """
INSERT OVERWRITE {catalog}.merchant_promotion_insights.mid_details
select 
    cast(merchantkey as string) as mid,
    case 
        when enddate is null then true
        else false
    end as isGo4More
from {catalog}.trlog_card.loyalty_merchants
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
    optimizeQuery = "OPTIMIZE {catalog}.merchant_promotion_insights.mid_details zorder by (mid)"
    display(spark.sql(optimizeQuery.format(catalog=catalog)))