# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics
# COMMAND ----------

query = """
INSERT OVERWRITE {catalog}.merchant_promotion_insights.mcc_hierarchy
with temp1 as (
    select distinct 
      mcc as ID, 
      concat_ws(' - ', mcc, description) as DisplayName,
      category as ParentId
    from {catalog}.merchant_promotion_core.mcc_categories
),
temp2 as (
    select distinct 
      category as ID, 
      category as DisplayName,
      hyper_category as ParentId
    from {catalog}.merchant_promotion_core.mcc_categories
),
temp3 as (
    select distinct 
      hyper_category as ID, 
      hyper_category as DisplayName,
      null as ParentId
    from {catalog}.merchant_promotion_core.mcc_categories
)
select * from temp1 union all select * from temp2 union all select * from temp3;
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
    optimizeQuery = "OPTIMIZE {catalog}.merchant_promotion_insights.mcc_hierarchy zorder by ID"
    display(spark.sql(optimizeQuery.format(catalog=catalog)))