# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics
# COMMAND ----------

query = """
INSERT OVERWRITE {catalog}.merchant_promotion_insights.customer_data
select distinct 
  ca.merchant_user_id, ifnull(ifnull(ca.customer_id, csc.customer_id), ccc.customer_id), 
  csc.channel_preference, csc.spending_profile,
  ccc.age, ccc.age_range_cat, ccc.gender, ccc.home_municipality, ccc.work_municipality,
  ccc.nbg_segment, ccc.occupation, ccc.shopping_interests, ca.activity
from 
  {catalog}.merchant_promotion_analytical.customer_activity ca
  full outer join 
  {catalog}.merchant_promotion_analytical.customer_sector_characteristics csc
  on ca.customer_id = csc.customer_id
  full outer join 
  {catalog}.merchant_promotion_analytical.cards_customer_characteristics ccc
  on ca.customer_id = ccc.customer_id 
where 
  1 = 1
  and csc.par_ym = {parym} 
  and ccc.par_ym = {parym}
  and csc.hyper_category in (
    SELECT 
      distinct hyper_category
    FROM 
        {catalog}.merchant_promotion_core.merchant m
      left join
        {catalog}.merchant_promotion_core.mcc_categories cat
      on 
        m.mcc = cat.mcc
    where 
      is_merchant_user = true
  )
  and ccc.par_ym = {parym}
  and ccc.ibank_ind is true 
  and ccc.sms_ind is true
  and array_contains(ccc.memberships, "Go4More")
  """

# COMMAND ----------

# DBTITLE 1,Check if run refers to historical data update and set flag
from datetime import datetime, timedelta
import pytz

catalog = dbutils.widgets.get("catalog")
pardt = int(dbutils.widgets.get("pardt"))#20240630
parym = extract_parym(pardt)

# COMMAND ----------

if is_daily_flow() or force_update():
    print(query.format(catalog=catalog, pardt=pardt, parym=parym))

# COMMAND ----------
# DBTITLE 1,If daily run, update the table
if is_daily_flow() or force_update():
    display(spark.sql(query.format(catalog=catalog, pardt=pardt, parym=parym)))


# COMMAND ----------
# DBTITLE 1,If daily run, optimize the table
if is_daily_flow() or force_update():
    optimizeQuery = "OPTIMIZE {catalog}.merchant_promotion_insights.customer_data zorder by customer_id"
    display(spark.sql(optimizeQuery.format(catalog = catalog)))


