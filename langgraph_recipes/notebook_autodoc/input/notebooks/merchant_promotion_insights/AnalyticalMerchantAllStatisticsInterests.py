# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics

# COMMAND ----------
# DBTITLE 1,Parse Workflow Parameters
catalog = get_catalog()
parym_range = get_parym_range_condition()
pardt_range = get_pardt_range_condition()

# COMMAND ----------
# DBTITLE 1,Build Query

query = f"""
INSERT INTO {catalog}.merchant_promotion_analytical.merchant_all_statistics_interests
REPLACE WHERE {pardt_range}
select distinct
  trns.merchant_id as merchant_id,
  trns.customer_id as customer_id,
  md.mcc as mcc,
  case when c.shopping_interests is null or c.shopping_interests='' then 'other_category' else c.shopping_interests end as shopping_interests,
  s.hyper_category,
  s.spending_profile,
  s.channel_preference,
  md.retailer as retailer,
  md.brand as brand,
  trns.par_dt as par_dt
from (
  select distinct merchant_id, customer_id, par_dt 
  from {catalog}.merchant_promotion_core.customer_daily_trns
  where {pardt_range}
) trns
left join (
  select par_ym, customer_id, ifnull(shopping_interests, 'other_category') as shopping_interests 
  from {catalog}.merchant_promotion_analytical.cards_customer_characteristics
  where {parym_range}
) c
on 
  trns.customer_id=c.customer_id and c.par_ym = cast(substring(cast(trns.par_dt as string), 1, 6) as int)
left join (
  select /*+ BROADCASTJOIN(cat) */
    distinct m.merchantid as mid, r.retailer, r.brand, cat.category, m.mcc, cat.hyper_category
  from 
    {catalog}.merchant_promotion_core.merchant m
    left join 
    {catalog}.merchant_promotion_core.retailer_info r
    on m.retailer_info_id = r.uuid
    left join 
    {catalog}.merchant_promotion_core.mcc_categories cat
    on m.mcc = cat.mcc
) md
on trns.merchant_id = md.mid
left join {catalog}.merchant_promotion_analytical.customer_sector_characteristics s 
on s.par_ym = cast(substring(cast(trns.par_dt as string), 1, 6) as int) 
and trns.customer_id=s.customer_id 
and md.hyper_category=s.hyper_category
  """

# COMMAND ----------
# DBTITLE 1,Print update query
print(query)

# COMMAND ----------
# DBTITLE 1,Update the table with insights data
if (not exclude_analytical_computations()) and generic_update():
    display(spark.sql(query))


# COMMAND ----------
# DBTITLE 1,Optimize the table
optimizeQuery = f"""
OPTIMIZE {catalog}.merchant_promotion_analytical.merchant_all_statistics_interests
WHERE {pardt_range}
zorder by (brand, retailer, merchant_id, customer_id)
"""

print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if (not exclude_analytical_computations()) and generic_update():
    display(spark.sql(optimizeQuery))


