# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics

# COMMAND ----------
# DBTITLE 1,Parse Workflow Parameters
catalog = get_catalog()
parym_range = get_parym_range_condition()
pardt_range = get_pardt_range_condition()
merchant_user_id_condition = parse_merchant_condition("merchant_user_id")
# COMMAND ----------
# DBTITLE 1,Build Query

query = f"""
INSERT INTO {catalog}.merchant_promotion_analytical.merchant_insights_customer_interests
REPLACE WHERE {pardt_range} {merchant_user_id_condition}
select /*+ RANGE_JOIN(trns, 10) */ distinct 
  promo.promotion_id as promotion_id,
  muser.merchant_user_id as merchant_user_id,
  trns.customer_id as customer_id,
  case when c.shopping_interests is null or c.shopping_interests='' then 'other_category' else c.shopping_interests end as shopping_interests,
  s.hyper_category,
  s.spending_profile, 
  s.channel_preference,
  trns.par_dt as par_dt
from (
  select distinct merchant_id, customer_id, par_dt
  from {catalog}.merchant_promotion_core.customer_daily_trns
  where {pardt_range}
  and country = 'GRC'
) trns
join (
  select /*+ BROADCASTJOIN(cat) */ distinct
    merchantid, merchant_user_id, m.merchant_mcc as mcc, cat.hyper_category, m.country
  from 
    {catalog}.merchant_promotion_core.merchant m
  left join 
    {catalog}.merchant_promotion_core.mcc_categories cat
  on 
    m.merchant_mcc = cat.mcc
  where is_merchant_user = true and m.country = 'GRC' {merchant_user_id_condition}
) muser on muser.merchantid = trns.merchant_id
left join (
select distinct par_ym, customer_id, shopping_interests 
from {catalog}.merchant_promotion_analytical.cards_customer_characteristics
where {parym_range}
) c
on 
trns.customer_id=c.customer_id
and CAST(floor(trns.par_dt / 100) AS INT) = c.par_ym
left join (
    select
      pr.promotion_id, customercode, merchant_id,
      cast(replace(startdate, '-','') as int) as startdate,
      cast(replace(enddate, '-','') as int) as enddate
    from
      {catalog}.merchant_promotion_engine.mpe_promotions pr
    join
      {catalog}.merchant_promotion_engine.mpe_promotion_customers c
    on pr.promotion_id = c.promotion_id
    where 1=1 {parse_merchant_condition("merchant_id")}
) promo
on 
trns.par_dt between promo.startdate and promo.enddate and trns.customer_id = promo.customercode and muser.merchant_user_id = promo.merchant_id
left join 
(
  select par_ym, customer_id, hyper_category, spending_profile, channel_preference
  from {catalog}.merchant_promotion_analytical.customer_sector_characteristics
  where {parym_range}
) s 
on 
trns.customer_id=s.customer_id 
and muser.hyper_category = s.hyper_category
and s.par_ym = cast(substring(cast(trns.par_dt as string), 1, 6) as int) 

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
OPTIMIZE {catalog}.merchant_promotion_analytical.merchant_insights_customer_interests 
WHERE {pardt_range} {merchant_user_id_condition}
zorder by (customer_id, promotion_id)
"""

print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if not exclude_analytical_computations():
    display(spark.sql(optimizeQuery))


