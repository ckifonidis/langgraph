# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics

# COMMAND ----------
# DBTITLE 1,Parse Workflow Parameters
catalog = get_catalog()
parym_range = get_parym_range_condition()
(pardt1, pardt2) = get_pardt_integer_range()
parym_end = extract_parym(pardt2)

# COMMAND ----------
# DBTITLE 1,Build Query

query = f"""
INSERT INTO {catalog}.merchant_promotion_analytical.customer_sector_characteristics 
REPLACE WHERE {parym_range}
with all_comb as (
  select 
    cast (date_format(ts, 'yyyyMM') as int) as par_ym,
    cast(date_format(ts - interval 6 months, 'yyyyMM') as int) as par_ym_semester_before,
    cast(date_format(ts - interval 12 months, 'yyyyMM') as int) as par_ym_year_before
  from (
    select 
      explode(sequence(to_timestamp(cast({pardt1} as string), 'yyyyMMdd'), to_timestamp(cast({pardt2} as string), 'yyyyMMdd'), interval 1 month)) as ts
  ) 
),
perc_rank as (
  select /*+ RANGE_JOIN(v, 1) */
    ac.par_ym, 
    customer_id, 
    hyper_category,
    case 
        when percent_rank() over (partition by ac.par_ym, hyper_category order by sum(monthly_amount)) >= 0.8
        then "high_spenders"
    end as spending_profile
  from 
  all_comb ac join 
  {catalog}.merchant_promotion_analytical.customer_sector_volumes v
  on v.par_ym between ac.par_ym_year_before and ac.par_ym
  where
  v.par_ym between cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 12 months, 'yyyyMM') as int) and {parym_end}
  and v.hyper_category is not null 
  group by ac.par_ym, customer_id,hyper_category
  qualify spending_profile is not null
),
channels as (
  select /*+ RANGE_JOIN(v, 1) */
    ac.par_ym as par_ym,
    hyper_category,
    customer_id,
    case when  
        ecommerce_ind = true and 
        round(sum(frequency)/sum(sum(frequency)) over (partition by ac.par_ym, customer_id) *100,2) > 66.66
    then 'digital_consumers'
    when 
        ecommerce_ind = false and 
        round(sum(frequency)/sum(sum(frequency)) over (partition by ac.par_ym, customer_id) *100,2) > 66.66
    then 'in_store_consumers'
    end as channel_preference
  from all_comb ac left join 
  {catalog}.merchant_promotion_analytical.customer_sector_volumes v
  on v.par_ym between ac.par_ym_semester_before and ac.par_ym
  where v.par_ym between cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 12 months, 'yyyyMM') as int) and {parym_end}
  and v.hyper_category is not null 
  group by ac.par_ym, hyper_category,customer_id, v.ecommerce_ind
  qualify channel_preference is not null
)
select
ifnull(ca.par_ym, c.par_ym) as par_ym,
ifnull(ca.customer_id, c.customer_id) as customer_id,
ifnull(ca.hyper_category, c.hyper_category) as hyper_category,
ca.spending_profile as spending_profile,
c.channel_preference as channel_preference
FROM (select * from perc_rank where spending_profile is not null) ca
full outer join (select * from channels where channel_preference is not null) c
on (ca.customer_id=c.customer_id and cA.hyper_category = c.hyper_category and ca.par_ym = c.par_ym)
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
OPTIMIZE {catalog}.merchant_promotion_analytical.customer_sector_characteristics 
WHERE {parym_range}
zorder by (customer_id, hyper_category)
"""

print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if (not exclude_analytical_computations()) and generic_update():
  display(spark.sql(optimizeQuery))


