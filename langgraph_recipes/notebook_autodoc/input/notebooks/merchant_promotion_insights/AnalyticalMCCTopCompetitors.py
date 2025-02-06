# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics

# COMMAND ----------
# DBTITLE 1,Parse Workflow Parameters
catalog = get_catalog()
year = get_year_for_competitors()

# COMMAND ----------
# DBTITLE 1,Build Query

query = f"""
INSERT INTO {catalog}.merchant_promotion_analytical.mcc_top_competitors
REPLACE WHERE year = {year}
with md as (
    select /*+ BROADCASTJOIN(cat) */ distinct
      m.merchantid as mid,
      r.retailer as brand,
      m.mcc,
      cat.category AS sector,
      cat.hyper_category AS hyper_category
    from 
      {catalog}.merchant_promotion_core.merchant m
      left join {catalog}.merchant_promotion_core.retailer_info r on m.retailer_info_id = r.uuid
      left join {catalog}.merchant_promotion_core.mcc_categories cat on m.mcc = cat.mcc
    where m.country = 'GRC' 
  ),
  trns as (
    select  
      brand, 
      md.mcc,
      sector,
      hyper_category,
      sum(num) as tot_amount
    from md
    join (
      select 
        merchant_id, amount as num
      from {catalog}.merchant_promotion_core.customer_daily_trns
      where par_dt >= cast(concat(cast({year} as string), "0101") as int) 
        and par_dt <= cast(concat(cast({year} as string), "1231") as int)
    ) trns
    on mid=trns.merchant_id
    group by brand, mcc, sector, hyper_category
  )
  select {year} as year, mcc,sector, hyper_category,brand, 
  sum(CAST(tot_amount as decimal(38,2))) as yearly_amount,
  cast(rank() over (partition by mcc order by sum(CAST(tot_amount as decimal(38,2))) desc) as int) as top_rank
  from trns
  group by mcc,brand,sector, hyper_category
  """

# COMMAND ----------
# DBTITLE 1,Print update query
print(query)

# COMMAND ----------
# DBTITLE 1,Update the table with insights data

## Execute the query only if daily flow on first day of the year or if historical include 1st day of the year
if (not exclude_analytical_computations()) and compute_competitors():
  display(spark.sql(query))


# COMMAND ----------
# DBTITLE 1,Optimize the table
optimizeQuery = f"""
OPTIMIZE {catalog}.merchant_promotion_analytical.mcc_top_competitors 
WHERE year = {year}
zorder by (mcc, top_rank)
"""

print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if (not exclude_analytical_computations()) and compute_competitors():
  display(spark.sql(optimizeQuery))


