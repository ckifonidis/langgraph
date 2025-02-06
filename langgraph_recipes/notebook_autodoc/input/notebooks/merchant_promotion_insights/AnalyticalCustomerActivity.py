# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics

# COMMAND ----------
# DBTITLE 1,Parse Workflow Parameters
catalog = get_catalog()
(pardt1, pardt2) = get_pardt_integer_range()

# COMMAND ----------
# DBTITLE 1,Build Query

query = f"""
INSERT OVERWRITE {catalog}.merchant_promotion_analytical.customer_activity
with customer_freq as (
  select
    users.merchant_user_id,
    customer_id,
    sum(number_of_transactions) as number_of_transactions
  from
  {catalog}.merchant_promotion_analytical.merchant_all_statistics dt
  join {catalog}.merchant_promotion_core.merchant  users
  on dt.merchant_id = users.merchantid
  where users.is_merchant_user = true 
  and par_dt >= cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 6 months, 'yyyyMMdd') as int)
  group by customer_id, users.merchant_user_id
),
merchant_avg as (
  select
    merchant_user_id,
    sum(number_of_transactions)/count(distinct customer_id) as threshold
  from customer_freq
  group by merchant_user_id
),
cust_interests as (
  select
    distinct
      customer_id,
      shopping_interests
  from 
   {catalog}.merchant_promotion_analytical.merchant_all_statistics_interests dt
  where dt.par_dt >= cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 12 months, 'yyyyMMdd') as int)
),
merchants as (
  select
    distinct
    m.merchant_user_id,
    cat.sector
  from {catalog}.merchant_promotion_core.merchant m
  left join {catalog}.merchant_promotion_core.mcc_categories cat
  on m.merchant_mcc = cat.mcc
  where array_contains(merchant_platforms, 'Go4More') and is_merchant_user = true
),
m_all as(
  select
    m.merchant_user_id,
    m.sector,
    c.customer_id,
    c.shopping_interests
  from merchants m
  cross join cust_interests c
  where m.sector=c.shopping_interests
),
merch_cust as (
    select
      distinct
      customer_id,
      merchant_user_id
    from 
    {catalog}.merchant_promotion_analytical.merchant_insights_customer_interests
    where merchant_user_id in (select distinct merchant_user_id from merchants)
    and par_dt >= cast(date_format(to_timestamp(cast({pardt1} as string), 'yyyyMMdd') - interval 12 months, 'yyyyMMdd') as int)
)
select
    customer_freq.merchant_user_id,
    customer_id,
    case when number_of_transactions > threshold then 'frequent' 
    when number_of_transactions <= threshold then 'less_frequent' end as activity
  from customer_freq
  join merchant_avg 
  on customer_freq.merchant_user_id = merchant_avg.merchant_user_id
union all
select
m_all.merchant_user_id,
m_all.customer_id,
'prospective' as activity
from 
    m_all
    left join merch_cust 
    on 
        m_all.merchant_user_id = merch_cust.merchant_user_id 
        and m_all.customer_id = merch_cust.customer_id
where merch_cust.customer_id is null

  """

# COMMAND ----------
# DBTITLE 1,Print update query
if is_daily_flow() or force_update():
    print(query)

# COMMAND ----------
# DBTITLE 1,Update the table with insights data
if is_daily_flow() or force_update():
    display(spark.sql(query))


# COMMAND ----------
# DBTITLE 1,Optimize the table
if is_daily_flow() or force_update():
    optimizeQuery = f"""
    OPTIMIZE {catalog}.merchant_promotion_analytical.customer_activity
    zorder by (customer_id)
    """

    print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if is_daily_flow() or force_update():
    display(spark.sql(optimizeQuery))


