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
INSERT INTO {catalog}.merchant_promotion_analytical.merchant_all_statistics 
REPLACE WHERE {pardt_range}
select
    trns.merchant_id,
    trns.customer_id,
    trns.number_of_transactions,
    trns.amount,
    md.mcc as mcc,
    trns.ecommerce_ind,
    trns.country,
    trns.payment_type as card_type,
    cust.age,
    case
        when age <=24 then '18-24'
        when age <=40 then '25-40'
        when age <=56 then '41-56'
        when age <=75 then '57-75'
        when age <=96 then '76-96'
    end as age_group,
    cust.home_location,
    cust.home_municipality,
    cust.home_regional_unit,
    cust.home_region,
    cust.work_location,
    cust.work_municipality,
    cust.work_regional_unit,
    cust.work_region,
    cust.gender,
    cust.occupation,
    cust.annual_income,
    cust.nbg_segment,
    case
        when cust.shopping_interests is null or cust.shopping_interests='' then 'other_category'
        else cust.shopping_interests
    end as shopping_interests,
    md.retailer as retailer,
    md.brand as brand,
    trns.par_dt
from (
    select 
        merchant_id,
        customer_id,
        number_of_transactions,
        amount,
        ecommerce_ind,
        country,
        payment_type, 
        par_dt
    FROM
        {catalog}.merchant_promotion_core.customer_daily_trns
    WHERE {pardt_range}
) trns
left join (
    select
        par_ym, customer_id, age, home_location, home_municipality, home_regional_unit, home_region,
        work_location, work_municipality, work_regional_unit, work_region, gender, occupation,
        annual_income,nbg_segment, array_join(collect_list(shopping_interests), ',') as shopping_interests
    from {catalog}.merchant_promotion_analytical.cards_customer_characteristics
    WHERE {parym_range}
    group by
        par_ym, customer_id, age, home_location, home_municipality, home_regional_unit, home_region,
        work_location, work_municipality, work_regional_unit, work_region, gender, occupation,
        annual_income,nbg_segment
) cust
on trns.customer_id = cust.customer_id
and CAST(floor(trns.par_dt / 100) AS INT) = cust.par_ym
left join (
    select distinct m.merchantid as mid, r.retailer, r.brand , m.mcc
    from {catalog}.merchant_promotion_core.merchant m
    left join
    {catalog}.merchant_promotion_core.retailer_info r
    on m.retailer_info_id = r.uuid
) md
on trns.merchant_id = md.mid
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
OPTIMIZE {catalog}.merchant_promotion_analytical.merchant_all_statistics 
WHERE {pardt_range}
zorder by (brand, retailer, merchant_id, customer_id)
"""

print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if generic_update():
    display(spark.sql(optimizeQuery))


