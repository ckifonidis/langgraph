# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics

# COMMAND ----------
# DBTITLE 1,Parse Workflow Parameters
catalog = get_catalog()
parym_range = get_parym_range_condition()
pardt_range = get_pardt_range_condition()
merchant_user_id_condition = parse_merchant_condition("merchant_user_id")
(pardt1, pardt2) = get_pardt_integer_range()

# COMMAND ----------
# DBTITLE 1,Build Query

query = f"""
INSERT INTO {catalog}.merchant_promotion_analytical.merchant_insights_customer
REPLACE WHERE {pardt_range} {merchant_user_id_condition}
select /*+ RANGE_JOIN(trns, 10) */
    muser.merchant_user_id,
    trns.merchant_id,
    trns.customer_id,
    trns.number_of_transactions,
    trns.amount, 
    muser.mcc,
    trns.ecommerce_ind,
    trns.payment_type as card_type,
    cust.age,
    case when age <=24 then '18-24'
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
    cust.sms_ind,
    cust.go4more_ind,
    cust.ibank_ind,
    cust.shopping_interests,
    case when act.activity is null or act.activity='' then 'other_category' else act.activity end as activity,
    promo.promotion_id,
    g4m.actualcollectedpoints as rewarded_points,
    g4m.rewarded_amount as rewarded_amount,
    g4m.redeempoints as redeemed_points ,
    g4m.redeemedamount as redeemed_amount,
    trns.par_dt as par_dt
from 
    (
      select 
        merchant_id,
        customer_id,
        number_of_transactions,
        amount, 
        mcc,
        ecommerce_ind,
        payment_type, 
        par_dt
      from {catalog}.merchant_promotion_core.customer_daily_trns
      where {pardt_range} and country = "GRC"
    ) trns
    join (
        select distinct merchantid as merchant_id, merchant_user_id, merchant_mcc as mcc
        from {catalog}.merchant_promotion_core.merchant 
        where is_merchant_user = True and country = "GRC" {merchant_user_id_condition}
    ) muser 
    on trns.merchant_id = muser.merchant_id
    left join 
        (
          select *
          from {catalog}.merchant_promotion_analytical.customer_activity_insights
          where {pardt_range} {merchant_user_id_condition}
        ) act 
    on 
        muser.merchant_user_id = act.merchant_user_id and trns.customer_id = act.customer_id and act.par_dt=trns.par_dt
    left join (
        select 
            customer_id, age, home_location, home_municipality, home_regional_unit, home_region,
            work_location, work_municipality, work_regional_unit, work_region, gender, occupation,
            annual_income,nbg_segment, sms_ind, case when array_contains(memberships, "Go4More") then True else False end as go4more_ind,ibank_ind, array_join(collect_list(shopping_interests) ,',') as shopping_interests, par_ym
        from {catalog}.merchant_promotion_analytical.cards_customer_characteristics
        where {parym_range}
        group by 
            customer_id, age, home_location, home_municipality, home_regional_unit, home_region,
            work_location, work_municipality, work_regional_unit, work_region, gender, occupation,
            annual_income,nbg_segment, sms_ind, case when array_contains(memberships, "Go4More") then True else False end,ibank_ind, par_ym
    ) cust
    on 
        trns.customer_id = cust.customer_id and CAST(floor(trns.par_dt / 100) AS INT) = cust.par_ym
    left join 
        (
            select 
            cast(replace(cast(to_date(transactiondate) as string), '-','') as int) as par_dt, 
            filler_2 as merchantid,
            cast(customerkey as string) as customerkey,
            CASE 
                WHEN substr(cardnumbermasked,1,4) IN ('5351','5355','4165','5892','5338','5346') THEN 'Debit'
                WHEN substr(cardnumbermasked,1,4) IN ('5278','5520','5472','4917','4593','4221') THEN 'Credit'
                WHEN substr(cardnumbermasked,1,4) IN ('5162','4423') THEN 'Prepaid' END 
            AS card_type,
            sum(actualcollectedpoints) as actualcollectedpoints, 
            sum(amount) as rewarded_amount, 
            sum(redeemedamount) as redeemedamount, 
            sum(redeempoints) as redeempoints
            from {catalog}.trlog_card.loyalty_business_transactions
            where filler_2 in (
                select distinct merchantid as merchant_id from {catalog}.merchant_promotion_core.merchant where is_merchant_user = True
            ) and cast(replace(cast(to_date(transactiondate) as string), '-','') as int) >= {pardt1} and cast(replace(cast(to_date(transactiondate) as string), '-','') as int) <= {pardt2}
            group by par_dt, filler_2, cast(customerkey as string), CASE 
                WHEN substr(cardnumbermasked,1,4) IN ('5351','5355','4165','5892','5338','5346') THEN 'Debit'
                WHEN substr(cardnumbermasked,1,4) IN ('5278','5520','5472','4917','4593','4221') THEN 'Credit'
                WHEN substr(cardnumbermasked,1,4) IN ('5162','4423') THEN 'Prepaid' END 
        ) g4m
    on 
        trns.par_dt = g4m.par_dt and trns.merchant_id = g4m.merchantid and trns.customer_id = g4m.customerkey and trns.payment_type = g4m.card_type
    left join (
        select 
            pr.promotion_id, customercode, merchant_id, cast(replace(startdate, '-','') as int) as startdate, cast(replace(enddate, '-','') as int) as enddate
        from
            {catalog}.merchant_promotion_engine.mpe_promotions pr
            join {catalog}.merchant_promotion_engine.mpe_promotion_customers c on pr.promotion_id = c.promotion_id
        where 1 = 1 {parse_merchant_condition("pr.merchant_id")}
    ) promo
    on 
        trns.par_dt between promo.startdate and promo.enddate and trns.customer_id = promo.customercode and muser.merchant_user_id = promo.merchant_id
        
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
OPTIMIZE {catalog}.merchant_promotion_analytical.merchant_insights_customer
WHERE {pardt_range} {merchant_user_id_condition}
zorder by (merchant_id, customer_id)
"""

print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if not exclude_analytical_computations():
    display(spark.sql(optimizeQuery))


