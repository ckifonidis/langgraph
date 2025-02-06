# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics
# COMMAND ----------

query = """
INSERT INTO {catalog}.merchant_promotion_insights.statistics_raw 
REPLACE WHERE statistics_type = "analytics" and {partition_conditions} {merchant_user_id_condition}
select distinct
    ifnull(mic.par_dt, mici.par_dt) as par_dt, 
    ifnull(mic.merchant_user_id, mici.merchant_user_id) as merchant_user_id,
    "analytics" as statistics_type,
    ifnull(mic.customer_id, mici.customer_id) as customer_id,
    mic.merchant_id as merchant_id, 
    mic.gender as gender,
    mic.home_location as home_location,
    mic.home_municipality as home_municipality,
    mic.home_regional_unit as home_regional_unit,
    mic.home_region as home_region,
    mic.work_location as work_location,
    mic.work_municipality as work_municipality,
    mic.work_regional_unit as work_regional_unit,
    mic.work_region as work_region,
    mic.age_group as age_group, 
    mic.age as age,
    mic.occupation as occupation, 
    mic.nbg_segment as nbg_segment, 
    mic.promotion_id as promotion_id, 
    mic.shopping_interests as shopping_interests,
    mic.activity as activity, 
    mici.spending_profile as spending_profile,
    mici.channel_preference as channel_preference,
    mic.ecommerce_ind as ecommerce_ind,
    mic.card_type as card_type,
    mic.sms_ind as sms_ind,
    mic.go4more_ind as go4more_ind,
    mic.ibank_ind as ibank_ind,
    mic.number_of_transactions as number_of_transactions,
    cast(mic.amount as decimal(38,2)) as amount, 
    mic.rewarded_points as rewarded_points,
    mic.redeemed_points as redeemed_points, 
    mic.rewarded_amount as rewarded_amount, 
    mic.redeemed_amount as redeemed_amount
from 
      (
        select *
        from
          {catalog}.merchant_promotion_analytical.merchant_insights_customer
        WHERE 
          {partition_conditions} {merchant_user_id_condition}
      ) as mic
    left outer join 
      (
        select *
        from
          {catalog}.merchant_promotion_analytical.merchant_insights_customer_interests 
        WHERE 
          {partition_conditions} {merchant_user_id_condition}
      ) as mici
    on 
        mic.par_dt = mici.par_dt and 
        mic.merchant_user_id = mici.merchant_user_id and 
        mic.customer_id = mici.customer_id

  """

# COMMAND ----------

# DBTITLE 1,Check if run refers to historical data update and set flag
catalog = get_catalog()
partition_conditions = get_pardt_range_condition()
merchant_user_id_condition = parse_merchant_condition("merchant_user_id")


# COMMAND ----------
if not exclude_insights_computations():
    print(query.format(catalog=catalog, partition_conditions=partition_conditions, merchant_user_id_condition=merchant_user_id_condition))

# COMMAND ----------
# DBTITLE 1,Update the table
if not exclude_insights_computations():
    display(spark.sql(query.format(catalog=catalog, partition_conditions=partition_conditions, merchant_user_id_condition=merchant_user_id_condition)))


# COMMAND ----------
# DBTITLE 1,Optimize the table
if not exclude_insights_computations():
    optimizeQuery = f"""
    OPTIMIZE {catalog}.merchant_promotion_insights.statistics_raw 
    WHERE statistics_type = "competition" and {partition_conditions} {merchant_user_id_condition}
    zorder by (gender, age, shopping_interests, home_municipality)
    """
    print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if not exclude_insights_computations():
    display(spark.sql(optimizeQuery))


