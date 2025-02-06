# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics
# COMMAND ----------

query = """
INSERT INTO {catalog}.merchant_promotion_insights.statistics_raw 
REPLACE WHERE statistics_type = "competition" and {pardt_conditions} {merchant_user_id_condition}
with merchant_data as (
	select
        	    distinct
              merchant_user_id, 
              retailer,
              merchant_mcc as mcc,
            	case
            	    when merchant_name='DUMMY1-ROCOCO' then 'ROCOCO'
            	    when merchant_name='DUMMY2-E FRESH' then 'E FRESH'
            	    when merchant_name='DUMMY3-IKEA' then 'IKEA'
            	    when merchant_name='DUMMY4-PUBLIC' then 'PUBLIC'
            	    when merchant_name='DUMMY5-INTERSPORT' then 'INTERSPORT'
            	    when merchant_name='DUMMY6-TOYS-SHOP' then 'TOYS-SHOP'
            	    when merchant_name='DUMMY7-SPARKLEAN' then 'SPARKLEAN'
            	    when merchant_name='DUMMY8-KRETYAX' then 'KRETYAX'
            	    when merchant_name='DUMMY9-PLAISIO' then 'PLAISIO COMPUTERS'
            	    else merchant_name
            	end as merchant_name
            from 
              (
                select merchant_user_id, retailer_info_id, merchant_mcc, merchant_name 
                from {catalog}.merchant_promotion_core.merchant
                where is_merchant_user = True
                {merchant_user_id_condition} 
              ) m
              inner join 
              {catalog}.merchant_promotion_core.retailer_info
              on m.retailer_info_id = retailer_info.uuid
            
),
mcc_top_comp as (
  select * from {catalog}.merchant_promotion_analytical.mcc_top_competitors
  where year = {year}
),
total_competitors as (
	select mcc, count(top_rank) as tc
	from mcc_top_comp
	where mcc in (
        select mcc from merchant_data
    )
  group by mcc
),
merchants_position as (
	select
    mtc.mcc, 
    mtc.brand as retailer,
    case 
      when top_rank <= 10 then 'top_10'
      when top_rank >= total_competitors.tc-10 then 'last'
      else 'other'
    end as place,
	  top_rank as position_rank
  from 
    mcc_top_comp mtc
    inner join  
    total_competitors
    on mtc.mcc = total_competitors.mcc
    inner join 
    merchant_data md
    on mtc.mcc = md.mcc and mtc.brand = md.merchant_name
),
competitors_from_mcc as (
	select distinct merchant_Data.merchant_user_id , merchant_data.mcc, mcc_top_comp.brand as comp_retailer
	from 
    merchant_data
  inner join 
    merchants_position
  on 
    merchant_data.mcc = merchants_position.mcc 
    and merchant_data.retailer = merchants_position.retailer 
    and merchants_position.place = 'other'
  inner join
    mcc_top_comp
  on 
    mcc_top_comp.mcc = merchant_data.mcc
    and mcc_top_comp.top_rank between merchants_position.position_rank -5 and merchants_position.position_rank +5
    and mcc_top_comp.brand != merchant_data.retailer

	union all

  select  merchant_user_id, mcc, comp_retailer from (
    select distinct merchant_Data.merchant_user_id , merchant_data.mcc, mcc_top_comp.brand as comp_retailer, mcc_top_comp.top_rank
    from 
      merchant_data 
    inner join 
      merchants_position
    on 
      merchant_data.mcc = merchants_position.mcc 
      and merchant_data.retailer = merchants_position.retailer 
      and merchants_position.place = 'top_10'
    inner join
      mcc_top_comp
    on 
      mcc_top_comp.mcc = merchant_data.mcc
      and mcc_top_comp.brand != merchant_data.retailer
  )
  qualify row_number() over (partition by merchant_user_id, mcc order by top_rank) <= 10

	union all

  select merchant_user_id, mcc, comp_Retailer from (
	  select distinct merchant_data.merchant_user_id, merchant_data.mcc, mcc_top_comp.brand as comp_retailer, mcc_top_comp.top_rank
    from 
      merchant_data
    inner join 
      merchants_position
    on 
      merchant_data.mcc = merchants_position.mcc 
      and merchant_data.retailer = merchants_position.retailer 
      and merchants_position.place = 'last'
    inner join
      mcc_top_comp
    on 
      mcc_top_comp.mcc = merchant_data.mcc
      and mcc_top_comp.brand != merchant_data.retailer
  )
  qualify row_number() over (partition by merchant_user_id, mcc order by top_rank desc) <= 10
),
competitors as (
    select mcomp.merchant_user_id, mcomp.brand as comp_retailer, count(*) over(partition by mcomp.merchant_user_id) as total 
    from 
        {catalog}.merchant_promotion_core.merchant_competitors as mcomp
    inner join 
        merchant_data
    on merchant_data.merchant_user_id = mcomp.merchant_user_id


  	union all

	  select merchant_user_id, comp_retailer, count(*) over(partition by merchant_user_id) as total 
    from competitors_from_mcc
	  where merchant_user_id not in (
      SELECT distinct merchant_user_id FROM {catalog}.merchant_promotion_core.merchant_competitors
    )
),
competitors_with_mcc as (
    select 
        competitors.merchant_user_id, comp_retailer, total, mcc
    from competitors inner join merchant_data
    on merchant_data.merchant_user_id = competitors.merchant_user_id
),
competitor_stores as (
    select 
        merchant_user_id, mas.merchant_id as comp_merchant_id, cmcc.total as total
    from 
        competitors_with_mcc as cmcc inner join 
        (
            select distinct mcc, retailer, merchant_id from  
            {catalog}.merchant_promotion_analytical.merchant_all_statistics 
            where {pardt_conditions} and country = "GRC"
        ) as mas
        on mas.retailer = cmcc.comp_retailer
    where not exists (
        select 1
        from 
            {catalog}.merchant_promotion_core.merchant innm 
        where is_merchant_user = true 
        and innm.merchant_user_id = cmcc.merchant_user_id and innm.merchantid = mas.merchant_id
    )
)
select
    ifnull(mas.par_dt, masi.par_dt) as par_dt, 
    mas.merchant_user_id as merchant_user_id,
    "competition" as statistics_type,
    ifnull(mas.customer_id, masi.customer_id) as customer_id,
    cast(mas.total as string) as merchant_id, 
    mas.gender as gender,
    mas.home_location as home_location,
    mas.home_municipality as home_municipality,
    mas.home_regional_unit as home_regional_unit,
    mas.home_region as home_region,
    mas.work_location as work_location,
    mas.work_municipality as work_municipality,
    mas.work_regional_unit as work_regional_unit,
    mas.work_region as work_region,
    mas.age_group as age_group, 
    mas.age as age,
    mas.occupation as occupation, 
    mas.nbg_segment as nbg_segment, 
    Cast(null as string) as promotion_id, 
    mas.shopping_interests as shopping_interests,
    Cast(null as string) as activity, 
    masi.spending_profile as spending_profile,
    masi.channel_preference as channel_preference,
    mas.ecommerce_ind as ecommerce_ind,
    mas.card_type as card_type,
    cast(null as boolean) as sms_ind,
    cast(null as boolean) as go4more_ind,
    cast(null as boolean) as ibank_ind,
    mas.number_of_transactions as number_of_transactions,
    mas.amount as amount, 
    cast(null as bigint) as rewarded_points,
    cast(null as bigint) as redeemed_points, 
    cast(null as decimal(38,2)) as rewarded_amount, 
    cast(null as decimal(38,2)) as redeemed_amount
from 
      (
        select /*+ BROADCAST(cs) */ mas_.par_dt, cs.merchant_user_id, mas_.customer_id, mas_.gender, mas_.home_location, mas_.home_municipality,
                mas_.home_regional_unit, mas_.home_region, mas_.work_location, mas_.work_municipality, mas_.work_regional_unit,
                mas_.work_region, mas_.age_group, mas_.age, mas_.occupation, mas_.nbg_segment, mas_.shopping_interests,
                mas_.ecommerce_ind, mas_.card_type, sum(mas_.number_of_transactions) as number_of_transactions, 
                sum(cast(mas_.amount as decimal(38,2))) as amount, cs.total
        from
          (select
                par_dt,
                mcc,
                retailer,
                merchant_id,
                customer_id,
                gender,
                home_location,
                home_municipality,
                home_regional_unit,
                home_region,
                work_location,
                work_municipality,
                work_regional_unit,
                work_region,
                age_group, 
                age,
                occupation, 
                nbg_segment, 
                shopping_interests,
                ecommerce_ind,
                card_type,
                number_of_transactions, 
                amount 
           from {catalog}.merchant_promotion_analytical.merchant_all_statistics 
          where {pardt_conditions} and country = "GRC"
          ) mas_
          inner join (select * from competitor_stores) as cs 
          on cs.comp_merchant_id = mas_.merchant_id
          group by mas_.par_dt, cs.merchant_user_id, cs.total, mas_.customer_id, mas_.gender, mas_.home_location, mas_.home_municipality,
                mas_.home_regional_unit, mas_.home_region, mas_.work_location, mas_.work_municipality, mas_.work_regional_unit,
                mas_.work_region, mas_.age_group, mas_.age, mas_.occupation, mas_.nbg_segment, mas_.shopping_interests,
                mas_.ecommerce_ind, mas_.card_type 
      ) as mas
    left outer join 
      (
        select /*+ BROADCAST(cs) */ distinct cs.merchant_user_id, masi_.customer_id, masi_.par_dt, masi_.spending_profile, masi_.channel_preference
        from
          (select * from {catalog}.merchant_promotion_analytical.merchant_all_statistics_interests where {pardt_conditions}) masi_ 
          inner join (select * from competitor_stores) as cs 
          on cs.comp_merchant_id = masi_.merchant_id        
      ) as masi
    on 
        mas.par_dt = masi.par_dt and
        mas.merchant_user_id = masi.merchant_user_id and 
        mas.customer_id = masi.customer_id 
  """

# COMMAND ----------

# DBTITLE 1,Check if run refers to historical data update and set flag
from datetime import datetime, timedelta
import pytz

catalog = get_catalog()
prev_year = get_year_for_competitors()
pardt_conditions = get_pardt_range_condition()
merchant_user_id_condition = parse_merchant_condition("merchant_user_id")

# COMMAND ----------
if not exclude_insights_computations():
    print(query.format(catalog=catalog, pardt_conditions=pardt_conditions, year = prev_year, merchant_user_id_condition = merchant_user_id_condition))

# COMMAND ----------
# DBTITLE 1,Update the table
if not exclude_insights_computations():
    display(spark.sql(query.format(catalog=catalog, pardt_conditions=pardt_conditions, year = prev_year, merchant_user_id_condition = merchant_user_id_condition)))


# COMMAND ----------
# DBTITLE 1,Optimize the table
if not exclude_insights_computations():
    optimizeQuery = f"""
    OPTIMIZE {catalog}.merchant_promotion_insights.statistics_raw 
    WHERE statistics_type = "competition" and {pardt_conditions} {merchant_user_id_condition}
    zorder by (gender, age, shopping_interests, home_municipality)
    """
    print(optimizeQuery)

# COMMAND ----------
# DBTITLE 1,Optimize the table
if not exclude_insights_computations():
    display(spark.sql(optimizeQuery))


