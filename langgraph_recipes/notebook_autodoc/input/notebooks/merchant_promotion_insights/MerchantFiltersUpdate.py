# Databricks notebook source
# DBTITLE 1,Load Generics Notebook
%run ./Generics
# COMMAND ----------

query = """
INSERT OVERWRITE {catalog}.merchant_promotion_insights.merchant_filters
select * from (
select distinct 
    "location_tree" as filter_id,
    municipality as filter_value_id, 
    "home or work location" as filter_category_description,
    case when municipality='ΝΕΟ ΗΡΑΚΛΕΙΟ' then 'ΗΡΑΚΛΕΙΟ' else municipality end as filter_value_description, 
    concat('ΠΕ ',regional_unit) as parent_filter_id,
    true as greek_ind, 
    null as merchant_user_id,
    now() as c_timestamp
from 
    {catalog}.merchant_promotion_core.postal_codes

union all

select distinct 
    "location_tree" as filter_id,
    concat('ΠΕ ',regional_unit) as filter_value_id, 
    "home or work location" as filter_category_description,
    regional_unit as filter_value_description, 
    region as parent_filter_id,
    true as greek_ind, 
    null as merchant_user_id,
    now() as c_timestamp
from 
    {catalog}.merchant_promotion_core.postal_codes

union all

select distinct 
    "location_tree" as filter_id,
    region as filter_value_id, 
    "home or work location" as filter_category_description,
    region as filter_value_description, 
    null as parent_filter_id,
    true as greek_ind, 
    null as merchant_user_id,
    now() as c_timestamp
from 
    {catalog}.merchant_promotion_core.postal_codes

union all

select distinct 
    "store" as filter_id,
    ms.store_id as filter_value_id,  
    "stores" as filter_category_description,
    trim(ms.name) as filter_value_description,
    null as parent_filter_id,
    false as greek_ind,
    m.internal_code as merchant_user_id,
    now() as c_timestamp
from 
        {catalog}.merchant_promotion_engine.mpe_merchant_stores ms
    inner join 
        {catalog}.merchant_promotion_engine.mpe_merchants m
    on ms.merchant_id = m.merchant_id
where 
    status='Active' 

union all

select 
    'promotion' as filter_id,
    promotion_id as filter_value_id, 
    'promotions' as filter_category_description,
    concat(name,',',startdate) as filter_value_description,
    null as parent_filter_id,
    false as greek_ind,
    merchant_id as merchant_user_id,
    now() as c_timestamp
from {catalog}.merchant_promotion_engine.mpe_promotions
where status='Completed'

union all


select distinct 'shopint' as filter_id, 
sector as filter_value_id,
'shopping interests' as filter_category_description,
case when hyper_category = 'Automotive & Fuel Products' then 'Αυτοκίνητο & Καύσιμα'
when hyper_category = 'Education' then 'Εκπαίδευση'
when hyper_category = 'Fashion, Cosmetics & Jewelry' then 'Ένδυση, Ομορφιά & Κόσμημα'
when hyper_category = 'Electronics & Household Appliances' then 'Ηλεκτρονικά & Οικιακές Συσκευές'
when hyper_category = 'Pets' then 'Κατοικίδια'
when hyper_category = 'Toys' then 'Παιχνίδια'
when hyper_category = 'Wellness & Personal Care' then 'Προσωπική Φροντίδα'
when hyper_category = 'Home & Garden' then 'Σπίτι & Κήπος'
when hyper_category = 'Travel & Transportation' then 'Ταξίδι & Μεταφορικά Μέσα'
when hyper_category = 'Telecommunication' then 'Τηλεπικοινωνίες'
when hyper_category = 'Tourism' then 'Τουρισμός'
when hyper_category = 'Food & Drinks' then 'Τρόφιμα, Ποτά & Supermarket'
when hyper_category = 'Restaurants, Bars, Fast Food & Coffee' then 'Υπηρεσίες Εστίασης'
when hyper_category = 'Health & Medical Care' then 'Υπηρεσίες Υγείας'
when hyper_category = 'Entertainment & Hobbies' then 'Ψυχαγωγία & Hobbies'
end as filter_value_description,
null as parent_filter_id,
true as greek_ind, 
null as merchant_user_id,
now() as c_timestamp
from {catalog}.merchant_promotion_core.mcc_categories


union all

select distinct 'locm' as filter_id, 
municipality as filter_value_id, 
'municipality' as filter_category_description,
municipality as filter_value_description, 
null as parent_filter_id,
true as greek_ind, 
null as merchant_user_id,
now() as c_timestamp
from {catalog}.merchant_promotion_core.postal_codes

union all

select distinct 'locru' as filter_id, 
regional_unit as filter_value_id, 
'regional_unit' as filter_category_description,
regional_unit as filter_value_description, 
null as parent_filter_id,
true as greek_ind,
null as merchant_user_id,
now() as c_timestamp
from {catalog}.merchant_promotion_core.postal_codes

union all

select distinct 'locr' as filter_id, 
region as filter_value_id, 
'region' as filter_category_description,
region as filter_value_description, 
null as parent_filter_id,
true as greek_ind, 
null as merchant_user_id,
now() as c_timestamp
from {catalog}.merchant_promotion_core.postal_codes
) where filter_value_id is not null



  """

# COMMAND ----------

# DBTITLE 1,Check if run refers to historical data update and set flag
catalog = get_catalog()

# COMMAND ----------

if is_daily_flow() or force_update():
    print(query.format(catalog=catalog))

# COMMAND ----------

# DBTITLE 1,If daily run, update the table
if is_daily_flow() or force_update():
    display(spark.sql(query.format(catalog=catalog)))

# COMMAND ----------

# DBTITLE 1,If daily run, optimize the table
if is_daily_flow() or force_update():
    optimizeQuery = "OPTIMIZE {catalog}.merchant_promotion_insights.merchant_filters zorder by (filter_id, filter_value_id)"
    display(spark.sql(optimizeQuery.format(catalog=catalog)))




