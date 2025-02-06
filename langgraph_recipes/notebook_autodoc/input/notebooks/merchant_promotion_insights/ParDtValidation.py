# Databricks notebook source
def extract_parym(pardt_integer):
    date_str = str(pardt_integer)
    year = date_str[:4]
    month = date_str[4:6]
    parym = int(f"{year}{month}")
    return parym

pardt = int(dbutils.widgets.get("pardt").strip())
initial_pardt = int(dbutils.widgets.get("initial_pardt")) #20240601


from datetime import datetime
import pytz

cur_pardt = int(datetime.now(pytz.timezone('Europe/Athens')).strftime("%Y%m%d"))

print("MerchantPromotionInsightsUpdate was requested to run for day %d"%(pardt))
print("Today is  %d"%(cur_pardt))

if (pardt >= cur_pardt) or (pardt == -1):
    raise Exception("You either did not provide the par_dt parameter which is mandatory, or requested a par_dt later or equal to today. MerchantPromotionInsightsUpdate is not allowed to run for today's data or later. Please check your inputs.")

if initial_pardt != -1:
    if extract_parym(pardt) != extract_parym(initial_pardt):
        raise Exception("Pardt range was not provided in the same year and month. Not all tasks can be properly completed. Please provide pardt values for pardt and initial_pardt in the same year and month.")

print("MerchantPromotionInsightsUpdate can proceed for execution.")
