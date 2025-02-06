# Databricks notebook source
from datetime import datetime, timedelta
import pytz

import calendar
def extract_parym(pardt_integer):
    date_str = str(pardt_integer)
    year = date_str[:4]
    month = date_str[4:6]
    parym = int(f"{year}{month}")
    return parym

def get_year_for_competitors():
    pardt = int(dbutils.widgets.get("pardt"))#20240630
    date_str = str(pardt)
    year = int(date_str[:4]) - 1
    return year

def fix_last_day(pardt_integer):
    date_str = str(pardt_integer)
    year = int(date_str[:4])
    month = int(date_str[4:6])
    day = int(date_str[6:])
    lastday = calendar.monthrange(year, month)[1]

    actualday = day if day <= lastday else lastday
    return int(f"{str(year).zfill(4)}{str(month).zfill(2)}{str(actualday).zfill(2)}")


def extract_monthly_pardt(pardt_integer, start_range):
    date_str = str(pardt_integer)
    year = date_str[:4]
    month = date_str[4:6]

    lastday = calendar.monthrange(int(year), int(month))[1]

    pardt = int(f"{year}{month}01") if start_range else int(f"{year}{month}{lastday}")
    return pardt

def parse_merchant_condition(merchant_user_id_table_field_name):
    merchant_user_id = dbutils.widgets.get("merchant_user_id") # Default -1, update for all
    merchant_user_id_condition = ""
    if merchant_user_id != "-1":
        merchant_user_id_condition += f" and {merchant_user_id_table_field_name} = \"{merchant_user_id}\""
    return merchant_user_id_condition

def is_daily_flow():
    pardt = int(dbutils.widgets.get("pardt"))#20240630
    initial_pardt = int(dbutils.widgets.get("initial_pardt")) #20240601

    tz = pytz.timezone('Europe/Athens')

    # Get the current time in the specified timezone
    now = datetime.now(tz)

    # Subtract one day
    previous_day = now - timedelta(days=1)

    # Format the result in yyyyMMdd format
    pardt_prev_day = int(previous_day.strftime('%Y%m%d'))

    return True if (pardt_prev_day == pardt) and (initial_pardt == -1) else False


def force_update():
    return True if dbutils.widgets.get("force_update").lower() == "true" else False


def get_catalog():
    return dbutils.widgets.get("catalog")

def get_pardt_integer_range():
    pardt = int(dbutils.widgets.get("pardt"))#20240630
    initial_pardt = int(dbutils.widgets.get("initial_pardt")) #20240601

    if (initial_pardt == -1) or (initial_pardt > pardt):
        pardt1 = pardt
        pardt2 = pardt
    else:
        pardt1 = initial_pardt
        pardt2 = pardt

    pardt2 = fix_last_day(pardt2)

    return (pardt1, pardt2)

def get_pardt_range_condition():
    (pardt1, pardt2) = get_pardt_integer_range()

    return f" par_dt >= {pardt1} and par_dt <= {pardt2} "

def get_monthly_pardt_range_condition():
    (pardt1, pardt2) = get_pardt_integer_range()
    pardt1_month = extract_monthly_pardt(pardt1, True)
    pardt2_month = extract_monthly_pardt(pardt2, False)

    return f" par_dt >= {pardt1_month} and par_dt <= {pardt2_month} "

def get_monthly_pardt_daily_range_condition():
    (pardt1, pardt2) = get_pardt_integer_range()
    pardt1_month = extract_monthly_pardt(pardt1, True)
    pardt2_month = pardt2
    return f" par_dt >= {pardt1_month} and par_dt <= {pardt2_month} "

def get_parym_range_condition():
    (pardt1, pardt2) = get_pardt_integer_range()
    parym1 = extract_parym(pardt1)
    parym2 = extract_parym(pardt2)
    return f" par_ym >= {parym1} and par_ym <= {parym2} "

def generic_update():
    return True if dbutils.widgets.get("merchant_user_id") == "-1" else False


def exclude_insights_computations():
    return True if dbutils.widgets.get("analytical_only").lower() == "true" else False

def exclude_analytical_computations():
    return True if dbutils.widgets.get("insights_only").lower() == "true" else False

def compute_competitors():
    (pardt1, pardt2) = get_pardt_integer_range()

    initial_pardt = str(pardt1)
    pardt = str(pardt2)

    if is_daily_flow() and pardt.endswith("0101"):
        return True
    elif (101 >= int(initial_pardt[4:])) and (101 <= int(pardt[4:])):
        return True
    else:
        return False
