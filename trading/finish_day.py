from trading import db_connection
import pandas as pd
from datetime import datetime, timedelta

def get_latest_crawl_date(engine):
    query = """
    select count(*) from stg.scd_control_table ct
    order by ct.date desc
    limit 1;
    """
    try:
        latest_crawl_date = pd.read_sql(query, engine)["date"]
        current_crawl_date = latest_crawl_date.values[0] + timedelta(days=1)
    except Exception as e:
        current_crawl_date = datetime.now().date() - timedelta(days=1)
    return current_crawl_date

def finish_day():
    engine = db_connection.setup_connection()
    current_crawl_date = get_latest_crawl_date(engine)
    query = f"""
    select * from stg.sdf_stock_prices
    where sdf_stock_prices.Date = {current_crawl_date};
    """
    df_todays_crawl = pd.read_sql(query, engine)
    print(current_crawl_date)
    print(df_todays_crawl)

finish_day()