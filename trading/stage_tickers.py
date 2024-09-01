import yfinance as yf
from trading import db_connection
import pandas as pd
from datetime import datetime, timedelta

def get_stock_prices_one_ticker(ticker, end_date, start_date):
    data = yf.download(ticker, start_date, end_date)
    return data

def get_latest_crawl_date(engine):
    query = """
    SELECT date from stg.scd_control_table
    where last = 1
    """
    try:
        latest_crawl_date = pd.read_sql(query, engine)["date"]
        current_crawl_date = latest_crawl_date.values[0] + timedelta(days=1)
    except Exception as e:
        current_crawl_date = datetime.now().date() - timedelta(days=1)
    return current_crawl_date.strftime("%Y-%m-%d")

def stage_all_tickers():
    all_tickers = ["AAPL", "MSFT", "AMZN", "GOOGL"]
    engine = db_connection.setup_connection()
    current_crawl_date = get_latest_crawl_date(engine)
    with engine.begin() as connection:
        for ticker in all_tickers:
            data = get_stock_prices_one_ticker(ticker, current_crawl_date, current_crawl_date)
            data["ticker"] = ticker
            try:
                data.to_sql('sdf_stock_prices', engine, schema = 'stg', if_exists = 'append')
            except Exception as e:
                print(e)

stage_all_tickers()