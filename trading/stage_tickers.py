import yfinance as yf
from sqlalchemy import create_engine

def get_stock_prices_one_ticker(ticker, end_date, start_date):
    data = yf.download(ticker, start_date, end_date)
    return data

def stage_all_tickers(current_date):
    all_tickers = ["AAPL", "MSFT", "AMZN", "GOOGL"]
    engine = create_engine('postgresql://postgres:password@localhost:5432/dev')
    for ticker in all_tickers:
        data = get_stock_prices_one_ticker(ticker, current_date, current_date)       
        data.to_sql('sdf_stock_prices', engine, schema = 'stg', if_exists = 'append')


current_date = "2024-08-31"
stage_all_tickers(current_date)
