import yfinance as yf
import pandas as pd

from trading import db_connection, truncate_table

def get_stock_prices_one_ticker(ticker, start_date, end_date):
    data = yf.download(ticker, start_date, end_date)
    return data

def get_all_tickers(engine):
    query = """
    select distinct ticker from stg.smd_tickers_to_crawl ctc
    """
    try:
        df_all_tickers = pd.read_sql(query, engine)
    except Exception as e:
        df_all_tickers = pd.DataFrame()
        print(e)
    return list(df_all_tickers.ticker)

def stage_all_tickers():
    engine = db_connection.setup_connection()

    all_tickers = get_all_tickers(engine)
    truncate_table.truncate_table(engine, 'stg', 'sdf_stock_prices')

    start_date = '2000-01-01'
    end_date = '2099-12-31'

    for ticker in all_tickers:
        data = get_stock_prices_one_ticker(ticker, start_date, end_date)
        data["ticker"] = ticker
        data.index.name = 'dat_date'
        data.columns = ['dec_open', 'dec_high', 'dec_low', 'dec_close', 'dec_adj_close', 'int_volume', 'str_ticker']
        try:
            data.to_sql('sdf_stock_prices', engine, schema = 'stg', if_exists = 'append')
        except Exception as e:
            print(e)

stage_all_tickers()