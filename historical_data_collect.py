from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockQuotesRequest
from alpaca.data.requests import StockTradesRequest
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from pandas_market_calendars import get_calendar

from datetime import datetime
import pandas as pd

import argparse
import os

from tqdm import tqdm

import ta

def get_bars(client, date_start_str, date_end_str, symbols):

    request_params = StockBarsRequest(
                        symbol_or_symbols=symbols,
                        timeframe=TimeFrame.Minute,
                        start=datetime.strptime(date_start_str, '%Y-%m-%d_%H-%M-%S'),
                        end=datetime.strptime(date_end_str,'%Y-%m-%d_%H-%M-%S'), adjustment='all'
                 )

    multisymbol_bars = client.get_stock_bars(request_params)

    return multisymbol_bars.df.reset_index()

def get_trades(client, date_start_str, date_end_str, symbols):

    request_params = StockTradesRequest(symbol_or_symbols=symbols,
                        timeframe=TimeFrame.Day,
                        start=datetime.strptime(date_start_str, '%Y-%m-%d_%H-%M-%S'),
                        end=datetime.strptime(date_end_str,'%Y-%m-%d_%H-%M-%S'))
    multisymbol_trades = client.get_stock_trades(request_params)

    return multisymbol_trades.df.reset_index()

def get_quotes(client, date_start_str, date_end_str, symbols):

    request_params = StockQuotesRequest(symbol_or_symbols=symbols,
                        timeframe=TimeFrame.Day,
                        start=datetime.strptime(date_start_str, '%Y-%m-%d_%H-%M-%S'),
                        end=datetime.strptime(date_end_str,'%Y-%m-%d_%H-%M-%S'))
    multisymbol_quotes = client.get_stock_quotes(request_params)

    return multisymbol_quotes.df.reset_index()

def get_ta(client={}, date_start_str="", date_end_str="", symbols=[]):
    for symbol in symbols:
        #TODO: check if file exist, if not generate it
        df = pd.read_csv(f"data/{date_start_str}-{date_end_str}/get_bars/get_bars-{symbol}.csv")
        df = ta.utils.dropna(df)
        bars_columns = ["open", "high", "low", "close","volume"]
        df = ta.add_all_ta_features(
            df, bars_columns[0], bars_columns[1], bars_columns[2], bars_columns[3], bars_columns[4], fillna=True
        )
    df.drop(columns=bars_columns+["trade_count","vwap"], inplace=True)
    return df

def split_dates(date_start_str, date_end_str):
    start_date = datetime.strptime(date_start_str, '%Y-%m-%d_%H-%M-%S')
    end_date = datetime.strptime(date_end_str, '%Y-%m-%d_%H-%M-%S')

    nyse = get_calendar('NYSE')
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)

    dates = schedule.index.strftime('%Y-%m-%d')

    return dates

def factory(func_name):
    if(func_name == "get_bars"):
        return get_bars
    elif(func_name == "get_trades"):
        return get_trades
    elif(func_name == "get_quotes"):
        return get_quotes
    elif(func_name == "get_ta"):
        return get_ta
    else:
        return

def execute_calls(data_functions,date_start_str,date_end_str):
    for f in data_functions:
        output = factory(f)(client,date_start_str,date_end_str,symbols)
        for symbol in symbols:
            directory_path = f'data/{date_start_str}-{date_end_str}/{f}'
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
            output[output["symbol"] == symbol].to_csv(f'{directory_path}/{f}-{symbol}.csv')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbols', type=str, default='AAPL', help='symbol you want to get data for')
    parser.add_argument('--start', type=str, default='2023-03-01_12-00-00', help='start time you want to get data for')
    parser.add_argument('--end', type=str, default='2023-03-31_12-59-59', help='end time you want to get data for')
    parser.add_argument('--data', type=str, default='get_bars,get_ta', help='provide the comma limited function names: get_bars, get_trades, get_quotes, get_ta')
    parser.add_argument('--split_dates', action='store_true', help='split dates')
    args = parser.parse_args()

    api_key_id = os.environ['ALPACA_API_KEY']
    api_key_secret = os.environ['ALPACA_API_SECRET']
    client = StockHistoricalDataClient(api_key_id, api_key_secret)

    split_date = args.split_dates

    date_start_str = f"{args.start}"
    date_end_str = f"{args.end}"
    symbols = args.symbols.split(',')
    data_functions = args.data.split(',')

    split_date = True
    if(split_date):
        dates = split_dates(date_start_str, date_end_str)
        for date in tqdm(dates):
            execute_calls(data_functions, date+"_12-00-00",date+"_12-59-59")
    else:
        execute_calls(data_functions,date_start_str,date_end_str)


