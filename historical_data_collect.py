from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.data.requests import StockQuotesRequest
from alpaca.data.requests import StockTradesRequest
from alpaca.data.requests import StockBarsRequest

from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import pandas as pd

import argparse
import os

from tqdm import tqdm


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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbols', type=str, default='GLD,AAPL,ADBE', help='symbol you want to get data for')
    parser.add_argument('--date', type=str, default='2023-04-04', help='date you want to get data for')
    parser.add_argument('--start', type=str, default='12-00-00', help='start time you want to get data for')
    parser.add_argument('--end', type=str, default='12-59-00', help='end time you want to get data for')
    args = parser.parse_args()

    api_key_id = os.environ['ALPACA_API_KEY']
    api_key_secret = os.environ['ALPACA_API_SECRET']
    client = StockHistoricalDataClient(api_key_id, api_key_secret)

    date_start_str = f"{args.date}_{args.start}"
    date_end_str = f"{args.date}_{args.end}"
    symbols = args.symbols.split(',')
    
    data_functions = [get_quotes,get_trades,get_bars]
    
    for f in tqdm(data_functions):
        output = f(client,date_start_str,date_end_str,symbols)
        for symbol in symbols:
            output[output["symbol"] == symbol].to_csv(f'data/{f.__name__}/{f.__name__}-{symbol}-{date_start_str}-{date_end_str}.csv')


