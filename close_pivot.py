import pandas as pd
import argparse
import sys
from tqdm import tqdm
from historical_data_collect import split_dates

def pivot_close(data_path, start_date, end_date, symbol, start_truncate, end_truncate):
    dates = split_dates(start_date, end_date)
    dfs = []
    for date in tqdm(dates):
        try:
            daybar = pd.read_csv(f'{data_path}{date}_00-00-00-{date}_23-59-59/get_bars/get_bars-{symbol}.csv')
        
            daybar['timestamp'] = pd.to_datetime(daybar['timestamp'])
            daybar['time'] = daybar['timestamp'].dt.time
            daybar['date'] = daybar['timestamp'].dt.date
            
            daybar = daybar[(daybar['time'] >= pd.to_datetime(start_truncate).time()) & (daybar['time'] <= pd.to_datetime(end_truncate).time())]
            
            columns = ['time', 'close','date']
            daybar = daybar[columns]
            
            daybar = populate_nan_bins(daybar,start_truncate, end_truncate)
            dfs.append(daybar)
        except Exception as e:
            print(e)
            
    alldfs = pd.concat(dfs)    
    alldfs.reset_index().pivot(index='time', columns='date',values='close' ).to_csv(f'{data_path}{symbol}_{start_date}_{end_date}_close_pivot.csv')

def populate_nan_bins(df, start_str, end_str):
    minute_bin = get_minute_bins(start_str,end_str)
    
    one_day = df
    one_day = one_day.set_index(one_day.time)
    one_day = one_day.drop('time', axis=1)
    oneday_timeline = minute_bin.merge(one_day, left_on='time', right_on='time', how='left')

    if(pd.isnull(oneday_timeline.iloc[0].close)):
        oneday_timeline.close.iloc[0] = oneday_timeline.close.mean()

    oneday_timeline.close = oneday_timeline.close.fillna(method='ffill')
    oneday_timeline.date = oneday_timeline.date.fillna(method='bfill')
    oneday_timeline.date = oneday_timeline.date.fillna(method='ffill')
    return oneday_timeline

def get_minute_bins(start_str, end_str):
    dt_range = pd.date_range(start=f'2023-04-19 {start_str}', end=f'2023-04-19 {end_str}', freq='Min')
    df = pd.DataFrame(index=dt_range)
    df.index = df.index.time
    df = df.rename_axis('time')
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, default='2020-03-01_00-00-00', help='start')
    parser.add_argument('--end_date', type=str, default='2023-04-16_23-59-59', help='end')
    parser.add_argument('--symbol', type=str, default='EURN', help='symbol')
    parser.add_argument('--data_path', type=str, default='/home/wolfie/Source/alpaca_historical_data/data/', help='path to data store')
    parser.add_argument('--truncate_start', type=str, default='13:30:00', help='start')
    parser.add_argument('--truncate_end', type=str, default='20:00:00', help='end')
    args = parser.parse_args()

    pivot_close(args.data_path, args.start_date, args.end_date,args.symbol, args.truncate_start, args.truncate_end)

