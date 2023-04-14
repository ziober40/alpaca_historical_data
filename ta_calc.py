import pandas as pd
import ta

bars_data = '/Users/bartziobrowski/Documents/research_and_development/Source/market_data_collector/data/get_bars/get_bars-AAPL-2023-04-04_12-00-00-2023-04-04_12-59-59.csv'

df = pd.read_csv(bars_data)
df = ta.utils.dropna(df)
bars_columns = "open", "high", "low", "close","volume"
df = ta.add_all_ta_features(
    df, bars_columns[0], bars_columns[1], bars_columns[2], bars_columns[3], bars_columns[4], fillna=True
)

print(df.columns)
print(len(df.columns))
df.drop(columns=bars_columns, inplace=True)

