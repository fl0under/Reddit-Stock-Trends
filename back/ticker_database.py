import datetime as dt
import json
import os
from pathlib import Path
import pandas as pd


data_directory = Path('./data')
data_directory.mkdir(parents=True, exist_ok=True)

with open('./config/ASX.json') as f:
    tickers = set(json.load(f))

df = pd.DataFrame(index=tickers)

try:
    with os.scandir(data_directory) as it:
        comment_csvs = list(
                sorted(
                    filter(lambda x: x.name.endswith('comments_df.csv') and x.is_file(), it)
                    ,key=lambda x: x.name[:10], reverse=True)
                )
except IndexError as e:
    print('No existing *comments_df.csv files found.')


for csv in comment_csvs:
    df = df.merge(
            pd.read_csv(csv).set_index('Ticker').rename(columns={'Mentions': csv.name[:10]}),
            how='outer', left_index=True, right_index=True
            )

print(df)

output_path = data_directory / 'comments_count.csv'
df.to_csv(output_path, index=True)
