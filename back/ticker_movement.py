import json
import os
from pathlib import Path
import pandas as pd


data_directory = Path('./data')
input_path = data_directory / 'comments_count.csv'

df = pd.read_csv(input_path)

with open('./config/ASX.json') as f:
    tickers = set(json.load(f))

df_hype = pd.DataFrame(index=tickers, columns=['1 Day Change','2 Day Change','3 Day Change'])

def change(counts, days=1):
    return (counts[0]-counts[0+days]) / counts[0+days]

for idx,row in df.iterrows():
    df_hype.at[row.values[0], '1 Day Change'] = change(row.values[1:])
    df_hype.at[row.values[0], '2 Day Change'] = change(row.values[1:], days=2)
    df_hype.at[row.values[0], '3 Day Change'] = change(row.values[1:], days=3)

print(df_hype)

output_path = data_directory / 'comments_movement.csv'
df_hype.to_csv(output_path, index=True)
