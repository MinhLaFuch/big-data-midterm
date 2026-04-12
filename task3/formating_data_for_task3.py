# This file is created to format baskets.csv into txt file which supports task 4 easier to read (after processing task 3)
import pandas as pd

df = pd.read_csv('baskets.csv')

grouped = df.groupby(['Member_number', 'Date'])['itemDescription'].apply(list).reset_index()

with open('task3_input.txt', 'w', encoding='utf-8') as f:
    for _, row in grouped.iterrows():
        basket_id = f"{row['Member_number']}_{row['Date']}"
        items_str = ",".join(row['itemDescription'])
        f.write(f"{basket_id},{items_str}\n")
