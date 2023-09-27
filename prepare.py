import pandas as pd
import numpy as np
import os

import acquire

import matplotlib.pyplot as plt

from env import get_connection

# def get_tsa():
#     query = '''
#             SELECT sale_date, sale_amount,
#             item_brand, item_name, item_price,
#             store_address, store_zipcode,
#             store_city, store_state
#             FROM sales
#             LEFT JOIN items USING(item_id)
#             LEFT JOIN stores USING(store_id)
#             '''

#     url = get_connection('tsa_item_demand')

#     df = pd.read_sql(query, url)
    
#     df.to_csv('stores.csv')
    
#     return df


def prep_tsa(df):
    
    df.sale_date = pd.to_datetime(df.sale_date)
    
    df = df.set_index('sale_date')
    
    df = df.sort_values('sale_date')
    
    df['month'] = df.index.month_name()
    
    df['day_of_week'] = df.index.day_name()
    
    df['sales_total'] = df.sale_amount * df.item_price
    
    return df


def tsa_pipeline(force_sql=False):
    
    if force_sql or not os.path.isfile('stores.csv'):
        # If force_sql is True or the CSV file does not exist, read fresh data from the database into a DataFrame.
        df = get_tsa()
    else:
        # Otherwise, if the CSV file exists, read in data from the CSV file.
        df = pd.read_csv('stores.csv', index_col=0)
    
    df = prep_tsa(df)
    
    return df


def prep_opsd(df):
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df['month'] = df.index.month_name()
    df['year'] = df.index.year
    df.fillna(0, inplace=True)
    return df

def opsd_pipeline(force_sql=False):
    
    if force_sql or not os.path.isfile('opsd_germany_daily.csv'):
        # If force_sql is True or the CSV file does not exist, read fresh data from the database into a DataFrame.
        df = get_opsd()
    else:
        # Otherwise, if the CSV file exists, read in data from the CSV file.
        df = pd.read_csv('opsd_germany_daily.csv', index_col=0)
    
    df = prep_opsd(df)
    
    return df


def plt_dist(df, col_name):
    
    plt.hist(df[col_name], bins=50)
    plt.xlabel(f'{col_name}')
    plt.ylabel('Count')
    plt.title(f'Distribution of {col_name}')
    plt.show()

def plt_all(df):
    df = df[~(df == 0).any(axis=1)]
    for col in df.columns:
        plt_dist(df, col)