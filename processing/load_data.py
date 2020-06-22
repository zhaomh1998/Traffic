"""
Functions to load datasets for the traffic project
"""
import os
import pandas as pd
import re


def load_dataset(dataset_path):
    """
    Load the dataset given the dataset name. Correct formatting and calculates a congestion % = 1 - expedite %.
    :param str dataset_path: Path to dataset
    :return: Pandas DataFrame containing the processed data
    """
    assert os.path.exists(dataset_path), 'Dataset file doesn\'t exist!'

    df = pd.read_csv(dataset_path, error_bad_lines=False,
                     names=['year', 'month', 'day', 'hour', 'minute', 'exp', 'cong', 'block', 'unknown'])
    date_cols = ['year', 'month', 'day', 'hour', 'minute']
    
    # Data validation
    def invalid_data(df):
        return ((df['year'] < 2000) | (df['year'] > 2100) |
                (df['month'] < 0) | (df['month'] > 12) |
                (df['day'] < 0) | (df['day'] > 31) |
                (df['exp'].apply(lambda x: not re.search(r'^(?:[0-9]|^[1-9][0-9]|^100).[0-9][0-9]%$', x))) |
                (df['cong'].apply(lambda x: not re.search(r'^(?:[0-9]|^[1-9][0-9]|^100).[0-9][0-9]%$', x))) |
                (df['block'].apply(lambda x: not re.search(r'^(?:[0-9]|^[1-9][0-9]|^100).[0-9][0-9]%$', x))) |
                (df['unknown'].apply(lambda x: not re.search(r'^(?:[0-9]|^[1-9][0-9]|^100).[0-9][0-9]%$', x)))
               )
    df.drop(index=df[invalid_data(df)].index, inplace=True)
    
    df['date'] = pd.to_datetime(df[date_cols])
    df = df.set_index('date').drop(date_cols, axis=1)
    for col in df.columns:
        df[col] = df[col].str.rstrip('%').astype('float') / 100.0  # Convert percentage into 0.xx

    df = df.resample('10Min').mean().dropna()
    df['congestion'] = (df['cong'] + df['block']) / (1 - df['unknown'])  # Calculate congestion ratio
    df['congestion2'] = (df['cong'] + 2*df['block']) / (1 - df['unknown'])  # Calculate congestion ratio
    df['heavy_congestion'] = (df['block']) / (1 - df['unknown'])  # Calculate congestion ratio
    return df


def load_rain_data(dataset_path, table_index):
    """
    Read rainfall dataset.
    :paran str dataset_folder: Folder containing dataset
    :param int table_index: Index of the table to read from
    :return: Pandas DataFrame containing rainfall data
    """
    import datetime
    assert os.path.exists(dataset_path), 'Dataset file doesn\'t exist!'
    translate_dict = {'区站号(字符)': 'station', '年(年)': 'year', '月(月)': 'month', '日(日)': 'day',
                      '时(时)': 'hour', '过去1小时降水量(毫米)': 'rain'}
    df = pd.read_excel(pd.ExcelFile(dataset_path), table_index)
    df = df.rename(translate_dict, axis=1).drop('station', axis=1)
    df.index = pd.to_datetime(df[['year', 'month', 'day', 'hour']]) + datetime.timedelta(hours=8)
    df.drop(['year', 'month', 'day', 'hour'], axis=1, inplace=True)
    return df
