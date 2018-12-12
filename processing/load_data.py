"""
Functions to load datasets for the traffic project
"""
import os
import pandas as pd


def load_dataset(name):
    """
    Load the dataset given the dataset name. Correct formatting and calculates a congestion % = 1 - expedite %.
    :param str name: Name of the dataset
    :return: Pandas DataFrame containing the processed data
    """
    assert isinstance(name, str)
    assert os.path.exists(os.getcwd() + '/data/' + name + '.csv'), 'Dataset file doesn\'t exist!'

    df = pd.read_csv(os.getcwd() + '/data/' + name + '.csv',
                     names=['year', 'month', 'day', 'hour', 'minute', 'exp', 'cong', 'block', 'unknown'])
    date_cols = ['year', 'month', 'day', 'hour', 'minute']
    df['date'] = pd.to_datetime(df[date_cols])
    df = df.set_index('date').drop(date_cols, axis=1)
    for col in df.columns:
        df[col] = df[col].str.rstrip('%').astype('float') / 100.0  # Convert percentage into 0.xx

    df = df.resample('10Min').mean().dropna()
    df['congestion'] = 1 - df['exp']  # Calculate congestion ratio
    return df


def load_rain_data(table_index):
    """
    Read rainfall dataset.
    :param table_index: Index of the table to read from
    :return: Pandas DataFrame containing rainfall data
    """
    translate_dict = {'区站号(字符)': 'station', '年(年)': 'year', '月(月)': 'month', '日(日)': 'day',
                      '时(时)': 'hour', '过去1小时降水量(毫米)': 'rain'}
    df = pd.read_excel(pd.ExcelFile(os.getcwd() + '/data/rain_data.xlsx'), table_index)
    df = df.rename(translate_dict, axis=1).drop('station', axis=1)
    df.index = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
    df = df[df.rain != 999999.0]  # Drop invalid data
    return df