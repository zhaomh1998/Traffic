"""
A collection of plotting and data processing functions for the traffic project
"""
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def weekday_weekend_ten_min_plot(df, ax, title):
    """
    Make a per 10 min average plot for traffic congestion and block %, for weekdays and weekends
    :param DataFrame df: Traffic DataFrame used to plot
    :param plt.axis ax: Matplotlib axis to plot on
    :param str title: Title for the plot
    """
    def process_average(df):
        """
        Calculate per 10 minute average for every dat
        :param df: Traffic DataFrame used to calculate
        :return: DataFrame containing the calculate per 10 minute data
        """
        avg = df.groupby([df.index.hour, df.index.minute]).mean()
        avg.index = avg.index.map('{0[0]}:{0[1]}'.format)
        avg.index = pd.to_datetime(avg.index, format='%H:%M').time
        return avg

    weekday = df[df.index.dayofweek < 5]
    weekend = df[df.index.dayofweek >= 5]
    avg_weekday = process_average(weekday)[['congestion', 'block']].rename({'congestion': 'weekday congestion',
                                                                            'block': 'weekday block'}, axis=1)
    avg_weekend = process_average(weekend)[['congestion', 'block']].rename({'congestion': 'weekend congestion',
                                                                            'block': 'weekend block'}, axis=1)
    plot_df = pd.concat([avg_weekday, avg_weekend], axis=1)
    sns.lineplot(data=plot_df, ax=ax)
    ax.set_xlim('00:00', '23:50')
    ax.set_ylim(0, 0.8)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=40)
    ax.set_xticks([f'{hr}:00' for hr in range(0, 24)]);
    ax.set_title(title)

def get_outlier_time(df, hour, col_name, iqr_multiplier=1.5):
    """
    Calculate and print time the hourly outliers in the dataset appears
    :param DataFrame df: Pandas DataFrame for the traffic data
    :param hour: Which hour's data to look at
    :param str col_name: Column name to calculate outlier
    :param int iqr_multiplier: Threshold for counting outliers, defaults to 1.5
    """
    df_hour = df[df.hour == hour].copy()
    Q1 = df_hour.quantile(0.25)[col_name]
    Q3 = df_hour.quantile(0.75)[col_name]
    exceed_outliers_threshold = Q3+(Q3-Q1)*iqr_multiplier
    for i in df_hour[df_hour[col_name] > exceed_outliers_threshold].index:
        print(i)

def process_hour_avg(df):
    """
    Calculate and return per hour traffic average
    :param DataFrame df: Pandas DataFrame for the traffic data
    :return: Pandas DataFrame containing per hour average
    """
    return df.groupby([df.index.hour]).mean()

def process_hour_group(df):
    """
    Prepare data to make hourly plots
    :param DataFrame df: Pandas DataFrame for the data to process
    :return: Processed Pandas DataFrame
    """
    df_hour = df.copy()
    df_hour['hour'] = df.index.hour
    df_hour['isweekday'] = df.index.dayofweek<5
    return df_hour

def hourly_boxplot(df, ax, title):
    """
    Produce hourly boxplot, separating weekdays and weekends
    :param DataFrame df: Traffic DataFrame used to plot
    :param plt.axis ax: Matplotlib axis to plot on
    :param str title: Plot title
    """
    df_hr = process_hour_group(df)
    sns.boxplot(x='hour', y='congestion', hue='isweekday', data=df_hr, ax=ax)
    ax.set_title(title)
    ax.set_ylim(0,1)
    # Set legend
    leg = ax.get_legend()
    leg.set_title('Day of week')
    for t, l in zip(leg.texts, ['Weekend', 'Weekday']): t.set_text(l)

def hourly_violinplot(df, ax, title):
    """
    Produce hourly violinplot, separting weekdays and weekends
    :param DataFrame df: Traffic DataFrame used to plot
    :param plt.axis ax: Matplotlib axis to plot on
    :param str title: Plot title
    """
    df_hr = process_hour_group(df)
    sns.violinplot(x='hour', y='congestion', hue='isweekday', data=df_hr, ax=ax, split=True)
    ax.set_ylim(0, 1)
    ax.set_title(title)
    # Set legend
    leg = ax.get_legend()
    leg.set_title('Day of week')
    for t, l in zip(leg.texts, ['Weekend', 'Weekday']): t.set_text(l)