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
    exceed_outliers_threshold = Q3 + (Q3 - Q1) * iqr_multiplier
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
    df_hour['isweekday'] = df.index.dayofweek < 5
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
    ax.set_ylim(0, 1)
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


def rain_data_summary(dict_of_data):
    """
    Print summary of rain data
    :param dict dict_of_data: Dictionary of the rain data
    """

    def get_rain_level(rain_data):
        rain_lv0 = rain_data[rain_data.rain == 0].index
        rain_lv1 = rain_data[(rain_data.rain > 0) & (rain_data.rain <= 1)].index
        rain_lv2 = rain_data[(rain_data.rain > 1) & (rain_data.rain <= 5)].index
        rain_lv3 = rain_data[(rain_data.rain > 5) & (rain_data.rain <= 10)].index
        rain_lv4 = rain_data[(rain_data.rain > 10) & (rain_data.rain <= 20)].index
        rain_lv5 = rain_data[rain_data.rain > 20].index

        return rain_lv0, rain_lv1, rain_lv2, rain_lv3, rain_lv4, rain_lv5

    for name, data in dict_of_data.items():
        print(name)
        for index, rain_lvl in zip(range(6), get_rain_level(data)):
            print(f'{index}: {len(rain_lvl)}\t', end='')
        print()


def process_rain_data(traffic_data, rain_data):
    """
    Prepare data to make plots regarding rainfall. Add a column storing rain information to the traffic DataFrame.
    :param DataFrame traffic_data: DataFrame containing traffic data
    :param DataFrame rain_data: DataFrame containing rain data
    :return: Processed Pandas DataFrame
    """
    # Since the rain data is hourly, we have to down sample traffic data to per hour
    df = traffic_data.resample('1H').mean().dropna().copy()
    df['rain'] = rain_data['rain']
    df = df.dropna()  # Drop missed data rows

    def rain_grouper(value):
        """
        Group value into rainfall groups
        :param float value:
        :return: Group number in int
        """
        if value == 0:
            return 0
        elif 0 < value <= 1:
            return 1
        elif 1 < value <= 5:
            return 2
        elif 5 < value <= 10:
            return 3
        elif 10 < value <= 20:
            return 4
        elif value > 20:
            return 5

    df['Rain Level'] = df['rain'].apply(rain_grouper)
    df['hour'] = df.index.hour
    return df


def rain_level_boxplot(combined_data, ax, title):
    """
    Make a box plot comparing rain levels for every hour traffic congestion
    :param DataFrame combined_data: Processed DataFrame containing both traffic and rain data
    :param plt.axis ax: Matplotlib axis to plot on
    :param str title: Title for the plot
    """
    sns.boxplot(x='hour', y='congestion', hue='Rain Level', data=combined_data, ax=ax)
    ax.set_title(title)
    leg = ax.get_legend()
    for t, l in zip(leg.texts, ['No rain', '0.1-1mm', '1-5mm', '5-10mm', '10-20mm', '20mm+']): t.set_text(l)


def process_rain(data, data_combined):
    """
    Calculates the difference of congestion from hourly average value, for plotting against rain level
    :param DataFrame data: Traffic data DataFrame
    :param DataFrame data_combined: Traffic and Rain data combined
    :return: DataFrame containing the calculated difference and rain level
    """
    hr_avg = process_hour_avg(data)['congestion']
    hr_avg.index = pd.to_datetime(hr_avg.index, format='%H').time

    rain_effect = data_combined.copy()
    rain_effect = rain_effect[rain_effect.rain > 1]
    rain_effect['hour'] = rain_effect.index.time
    rain_effect.index = rain_effect.index.time
    rain_effect['cong_diff'] = rain_effect['congestion'] - hr_avg[rain_effect['hour']]
    rain_effect = rain_effect.reset_index(drop=True)
    rain_effect = rain_effect[['rain', 'cong_diff']]
    return rain_effect

def cong_diff_scatter_plot(cong_diff_data, ax, title):
    """
    Produce the congestion difference in different raining condition scatter plot
    :param DataFrame cong_diff_data: DataFrame calculated by process_rain()
    :param plt.axis ax: Matplotlib axis to plot on
    :param str title: Title for the plot
    """
    sns.scatterplot(x="rain", y="cong_diff", data=cong_diff_data, ax=ax)
    ax.axhline(y=0, color='black')
    ax.set_title(title)