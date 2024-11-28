"""
utilities for reading in and pre-processing air quality related data
"""
import os

import pandas as pd

# TODO this structure is not great
from air_brain.data.get_data import DATA_DIR

def daily_pm25():
    """
    daily PM 2.5 measurements, averaged across all sites
    TODO this needs better research into how to aggregate PM 2.5 measurements

    TODO read once and store, since this isn't very big (?)
    TODO or just pull from API directly?? seems rude tbh for this context
    TODO probably data should handle actually reading the data

    :return:
    pandas DataFrame of daily PM 2.5 measurements, with columns
    - date
    - pm25
    """
    filename = os.path.join(DATA_DIR, 'daily_air_quality.csv')
    air_df = pd.read_csv(filename)
    air_df.date = pd.to_datetime(air_df.date)

    pm25 = air_df.loc[air_df.parameter.isin(['PM25', 'PM25(2)', 'PM25B', 'PM25T', 'PM25_640'])]
    pm25_daily_avg = pm25.groupby('date').agg({'index_value': 'mean'}).reset_index().rename(
        columns={'index_value': 'pm25'})
    return pm25_daily_avg


