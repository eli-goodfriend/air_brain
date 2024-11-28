"""
utilities for reading in and pre-processing air quality related data
"""
import os

import pandas as pd

# TODO this structure is not great
from air_brain.data.get_data import DATA_DIR


def daily_air():
    """
    Pull daily air quality measurements, in long format stored by WPRDC
    For now, this uses a pre-downloaded csv to not irritate their API too much,
    but could maybe just pull directly
    Also cleans up data types and drops unused _id column

    :return:
    pandas DataFrame of daily air quality measurements, with columns
    - date : pd.datetime
    - site : str
    - parameter : str
    - index_value : float
    - description : str
    - health_advisory : str
    - health_effects : str
    """
    filename = os.path.join(DATA_DIR, "daily_air_quality.csv")
    df = pd.read_csv(filename)
    df.drop(columns="_id", inplace=True)
    df.date = pd.to_datetime(df.date)

    # TODO verify column names, since use them later
    return df

def site_loc():
    """
    Pull locations of daily air quality measurements
    For now, this uses a pre-downloaded csv to not irritate their API too much,
    but could maybe just pull directly
    Also drops unused _id column and aligns site name with site names in daily air quality

    :return:
    pandas DataFrame of air quality measurement stations, with columns
    - site : str
    - description : str
    - air_now_mnemonic : str
    - address : str
    - latitude : float
    - longitude : float
    - enabled : bool
    """
    filename = os.path.join(DATA_DIR, "air_sensors.csv")
    df = pd.read_csv(filename)
    df.drop(columns="_id", inplace=True)
    df.rename(columns={'site_name': 'site'}, inplace=True)
    df.enabled = df.enabled == 't'
    # TODO verify column names, since use them later
    return df

def pm25_by_site():
    """
    Pull daily PM 2.5 measurements for each site

    There are a variety of different PM 2.5 measurement recorded here
    I believe these are different ways of measuring PM 2.5, and they may not be directly comparable
    But none of them overlap at the same place at the same time, so it's tricky to check directly from the data
    # TODO check with DHS

    # TODO I suspect Lawrenceville and Pittsburgh are the same site for this measurement
    # TODO but need to confirm with DHS

    :return:
    pandas DataFrame, indexed on date, with columns for each site
    - Avalon
    - Clairton
    - Lawrenceville
    - Liberty 2
    - Lincoln
    - North Braddock
    - Parkway East
    - Pittsburgh
    """
    all_air = daily_air()
    pm25 = all_air.loc[all_air.parameter.isin(["PM25", "PM25(2)", "PM25B", "PM25T", "PM25_640"])].copy()

    # TODO verify that no site has multiple PM 2.5 measurements on the same day
    df = pm25.pivot(index="date", columns="site", values="index_value")
    return df


