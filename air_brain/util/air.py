"""
utilities for reading in and pre-processing air quality related data
"""
from abc import ABCMeta, abstractmethod
import os

import pandas as pd

# TODO this structure is not great
from air_brain.data.get_data import DATA_DIR


class Air(metaclass=ABCMeta):
    """
    abc for pulling, preprocessing, and using air quality data
    """
    def __init__(self,
                 data_dir=DATA_DIR,
                 data_file="daily_air_quality.csv",
                 sensor_file="air_sensors.csv"):
        self.data_dir = data_dir
        self.data_file = data_file
        self.sensor_file = sensor_file

    def all_daily_air(self):
        """
        Pull daily air quality measurements, for all parameters, in long format stored by WPRDC
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
        filename = os.path.join(self.data_dir, self.data_file)
        df = pd.read_csv(filename)
        df.drop(columns="_id", inplace=True)
        df.date = pd.to_datetime(df.date)

        # TODO verify column names, since use them later
        return df

    def site_loc(self):
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
        filename = os.path.join(self.data_dir, self.sensor_file)
        df = pd.read_csv(filename)
        df.drop(columns="_id", inplace=True)
        df.rename(columns={'site_name': 'site'}, inplace=True)
        df.enabled = df.enabled == 't'
        # TODO verify column names, since use them later
        return df

    @abstractmethod
    def daily_air(self):
        """
        Subset air quality data to the parameter of interest

        :return:
        pandas DataFrame of daily air quality measurements, with columns
        - date : pd.datetime
        - site : str
        - index_value : float
        - description : str
        - health_advisory : str
        - health_effects : str
        """

    def by_site(self):
        """
        Subset air quality data to just the parameter of interest, organized by measurement site
        :return:
        pandas DataFrame, indexed on date, with one column for each measurement site
        """
        df = self.daily_air()

        # TODO check that there is only one measurement per site per day
        ret = df.pivot(index="date", columns="site", values="index_value")
        return ret

class PM25(Air):
    """
    particulate matter of 2.5 microns or smaller

    There are a variety of different PM 2.5 measurement recorded here
    I believe these are different ways of measuring PM 2.5, and they may not be directly comparable
    But none of them overlap at the same place at the same time, so it's tricky to check directly from the data
    # TODO check with DHS

    # TODO I suspect Lawrenceville and Pittsburgh are the same site for this measurement
    # TODO but need to confirm with DHS
    """

    def daily_air(self):
        """
        Pull daily PM 2.5 measurements for each site

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
        all_air = self.all_daily_air()
        pm25 = all_air.loc[all_air.parameter.isin(["PM25", "PM25(2)", "PM25B", "PM25T", "PM25_640"])].copy()
        return pm25

