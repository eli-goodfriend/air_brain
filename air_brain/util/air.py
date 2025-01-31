"""
utilities for reading in and pre-processing air quality related data
"""
from abc import ABCMeta, abstractmethod
import os

import pandas as pd
import geopandas as gpd

# TODO this structure is not great
from air_brain.data.get_data import DATA_DIR
from air_brain.util.loc import distance

class DailyAir(metaclass=ABCMeta):
    """
    abc for pulling, preprocessing, and using daily air quality data

    daily air quality is reported as an Air Quality Index (AQI), not a mean/median daily measurement
    this incorporates health science knowledge of how pollutants operate on the body to compute a number on a scale
     of 0 - 500 for all pollutants
    """
    def __init__(self,
                 data_dir=DATA_DIR,
                 data_file="daily_air_quality.csv",
                 sensor_file="sensor_json.geojson"):
        self.data_dir = data_dir
        self.data_file = data_file
        self.sensor_file = sensor_file

    @property
    @abstractmethod
    def param_names(self):
        """
        list of names used for this parameter in the daily AQI file
        """

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

    def all_site_loc(self):
        """
        Pull locations of daily air quality measurements
        For now, this uses a pre-downloaded geojson to not irritate their API too much,
        but could maybe just pull directly
        Also align site name with site names in daily air quality

        :return:
        geopandas dataframe of air quality measurement stations, with columns
        - site : str
        - Description : str
        - AirNowMnemonic : str
        - address : str
        - County : str
        - Enabled : bool
        - geometry : geopandas POINT(longitude, latitude) EPSG:4326
        """
        filename = os.path.join(self.data_dir, self.sensor_file)
        df = gpd.read_file(filename)
        df.rename(columns={"SiteName": "site"}, inplace=True)
        # TODO verify column names, since use them later
        return df

    def daily_air(self):
        """
        Subset air quality data to the parameter of interest

        :return:
        pandas DataFrame of daily AQI, with columns
        - date : pd.datetime
        - site : str
        - index_value : float
        - description : str
        - health_advisory : str
        - health_effects : str
        """
        all_air = self.all_daily_air()
        return all_air.loc[all_air.parameter.isin(self.param_names)].copy()

    def daily_air_gdf(self):
        """
        AQI for each date and site, with geopandas location for each measurement

        :return:
        geopandas dataframe of daily AQI, with columns
        - date : pd.datetime
        - site : str
        - index_value : float
        - description : str
        - health_advisory : str
        - health_effects : str
        - geometry : lat/lon of measurement site
        """
        air_df = self.daily_air()
        gdf = self.all_site_loc()[["site", "geometry"]]
        df = gdf.merge(air_df, on="site", how="right", validate="1:m")
        return df

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

    def site_loc(self):
        """
        Subset site location data to just those sites with measurements for this parameter
        Only interested in lat/lon right now
        Also check that all those sites have locations

        :return:
        pandas DataFrame with columns
        - site
        - latitude
        - longitude
        """
        df = self.all_site_loc()
        df = df.loc[df.geometry.notna()]

        # what sites have data for this measurement
        # TODO this is a bit silly and inefficient
        sites = self.by_site().columns
        # make sure that all sites have location information
        for site in sites:
            assert site in df.site.values, "{} has no location information".format(site)

        ret = df.loc[df.site.isin(sites)][["site", "geometry"]]
        return ret

class PM25(DailyAir):
    """
    daily AQI related to particulate matter of 2.5 microns or smaller

    There are a variety of different PM 2.5 measurement recorded here
    I believe these are different ways of measuring PM 2.5, and they may not be directly comparable
    But none of them overlap at the same place at the same time, so it's tricky to check directly from the data
    TODO check with DHS

    I suspect Lawrenceville and Pittsburgh are the same site for this measurement
    because the DHS website shows 6 active sites for PM 2.5, and one of them is Lawrenceville
    but the Lawrenceville site in this dataset has no current data
    and there is no location information for the Pittsburgh sensor
    ...except that they briefly have overlap data in May/June 2021
    I'm going to run with this assumption for now
    TODO but need to confirm with DHS
    """
    param_names = ["PM25", "PM25(2)", "PM25B", "PM25T", "PM25_640"]

    def daily_air(self):
        """
        Subset to just PM 2.5 daily AQI

        :return:
        pandas DataFrame of daily AQI, with columns
        - date : pd.datetime
        - site : str
        - parameter : str
        - index_value : float
        - description : str
        - health_advisory : str
        - health_effects : str
        """
        pm25 = super().daily_air()

        # TODO danger need to verify with DHS that the below is true
        # merge sites Lawrenceville and Pittsburgh -> Lawrenceville
        # for overlap, keep Pittsburgh data, since I assume it's the more recent sensor
        pm25 = pm25.sort_values("site")
        pm25.loc[pm25.site == "Pittsburgh", "site"] = "Lawrenceville"
        pm25 = pm25.drop_duplicates(subset=["date", "site"], keep="last")
        return pm25

class SO2(DailyAir):
    """
    daily AQI related to sulfur dioxide
    """
    param_names = ["SO2"]
