"""
utilities for reading in and pre-processing air quality related data
"""
from abc import ABCMeta, abstractmethod
import os

import pandas as pd

# TODO this structure is not great
from air_brain.data.get_data import DATA_DIR
from air_brain.util.loc import distance

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

    def all_site_loc(self):
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
        df.rename(columns={"site_name": "site"}, inplace=True)
        df.enabled = df.enabled == "t"
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
        df = df.loc[df.latitude.notna() & df.longitude.notna()]

        # what sites have data for this measurement
        # TODO this is a bit silly and inefficient
        sites = self.by_site().columns
        # make sure that all sites have location information
        for site in sites:
            assert site in df.site.values, "{} has no location information".format(site)

        ret = df.loc[df.site.isin(sites)][["site", "latitude", "longitude"]]
        return ret

    def site_distances(self, df_in):
        """
        Given a dataframe with columns latitude and longitude, return a dataframe with one column for each
        measurement site, with the distance from that lat/lon to the measurement site (in miles)

        :param df_in:
        :return:
        """
        assert 'latitude' in df_in.columns
        assert 'longitude' in df_in.columns
        df = df_in[['latitude', 'longitude']].copy()

        site_loc_df = self.site_loc()

        def apply_distance(latlon_tuple):
            return distance(latlon_tuple[0], latlon_tuple[1],
                            latlon_tuple[2], latlon_tuple[3])

        for row in site_loc_df.itertuples():
            df["site_lat"] = row.latitude
            df["site_lon"] = row.longitude
            df["latlon_tuple"] = list(zip(df.site_lat, df.site_lon, df.latitude, df.longitude))
            df[row.site] = df.latlon_tuple.apply(apply_distance)

        return df[site_loc_df.site]

    def closest_site(self, df_in):
        """
        Given a dataframe with columns latitude and longitude, return a dataframe with columns
        site and dist, where
        - site is closest measurement site
        - dist is distance to closest measurement site

        :param df_in:
        :return:
        """
        assert 'latitude' in df_in.columns
        assert 'longitude' in df_in.columns
        df = df_in[['latitude', 'longitude']].copy()

        site_loc_df = self.site_loc()

        def apply_distance(latlon_tuple):
            return distance(latlon_tuple[0], latlon_tuple[1],
                            latlon_tuple[2], latlon_tuple[3])

        df["min_dist"] = 10000000.
        df["site"] = ''
        for row in site_loc_df.itertuples():
            df["site_lat"] = row.latitude
            df["site_lon"] = row.longitude
            df["latlon_tuple"] = list(zip(df.site_lat, df.site_lon, df.latitude, df.longitude))
            df["dist"] = df.latlon_tuple.apply(apply_distance)
            df.loc[df.dist < df.min_dist, "site"] = row.site
            df.loc[df.dist < df.min_dist, "min_dist"] = df.loc[df.dist < df.min_dist, "dist"]

        return df[["site", "min_dist"]]


class PM25(Air):
    """
    particulate matter of 2.5 microns or smaller

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

    def daily_air(self):
        """
        Subset to just PM 2.5 daily air measurements

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
        all_air = self.all_daily_air()
        pm25 = all_air.loc[all_air.parameter.isin(["PM25", "PM25(2)", "PM25B", "PM25T", "PM25_640"])].copy()

        # TODO verify that no sites have multiple measurements on the same day

        # TODO danger need to verify with DHS that the below is true
        # merge sites Lawrenceville and Pittsburgh -> Lawrenceville
        # for overlap, keep Pittsburgh data, since I assume it's the more recent sensor
        pm25 = pm25.sort_values("site")
        pm25.loc[pm25.site == "Pittsburgh", "site"] = "Lawrenceville"
        pm25 = pm25.drop_duplicates(subset=["date", "site"], keep="last")
        return pm25