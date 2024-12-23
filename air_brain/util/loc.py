"""
utilities around handling locations

TODO some of this needs to get moved into the data pullers
"""
import os
from math import cos, asin, sqrt, pi

import pandas as pd
import geopandas as gpd

from air_brain.data.get_data import DATA_DIR

# canonical CRS for this project
# TODO... is this the best one?
CRS = "EPSG:2272"

# geometry files
BG_FILE = os.path.join(DATA_DIR, "tl_2010_42003_bg10", "tl_2010_42003_bg10.shp")
ZIP_FILE = os.path.join(DATA_DIR, "zipcodes.geojson")

def zip_by_bg():
    """
    generate a dataframe of the area of each zipcode intersected with each census block group
    """
    # census block groups
    bg = gpd.read_file(BG_FILE)[["GEOID10", "geometry"]].rename(columns={"GEOID10": "ID"})
    bg.ID = bg.ID.astype(int)
    bg = bg.to_crs(CRS)
    # zip codes
    zc = gpd.read_file(ZIP_FILE)[["ZIP", "geometry"]]
    zc = zc.to_crs(CRS)
    # overlap
    df = gpd.overlay(zc, bg, how="intersection")
    df["int_area"] = df["geometry"].area
    return df[["ZIP", "ID", "int_area"]]

def bg2zip(df_in, cols, bg_col="ID"):
    """
    given a df_in with columns
    - bg_col: column with census block group IDs
    - cols: list of string of data columns of interest

    generate a df with columns
    - ZIP: zipcode
    - cols: original column cols averaged to zipcodes
    """
    df = df_in.merge(zip_by_bg(), left_on=bg_col, right_on="ID", how="outer", validate="1:m")
    agg_dict = {"int_area": "sum"}
    for col in cols:
        df["{}_x_area".format(col)] = df[col] * df.int_area
        agg_dict["{}_x_area".format(col)] = "sum"
    grped_df = df.groupby("ZIP").agg(agg_dict).reset_index()
    for col in cols:
        grped_df[col] = grped_df["{}_x_area".format(col)] / grped_df.int_area
    return grped_df[["ZIP"] + cols]


# below needs a re-think

def all_zip2latlon():
    """
    read in the full zip code to lat/lon file downloaded from
    http://download.geonames.org/export/zip/US.zip
    with notes at
    http://download.geonames.org/export/zip/

    do zip codes match to a clear location? no, they do not, they match to mail carrier routes
    can we fudge it? yes
    eventually would prefer to do this myself with US Census ZCTA, but this is ok for now

    :return:
    pandas DataFrame with columns
    - zipcode
    - state
    - latitude
    - longitude
    """
    df = pd.read_csv(os.path.join(DATA_DIR, "US.txt"),
                     sep="\t",
                     names=["country", "zipcode", "place", "state_name", "state",
                            "county", "county_code", "blank1", "blank2", "latitude", "longitude", "accuracy"])
    df = df[["zipcode", "place", "county", "state", "latitude", "longitude"]]
    return df

def zip2latlon():
    """
    subset to just Allegheny County, Pennsylvania
    :return:
    pandas DataFrame with columns
    - zipcode
    - latitude
    - longitude
    """
    df = all_zip2latlon()
    return df.loc[(df.state == "PA") & (df.county == "Allegheny")].drop(columns=["state", "county"])


def distance(lat1, lon1, lat2, lon2):
    """
    use the haversine to compute the distance between two lat/lon points
    from
    https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206#21623206

    :param lat1: float
    :param lon1: float
    :param lat2: float
    :param lon2: float
    :return: float
    """
    r = 3963 # radius of the earth in miles
    p = pi / 180

    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return 2 * r * asin(sqrt(a))

if __name__ == "__main__":
    # save a PA only zip file for general use
    df = zip2latlon()
    df.to_csv(os.path.join(DATA_DIR, "zip2latlon.csv"), index=False)
