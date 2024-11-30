"""
utilities around handling locations

TODO some of this needs to get moved into the data pullers
"""
import os
from math import cos, asin, sqrt, pi

import pandas as pd

from air_brain.data.get_data import DATA_DIR

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
