"""
utilities around handling locations

TODO some of this needs to get moved into the data pullers
"""
import os

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
                            "admin", "admin_code", "blank1", "blank2", "latitude", "longitude", "accuracy"])
    df = df[["zipcode", "state", "latitude", "longitude"]]
    return df

def zip2latlon():
    """
    subset to just Pennsylvania, since we are only in PA
    :return:
    pandas DataFrame with columns
    - zipcode
    - latitude
    - longitude
    """
    df = all_zip2latlon()
    return df.loc[df.state == "PA"].drop(columns="state")


if __name__ == "__main__":
    # save a PA only zip file for general use
    df = zip2latlon()
    df.to_csv(os.path.join(DATA_DIR, "zip2latlon.csv"))
