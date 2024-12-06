"""
utilities around handling the overdose data
"""
import os

import numpy as np
import pandas as pd

from air_brain.data.get_data import DATA_DIR

FILENAME = os.path.join(DATA_DIR, "accidental_overdose.csv")
LATLON_FILENAME = os.path.join(DATA_DIR, "zip2latlon.csv")

def od():
    """
    utility for pulling and cleaning overdose data
    :return:
    """
    df = pd.read_csv(FILENAME)

    # drop unused columns
    df.drop(columns=["_id", # meaningless identifier
                     "decedent_zip", # all NaN
                     ], inplace=True)

    # data types
    df.death_date_and_time = pd.to_datetime(df.death_date_and_time)

    # will want just dates, as well as datetimes
    df["date"] = pd.to_datetime(df.death_date_and_time.dt.date)

    # clean up zipcodes
    df.rename(columns={"incident_zip": "zipcode"}, inplace=True)
    df.zipcode = df.zipcode.astype(str).str[:5]
    df.loc[~df.zipcode.str.isdigit(), "zipcode"] = np.nan
    df.zipcode = df.zipcode.astype(float)

    return df

def od_latlon():
    """
    utility to pull overdose data with latitude/longitude data
    :return:
    """
    df = od()
    zip_df = pd.read_csv(LATLON_FILENAME)
    df = df.merge(zip_df, on="zipcode", how="left", validate="m:1")
    return df


