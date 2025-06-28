"""
utilities for the air quality vs asthma analysis
"""
import geopandas as gpd
import pandas as pd
from sklearn.preprocessing import StandardScaler

from air_brain.config import data_dir

def build_dataset():
    # census tract geometry
    tracts = gpd.read_file(data_dir / "tract_2010" / "tl_2010_42003_tract10.shp")
    tracts.GEOID10 = tracts.GEOID10.astype(int)

    # import the childhood asthma healthcare utilization data
    asthma = pd.read_csv(data_dir / "asthma.csv")
    # ED hospitalizations should be a subset of ED visits
    # so if ED_visits < ED_hosp, set ED_visits to ED_hosp
    asthma.loc[asthma.ED_visits < asthma.ED_hosp, "ED_visits"] = asthma.loc[
        asthma.ED_visits < asthma.ED_hosp, "ED_hosp"]
    # convert all counts to fractions of potential patients
    # if no members in census tract, fill with 0
    for col in ["Asthma_use", "UC_visits", "ED_visits", "ED_hosp"]:
        asthma["{}_frac".format(col)] = (asthma[col] / asthma.Total_members).fillna(0)
    # too little data, remove
    asthma = asthma.loc[asthma.Total_members > 4]

    # import the EPA's air quality and demographic data
    aq = pd.read_csv(data_dir / "epa_ej" / "2017_tract.csv")

    # merge asthma data with air quality / demographic data
    df = asthma.merge(aq, left_on="Census_tract", right_on="ID", how="left", validate="1:1")
    # put the geometry in
    df = tracts.merge(df, right_on="Census_tract", left_on="GEOID10", validate="1:1")

    # add the intercept
    df["intercept"] = 1

    # generate combined air quality and demographic variables
    # for more details on why these are needed, see the notebooks
    df[["PM25_scale", "dpm_scale"]] = StandardScaler().fit_transform(df[["PM25", "dpm"]])
    df["PM25_dpm_max"] = df[["PM25_scale", "dpm_scale"]].max(axis=1)
    # PoC and lowincome are naturally on the same scale, so don't need to re-scale
    df["poc_lowincome_max"] = df[["poc", "lowincome"]].max(axis=1)

    return df