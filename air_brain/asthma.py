"""
utilities for the air quality vs asthma analysis
"""
import geopandas as gpd
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

from air_brain.config import data_dir

def build_dataset():
    # census tract geometry
    tracts = gpd.read_file(data_dir / "raw" / "tract_2010" / "tl_2010_42003_tract10.shp")
    tracts.GEOID10 = tracts.GEOID10.astype(int)

    # import the childhood asthma healthcare utilization data
    asthma = pd.read_csv(data_dir / "preprocessed" / "asthma.csv")

    # import the EPA's air quality and demographic data
    aq = pd.read_csv(data_dir / "raw" / "epa_ej" / "2017_tract.csv")

    # merge asthma data with air quality / demographic data
    df = asthma.merge(aq, left_on="Census_tract", right_on="ID", how="left", validate="1:1")
    # put the geometry in
    df = tracts.merge(df, right_on="Census_tract", left_on="GEOID10", validate="1:1")

    # add the intercept
    df["intercept"] = 1

    # generate combined air quality and demographic variables
    # for more details on why these are needed, see the Appendix notebook
    df[["PM25_scale", "dpm_scale"]] = StandardScaler().fit_transform(df[["PM25", "dpm"]])
    df["PM25_dpm_mean"] = df[["PM25_scale", "dpm_scale"]].mean(axis=1)
    pca_demo = PCA(n_components=1)
    pca_demo.fit(df[["lowincome", "poc"]])
    df["poc_lowincome_pca"] = pca_demo.transform(df[["lowincome", "poc"]])

    return df