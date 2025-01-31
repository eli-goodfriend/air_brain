"""
links for downloading data from Western Pennsylvania Regional Data Center (WPRDC)

right now, this uses links that download the whole dataset, so it takes a while
but the WPRDC implemented their API as queryable, so this could become more sophisticated
as I figure out what data I actually need

TODO elsewhere include download of zip code to lat-lon from
http://download.geonames.org/export/zip/US.zip
"""
import os
import requests

from air_brain.config import data_dir

csv_data = {
    ## air quality
    "hourly air quality": "https://tools.wprdc.org/downstream/36fb4629-8003-4acc-a1ca-3302778a530d",
    # note that daily air quality data is AQI (air quality index), aligned to a scale of 0 - 500
    # whereas the hourly air quality data is the actual measurements
    "daily_air_quality": "https://data.wprdc.org/datastore/dump/4ab1e23f-3262-4bd3-adbf-f72f0119108b",
    "air_toxic_releases": "https://data.wprdc.org/datastore/dump/2750b8c8-246b-430f-b1e0-1aa96e00b013",
    "air_sensors": "https://data.wprdc.org/datastore/dump/b646336a-deb4-4075-aee4-c5d28d88c426",
    ## behavioral health indicators
    "accidental_overdose": "https://data.wprdc.org/datastore/dump/1c59b26a-1684-4bfb-92f7-205b947530cf",
    "anxiety_meds_2015": "https://data.wprdc.org/dataset/6ee6d30f-c27a-41b6-9f94-0c071ce266e4/resource/d0676374-52ca-452c-9612-87b278997cfc/download/2.anxiety2015.csv",
    "anxiety_meds_2016": "https://data.wprdc.org/dataset/6ee6d30f-c27a-41b6-9f94-0c071ce266e4/resource/1d630832-b5d6-4cf2-b3f4-e9fffe6f1000/download/anxiety_all_2016.csv",
    "depression_meds_2015": "https://data.wprdc.org/dataset/873931dc-7c9d-4d46-890c-345891c221b4/resource/9ef39270-8a95-4069-b61f-6c40b5e45c12/download/1.depression2015.csv",
    "depression_meds_2016": "https://data.wprdc.org/dataset/873931dc-7c9d-4d46-890c-345891c221b4/resource/56fb7a63-7a97-4c23-9ffb-666651546381/download/depression_all_2016.csv",
    "arrest": "https://data.wprdc.org/datastore/dump/e03a89dd-134a-4ee8-a2bd-62c40aeebc6f",
    "police_blotter": "https://data.wprdc.org/datastore/dump/044f2016-1dfd-4ab0-bc1e-065da05fca2e", # 2016 to 2023, datetime, latlon, city of PGH only
    "ems": "https://tools.wprdc.org/downstream/ff33ca18-2e0c-4cb5-bdcd-60a5dc3c0418", # 2015 to present, time by quarter, location by census block group
    ## respiratory health
    "covid_deaths": "https://data.wprdc.org/datastore/dump/dd92b53c-6a90-4b83-9810-c6e8689d325c",
    "asthma": "https://data.wprdc.org/dataset/3bdca0be-5768-4061-a069-aa7c7121e364/resource/61022ad9-c601-4152-9ba6-da915fd05be5/download/dataset_asthma-2017.csv",
    ## TODO covariates, especially related to poverty
}

geojson_data = {
    ## general location data
    "county": "https://data.wprdc.org/dataset/e80cb6b3-b31b-4ca8-a8ae-aee164608100/resource/09900a13-ab5d-4e41-94f8-7e4d129e9a4c/download/county_boundary.geojson",
    "zipcodes": "https://data.wprdc.org/dataset/1a5135de-cabe-4e23-b5e4-b2b8dd733817/resource/14e5de97-0a5f-4521-84f6-ba74413db598/download/alcogisallegheny-county-zip-code-boundaries.geojson",
    "municipality": "https://data.wprdc.org/dataset/2fa577d6-1a6b-46a8-8165-27fecac1dee5/resource/b0cb0249-d1ba-45b7-9918-dc86fa8af04c/download/muni_boundaries.geojson",
    "neighborhood": "https://data.wprdc.org/dataset/e672f13d-71c4-4a66-8f38-710e75ed80a4/resource/4af8e160-57e9-4ebf-a501-76ca1b42fc99/download/neighborhoods.geojson",
    ## air quality sensor locations
    # TODO also included in csv_data, probably only need one
    "sensor_json": "https://data.wprdc.org/dataset/c7b3266c-adc6-41c0-b19a-8d4353bfcdaf/resource/7f7072ce-7c19-4813-a45c-6135cf4505bb/download/sourcesites.geojson",
}

def download_csv(name: str):
    url = csv_data[name]
    fileout = os.path.join(data_dir, "{}.csv".format(name))
    print("Downloading {} from WPRDC to {}".format(name, fileout))
    response = requests.get(url)
    with open(fileout, "wb") as f:
        f.write(response.content)

def download_geojson(name: str):
    url = geojson_data[name]
    fileout = os.path.join(data_dir, "{}.geojson".format(name))
    print("Downloading {} from WPRDC to {}".format(name, fileout))
    response = requests.get(url)
    with open(fileout, "wb") as f:
        f.write(response.content)
