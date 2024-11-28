"""
script for downloading data sources used in this project
all data is available from the Western Pennsylvania Regional Data Center (WPRDC)

right now, this uses links that download the whole dataset, so it takes a while
but the WPRDC implemented their API as queryable, so this should become more sophisticated
as I figure out what data I actually need :)
"""
import os
import requests

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_SOURCES = {
    ## air quality
    # TODO too much? 'hourly air quality': 'https://tools.wprdc.org/downstream/36fb4629-8003-4acc-a1ca-3302778a530d',
    "daily_air_quality": "https://data.wprdc.org/datastore/dump/4ab1e23f-3262-4bd3-adbf-f72f0119108b",
    "air_toxic_releases": "https://data.wprdc.org/datastore/dump/2750b8c8-246b-430f-b1e0-1aa96e00b013",
    ## behavioral health indicators
    "accidental_overdose": "https://data.wprdc.org/datastore/dump/1c59b26a-1684-4bfb-92f7-205b947530cf",
    "anxiety_meds_2015": "https://data.wprdc.org/dataset/6ee6d30f-c27a-41b6-9f94-0c071ce266e4/resource/d0676374-52ca-452c-9612-87b278997cfc/download/2.anxiety2015.csv",
    "anxiety_meds_2016": "https://data.wprdc.org/dataset/6ee6d30f-c27a-41b6-9f94-0c071ce266e4/resource/1d630832-b5d6-4cf2-b3f4-e9fffe6f1000/download/anxiety_all_2016.csv",
    "depression_meds_2015": "https://data.wprdc.org/dataset/873931dc-7c9d-4d46-890c-345891c221b4/resource/9ef39270-8a95-4069-b61f-6c40b5e45c12/download/1.depression2015.csv",
    "depression_meds_2016": "https://data.wprdc.org/dataset/873931dc-7c9d-4d46-890c-345891c221b4/resource/56fb7a63-7a97-4c23-9ffb-666651546381/download/depression_all_2016.csv",
    "arrest": "https://data.wprdc.org/datastore/dump/e03a89dd-134a-4ee8-a2bd-62c40aeebc6f",
    # TODO too much? probably want description_short PSYCH or OVERDOSE; aggregated by quarter "ems_dispatch": "https://tools.wprdc.org/downstream/ff33ca18-2e0c-4cb5-bdcd-60a5dc3c0418",
    ## TODO covariates, especially related to poverty
}

def download_one(name: str, url: str):
    response = requests.get(url)
    with open(os.path.join(DATA_DIR, "{}.csv".format(name)), "wb") as f:
        f.write(response.content)

if __name__ == "__main__":
    for name, url in DATA_SOURCES.items():
        print("Downloading {}".format(name))
        download_one(name, url)