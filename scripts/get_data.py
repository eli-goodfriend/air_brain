"""
utility to download all data required in notebooks

in the future, may want to split this into multiple scripts
"""
import air_brain.data.wprdc as wprdc
import air_brain.data.epa_ej as epa_ej
import air_brain.data.census as census

def get_wprdc():
    # asthma by census tract for 2017
    wprdc.download_csv("asthma")

def get_epa_ej():
    # air pollution and demographics by census block group for 2017
    epa_ej.EJ2017().get_data()

def get_census():
    # 2010 census tracts
    census.get_2010_tracts()

if __name__ == "__main__":
    get_wprdc()
    get_epa_ej()
    get_census()