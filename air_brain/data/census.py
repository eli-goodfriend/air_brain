"""
utilities for downloading files from the US Census

right now this is only Allegheny County 2010 census tract shapefiles, but could also include
- ACS data (for demographics)
- 2020 census tract shapefiles
- block group shapefiles
"""
from air_brain.config import data_dir
from air_brain.data.util import download_zip

base_url_2010 = "https://www2.census.gov/geo/pvs/tiger2010st/42_Pennsylvania/42003/"
tract_zip_url = "{}tl_2010_42003_tract10.zip".format(base_url_2010)

def get_2010_tracts():
    save_dir = data_dir / "tract_2010"
    print("Downloading 2010 Census tract data to {}".format(save_dir))
    download_zip(tract_zip_url, save_dir)
