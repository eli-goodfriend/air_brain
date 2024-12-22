"""
utilities for downloading the EPA Environmental Justice Screening datasets
their website has an API for this, but afaik it only returns the most recent data
and I want to be able to look at the historical data

currently this is available for 2015 - 2024
"""
import os
import shutil
import requests

from abc import ABCMeta, abstractmethod

import pandas as pd

# TODO c/p
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files")
# TODO higher up
def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True, verify=False)
    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


class AbcEJ(metaclass=ABCMeta):
    """
    ABC for
    - downloading EPA Environmental Justice Screen data
    - subsetting to Allegheny County and unifying used column names
    - averaging census block group data (weighed by area) to
        - census tract (asthma)
        - zip code (overdose deaths)
        - neighborhood (COVID)
    """
    save_dir = os.path.join(DATA_DIR, "epa_ej")
    base_url = "https://gaftp.epa.gov/EJScreen"
    id_col = "ID"

    @property
    @abstractmethod
    def year(self):
        """
        integer for which year this class is downloading for
        """

    @property
    def filename(self):
        """
        string base filename to be downloaded from EPA site
        """
        return "EJSCREEN_{}_USPR".format(self.year)

    @property
    def url(self):
        """
        string full path to download from EPA EJ site
        """
        return "{}/{}/{}.csv.zip".format(self.base_url, self.year, self.filename)

    @property
    def zip_file(self):
        """
        string full path to where the zip file should be downloaded
        """
        return os.path.join(self.save_dir, "{}.csv.zip".format(self.filename))

    @property
    def orig_file(self):
        """
        string full path to where the unzipped file should be saved
        """
        return os.path.join(self.save_dir, "{}.csv".format(self.filename))

    @property
    def data_file(self):
        """
        string full path to where the usable file is saved
        by default this is the orig_file subsetted to only Allegheny County
        """
        return os.path.join(self.save_dir, "{}.csv".format(self.year))

    def download(self):
        download_url(self.url, self.zip_file)

    def extract(self):
        shutil.unpack_archive(self.zip_file, self.save_dir)

    def preprocess(self):
        """
        by default, just want Allegheny County
        """
        df = pd.read_csv(self.orig_file)
        df = df.loc[df[self.id_col].astype(str).str.startswith('42003')]
        df.to_csv(self.data_file)

    def clean_up(self):
        """
        by default, remove larger original files
        """
        try:
            os.remove(self.zip_file)
        except FileNotFoundError:
            pass
        try:
            os.remove(self.orig_file)
        except FileNotFoundError:
            pass

    def get_data(self, clean_up=True):
        """
        download, unzip, and preprocess data from EPA website, if needed
        """
        if os.path.exists(self.data_file):
            print("Skipping {}, already downloaded".format(self.year))
            return

        if (not os.path.exists(self.zip_file)) & (not os.path.exists(self.orig_file)):
            print("Downloading {} from {}, this will take a minute".format(self.year, self.url))
            self.download()

        if not os.path.exists(self.orig_file):
            print("Extracting and preprocessing {}".format(self.year))
            self.extract()

        self.preprocess()

        if clean_up:
            self.clean_up()


class EJ2015(AbcEJ):
    year = 2015
    filename = "EJSCREEN_20150505"
    id_col = "FIPS"


class EJ2016(AbcEJ):
    year = 2016
    filename = "EJSCREEN_V3_USPR_090216"

    @property
    def url(self):
        """
        string full path to download from EPA EJ site
        """
        return "{}/{}/{}_CSV.zip".format(self.base_url, self.year, self.filename)

    @property
    def orig_file(self):
        """
        string full path to where the unzipped file should be saved
        """
        return os.path.join(self.save_dir, "EJSCREEN_Full_V3_USPR_TSDFupdate.csv")


class EJ2017(AbcEJ):
    year = 2017
    filename = "EJSCREEN_2017_USPR_Public"

    @property
    def url(self):
        """
        downloads as csv, not zip
        """
        return "{}/{}/{}.csv".format(self.base_url, self.year, self.filename)

    @property
    def zip_file(self):
        """
        downloads as csv, not zip
        """
        return os.path.join(self.save_dir, "{}.csv".format(self.filename))

    def extract(self):
        pass


class EJ2018(AbcEJ):
    year = 2018

    @property
    def url(self):
        """
        string full path to download from EPA EJ site
        """
        return "{}/{}/{}_csv.zip".format(self.base_url, self.year, self.filename)

    @property
    def orig_file(self):
        """
        string full path to where the unzipped file should be saved
        """
        return os.path.join(self.save_dir, "EJSCREEN_Full_USPR_2018.csv")


class EJ2019(AbcEJ):
    year = 2019


class EJ2020(AbcEJ):
    year = 2020


class EJ2021(AbcEJ):
    year = 2021


class EJ2022(AbcEJ):
    year = 2022
    filename = "EJSCREEN_2022_Full_with_AS_CNMI_GU_VI_Tracts"


class EJ2023(AbcEJ):
    year = 2023
    filename = "EJSCREEN_2023_Tracts_with_AS_CNMI_GU_VI"

    @property
    def url(self):
        """
        string full path to download from EPA EJ site
        """
        return "{}/{}/2.22_September_UseMe/{}.csv.zip".format(self.base_url, self.year, self.filename)


class EJ2024(AbcEJ):
    year = 2024
    filename = "EJScreen_2024_Tract_with_AS_CNMI_GU_VI"

    @property
    def url(self):
        """
        string full path to download from EPA EJ site
        """
        return "{}/{}/2.32_August_UseMe/{}.csv.zip".format(self.base_url, self.year, self.filename)


def get_all():
    """
    utility to download all EPA EJ data
    """
    EJ2015().get_data()
    EJ2016().get_data()
    EJ2017().get_data()
    EJ2018().get_data()
    EJ2019().get_data()
    EJ2020().get_data()
    EJ2021().get_data()
    EJ2022().get_data()
    EJ2023().get_data()
    EJ2024().get_data()


if __name__ == "__main__":
    get_all()