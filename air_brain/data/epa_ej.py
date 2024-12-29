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

from air_brain.util.loc import bg2zip

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
    rename_dict = {"OZONE": "O3",
                   "PTRAF": "traffic",
                   "DSLPM": "dpm",
                   "ACSTOTPOP": "totalpop",
                   "LOWINCPCT": "lowincome",
                   "MINORPCT": "poc",
                   }
    subs = ["PM25", "O3",
            "traffic", "dpm",
            ]
    demos = ["lowincome",
             "poc",
             ]

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
        with unified column names
        - ID
        - PM25
        - O3
        - area
        """
        return os.path.join(self.save_dir, "{}.csv".format(self.year))

    @property
    def tract_file(self):
        """
        string full path to where the census tract averaged file is saved
        """
        return os.path.join(self.save_dir, "{}_tract.csv".format(self.year))

    @property
    def zipcode_file(self):
        """
        string full path to where the zip code averaged file is saved
        """
        return os.path.join(self.save_dir, "{}_zipcode.csv".format(self.year))

    def download(self):
        download_url(self.url, self.zip_file)

    def extract(self):
        shutil.unpack_archive(self.zip_file, self.save_dir)

    def preprocess(self):
        """
        by default, preprocess extracted full data file to
        - unify column names
            census block group column: ID

            PM 2.5: PM25
            ozone: O3
            TODO nitrous oxide (where available): NO2

            lowincome: count of low income population

            total area of region: area
            total population of region: totalpop
        - subset to Allegheny County
        """
        df = pd.read_csv(self.orig_file)
        # unify column names
        df = df.rename(columns=self.rename_dict)
        # if applicable, sum land and water area to just get area
        if "AREALAND" in df.columns:
            df["area"] = df.AREALAND + df.AREAWATER
        # check that we have all the columns we need
        for col in ["ID", "area", "totalpop"] + self.subs + self.demos:
            assert col in df.columns, "{} not in columns for {}".format(col, self.year)
        # subset to Allegheny County
        df = df.loc[df.ID.astype(str).str.startswith("42003")]
        df.to_csv(self.data_file)

    def avg_by_tract(self):
        """
        EPA EJ data is provided averaged over census block group
        re-average that to the census tract, weighted by area

        this will subset the data to only self.subs, e.g. PM 2.5 and ozone
        and self.demos, e.g. lowincome
        we can include demos here because census block groups are subsets of census tracts
        """
        df = pd.read_csv(self.data_file)
        # overwrite the ID to be the tract number, not the block group
        df.ID = df.ID.astype(str).str[:-1]
        # average over tract, weighting by area, for each substance
        # average over tract, weighting by population, for each demographic
        agg_dict = {"area": "sum",
                    "totalpop": "sum"}
        for sub in self.subs:
            df["{}_x_area".format(sub)] = df[sub] * df.area
            agg_dict["{}_x_area".format(sub)] =  "sum"
        for demo in self.demos:
            df["{}_x_pop".format(demo)] = df[demo] * df.totalpop
            agg_dict["{}_x_pop".format(demo)] = "sum"
        avg_df = df.groupby("ID").agg(agg_dict).reset_index()
        for sub in self.subs:
            avg_df[sub] = avg_df["{}_x_area".format(sub)] / avg_df.area
        for demo in self.demos:
            avg_df[demo] = avg_df["{}_x_pop".format(demo)] / avg_df.totalpop
            avg_df.loc[avg_df.totalpop == 0, demo] = 0
        # write out for later
        avg_df[["ID"] + self.subs + self.demos].to_csv(self.tract_file, index=False)

    def avg_by_zipcode(self):
        """
        EPA EJ data is provided averaged over census block group
        re-average that to the zipcode, weighted by area

        this will subset the data to only self.subs, e.g. PM 2.5 and ozone
        it doesn't make sense to average demographics by area, so I won't do it
        """
        bg_df = pd.read_csv(self.data_file)
        df = bg2zip(bg_df, self.subs)
        df.to_csv(self.zipcode_file, index=False)
        return df

    def clean_up(self):
        """
        by default, remove larger original files
        """
        return
        try:
            os.remove(self.zip_file)
        except FileNotFoundError:
            pass
        try:
            os.remove(self.orig_file)
        except FileNotFoundError:
            pass

    def get_data(self,
                 by_tract=True, # re-average over census tracts
                 by_zipcode=True, # re-average over zip codes
                 clean_up=True):
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

        if by_tract:
            self.avg_by_tract()

        if by_zipcode:
            self.avg_by_zipcode()

        if clean_up:
            self.clean_up()


class EJ2015(AbcEJ):
    year = 2015
    filename = "EJSCREEN_20150505"
    rename_dict = {"FIPS": "ID",
                   "pm": "PM25",
                   "o3": "O3",
                   "traffic.score": "traffic",
                   "pctlowinc": "lowincome",
                   "pctmin": "poc",
                   "pop": "totalpop"}


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
    rename_dict = {"OZONE": "O3",
                   "PTRAF": "traffic",
                   "DSLPM": "dpm",
                   "ACSTOTPOP": "totalpop",
                   "LOWINCPCT": "lowincome",
                   "PEOPCOLORPCT": "poc",
                   }

    @property
    def url(self):
        """
        string full path to download from EPA EJ site
        """
        return "{}/{}/2.22_September_UseMe/{}.csv.zip".format(self.base_url, self.year, self.filename)


class EJ2024(AbcEJ):
    year = 2024
    filename = "EJScreen_2024_Tract_with_AS_CNMI_GU_VI"
    rename_dict = {"OZONE": "O3",
                   "PTRAF": "traffic",
                   "DSLPM": "dpm",
                   "ACSTOTPOP": "totalpop",
                   "LOWINCPCT": "lowincome",
                   "PEOPCOLORPCT": "poc",
                   }

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