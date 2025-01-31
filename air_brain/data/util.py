"""
general utilities for downloading data
"""
import os
import requests
import shutil

from air_brain.config import data_dir

def download_url(url, save_path,
                 verify=True,
                 chunk_size=128):
    r = requests.get(url, stream=True, verify=verify)
    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

def download_zip(url: str, save_dir, verify=True):
    """
    download a zipfile from url, save temporarily, and extract to save_dir
    """
    zip_filename = data_dir / "tmp.zip"
    download_url(url, zip_filename, verify=verify)
    shutil.unpack_archive(zip_filename, save_dir)
    os.remove(zip_filename)