#!/usr/bin/env python

import os
import pathlib
import requests
import ssl
import pandas as pd
import urllib3

data_path = "data/"

def download_bp_data():
    # BP energy data
    # https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy.html
    bp_url = "https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-2022-all-data.xlsx"
    bp_file = data_path + bp_url.split('/')[-1]
    pathlib.Path(data_path).mkdir(parents=True, exist_ok=True)
    if not os.path.isfile(bp_file):
        response = requests.get(bp_url)
        with open(bp_file, "wb") as f: f.write(response.content)
    return bp_file

def download_un_data():
    # UN population data
    # https://population.un.org/wpp/Download/Standard/CSV/
    un_url = "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_TotalPopulationBySex.zip"
    un_file = data_path + un_url.split('/')[-1]
    pathlib.Path(data_path).mkdir(parents=True, exist_ok=True)

    # UN web server is not up to the modern safety standards of OpenSSL 3, needs special connection
    # https://stackoverflow.com/questions/71603314/ssl-error-unsafe-legacy-renegotiation-disabled
    class CustomHttpAdapter (requests.adapters.HTTPAdapter):
        def __init__(self, ssl_context=None, **kwargs):
            self.ssl_context = ssl_context
            super().__init__(**kwargs)
        def init_poolmanager(self, connections, maxsize, block=False):
            self.poolmanager = urllib3.poolmanager.PoolManager(num_pools=connections, maxsize=maxsize, block=block, ssl_context=self.ssl_context)

    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    legacy_session = requests.session()
    legacy_session.mount('https://', CustomHttpAdapter(context))

    if not os.path.isfile(un_file):
        response = legacy_session.get(un_url)
        with open(un_file, "wb") as f: f.write(response.content)
    return un_file

def parse_bp_data(bp_file):

    sheet_names = {
        'nuclear': "Nuclear Generation - TWh",
        'hydro':   "Hydro Generation - TWh",
        'solar':   "Solar Generation - TWh",
        'wind':    "Wind Generation - TWh"
    }
    header_row = 2

    df_nuclear = pd.read_excel(bp_file, sheet_name=sheet_names['nuclear'], header=header_row)
    df_hydro   = pd.read_excel(bp_file, sheet_name=sheet_names['hydro'], header=header_row)
    df_wind    = pd.read_excel(bp_file, sheet_name=sheet_names['wind'], header=header_row)
    df_solar   = pd.read_excel(bp_file, sheet_name=sheet_names['solar'], header=header_row)

    print(df_nuclear)
    print(df_hydro)
    print(df_wind)
    print(df_solar)

def main():
    bp_file, un_file = download_bp_data(), download_un_data()
    parse_bp_data(bp_file)

if __name__ == "__main__": main()
