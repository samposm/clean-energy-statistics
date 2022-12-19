#!/usr/bin/env python

import numpy as np
import openpyxl  # not used directly, but used by pd.read_excel
import os
import pathlib
import pycountry
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

def read_bp_data(bp_file):
    sheet_names = {
        'hydro':   "Hydro Generation - TWh",
        'nuclear': "Nuclear Generation - TWh",
        'solar':   "Solar Generation - TWh",
        'wind':    "Wind Generation - TWh"
    }
    header_row = 2

    df_hydro   = pd.read_excel(bp_file, sheet_name=sheet_names['hydro'], header=header_row)
    df_nuclear = pd.read_excel(bp_file, sheet_name=sheet_names['nuclear'], header=header_row)
    df_solar   = pd.read_excel(bp_file, sheet_name=sheet_names['solar'], header=header_row)
    df_wind    = pd.read_excel(bp_file, sheet_name=sheet_names['wind'], header=header_row)

    return df_hydro, df_nuclear, df_solar, df_wind

def read_un_data(un_file):
    df = pd.read_csv(un_file, low_memory=False)
    return df[['ISO3_code', 'Location', 'Time', 'PopTotal']]  # Take only the columns we need

not_countries = ['Total North America', 'Central America', 'Other Caribbean', 'Other South America', 'Total S. & Cent. America',
    'Other Europe', 'Total Europe', 'Other CIS', 'Total CIS', 'Other Middle East', 'Total Middle East',
    'Eastern Africa', 'Middle Africa', 'Western Africa', 'Other Northern Africa', 'Other Southern Africa', 'Total Africa',
    'Other Asia Pacific', 'Total Asia Pacific', 'Total World']

def clean_bp(df):
    # drop rows from the end of the excel table
    imax = (df.iloc[:, 0] == "Total World").idxmax()
    rows1 = df.index[df.index > imax].tolist()
    # drop empty rows
    rows2 = list(np.where(df.iloc[:, 0].isnull())[0])
    # drop rows that are not countries
    rows3 = list(np.where(df.iloc[:, 0].isin(not_countries))[0])
    # now drop the rows
    rows_to_drop = rows1 + rows2 + rows3
    df.drop(labels=rows_to_drop, axis=0, inplace=True)
    # drop last 3 columns
    df = df.iloc[:, :-3]
    return df

def list_countries(*args):
    countries_set = set()
    for df in args:
        countries_set.update(df.iloc[:, 0].tolist())
    return sorted(countries_set)

def main():
    bp_file, un_file = download_bp_data(), download_un_data()
    df_hydro, df_nuclear, df_solar, df_wind = read_bp_data(bp_file)
    df_hydro, df_nuclear, df_solar, df_wind = clean_bp(df_hydro), clean_bp(df_nuclear), clean_bp(df_solar), clean_bp(df_wind)

    df_population = read_un_data(un_file)

    bp_countries = list_countries(df_hydro, df_nuclear, df_solar, df_wind)
    un_countries = set(df_population["Location"].unique())

    for c in bp_countries:
        print(f"{c:<22} {c in un_countries}")
        if not c in un_countries:
            try:
                names = pycountry.countries.search_fuzzy(c)
                for n in names: print(f"  {n}")
            except:
                pass

    #print(sorted(un_countries))

    #print(df_hydro)
    #print(df_nuclear)
    #print(df_solar)
    #print(df_wind)
    #print(df_population)
    #print(df_population.columns)

if __name__ == "__main__": main()
