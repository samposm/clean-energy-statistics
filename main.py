#!/usr/bin/env python

import os
import pathlib
import urllib.request
import pandas as pd

def get_data():
    # BP energy data
    # https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy.html
    bp_url = "https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-2022-all-data.xlsx"

    # UN population data
    # https://population.un.org/wpp/Download/Standard/CSV/
    un_url = "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2022_TotalPopulationBySex.zip"

    path = "data/"
    bp_file, un_file = path + bp_url.split('/')[-1], path + un_url.split('/')[-1]
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    for file_name in [bp_file, un_file]:
        if not os.path.isfile(file_name):
            urllib.request.urlretrieve(bp_url, file_name)

    return bp_file, un_file

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
    bp_file, un_file = get_data()
    parse_bp_data(bp_file)

if __name__ == "__main__": main()
