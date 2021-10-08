#!/usr/bin/env python

import os
import pathlib
import urllib.request

def get_data():
    # BP energy data
    bp_url = "https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-2021-all-data.xlsx"
    # UN population data
    un_url = "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_TotalPopulationBySex.csv"

    bp_file, un_file = bp_url.split('/')[-1], un_url.split('/')[-1]
    path = "data/"
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    for file_name in [bp_file, un_file]:
        if not os.path.isfile(path + file_name):
            urllib.request.urlretrieve(bp_url, path + file_name)

    return bp_file, un_file

def main():
    bp_file, un_file = get_data()

if __name__ == "__main__": main()
