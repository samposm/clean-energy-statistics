import os
from pathlib import Path

import numpy as np
import pandas as pd
import pycountry
import requests

data_path = Path(__file__).parent.absolute() / "data"
Path(data_path).mkdir(parents=True, exist_ok=True)

# Statistical Review of World Energy, data
# https://www.energyinst.org/statistical-review
energy_url = "https://www.energyinst.org/__data/assets/excel_doc/0020/1540550/EI-Stats-Review-All-Data.xlsx"
# UN population data
# https://population.un.org/wpp/downloads?folder=Standard%20Projections&group=CSV%20format
population_url = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_TotalPopulationBySex.csv.gz"

def download_data(url):
    filename = os.path.basename(url)
    file = data_path / filename
    if not os.path.isfile(file):
        response = requests.get(url)
        with open(file, "wb") as f:
            f.write(response.content)
    return file

def clean_data(df):
    # drop rows at the end of the excel table
    imax = (df.iloc[:, 0] == "Total World").idxmax()
    rows1 = df.index > imax
    # drop empty rows
    rows2 = df.iloc[:, 0].isnull()
    # drop rows that are not countries
    rows3 = df.iloc[:, 0].isin(not_countries)

    rows_to_keep = ~(rows1 | rows2 | rows3)
    return df.loc[rows_to_keep].iloc[:, :-3]  # also drop last 3 columns

def read_energy_data(file):
    sheet_names = [
        "Hydro Generation - TWh",
        "Nuclear Generation - TWh",
        "Solar Generation - TWh",
        "Wind Generation - TWh"
    ]
    def make_data(sheet_name):
        return (
            # read excel sheet, get column names from 3rd row
            pd.read_excel(file, sheet_name=sheet_name, header=2)
            # clean data by dropping some rows and columns
            .pipe(clean_data)
            # change from wide to long format
            .melt(id_vars="Terawatt-hours", var_name="Year", value_name=sheet_name)
            .rename(columns={"Terawatt-hours": "Country"})
            .sort_values(by=["Country", "Year"])
            .set_index(["Country", "Year"])
        )
    dfs = [make_data(sheet_name) for sheet_name in sheet_names]
    return pd.concat(dfs, axis=1).reset_index()

def read_population_data(file):
    return (
        pd.read_csv(file, compression='gzip', low_memory=False)
        .loc[:, ['ISO3_code', 'Location', 'Time', 'PopTotal']]  # only the columns we need
    )

not_countries = ['Total North America', 'Central America', 'Other Caribbean', 'Other South America', 'Total S. & Cent. America',
    'Other Europe', 'Total Europe', 'Other CIS', 'Total CIS', 'Other Middle East', 'Total Middle East',
    'Eastern Africa', 'Middle Africa', 'Western Africa', 'Other Northern Africa', 'Other Southern Africa', 'Total Africa',
    'Other Asia Pacific', 'Total Asia Pacific', 'Total World']

def list_countries(*args):
    countries_set = set()
    for df in args:
        countries_set.update(df.iloc[:, 0].tolist())
    return sorted(countries_set)

def main():
    energy_file, population_file = download_data(energy_url), download_data(population_url)
    energy_df = read_energy_data(energy_file)
    population_df = read_population_data(population_file)

    print(energy_df)
    print(energy_df.columns)
    #print(population_df)

    #bp_countries = list_countries(df_hydro, df_nuclear, df_solar, df_wind)
    #un_countries = set(df_population["Location"].unique())

    #for c in bp_countries:
    #    print(f"{c:<22} {c in un_countries}")
    #    if not c in un_countries:
    #        try:
    #            names = pycountry.countries.search_fuzzy(c)
    #            for n in names: print(f"  {n}")
    #        except:
    #            pass

    #print(sorted(un_countries))

    #print(df_hydro)
    #print(df_nuclear)
    #print(df_solar)
    #print(df_wind)
    #print(df_population)
    #print(df_population.columns)

if __name__ == "__main__": main()
