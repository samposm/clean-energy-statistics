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
sheet_names = [
    "Hydro Generation - TWh",
    "Nuclear Generation - TWh",
    "Solar Generation - TWh",
    "Wind Generation - TWh"
]

# UN population data
# https://population.un.org/wpp/downloads?folder=Standard%20Projections&group=CSV%20format
population_url = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_TotalPopulationBySex.csv.gz"

not_countries = ['Total North America', 'Central America', 'Other Caribbean',
    'Other South America', 'Total S. & Cent. America', 'Other Europe', 'Total Europe', 'Other CIS',
    'Total CIS', 'Other Middle East', 'Total Middle East', 'Eastern Africa', 'Middle Africa',
    'Western Africa', 'Other Northern Africa', 'Other Southern Africa', 'Total Africa',
    'Other Asia Pacific', 'Total Asia Pacific', 'Total World']


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

def handle_country_names(energy_df, population_df):
    # In energy data and in population data, some countries go by different names.
    #
    # Also, in population data "Russian Federation" starts from 1965. But energy data
    # has non-zero data for "USSR" from 1965 to 1984 and then zeros. And for
    # "Russian Federation" from pre-1985 is zeros, and non-zero from 1985. 
    country_replacements_energy = {
        "China Hong Kong SAR": "Hong Kong",
        "Czech Republic": "Czechia",
        "Trinidad & Tobago": "Trinidad and Tobago",
        "Turkey": "Türkiye",
        "US": "United States",
    }

    country_replacements_population = {
        "China, Hong Kong SAR": "Hong Kong",
        "Iran (Islamic Republic of)" : "Iran",
        "Republic of Korea": "South Korea",
        "China, Taiwan Province of China": "Taiwan",
        "United States of America": "United States",
        "Viet Nam": "Vietnam",
        "Venezuela (Bolivarian Republic of)": "Venezuela",
    }

    energy_df["Country"] = energy_df["Country"].replace(country_replacements_energy)
    population_df["Location"] = population_df["Location"].replace(country_replacements_population)

    # Combine "Russian Federation" and "USSR" energy data
    for col in sheet_names:
        i_selection_rus = energy_df["Country"] == "Russian Federation"
        i_selection_ussr = energy_df["Country"] == "USSR"
        rus_vals = energy_df.loc[i_selection_rus, col].to_numpy()
        ussr_vals = energy_df.loc[i_selection_ussr, col].to_numpy()
        energy_df.loc[i_selection_rus, col] = rus_vals + ussr_vals

    # Drop "USSR" rows
    energy_df = energy_df[energy_df["Country"] != "USSR"].reset_index(drop=True)

    return energy_df, population_df

def main():

    energy_file, population_file = download_data(energy_url), download_data(population_url)
    energy_df = read_energy_data(energy_file)
    # df columns:
    #   Country
    #   Year
    #   Hydro Generation - TWh
    #   Nuclear Generation - TWh
    #   Solar Generation - TWh
    #   Wind Generation - TWh

    population_df = read_population_data(population_file)
    # df columns: ISO3_code, Location, Time, PopTotal

    energy_df, population_df = handle_country_names(energy_df, population_df)

    df = energy_df.merge(
            population_df[["Location", "Time", "PopTotal"]]
            .rename(columns={"Location": "Country", "Time": "Year", "PopTotal": "Population"}),
        how="left",
        on=["Country", "Year"],
    )

    # TWh produced per capita in one year
    for col in sheet_names:
        df[col + " per capita"] = df[col] / df["Population"]

    # We don't need these columns anymore    
    df = df.drop(columns=sheet_names + ["Population"])

    print(df)

if __name__ == "__main__": main()
