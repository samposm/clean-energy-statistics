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
    "Wind Generation - TWh",
]

not_countries = ["Total North America", "Central America", "Other Caribbean",
    "Other South America", "Total S. & Cent. America", "Other Europe", "Total Europe", "Other CIS",
    "Total CIS", "Other Middle East", "Total Middle East", "Eastern Africa", "Middle Africa",
    "Western Africa", "Other Northern Africa", "Other Southern Africa", "Total Africa",
    "Other Asia Pacific", "Total Asia Pacific", "Total World"]

country_replacements_energy = {
    "China Hong Kong SAR": "Hong Kong",
    "Czech Republic": "Czechia",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "Turkey": "Türkiye",
    "US": "United States",
}

# UN population data
# https://population.un.org/wpp/downloads?folder=Standard%20Projections&group=CSV%20format
population_url = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_TotalPopulationBySex.csv.gz"

country_replacements_population = {
    "China, Hong Kong SAR": "Hong Kong",
    "Iran (Islamic Republic of)" : "Iran",
    "Republic of Korea": "South Korea",
    "China, Taiwan Province of China": "Taiwan",
    "United States of America": "United States",
    "Viet Nam": "Vietnam",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
}

def download_data(url):
    filename = Path(url).name
    file = data_path / filename
    if not file.exists():
        response = requests.get(url)
        with open(file, "wb") as f:
            f.write(response.content)
    return file

def clean_energy_data(df):
    imax = (df.iloc[:, 0] == "Total World").idxmax()
    mask = (
        (df.index <= imax) &  # drop rows at the end
        (df.iloc[:, 0].notnull()) &  # drop empty rows
        (~df.iloc[:, 0].isin(not_countries))  # drop rows that are not countries
    )
    return df.loc[mask].iloc[:, :-3]  # also drop last 3 columns

def read_energy_data(file):
    dfs = [
        (
            # read excel sheet, get column names from 3rd row
            pd.read_excel(file, sheet_name=sheet_name, header=2)
            # clean data by dropping some rows and columns
            .pipe(clean_energy_data)
            # change from wide to long format
            .melt(id_vars="Terawatt-hours", var_name="Year", value_name=sheet_name)
            .rename(columns={"Terawatt-hours": "Country"})
            .sort_values(by=["Country", "Year"])
            .set_index(["Country", "Year"])
        )
        for sheet_name in sheet_names
    ]
    return pd.concat(dfs, axis=1).reset_index()

def read_population_data(file):
    return (
        pd.read_csv(file, compression='gzip', low_memory=False)
        .loc[:, ['ISO3_code', 'Location', 'Time', 'PopTotal']]  # only the columns we need
    )

def combine_russian_federation_and_ussr(energy_df):
    # Combine "Russian Federation" and "USSR" energy data
    for col in sheet_names:
        i_selection_rus = energy_df["Country"] == "Russian Federation"
        i_selection_ussr = energy_df["Country"] == "USSR"
        rus_vals = energy_df.loc[i_selection_rus, col].to_numpy()
        ussr_vals = energy_df.loc[i_selection_ussr, col].to_numpy()
        energy_df.loc[i_selection_rus, col] = rus_vals + ussr_vals

    # Drop "USSR" rows
    return energy_df[energy_df["Country"] != "USSR"].reset_index(drop=True)

def handle_country_names(energy_df, population_df):
    # In energy data and in population data, some countries go by different names.
    #
    # Also, in population data "Russian Federation" starts from 1965. But energy data
    # has non-zero data for "USSR" from 1965 to 1984 and then zeros. And for
    # "Russian Federation" from pre-1985 is zeros, and non-zero from 1985. 
    energy_df["Country"] = energy_df["Country"].replace(country_replacements_energy)
    population_df["Location"] = population_df["Location"].replace(country_replacements_population)
    energy_df = combine_russian_federation_and_ussr(energy_df)
    return energy_df, population_df

def combine_data(energy_df, population_df):
    energy_df, population_df = handle_country_names(energy_df, population_df)
    population_df = (
        population_df[["Location", "Time", "PopTotal"]]
        .rename(columns={"Location": "Country", "Time": "Year", "PopTotal": "Population"})
    )
    return energy_df.merge(population_df, how="left", on=["Country", "Year"])

def calculate_per_capita(df):
    # TWh produced per capita in one year
    for col in sheet_names:
        new_col = col.replace("Generation - TWh", "")
        df[new_col] = df[col] / df["Population"]
    # We don't need these columns anymore    
    return df.drop(columns=sheet_names + ["Population"])

def calculate_increase_in_10_year_windows(df):
    return (
        df.set_index(["Country", "Energy Source", "Year"])
        .groupby(["Country", "Energy Source"])
        # "Year" remains as index in the group objects, we apply calculations only
        # to the "kWh per Capita" column
        .diff().rolling(window=10, min_periods=10).mean()
        .reset_index()
    )

def calculate_10_year_increases(energy_df, population_df):
    return (
        combine_data(energy_df, population_df)
        # columns:
        #   Country
        #   Year
        #   Hydro Generation - TWh
        #   Nuclear Generation - TWh
        #   Solar Generation - TWh
        #   Wind Generation - TWh
        #   Population
        .pipe(calculate_per_capita)
        # columns: Country, Year, Hydro, Nuclear, Solar, Wind
        # to long format
        .melt(id_vars=["Country", "Year"], var_name="Energy Source", value_name="TWh per Capita")
        # columns: Country, Year, Energy Source, TWh per Capita
        # from TWh to kWh
        .assign(**{"kWh per Capita": lambda x: x["TWh per Capita"] * 1e6})
        .drop(columns="TWh per Capita")
        # from kWh per capita to 10-year rolling average of yearly increases
        .pipe(calculate_increase_in_10_year_windows)
    )

def find_max_increases(increase_df, num_countries=20):
    # Countries by maximum 10 year increases, ordered
    max_df = (
        increase_df.drop(columns="Energy Source")
        .groupby(["Country", "Year"])
        .sum()
        .reset_index()
        .sort_values(by="kWh per Capita", ascending=False)
    )
    # Drop second etc. maximums from a same country
    is_duplicate_country = max_df.duplicated(subset=["Country"], keep="first")
    max_df = max_df[~is_duplicate_country].reset_index(drop=True).head(num_countries)

    # Merge back the energy sources contributing to the maximums
    df = (
        max_df.merge(increase_df, how="left", on=["Country", "Year"])
        .rename(columns={"kWh per Capita_x": "Combined kWh per Capita", "kWh per Capita_y": "kWh per Capita"})
    )
    # Maximum in column "kWh per Capita", its components in column "kWh per Capita"
    nonzero_rows = df["kWh per Capita"] > 0 
    return df[nonzero_rows].reset_index(drop=True)

def main():

    energy_file, population_file = download_data(energy_url), download_data(population_url)
    energy_df = read_energy_data(energy_file)
    # columns:
    #   Country
    #   Year
    #   Hydro Generation - TWh
    #   Nuclear Generation - TWh
    #   Solar Generation - TWh
    #   Wind Generation - TWh

    population_df = read_population_data(population_file)
    # columns: ISO3_code, Location, Time, PopTotal

    increase_df = calculate_10_year_increases(energy_df, population_df)
    # columns: Country, Year, Energy Source, kWh per Capita

    df = find_max_increases(increase_df)
    # columns: Country, Year (end of 10-year window), Combined kWh per Capita, Energy Source, kWh per Capita,

    print(df.to_string())


if __name__ == "__main__": main()
