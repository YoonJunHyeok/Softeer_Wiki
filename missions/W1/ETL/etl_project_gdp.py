# Importing the libraries
import re
import requests
from io import StringIO
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

def log_time(message):
    current_time = datetime.now().strftime("%Y-%B-%d-%H-%M-%S")
    log_entry = f"{current_time}, {message}"
    with open("etl_project_log.txt", "a") as log_file:
        log_file.write(f"{log_entry}\n")

def extract_gdp_data(url: str) -> pd.DataFrame:
    log_time("Start of extraction from Wikipedia")

    html_content = requests.get(url).text

    soup = BeautifulSoup(html_content, "lxml")

    gdp_table = soup.find("table", "wikitable")

    table_df_list= pd.read_html(StringIO(str(gdp_table)))
    gdp_df = table_df_list[0]

    selected_columns = [
        ("Country/Territory", "Country/Territory"),
        ("IMF[1][13]", "Forecast"),
        ("IMF[1][13]", "Year")
    ]

    gdp_df = gdp_df[selected_columns]
    gdp_df.columns = ["Country", "GDP", "Year"]
    gdp_df = gdp_df[gdp_df["Country"] != "World"]

    gdp_df = gdp_df[(gdp_df["GDP"] != "—") & (gdp_df["Year"] != "—")]
    gdp_df["Year"] = gdp_df["Year"].apply(lambda x: re.sub(r"\[\w+ \d+\]", "", x).strip())

    return gdp_df

def transform_gdp_data(gdp_df: pd.DataFrame) -> pd.DataFrame:
    log_time("Start of transformation")
    
    gdp_df["GDP"] = (gdp_df["GDP"].astype(float) / 1000).map("{:.2f}".format)

    gdp_df["GDP"] = gdp_df["GDP"].astype(float)
    gdp_df["Year"] = gdp_df["Year"].astype(int)

    gdp_df.sort_values(by=["GDP"], ascending=False, inplace=True)

    log_time("End of transformation")
    return gdp_df

def load_gdp_data(gdp_df: pd.DataFrame):
    log_time("Start of load")
    
    gdp_df.to_json("Countries_by_GDP.json", orient="index", indent=4, force_ascii=False)

    log_time("End of load")

def get_country_upper_100() -> list[str]:
    gdp_df = pd.read_json("Countries_by_GDP.json", orient="index")
    gdp_df = gdp_df[gdp_df["GDP"] > 100]
    return gdp_df["Country"].tolist()

def top5_mean_gdp_by_region() -> dict:
    response = requests.get("https://restcountries.com/v3.1/all?fields=name,region")
    regions_json = response.json()

    data = [{"Country": item["name"]["common"], "Region": item["region"]} for item in regions_json]
    region_df = pd.DataFrame(data)
    region_df.loc[region_df["Country"] == "Czechia", "Country"] = "Czech Republic"
    region_df.loc[region_df["Country"] == "Republic of the Congo", "Country"] = "Congo"
    region_df.loc[region_df["Country"] == "Timor-Leste", "Country"] = "East Timor"

    gdp_df = pd.read_json("Countries_by_GDP.json", orient="index")

    merged_df = pd.merge(left=gdp_df, right=region_df, on="Country", how="left")

    top_5_gdp_means = (
        merged_df.groupby("Region")["GDP"]
        .apply(lambda x: x.nlargest(5).mean())
        .reset_index(name="Top 5 GDP Mean")
    )

    print(top_5_gdp_means)


if __name__ == "__main__":
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"

    gdp_df = extract_gdp_data(url)
    transformed_gdp_df = transform_gdp_data(gdp_df)
    load_gdp_data(transformed_gdp_df)

    countries_upper_100 = get_country_upper_100()
    print(countries_upper_100)

    top5_mean_gdp_by_region()