# Importing the libraries
import re
import json
import requests
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

def log_time(message):
    current_time = datetime.now().strftime('%Y-%B-%d-%H-%M-%S')
    log_entry = f"{current_time}, {message}"
    with open("etl_project_log.txt", "a") as log_file:
        log_file.write(f"{log_entry}\n")

def extract_gdp_data(url: str) -> pd.DataFrame:
    # Log start time
    log_time("Start of extraction from Wikipedia")

    # Step 1: Sending a HTTP request to a URL
    html_content = requests.get(url).text

    # Step 2: Parse the html content
    soup = BeautifulSoup(html_content, "lxml")

    # Step 3: Analyze the HTML tag, where your content lives
    gdp_table = soup.find("table", "wikitable")
    gdp_data_by_country = gdp_table.tbody.find_all("tr")

    data = []
    for gdps in gdp_data_by_country:
        gdp = gdps.find_all("td")
        
        if not gdp:
            continue

        country = gdp[0].get_text().strip()
        IMF_gdp = gdp[1].get_text().strip()
        year = gdp[2].get_text().strip()
        
        if country == "World" or IMF_gdp == "—":
            continue

        # Remove [n #] from the year using regex
        year = int(re.sub(r'\[.*?\]', '', year))
        
        # Convert IMF_gdp to an integer
        IMF_gdp = int(IMF_gdp.replace(',', ''))

        data.append([country, IMF_gdp, year])

    # Log end time
    log_time("End of extraction from Wikipedia")

    # Create a DataFrame
    gdp_df = pd.DataFrame(data, columns=["Country", "GDP", "Year"])
    return gdp_df

def transform_gdp_data(gdp_df: pd.DataFrame) -> pd.DataFrame:
    log_time("Start of transformation")
    
    # Convert GDP from millions to billions and format to two decimal places
    gdp_df["GDP"] = (gdp_df["GDP"] / 1000).map('{:.2f}'.format)

    log_time("End of transformation")
    return gdp_df

def load_gdp_data(gdp_df: pd.DataFrame):
    log_time("Start of load")
    
    gdp_df.to_json("Countries_by_GDP.json", orient="records", indent=4, force_ascii=False)

    log_time("End of load")

if __name__ == "__main__":
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"

    gdp_df = extract_gdp_data(url)
    transformed_gdp_df = transform_gdp_data(gdp_df)
    load_gdp_data(transformed_gdp_df)
