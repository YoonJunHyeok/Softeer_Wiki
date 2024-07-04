import re
import os
import logging
import requests
from io import StringIO
from datetime import datetime

import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

gdp_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
region_url = "https://restcountries.com/v3.1/all?fields=name,region"
db_path = "World_Economies.db"
table_name = "Countries_by_GDP"
log_path = "etl_project_log.txt"

class SQLExecutor:
    def __init__(self, database, table=None):
        self.database = database
        self.table = table

        if not os.path.exists(database):
            open(database, 'w').close()

        if table:
            self.create_table()

    def create_table(self):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Country TEXT NOT NULL,
            GDP_USD_billion REAL NOT NULL,
            Region TEXT NOT NULL,
            Year INTEGER NOT NULL
        )
        """
        self.run_sql(create_table_query)

    """ 
    query에서 주석 제거
    """
    def strip_sql_comments(self, query):
        # (--) 제거
        query = re.sub(r'--.*', '', query)
        # (/* */) 제거, flags=re.DOTALL으로 \n도 .에 포함되도록
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
        return query

    def run_sql(self, query, data=None):
        try:
            cleaned_query = self.strip_sql_comments(query).strip()
            
            with sqlite3.connect(self.database) as conn:
                cur = conn.cursor()

                if data:
                    cur.executemany(cleaned_query, data)
                else:
                    cur.execute(cleaned_query)

                query_type = cleaned_query.split()[0].upper()

                if query_type in ['SELECT', 'WITH']:
                    result = cur.fetchall()
                    return result
                elif query_type in ['INSERT', 'UPDATE', 'DELETE', 'CREATE']:
                    conn.commit()
                    if query_type == 'INSERT':
                        return cur.lastrowid
                    else:
                        return cur.rowcount
                else:
                    print("Unsupported query type")
                    return None
        except sqlite3.Error as e:
            print(e)
            return None

def logging(message):
    current_time = datetime.now().strftime("%Y-%B-%d-%H-%M-%S")
    log = f"{current_time}, {message}"
    with open(log_path, "a") as log_file:
        log_file.write(f"{log}\n")

def extract_gdp_data(url: str) -> pd.DataFrame:
    logging("Start of extraction from Wikipedia")

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "lxml")

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

        logging("End of extraction from Wikipedia")
        return gdp_df    
    else:
        raise Exception("Failed to fetch the webpage")

def transform_gdp_data(gdp_df: pd.DataFrame) -> pd.DataFrame:
    logging("Start of transformation")

    # World 제거
    gdp_df = gdp_df[gdp_df["Country"] != "World"]

    # 결측값 제거
    gdp_df = gdp_df[(gdp_df["GDP"] != "—") & (gdp_df["Year"] != "—")]
    # 연도에 같이 있는 주석 제거
    gdp_df["Year"] = gdp_df["Year"].apply(lambda x: re.sub(r"\[\w+ \d+\]", "", x).strip())
    # GDP, Year를 float로 변환
    gdp_df["GDP"] = gdp_df["GDP"].astype(float)
    gdp_df["Year"] = gdp_df["Year"].astype(int)

    # 1B USD로 변환
    gdp_df["GDP"] = (gdp_df["GDP"] / 1000).map("{:.2f}".format).astype(float)

    # GDP 기준으로 내림차순 정렬
    gdp_df.sort_values(by=["GDP"], ascending=False, inplace=True)

    logging("End of transformation")
    return gdp_df

def get_region_info() -> pd.DataFrame:
    response = requests.get(region_url)
    regions_json = response.json()

    data = [{"Country": item["name"]["common"], "Region": item["region"]} for item in regions_json]
    region_df = pd.DataFrame(data)
    region_df.loc[region_df["Country"] == "Czechia", "Country"] = "Czech Republic"
    region_df.loc[region_df["Country"] == "Republic of the Congo", "Country"] = "Congo"
    region_df.loc[region_df["Country"] == "Timor-Leste", "Country"] = "East Timor"

    return region_df

def load_gdp_data(gdp_df: pd.DataFrame, db_path: str, table_name: str):
    logging("Start of load")

    executor = SQLExecutor(database=db_path, table=table_name)

    region_df = get_region_info()
    merged_df = pd.merge(left=gdp_df, right=region_df, on="Country", how="left")

    insert_query = f"""INSERT INTO {table_name} (Country, GDP_USD_billion, Region, Year) VALUES (?, ?, ?, ?)"""
    data = merged_df[['Country', 'GDP', 'Region', 'Year']].values.tolist()
    executor.run_sql(insert_query, data)

    logging("End of load")

def get_country_upper_n(db_path: str, table_name: str, n: int) -> list[str]:
    executor = SQLExecutor(database=db_path)

    query = f"""SELECT Country 
                FROM {table_name} 
                WHERE GDP_USD_billion >= {n}"""

    result = executor.run_sql(query)
    print(result)

def topN_mean_gdp_by_region(db_path: str, table_name: str, n: int) -> dict:
    executor = SQLExecutor(database=db_path)

    query = f"""WITH Ranked_{table_name} AS (
                    SELECT *
                        , RANK() OVER(PARTITION BY Region ORDER BY GDP_USD_billion DESC) AS Rank_Per_Region
                    FROM {table_name}
                )
                SELECT Region, AVG(GDP_USD_billion) AS Mean_GDP
                FROM Ranked_{table_name}
                WHERE Rank_Per_Region <= {n}
                GROUP BY Region
                ORDER BY Mean_GDP DESC
            """

    result = executor.run_sql(query)
    print(result)

def run() -> None:
    get_country_upper_n(db_path, table_name, 100)
    topN_mean_gdp_by_region(db_path, table_name, 5)

def ETL(url: str, db_path: str, table_name: str) -> None:
    gdp_df = extract_gdp_data(url)
    transformed_gdp_df = transform_gdp_data(gdp_df)
    load_gdp_data(transformed_gdp_df, db_path, table_name)

if __name__ == "__main__":
    ETL(gdp_url, db_path, table_name)

    run()