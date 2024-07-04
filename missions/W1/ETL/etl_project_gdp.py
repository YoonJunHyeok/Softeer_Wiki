import re
import requests
from io import StringIO
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

gdp_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
region_url = "https://restcountries.com/v3.1/all?fields=name,region"
data_path = "Countries_by_GDP.json"
log_path = "etl_project_log.txt"

"""
로그 파일에 로그 기록
"""
def logging(message):
    current_time = datetime.now().strftime("%Y-%B-%d-%H-%M-%S")
    log = f"{current_time}, {message}"
    with open(log_path, "a") as log_file:
        log_file.write(f"{log}\n")

"""
Wikipedia에서 GDP 데이터 추출 후 DataFrame으로 반환
"""
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

        return gdp_df    
    else:
        raise Exception("Failed to fetch the webpage")

"""
GDP 데이터를 조건에 맞게 변환
"""
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

"""
JSON 파일로 저장
"""
def load_gdp_data(gdp_df: pd.DataFrame, data_path: str):
    logging("Start of load")
    
    # JSON 파일로 저장
    gdp_df.to_json(data_path, orient="records", indent=4, force_ascii=False)

    logging("End of load")

"""
n Billion USD 이상의 GDP를 가진 국가 출력
"""
def get_country_upper_n(data_path: str, n: int) -> list[str]:
    gdp_df = pd.read_json(data_path, orient="records")
    gdp_df = gdp_df[gdp_df["GDP"] >= n]
    print(gdp_df["Country"].tolist())

"""
API를 통해 각 Country의 Region 정보 DataFrame으로 반환
"""
def get_region_info() -> pd.DataFrame:
    response = requests.get(region_url)
    regions_json = response.json()

    data = [{"Country": item["name"]["common"], "Region": item["region"]} for item in regions_json]
    region_df = pd.DataFrame(data)
    region_df.loc[region_df["Country"] == "Czechia", "Country"] = "Czech Republic"
    region_df.loc[region_df["Country"] == "Republic of the Congo", "Country"] = "Congo"
    region_df.loc[region_df["Country"] == "Timor-Leste", "Country"] = "East Timor"

    return region_df

"""
각각의 Region 별로 GDP 상위 5개 국가의 평균 GDP 값 반환
"""
def top5_mean_gdp_by_region(data_path: str) -> dict:
    region_df = get_region_info()

    gdp_df = pd.read_json(data_path, orient="records")

    merged_df = pd.merge(left=gdp_df, right=region_df, on="Country", how="left")

    top_5_gdp_means = (
        merged_df.groupby("Region")["GDP"]
        .apply(lambda x: x.nlargest(5).mean())
        .reset_index(name="Top 5 GDP Mean")
    )

    print(top_5_gdp_means)

"""
화면 출력 요구사항 실행
"""
def run() -> None:
    get_country_upper_n(data_path, 100)
    top5_mean_gdp_by_region(data_path)

"""
ETL 프로세스 실행
"""
def ETL(url: str, data_path: str) -> None:
    gdp_df = extract_gdp_data(url)
    transformed_gdp_df = transform_gdp_data(gdp_df)
    load_gdp_data(transformed_gdp_df, data_path)

if __name__ == "__main__":
    ETL(gdp_url, data_path)

    run()