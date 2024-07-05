import re
import requests
from enum import Enum
from io import StringIO
from datetime import datetime
from multiprocessing import Pool

import pandas as pd
from bs4 import BeautifulSoup

gdp_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
region_url = "https://restcountries.com/v3.1/all?fields=name,region"
data_path = "Countries_by_GDP.json"
log_path = "etl_project_log.txt"

class LogLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"

"""
로그 파일에 로그 기록
"""
def logging(message: str, level: LogLevel) -> None:
    current_time = datetime.now().strftime("%Y-%B-%d-%H-%M-%S")
    log = f"[{level.value}]: {current_time}, {message}"
    with open(log_path, "a") as log_file:
        log_file.write(f"{log}\n")

"""
Wikipedia에서 GDP 데이터 추출 후 DataFrame으로 반환
"""
def extract_gdp_data(url: str) -> pd.DataFrame:
    logging("Starting extraction", LogLevel.INFO)

    try: 
        response = requests.get(url)

        # HTTP 응답 코드가 400 이상인 경우 HTTPError 예외를 발생
        response.raise_for_status() 

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

        logging("Extraction finished successfully", LogLevel.INFO)
        return gdp_df 
    except Exception as e:
        logging(f"Error during extraction: {e}", LogLevel.ERROR)
        raise Exception("Error during extraction")

"""
API를 통해 각 Country의 Region 정보 DataFrame으로 반환
"""
def get_region_info() -> pd.DataFrame:
    try:
        response = requests.get(region_url)
        regions_json = response.json()

        data = [{"Country": item["name"]["common"], "Region": item["region"]} for item in regions_json]
        region_df = pd.DataFrame(data)
        # 예외처리
        region_df.replace({"Czechia": "Czech Republic", "Republic of the Congo": "Congo", "Timor-Leste": "East Timor"}, inplace=True)

        return region_df
    except Exception as e:
        raise Exception("Error during region info extraction")

def process_row(row: pd.Series) -> pd.Series:
    # World 제거
    if row["Country"] == "World":
        return None

    # 결측값 제거
    if row["GDP"] == "—" or row["Year"] == "—":
        return None

    # 연도에 같이 있는 주석 제거
    row["Year"] = re.sub(r"\[\w+ \d+\]", "", row["Year"]).strip()

    # GDP, Year 변환
    row["GDP"] = round(float(row["GDP"]) / 1000, 2)
    row["Year"] = int(row["Year"])
    
    return row

"""
GDP 데이터를 조건에 맞게 변환
"""
def transform_gdp_data(gdp_df: pd.DataFrame) -> pd.DataFrame:
    logging("Starting transformation", LogLevel.INFO)

    try:
        # 열 이름 변경
        gdp_df.columns = ["Country", "GDP", "Year"]

        with Pool() as pool:
            processed_rows = pool.map(process_row, [row for _, row in gdp_df.iterrows()])
        
        gdp_df = pd.DataFrame([row for row in processed_rows if row is not None])

        # Region 정보 추가
        region_df = get_region_info()
        gdp_region_df = pd.merge(left=gdp_df, right=region_df, on="Country", how="left")
        
        # GDP 기준으로 내림차순 정렬
        gdp_region_df.sort_values(by=["GDP"], ascending=False, inplace=True)

        logging("Transformation finished successfully", LogLevel.INFO)
        return gdp_region_df
    except Exception as e:
        logging(f"Error during transformation: {e}", LogLevel.ERROR)
        raise Exception("Error during transformation")

"""
JSON 파일로 저장
"""
def load_gdp_data(gdp_df: pd.DataFrame, data_path: str):
    logging("Start of load", LogLevel.INFO)
    
    try:
        # JSON 파일로 저장
        gdp_df.to_json(data_path, orient="records", indent=4, force_ascii=False)

        logging("End of load", LogLevel.INFO)
    except Exception as e:
        logging(f"Error during load: {e}", LogLevel.ERROR)
        raise Exception("Error during load")

"""
n Billion USD 이상의 GDP를 가진 국가 출력
"""
def get_country_upper_n(data_path: str, n: int) -> list[str]:
    gdp_df = pd.read_json(data_path, orient="records")
    gdp_df = gdp_df[gdp_df["GDP"] >= n]
    return gdp_df["Country"].tolist()


"""
각각의 Region 별로 GDP 상위 5개 국가의 평균 GDP 값 반환
"""
def top5_mean_gdp_by_region(data_path: str) -> dict:
    gdp_df = pd.read_json(data_path, orient="records")

    top_5_gdp_means = (
        gdp_df.groupby("Region")["GDP"]
        .apply(lambda x: x.nlargest(5).mean())
        .reset_index(name="Top 5 GDP Mean")
    )

    return top_5_gdp_means

"""
화면 출력 요구사항 실행
"""
def run() -> None:
    print("GDP가 100B USD이상이 되는 국가: ")
    print(get_country_upper_n(data_path, 100))
    print()
    print("각 Region별로 top5 국가의 GDP 평균: ")
    print(top5_mean_gdp_by_region(data_path))

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