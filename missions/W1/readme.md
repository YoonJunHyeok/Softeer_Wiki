week1

# python 환경설정

1. venv 환경 생성

- 작업하고자 하는 디렉토리에 들어가서 아래 명령어 입력

```bash
python3 -m venv .venv
```

2. venv 환경 activate

```bash
source .venv/bin/activate
```

3. 추가 패키지 다운로드

```bash
python3 -m pip install ipykernel
```

# Jupyter notebook, Jupyter lab

1. 다운로드

```bash
pip install jupyterlab
pip install jupyter
```

```bash
jupyter lab
jupyter notebook
```

으로 실행

2. kernel 설치

```bash
python3 -m pip install ipykernel
```

3. 만든 python 가상환경과 jupyter 연결

```bash
python3 -m pip install ipykernel
```

```bash
python -m ipykernel install --user --name w1 --display-name w1
```

# ETL

## GDP 데이터를 IMF 홈페이지에서 ?

- IMF에서 제공하는 API 사용
- IMF에 올라와 있는 데이터를 selenium을 통해 추출
  - [IMF DB](https://www.imf.org/en/Publications/WEO/weo-database/2024/April)

## 과거 데이터 처리 방법 ?

- 상반기와 하반기로 나뉘어서 GDP 데이터가 발표된다고 가정한다.
- gdp를 저장하는 테이블에 분기를 나타내는 열을 추가한다.

  - 최종 칼럼들은 country, year, quarter, region, gdp
  - 기존의 id 대신에 (country, year, quarter)가 primary key

  - 장점  
    | etl 파이프라인이 불렸을 때, 테이블에 존재하는 데이터를 요청하는 경우, 전체 skip 할 수 있다.  
    | 연도와 분기로 원하는 시점의 과거 데이터를 얻을 수 있다.

  - 문제점  
    | 데이터를 저장할 때, 해당 데이터가 속하는 분기를 알 수 있어야 한다.
