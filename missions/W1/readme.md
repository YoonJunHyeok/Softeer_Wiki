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
