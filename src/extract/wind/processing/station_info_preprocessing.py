import pandas as pd


# CSV 파일 읽기 (헤더 없음)
PATH = "./data/wind/"
file_name = "station_info.csv"
df = pd.read_csv(PATH +"raw_data/"+file_name, header=None)

# 필요한 컬럼만 인덱스로 선택
df = df[[0, 1, 2, 10, 11, 12]]  # STN_ID, LON, LAT, STN_KO, STN_EN, FCT_ID, LAW_ID

# 컬럼명 지정
df.columns = ['std_id', 'lon', 'lat', 'std_ko', 'std_en', 'fct_id']

# 데이터 타입 지정
df = df.astype({
    'std_id': 'int64',
    'lon': 'float',
    'lat': 'float',
    'std_ko': 'string',
    'std_en': 'string',
    'fct_id': 'string'
})

# CSV 저장
df.to_csv(PATH+"processing_1/observation_location.csv", index=False)

print("[INFO] 전처리 완료. 최종 컬럼:", df.columns.tolist())
print(df.head())
