import pandas as pd

year = 2022
all_df = []

for i in range(3):
    # CSV 파일 읽기
    df = pd.read_csv(f"./data/wind/raw_data/sfc_tm2_{year+i}.csv")

    # 필요한 컬럼만 선택
    df = df[['TM', 'STN', 'WD', 'WS']]

    # TM 컬럼 datetime 변환
    df['TM'] = pd.to_datetime(df['TM'])

    # 시간 기준 필터링 (12:00 또는 00:00)
    df = df[df['TM'].dt.hour.isin([0, 6, 12, 18])]

    # TM 컬럼을 YYYYMMDDHH 형태의 int로 변환
    df['TM'] = df['TM'].dt.strftime('%Y%m%d%H').astype(int)

    # 컬럼명 변경
    df.rename(columns={
        'TM': 'tm_id',
        'STN': 'std_id',
        'WD': 'wind_dir',
        'WS': 'wind_sp'
    }, inplace=True)

    # 중복 제거
    df = df.drop_duplicates(subset=['std_id', 'tm_id'])

    # 리스트에 추가
    all_df.append(df)

# 3년치 합치기
df_all = pd.concat(all_df, ignore_index=True)

# 복합키로 인덱스 설정
df_all.set_index(['std_id', 'tm_id'], inplace=True)

# 결과 확인
print(df_all.head())

# CSV 저장
df_all.to_csv("./data/wind/processing_1/wind_quarter_daily.csv")
