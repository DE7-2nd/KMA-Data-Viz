import pandas as pd
import numpy as np
from pathlib import Path

# --- 경로 설정 ---
BASE_DIR = Path(__file__).resolve().parent  # 스크립트 위치 기준
print(BASE_DIR)
# 입력
#LOCATION_SRC = BASE_DIR / "./data/wind/observation_location.csv"
PRE_LOCATION_SRC = BASE_DIR / "../../../data/wind/processed_data/observation_location_vw.csv"
WIND_SRC = BASE_DIR / "../../../data/wind/processing_1/wind_quarter_daily.csv"

# 출력
OUTPUT_DIR = BASE_DIR / "../../../data/wind/processed_data/"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- 1. 관측소 정보 가공 ---
#def transform_location():
    #df_loc = pd.read_csv(LOCATION_SRC)
    #out_file = OUTPUT_DIR / "observation_location_vw.csv"
    #df_loc.to_csv(out_file, index=False)
    #print(f"[SAVE] {out_file}")
    #return df_loc
    
# --- 1.1 관측소 정보 열기
def open_location_csv():
    df_loc = pd.read_csv(PRE_LOCATION_SRC)
    return df_loc

def transform_wind(df_loc):
    df_wind = pd.read_csv(WIND_SRC)

    # tm_id를 정수형
    df_wind['tm_id'] = df_wind['tm_id'].astype(int)

    # tm_id에서 연도 추출
    df_wind['year'] = df_wind['tm_id'].astype(str).str[:4]

    # 16방위 → degree
    df_wind['wind_deg'] = df_wind['wind_dir'] * 22.5

    # degree → radian
    df_wind['wind_rad'] = np.deg2rad(df_wind['wind_deg'])

    # 벡터 계산 (기상학 기준: 불어오는 방향)
    df_wind['u'] = - df_wind['wind_sp'] * np.sin(df_wind['wind_rad'])
    df_wind['v'] = - df_wind['wind_sp'] * np.cos(df_wind['wind_rad'])

    # std_id merge
    df_merged = df_wind.merge(df_loc, on='std_id', how='left')

    # 최종 컬럼 선택
    df_final = df_merged[['std_id', 'tm_id', 'wind_dir', 'wind_sp', 'wind_deg', 'wind_rad', 'u', 'v', 'year']]

    # 연도별 CSV 저장
    for year in sorted(df_final['year'].unique()):
        df_year = df_final[df_final['year'] == year].copy()
        out_file = OUTPUT_DIR / f"wind_{year}_quarter_daily_vw1.csv"
        df_year.to_csv(out_file, index=False)
        print(f"[SAVE] {out_file}")

# --- main ---
def main():
    #df_loc = transform_location()
    df_loc = open_location_csv()
    transform_wind(df_loc)

if __name__ == "__main__":
    main()
