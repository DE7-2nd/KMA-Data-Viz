import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import time
import numpy as np

# ===============================
# 설정
# ===============================
url = "https://apihub.kma.go.kr/api/typ01/openApi/kma_sfctm2.php"
stn = "0"                     # 전국
authKey = "__YYVgKkQqy2GFYCpHKssA"
max_retries = 3               # 요청 실패 시 최대 재시도
time_interval = timedelta(hours=1)  # 1시간 단위 호출

columns = [
    'TM','STN','WD','WS','GST_WD','GST_WS','GST_TM','PA','PS','PT','PR','TA','TD','HM',
    'PV','RN','RN_DAY','RN_JUN','RN_INT','SD_HR3','SD_DAY','SD_TOT','WC','WP','WW',
    'CA_TOT','CA_MID','CH_MIN','CT','CT_TOP','CT_MID','CT_LOW','VS','SS','SI','ST_GD',
    'TS','TE_005','TE_01','TE_02','TE_03','ST_SEA','WH','BF','IR','IX'
]

# ===============================
# 연도별 반복
# ===============================
for year in range(2025, 2026):
    start_date = datetime(year, 1, 1, 1, 0)
    end_date = datetime(year, 12, 31, 23, 0)
    
    # 마지막 연도는 전체 종료일로 조정
    if year == 2025:
        end_date = datetime(2025, 10, 23, 16, 0)
    
    current_time = start_date
    yearly_data = []

    while current_time <= end_date:
        tm = current_time.strftime("%Y%m%d%H%M")
        retries = 0
        success = False

        while retries < max_retries and not success:
            try:
                params = {
                    "tm": tm,
                    "stn": stn,
                    "help": "1",
                    "authKey": authKey
                }
                response = requests.get(url, params=params, timeout=60)
                response.raise_for_status()
                text_data = response.text

                # 주석 제거
                lines = [line for line in text_data.splitlines() if not line.startswith("#")]
                data_str = "\n".join(lines)

                # 공백 기반 읽기
                df = pd.read_csv(StringIO(data_str), sep=r'\s+', header=None)
                df.columns = columns[:len(df.columns)]

                # 결측치 처리
                df.replace([-9, -9.0, -99, -99.0], np.nan, inplace=True)

                # 시간 컬럼 변환
                df['TM'] = pd.to_datetime(df['TM'], format='%Y%m%d%H%M')

                yearly_data.append(df)
                success = True
                print(f"[{year}] 완료: {tm} | {len(df)}행")

            except Exception as e:
                retries += 1
                print(f"[{year}] {tm} 요청 실패 ({retries}/{max_retries})... 재시도 중: {e}")
                time.sleep(5)

        current_time += time_interval

    # 연도별 CSV 저장
    if yearly_data:
        final_df = pd.concat(yearly_data, ignore_index=True)
        filename = f"sfc_tm2_{year}.csv"
        final_df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"[{year}] CSV 저장 완료: {filename} | 총 행: {len(final_df)}\n")
