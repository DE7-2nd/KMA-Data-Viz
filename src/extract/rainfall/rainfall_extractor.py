"""
강수량 기간에 따른 데이터 추출기
- 강수에 따라 추출하여 강수 여부 파생필드 추가
- 강수량 기간에 따른 데이터 프레임 생성 및 csv 저장
"""

import os
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import pandas as pd

from src.extract.common.api_client import TextAPIClient

load_dotenv()
PROJECT_ROOT = os.getenv("PROJECT_ROOT")


class RainfallExtractor:
    """강수량 기간에 따른 데이터 추출기
    - 2025년 1월 1일부터 현재까지 데이터 추출"""

    HEADER_RAINFALL = [
        "TM",
        "STN",
        "RN_DUR",
        "RN_D99",
    ]

    NEW_HEADERS = [
        "date",
        "stn_id",
        "rain_dur",
        "rain_d99",
        "rain_yn",
    ]

    def __init__(self, api_key: Optional[str] = None):
        self.api_client = TextAPIClient(api_key)
        self.tm1 = "20250101"
        self.tm2 = datetime.now().strftime("%Y%m%d")
        self.url = f"https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php?tm1={self.tm1}&tm2={self.tm2}&stn=0&help=1&authKey={self.api_client.api_key}&mode=0"

    def extract_raw_data(self) -> pd.DataFrame:
        """원본 데이터 추출"""
        return self.api_client.create_dataframe_from_text(self.url)

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터 처리"""
        df_selected = df[self.HEADER_RAINFALL].copy()
        df_selected["RN_DUR"] = pd.to_numeric(
            df_selected["RN_DUR"], errors="coerce"
        )
        df_selected["RN_D99"] = pd.to_numeric(
            df_selected["RN_D99"], errors="coerce"
        )
        df_selected["RAIN_YN"] = (df_selected["RN_DUR"] > 0) | (
            df_selected["RN_D99"] > 0
        )
        df_selected.columns = self.NEW_HEADERS
        return df_selected

    def extract_and_process(self) -> pd.DataFrame:
        """데이터 추출 및 처리"""
        raw_df = self.extract_raw_data()
        processed_df = self.process_data(raw_df)
        return processed_df

    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> None:
        """CSV 파일로 저장"""
        df.to_csv(filepath, index=False)


if __name__ == "__main__":
    extractor = RainfallExtractor()
    rainfall_df = extractor.extract_and_process()

    # 저장
    extractor.save_to_csv(
        rainfall_df,
        f"{PROJECT_ROOT}/src/data/rainfall/processed_data/rainfall_daily.csv",
    )

    print(f"저장 완료: {len(rainfall_df)}개 데이터")
