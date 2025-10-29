"""
관측소 위치 정보 추출
"""

from datetime import datetime
from typing import Optional
import pandas as pd
from src.extract.common.api_client import TextAPIClient


class LocationExtractor:
    """관측소 위치 정보 추출기"""

    # 상수 정의
    HEADER_LOCATION = [
        "STN_ID",
        "LON",
        "LAT",
        "STN_SP",
        "HT",
        "HT_PA",
        "HT_TA",
        "HT_WD",
        "HT_RN",
        "STN_CD",
        "STN_KO",
        "STN_EN",
        "FCT_ID",
        "LAW_ID",
        "BASIN",
    ]

    SELECTED_COLUMNS = ["STN_ID", "LON", "LAT", "STN_KO", "STN_EN", "FCT_ID"]
    NEW_HEADERS = ["std_id", "lon", "lat", "stn_ko", "stn_en", "fct_id"]

    def __init__(self, api_key: Optional[str] = None):
        self.api_client = TextAPIClient(api_key)
        self.tm = datetime.now().strftime("%Y%m%H%M")
        self.url = f"https://apihub.kma.go.kr/api/typ01/url/stn_inf.php?inf=SFC&tm={self.tm}&help=1&authKey={self.api_client.api_key}"

    def extract_raw_data(self) -> pd.DataFrame:
        """원본 데이터 추출"""
        return self.api_client.create_dataframe_from_text(
            self.url, self.HEADER_LOCATION
        )

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터 처리 및 컬럼명 변경"""
        df_selected = df[self.SELECTED_COLUMNS].copy()
        df_selected.columns = self.NEW_HEADERS
        return df_selected

    def extract_and_process(self) -> pd.DataFrame:
        """데이터 추출 및 처리"""
        raw_df = self.extract_raw_data()
        return self.process_data(raw_df)

    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> None:
        """CSV 파일로 저장"""
        df.to_csv(filepath, index=False)


if __name__ == "__main__":
    extractor = LocationExtractor()
    location_df = extractor.extract_and_process()

    # 저장
    extractor.save_to_csv(
        location_df, "../../data/common/observation_location.csv"
    )
    print(f"저장 완료: {len(location_df)}개 관측소")
    print(location_df.head())
