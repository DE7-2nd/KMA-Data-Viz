"""
중기 예보 데이터 추출기
- 2025년 예보 데이터를 처리하여 CSV 파일로 저장
- 지역 코드 매핑 및 데이터 전처리 포함
"""

import os
from typing import Optional
from dotenv import load_dotenv
import pandas as pd

from src.extract.common.api_client import JsonAPIClient

load_dotenv()
PROJECT_ROOT = os.getenv("PROJECT_ROOT")


class ForecastExtractor:
    """중기 예보 데이터 추출기
    - 2025년 1월 1일부터 10월 28일까지의 데이터 처리"""

    # 지역명과 코드 매핑
    REGION_MAPPING = {
        "서울.인천.경기": "11B00000",
        "충청북도": "11C10000",
        "충청남도": "11C20000",
        "강원영서": "11D10000",
        "강원영동": "11D20000",
        "전북자치도": "11F10000",
        "전라남도": "11F20000",
        "제주도": "11G00000",
        "경상북도": "11H10000",
        "경상남도": "11H20000",
    }

    # 컬럼명 매핑
    COLUMN_MAPPING = [
        "forecast_time",
        "region",
        "forecast_issue_time",
        "weather_condition",
        "rain_probability",
        "region_code",
    ]

    def __init__(self, api_key: Optional[str] = None):
        self.api_client = JsonAPIClient(api_key)
        self.input_file = (
            f"{PROJECT_ROOT}/src/data/rainfall/raw_data/forecast_2025.csv"
        )
        self.output_file = (
            f"{PROJECT_ROOT}/src/data/rainfall/processed_data/forecast.csv"
        )

    def load_forecast_data(self) -> pd.DataFrame:
        """예보 데이터 로드 (EUC-KR 인코딩)"""
        return pd.read_csv(self.input_file, encoding="euc-kr")

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터 처리"""
        # 지역 코드 추가
        df["지역코드"] = df["지역"].map(self.REGION_MAPPING)

        # 컬럼명 변경
        df.columns = self.COLUMN_MAPPING

        # 1. forecast_time (발표시각) 변환: "2025-01-01 06시" -> datetime
        df["forecast_time"] = pd.to_datetime(
            df["forecast_time"].str.replace("시", ""),
            format="%Y-%m-%d %H",
        )

        # 2. forecast_issue_time (예보시각) 변환: "2025-01-05 오전" -> datetime
        def convert_forecast_issue_time(time_str):
            # "2025-01-05 오전" -> "2025-01-05 06:00"
            # "2025-01-05 오후" -> "2025-01-05 18:00"
            date_part = time_str.split(" ")[0]
            time_part = time_str.split(" ")[1]

            if time_part == "오전":
                return pd.to_datetime(f"{date_part} 06:00")

            if time_part == "오후":
                return pd.to_datetime(f"{date_part} 18:00")

            return pd.to_datetime(f"{date_part} 12:00")

        df["forecast_issue_time"] = df["forecast_issue_time"].apply(
            convert_forecast_issue_time
        )

        # 3. 강수확률을 숫자로 변환
        df["rain_probability"] = pd.to_numeric(
            df["rain_probability"], errors="coerce"
        )

        return df

    def extract_and_process(self) -> pd.DataFrame:
        """데이터 추출 및 처리"""
        raw_df = self.load_forecast_data()
        processed_df = self.process_data(raw_df)
        return processed_df

    def save_to_csv(self, df: pd.DataFrame, filepath: str) -> None:
        """CSV 파일로 저장"""
        df.to_csv(filepath, index=False)

    def extract_forecast_locations(self) -> None:
        """예보 지역 정보 추출 (API 사용)
        - 하지만 S3 데이터 레이크에 올라갈 csv 파일이 아니므로 실행하지 않음
        """
        for idx in range(1, 9):
            url = f"https://apihub.kma.go.kr/api/typ02/openApi/FcstZoneInfoService/getFcstZoneCd?pageNo={idx}&numOfRows=100&dataType=JSON&authKey={self.api_client.api_key}"
            data = self.api_client.create_dataframe_from_json(url)
            data.to_csv(
                f"{PROJECT_ROOT}/src/extract/rainfall/forecast_location.csv",
                mode="a",
                header=False,
                index=False,
            )

        print(f"예보 지역 저장 완료: {len(data)}개 데이터")


if __name__ == "__main__":
    extractor = ForecastExtractor()
    forecast_df = extractor.extract_and_process()

    # 저장
    extractor.save_to_csv(forecast_df, extractor.output_file)

    print(f"저장 완료: {len(forecast_df)}개 데이터")
    print(forecast_df.head())
