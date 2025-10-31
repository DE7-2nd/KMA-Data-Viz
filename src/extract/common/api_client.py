"""
기상청 API 클라이언트
- 텍스트 형태 API 응답 처리
- JSON 형태 API 응답 처리
- 데이터 유효성 검사
- 데이터 파싱
- 데이터 프레임 생성
"""

import os
from typing import Optional, List
from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

# 전역 변수 & 환경변수 설정
API_TIMEOUT = 10
API_KEY = os.getenv("API_KEY")


class KMAAPIClient:
    """기상청 API 기본 클라이언트"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("API_KEY")
        if not self.api_key:
            raise ValueError("API_KEY가 설정되지 않았습니다.")

    def get_response(self, url: str) -> requests.Response:
        """API 응답 가져오기"""
        response = requests.get(url, timeout=API_TIMEOUT)
        response.raise_for_status()  # 응답 상태 코드 체크
        return response


class TextAPIClient(KMAAPIClient):
    """텍스트 형태 API 응답 처리 클라이언트"""

    def get_text_response(self, url: str) -> str:
        """텍스트 응답 가져오기"""
        response = self.get_response(url)
        return response.text

    def parse_text_data(self, text: str) -> List[List[str]]:
        """텍스트 데이터 파싱"""

        # 예외 케이스 처리
        text = text.replace(" Gun", "gun")

        lines = text.strip().split("\n")
        for idx, line in enumerate(lines):
            if not line.startswith("#"):
                break

        data_rows = lines[idx:-1]  # 주석 제외한 데이터 행
        data = [data_row.strip().split() for data_row in data_rows]
        return data

    def extract_headers(self, text: str) -> List[str]:
        """주석에서 헤더 정보 추출"""
        headers = []
        lines = text.strip().split("\n")

        for line in lines:
            if line.startswith("#") and ":" in line:
                header_name = line.split(":")[0].split()[-1]
                headers.append(header_name)

        return headers

    def validate_data(self, header: List[str], data: List[List[str]]) -> bool:
        """데이터 유효성 검사
        - 헤더와 데이터 행의 길이가 다른 경우 예외 발생
        """
        print(f"header {header}, len: {len(header)}")
        for i, row in enumerate(data):
            if len(row) != len(header):
                print(f"row {i} len: {len(row)}, expected: {len(header)}")
                return False
        return True

    def create_dataframe_from_text(
        self, url: str, header: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """텍스트 API에서 DataFrame 생성"""
        text = self.get_text_response(url)
        data = self.parse_text_data(text)

        if header:
            df_header = header
        else:
            df_header = self.extract_headers(text)

        if not self.validate_data(df_header, data):
            raise ValueError("헤더와 데이터 행의 길이가 다릅니다.")

        return pd.DataFrame(data, columns=df_header)


class JsonAPIClient(KMAAPIClient):
    """JSON API 클라이언트"""

    def create_dataframe_from_json(self, url: str) -> pd.DataFrame:
        """JSON API에서 DataFrame 생성"""
        json_data = self.get_response(url).json()
        items = json_data["response"]["body"]["items"]["item"]
        return pd.DataFrame(items)
