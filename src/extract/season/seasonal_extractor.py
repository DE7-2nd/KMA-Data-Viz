import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import urljoin


def fetch_seasonal_api_data(base_url: str, api_url: str, auth_key: str,
                            tm1: str, tm2: str, stn: int = 0, ssn: int = 0) -> str:
    """
    KMA API 호출하여 데이터 가져오기
    """
    endpoints = urljoin(base_url, api_url)
    params = {
        'authKey': auth_key,
        'tm1': tm1,
        'tm2': tm2,
        'stn': stn,
        'ssn': ssn
    }

    try:
        response = requests.get(endpoints, params=params, timeout=15)
        response.raise_for_status()
        print(f"[INFO] API 호출 성공: {response.url}")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API 호출 중 에러 발생: {e}")
        return None


def parse_txt_to_df(txt_data: str) -> pd.DataFrame:
    """
    API 응답 TXT → DataFrame 변환 (주석제거 후 원시데이터 그대로 반환)
    """
    lines = txt_data.splitlines()

    # 1. 헤더 찾기
    header = None
    header_idx = None

    for i, line in enumerate(lines):
        s = line.strip()
        if s.startswith('#') and ('YY' in s and 'STN' in s and 'TM' in s):
            header = s.lstrip('#').split()
            header_idx = i
            break

    if header is None:
        raise ValueError("TXT 상단에서 헤더 정보를 찾을 수 없습니다.")

    # 2. 데이터 추출
    data_rows = []

    for idx, line in enumerate(lines[header_idx + 1:]):
        stripped = line.strip()
        # 빈 줄이나 주석 제외
        if not stripped or stripped.startswith('#'):
            continue
        
        # 각 필드 뒤에 붙는 "," 제거
        clean_parts = stripped.replace(',', '').split()
        header_len = len(header)

        # 컬럼 수와 일치하는지 확인
        if len(clean_parts) == header_len:
            data_rows.append(clean_parts)
        else:
            # 건너뛴 행 기록: (실제 줄 번호, 고정 필드 개수, 분리된 필드 개수)
            print(f"🚨 SKIP LINE {idx + 1} | Expected {header_len}, Found {len(clean_parts)} | Data: {line.strip()[:60]}...")

    if not data_rows:
        raise ValueError("데이터가 비어있습니다. API 응답을 확인하세요.")

    df = pd.DataFrame(data_rows, columns=header)

    return df


def save_to_csv(df: pd.DataFrame, output_path: str):
    """
    df → CSV 저장 (원본 그대로 저장)
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"[SUCCESS] CSV 저장 완료: {output_path} | rows={len(df):,}")


def main():
    
    # 0) 사전준비
    # 환경변수 로드
    load_dotenv()   
    
    # API 요청 위한 상수값 정의
    BASE_URL = os.getenv("BASE_URL")
    API_URL  = os.getenv("API_URL")
    AUTH_KEY = os.getenv("AUTH_KEY")

    # API 호출 검증
    if not BASE_URL or not API_URL:
        print("[ERROR] BASE_URL 또는 API_URL 환경변수가 없습니다.")
        sys.exit(1)
    if not AUTH_KEY:
        print("[ERROR] 환경변수 'AUTH_KEY'가 설정되지 않았습니다. '.env' 파일을 확인하세요.")
        sys.exit(1)

    # API 요청구간 지정
    TM1, TM2 = '19741101', '20251028'

    # 1) API 호출
    data = fetch_seasonal_api_data(BASE_URL, API_URL, AUTH_KEY, TM1, TM2)
    if not data:
        print("[ERROR] API 응답이 없습니다. 프로그램을 종료합니다.")
        sys.exit(1)

    # 2) 응답데이터(TXT) -> df 형태로 변환
    df = parse_txt_to_df(data)

    # 3) df -> CSV 형태로 저장
    RAW_DATA_DIR = "src/data/season/raw_data"
    output_csv = os.path.join(RAW_DATA_DIR, f'KMA_seasonal_data_{TM1}_{TM2}.csv')
    save_to_csv(df, output_csv)


if __name__ == '__main__':
    main()
