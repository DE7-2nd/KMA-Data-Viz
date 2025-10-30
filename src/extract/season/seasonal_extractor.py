import os
import sys
import requests
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


def save_to_txt(data: str, output_file: str):
    """
    응답 데이터를 txt 파일로 저장
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(data)
        print(f"[INFO] 파일 저장 완료 → {output_file}")
    except Exception as e:
        print(f"[ERROR] 파일 저장 중 오류 발생: {e}")


def main():
    # API 요청 위한 상수값 정의
    BASE_URL = 'https://apihub.kma.go.kr/api/'
    SEASONAL_OBS_API_URL = 'typ01/url/sfc_ssn.php'

    # 환경변수 로드
    load_dotenv()
    auth_key = os.getenv('KMA_API_KEY')
    if not auth_key:
        print("[ERROR] 환경변수 'KMA_API_KEY'가 설정되지 않았습니다. '.env' 파일을 확인하세요.")
        sys.exit(1)

    tm1, tm2 = '19741101', '20251028'

    # API 호출
    data = fetch_seasonal_api_data(BASE_URL, SEASONAL_OBS_API_URL, auth_key, tm1, tm2)
    if not data:
        print("[ERROR] API 응답이 없습니다. 프로그램을 종료합니다.")
        sys.exit(1)

    # 현재 실행 파일과 동일한 디렉토리에 저장
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(current_dir, f'KMA_seasonal_data_{tm1}_{tm2}.txt')

    # 파일 저장
    save_to_txt(data, output_file)


if __name__ == '__main__':
    main()
