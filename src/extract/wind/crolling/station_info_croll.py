import requests
import pandas as pd
from io import StringIO

# =============================
# 1️⃣ API 정보
# =============================
url = "https://apihub.kma.go.kr/api/typ01/url/stn_inf.php"
params = {
    "inf": "SFC",               # 지점종류
    "stn": "",                  # 전체 지점
    "tm": "202211300900",       # 시각
    "help": "0",                # 도움말 제외
    "authKey": "__YYVgKkQqy2GFYCpHKssA",
}

# =============================
# 2️⃣ API 요청
# =============================
response = requests.get(url, params=params)
response.encoding = "euc-kr"
text = response.text.strip()

print("응답 상태코드:", response.status_code)

# =============================
# 3️⃣ JSON or 텍스트 구분
# =============================
if text.startswith("{") or text.startswith("["):
    df = pd.DataFrame(response.json())
else:
    print("⚠️ JSON이 아닌 텍스트 형식입니다. 텍스트 파싱으로 전환합니다.")

    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
    
    # 헤더 추출 (첫 번째 줄)
    header_line = lines[0]
    headers = header_line.split()
    
    # 데이터 라인
    data_lines = lines[1:]

    parsed_data = []
    for line in data_lines:
        parts = line.split()
        # 열 수가 맞지 않으면 패딩 또는 자르기
        if len(parts) < len(headers):
            parts += [""] * (len(headers) - len(parts))
        elif len(parts) > len(headers):
            parts = parts[:len(headers)]
        parsed_data.append(parts)

    df = pd.DataFrame(parsed_data, columns=headers)

# =============================
# 4️⃣ 결과 확인
# =============================
print("데이터 미리보기:")
print(df.head())
print(f"총 {len(df)}개 지점 불러옴")

# =============================
# 5️⃣ CSV 저장 (선택)
# =============================
df.to_csv("../../raw_date/station_info.csv", index=False, encoding="utf-8-sig")
print("✅ station_info.csv 저장 완료!")
