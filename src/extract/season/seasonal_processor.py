import os
import pandas as pd
import numpy as np
import zipfile
from pathlib import Path

# =========================================================
# 설정 (필터링 조건)
# =========================================================
TARGET_STN = ["서울", "대전", "북강릉", "광주", "울산"]
TARGET_DATE = "2009-11-01"

# =========================================================
# 유틸 함수 (로그출력 / zip 파일 해제)
# =========================================================
def log_df(df: pd.DataFrame, msg: str = ""):
    """df 정보 로그 출력"""
    print(f"  → {msg} | rows={len(df):,}, cols={len(df.columns)}")
    return df


def read_zip_csv(filepath: str) -> pd.DataFrame:
    """ZIP 파일에서 CSV 파일을 안전하게 로드"""
    try:
        with zipfile.ZipFile(filepath, "r") as zipped:
            csv_file = [f for f in zipped.namelist() if f.endswith(".csv")][0]

            if not csv_file:
                raise ValueError(f"❌ ZIP 내부에 CSV 파일이 없습니다: {filepath}")
            
            print(f"  → ZIP 내부 CSV 로드: {csv_file}")
            return pd.read_csv(zipped.open(csv_file), dtype=str)

    except zipfile.BadZipFile:
        raise ValueError(f"❌ ZIP 파일이 손상되었거나 잘못된 형식입니다: {filepath}")


# =========================================================
# Step 1. RAW CSV 로드
# =========================================================
def load_raw_data(filepath: str) -> pd.DataFrame:
    print("[1/8] RAW 데이터 로드")

    # ZIP 또는 CSV 파일 읽기
    try:
        if str(filepath).lower().endswith(".zip"):
            df = read_zip_csv(filepath)
        else:
            df = pd.read_csv(filepath, dtype=str)
    except Exception as e:
        raise RuntimeError(f"❌ RAW 데이터 로드 중 오류 발생: {e}")

    # 컬럼 전처리
    df = pd.read_csv(filepath, dtype=str)
    df = df.drop(columns=['YY'], errors='ignore')
    df["STN"] = df["STN"].astype(int)
    df["SSN_ID"] = df["SSN_ID"].astype(int)
    df["SSN_MD"] = df["SSN_MD"].astype(int)

    df["TM"] = pd.to_datetime(df["TM"], errors="coerce")
    df["TM_YEAR"] = df["TM"].dt.year
    df["TM_MONTH"] = df["TM"].dt.month

    return log_df(df, "RAW 로드 완료")


# =========================================================
# Step 2. 매핑 파일 병합 (SSN_ID / SSN_MD / 관측지점)
# =========================================================
def merge_reference_data(df, ssn_id_csv, ssn_md_csv, stn_csv):
    print("[2/8] 매핑 데이터 병합...")

    df = df.merge(
        pd.read_csv(ssn_id_csv, usecols=["SSN_ID", "ID_Name"]),
        on="SSN_ID", how="left"
    )

    df = df.merge(
        pd.read_csv(ssn_md_csv, usecols=["SSN_MD", "MD_Name"]),
        on="SSN_MD", how="left"
    )

    stn_df = pd.read_csv(stn_csv, dtype={"STN_ID": int})
    stn_df = stn_df.rename(columns={"STN_ID": "STN"})[["STN", "STN_KO"]]

    df = df.merge(stn_df, on="STN", how="left")

    return log_df(df, "매핑 완료")


# =========================================================
# Step 3. NULL 제거
# =========================================================
def remove_null(df: pd.DataFrame) -> pd.DataFrame:
    print("[3/8] NULL 제거...")

    before = len(df)
    df = df.dropna(subset=["ID_Name", "MD_Name", "STN_KO"])
    print(f"  → 제거된 행: {before - len(df):,}")

    return log_df(df, "NULL 제거 완료")


# =========================================================
# Step 4. 필터링 (지점 + 기간)
# =========================================================
def filter_valid_data(df: pd.DataFrame, target_stn, target_date) -> pd.DataFrame:
    print("[4/8] 분석 대상 필터링...")

    df = df[df["STN_KO"].isin(target_stn)]
    df = df[df["TM"] >= target_date]

    return log_df(df, "필터링 완료")


# =========================================================
# Step 5. main_season 계산 (동적 생성)
# =========================================================
def get_main_season_mapping(df, id_col="SSN_ID", md_col="SSN_MD", month_col="TM_MONTH"):
    """SSN ID + SSN MD 조합의 대표 계절(main_season) 동적 생성"""

    def month_to_season(month):
        if month in [12, 1, 2]: return "겨울"
        if month in [3, 4, 5]: return "봄"
        if month in [6, 7, 8]: return "여름"
        return "가을"

    def pick_main_season_from_ratios(row):
        season_ratios = {}
        for season, ratio in zip(row["top_seasons"], row["month_ratios"]):
            season_ratios[season] = season_ratios.get(season, 0) + ratio
        return max(season_ratios, key=season_ratios.get)

    # (1) 조합별 월별 count
    month_counts = (
        df.groupby([id_col, md_col, month_col])
        .size()
        .reset_index(name="count")
    )

    # (2) 비율 계산
    month_counts["ratio"] = (
        month_counts.groupby([id_col, md_col])["count"]
        .transform(lambda x: x / x.sum())
    )

    # (3) 상위 3개월
    top3_months = (
        month_counts.sort_values([id_col, md_col, "count"], ascending=[True, True, False])
        .groupby([id_col, md_col], group_keys=False)
        .head(3)
    )

    # (4) 월 → 계절 변환
    top3_months["season"] = top3_months[month_col].apply(month_to_season)

    # (5) 정리
    top3_summary = (
        top3_months.groupby([id_col, md_col])
        .apply(lambda x: pd.Series({
            "top_months": list(x[month_col]),
            "month_ratios": list(x["ratio"].round(3)),
            "top_seasons": list(x["season"])
        }), include_groups=False)
        .reset_index()
    )

    # (6) 대표 main_season
    top3_summary["main_season"] = top3_summary.apply(pick_main_season_from_ratios, axis=1)

    return top3_summary[[id_col, md_col, "main_season"]]


def assign_main_season(df: pd.DataFrame) -> pd.DataFrame:
    print("[5/8] main_season 컬럼 생성")
    mapping = get_main_season_mapping(df)
    df = df.merge(mapping, on=["SSN_ID", "SSN_MD"], how="left")
    return log_df(df, "main_season 완료")


# =========================================================
# Step 6. season_year 생성 (10월~11월 & 겨울 → 다음 해)
# =========================================================
def assign_season_year(df: pd.DataFrame) -> pd.DataFrame:
    print("[6/8] season_year 컬럼 생성")

    winter_nov_dec = (df["TM_MONTH"].isin([11, 12])) & (df["main_season"] == "겨울")
    df["season_year"] = np.where(winter_nov_dec, df["TM_YEAR"] + 1, df["TM_YEAR"])

    return log_df(df, "season_year 완료")


# =========================================================
# Step 7. season-year 월 검증 (오차범위 외 이상치 제거)
# =========================================================
def remove_season_year_outlier(df: pd.DataFrame) -> pd.DataFrame:
    print("[7/8] 계절-월 이상치 제거")
    before = len(df)

    season_fuzzy_month_range = {
        "봄": [2, 3, 4, 5, 6],
        "여름": [5, 6, 7, 8, 9],
        "가을": [8, 9, 10, 11, 12],
        "겨울": [11, 12, 1, 2, 3],
    }
    
    # 계절이 유효한 행만 체크
    valid_season_mask = df["main_season"].notna() & (df["main_season"] != "")
    
    # 불일치 확인
    def is_month_valid_for_season(row):
        season = row["main_season"]
        month = row["TM_MONTH"]
        return month in season_fuzzy_month_range.get(season, [])
    
    # 유효한 계절을 가진 행 중 월이 맞는 것만 필터링
    valid_rows = df[valid_season_mask].apply(is_month_valid_for_season, axis=1)
    df = df[~valid_season_mask | valid_rows].copy()
    
    print(f"  → 제거된 행: {before - len(df):,}")
    return log_df(df, "이상치 제거 완료")


# =========================================================
# Step 8. CSV 형태로 저장
# =========================================================
def save_processed_to_csv(df, output_path):
    """df -> CSV 파일 저장"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"[8/8] 저장 완료 → {output_path}")


# =========================================================
# MAIN 실행
# =========================================================
def main():
    
    ROOT_DIR = Path(__file__).resolve().parents[3]
    DATA_DIR = ROOT_DIR / "src/data/season"

    RAW_CSV    = DATA_DIR / "raw_data/KMA_seasonal_data_19741101_20251028_v2.zip"
    SSN_ID_MAP = DATA_DIR / "mapping_data/ssn_id_map.csv"
    SSN_MD_MAP = DATA_DIR / "mapping_data/ssn_md_map.csv"
    STN_CSV    = ROOT_DIR / "src/data/common/raw_data/location.csv"
    OUTPUT_CSV = DATA_DIR / "processed_data/season_yearly.csv"

    print("=" * 50)
    print("계절 데이터 전처리 시작")
    print("=" * 50)
    print(f"입력 파일: {RAW_CSV}\n")

    if not RAW_CSV.exists():
        print(f"❌ Raw 파일이 존재하지 않습니다: {RAW_CSV}")
        return

    df = load_raw_data(RAW_CSV)
    df = merge_reference_data(df, SSN_ID_MAP, SSN_MD_MAP, STN_CSV)
    df = remove_null(df)
    df = filter_valid_data(df, TARGET_STN, TARGET_DATE)
    df = assign_main_season(df)
    df = assign_season_year(df)
    df = remove_season_year_outlier(df)
    save_processed_to_csv(df, OUTPUT_CSV)

    print("\n" + "=" * 50)
    print("✅ 전처리 완료")
    print("=" * 50)

if __name__ == "__main__":
    main()
