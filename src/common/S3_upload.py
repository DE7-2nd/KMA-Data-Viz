import boto3
from pathlib import Path
from collections import defaultdict

def Check_dir_S3(S3):
    s3 = S3
    bucket_name = "kma-data-storage"

    def build_tree(objects):
        """S3 객체 리스트를 트리 구조(dict)로 변환"""
        tree = defaultdict(dict)
        for obj in objects:
            key = obj['Key'].rstrip('/')
            parts = key.split('/')
            current = tree
            for part in parts:
                current = current.setdefault(part, {})
        return tree

    def print_tree(d, prefix=''):
        """재귀적으로 트리 출력"""
        for i, (key, subtree) in enumerate(d.items()):
            connector = '└── ' if i == len(d)-1 else '├── '
            print(f"{prefix}{connector}{key}")
            if subtree:
                extension = '    ' if i == len(d)-1 else '│   '
                print_tree(subtree, prefix + extension)

    # S3 객체 가져오기
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)
    
    objects = []
    for page in pages:
        objects.extend(page.get('Contents', []))
    
    # 트리 구조 생성 후 출력
    tree = build_tree(objects)
    print(f"\n[{bucket_name}]")
    print_tree(tree)



def S3_upload(S3, FOLDER_NAME, FILE_NAME, TARGET_FOLDER_NAME="wrong_path"):
    # -------------------------------
    # S3 설정
    # -------------------------------
    S3_BUCKET = "kma-data-storage"

    # -------------------------------
    # 로컬 경로 설정
    # -------------------------------
    # 현재 파일: src/common/S3_upload.py
    BASE_DIR = Path(__file__).resolve().parent.parent  # src/
    print(BASE_DIR)
    DATA_DIR = BASE_DIR / "data"

    # boto3 클라이언트
    s3 = S3

    local_file = DATA_DIR / FOLDER_NAME / "processed_data" / FILE_NAME
    print("\n**********************")
    print("* 업로드할 파일 PATH * :",local_file)
    print("**********************\n")

    # -------------------------------
    # 업로드 수행
    # -------------------------------

    if local_file.exists():
        s3_key = Path(TARGET_FOLDER_NAME) / FILE_NAME
        s3_key = s3_key.as_posix()  # S3 경로용

        print("*************************")
        print(f"* 업로드 진행 목표 S3폴더 * → s3://{S3_BUCKET}/{s3_key}")
        print("*************************")

        # 실제 업로드 (테스트 중이면 주석)
        s3.upload_file(str(local_file), S3_BUCKET, s3_key)
        print(f"[OK] 업로드 완료 ✅ → s3://{S3_BUCKET}/{s3_key}\n\n")
        
        return True
    else:
        print(f"[ERROR] 파일이 존재하지 않습니다: {local_file}")
        return False
    
    
    
def main():
    S3 = boto3.client("s3") # 이전에 사용자 설정을 완료해야 함
    
    # 업로드할 폴더명 
    FOLDER_NAME = "folder_name"
    # 업로드할 파일명
    FILE_NAME = "file_name"  
    
    # S3에서 파일을 업로드할 폴더명
    TARGET_FOLDER_NAME = "target_file_name" # 업로드 목표 폴더명 
    '''
    예시>>
    FOLDER_NAME = "wind"                     # 업로드할 폴더명 
    FILE_NAME = "observation_location.csv"   # 업로드할 파일명
    TARGET_FOLDER_NAME = "common"              # 업로드 목표 폴더명 
    
    이때
    KMA-Data-Viz/src/data/wind/processed_data/observation_location.csv
    파일이 선택 됨
    따라서 업로드를 진행할 파일을 processed_data 폴더 하위에 넣어야함
    
    그러면 실제 S3에서 파일이 
    kma-data-storage/TARGET_FOLDER_NAME/FILE_NAME 
    즉 TARGET_FOLDER_NAME = "common", FILE_NAME = "observation_location.csv" 이므로
    
    kma-data-storage/common/observation_location.csv
    에 파일이 업로드 됨
    
    
    [kma-data-storage]
    │── session_timestamp.csv
    ├── common/
    │     └── observation_location.csv           #  <<<<<<<<< common 폴더 하위로 observation_location.csv 파일이 업로드
    ├── processed/
    │     └── air_quality/airquality_daily.csv
    ├── wind/                                    
    │     └── wind_2022_quarter_daily_vw.csv
    │     └── observation_location.csv
    '''
    
    
    success = S3_upload(S3, FOLDER_NAME, FILE_NAME, TARGET_FOLDER_NAME)
    if success:
        Check_dir_S3(S3)
    

if __name__ == "__main__":
    main()