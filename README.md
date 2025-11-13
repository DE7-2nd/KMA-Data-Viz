# KMA-Data-Viz

기상청 데이터를 바탕으로 시각화 대시보드 구성 프로젝트

## 📌 프로젝트 개요

기상청 API 허브를 주로 사용하며 추가적으로 제공하는 기상 데이터를 수집하여, 기온·강수·풍향·대기질 등의 변화 추이를 시각화하고 **기후 변화 및 환경 변화가 실제로 일상에 미치는 영향**을 분석하는 데이터 기반 기상 인사이트 프로젝트입니다.

### 주요 분석 주제

- 계절 길이 변화 추이 (최근 수십 년간 봄·가을 지속 기간이 감소하고 있는지 정량적으로 검증)
- 풍향 데이터 기반 풍향장미도 & 지역별 풍향 정보 시각화
- 예보 강수량 vs 실제 강수량 비교를 통한 강수 예측 정확도
- 년도 변화에 따른 미세먼지 현황 및 추이

### 프로젝트 목표

**기후 변화가 실제 생활 환경에 미치는 영향**을 정량적으로 분석·시각화함으로써 **사용자가 직관적으로 이해할 수 있는 기상 인사이트를 제공**하는 것을 목표로 합니다.

## 👥 팀원 역할

| 이름              | 역할                                     | 주요 담당 업무                                                                                  |
| ----------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------- |
| **박정은 (팀장)** | 데이터 인프라 <br> 데이터 수집 및 시각화 | - S3, Snowflake, Preset 등 데이터 인프라 및 접근 권한 관리<br>- 계절 관측 데이터 수집 및 시각화 |
| **최혁주**        | 데이터 수집 및 시각화                    | - 풍향 데이터 수집 및 풍향장미도(풍향 시각화) 및 지역별 풍향 정보 시각화                        |
| **송여름**        | 데이터 분석 및 시각화                    | - 7개 도시의 미세먼지 데이터 수집 및 시각화                                                     |
| **김지연**        | 데이터 검증 및 시각화                    | - 예보 강수와 실제 강수량 수집 및 정확도 분석 시각화                                            |

## 🏗️ 프로젝트 아키텍처

- 원천 데이터 → AWS S3 → Snowflake → Preset 대시보드
  <img width="999" height="400" alt="image" src="https://github.com/user-attachments/assets/6ece9801-db78-4ea3-94aa-f7f7bef85887" />

### 주요 구성요소

- **AWS S3 + IAM**: 데이터 저장 및 접근 제어
- **Snowflake**: 데이터웨어하우스 (RAW_DATA, ANALYTICS 스키마)

  - raw_data의 스키마
    <img width="1199" height="659" alt="image" src="https://github.com/user-attachments/assets/dc0db6e2-043c-416a-b4a8-7a461a353349" />

- **Preset**: 시각화 대시보드 도구

## 📁 프로젝트 구조

```KMA-Data-Viz/
├── src/
│ ├── common/ # 공통 모듈 (S3 업로드 등)
│ ├── data/ # 로컬 데이터 저장소
│ │ ├── common/
│ │ ├── rainfall/
│ │ ├── season/
│ │ ├── air_quality/
│ │ └── wind/
│ ├── extract/ # 도메인별 ETL 모듈
│ │ ├── rainfall/
│ │ ├── season/
│ │ ├── air_quality/
│ │ └── wind/
│ └── analytics/ # Snowflake SQL 분석 쿼리
├── config/ # 설정 파일
├── requirements.txt
└── README.md
```

## 🔄 데이터 파이프라인

### 1. 데이터 수집

- **강수량**: 기상청 API 허브 (지상관측, 중기예보)
- **미세먼지**: Air Quality Historical Data Platform
- **풍속**: 기상청 API 허브 (지상관측)
- **계절**: 기상청 API 허브 (계절관측)

### 2. 데이터 전처리

- 텍스트/JSON/CSV 기반 데이터 파싱
- 결측치 제거 및 불필요한 컬럼 정제
- 원본(raw) 데이터와 정제(processed) 데이터 분리 저장

### 3. AWS S3 업로드

- `boto3` 라이브러리를 활용한 전처리 데이터 업로드
- 디렉토리 구조: `kma-data-storage/{domain}/`

### 4. Snowflake 적재 및 분석

- S3 Stage를 통한 데이터 COPY
- RAW_DATA 스키마: 원천 데이터 저장
- ANALYTICS 스키마: 시각화용 뷰 및 가공 데이터

## 📊 시각화 대시보드

### 강수량

- **수도권 강수 예측 확률 및 실제 강수 여부**: Line & Scatter Mixed Chart
- **강수 일별 예측**: Pivot 차트 (예측일별 강수 확률 비교)

### 미세먼지

- **월별 미세먼지 농도**: Line Chart (PM2.5, PM10, O3, NO2, SO2, CO)
- **월별 초미세먼지 PM2.5**: Line Chart (도시별)
- **도시별 평균 미세먼지 농도**: Horizontal Bar Chart
- **지역별 대기오염물질 농도 히트맵**: 월/연도별 추이

### 풍향

- **월별 풍향장미도**: 16방위 풍향 시각화 (풍속별 두께)
- **지역별 평균 풍속 트리맵**: 지역별 풍속 비교
- **위경도 풍속 버블맵**: 한반도 지도 기반 풍속 시각화

### 계절관측

- **계절 시작일 및 전년도 시작일 차이**: BigNumber 차트
- **연도 및 관측비율별 계절 비율**: Pie Chart
- **계절별 TOP5 관측라벨 조합 비율**: Sunburst Chart
- **관측지점별 계절 분포 추이**: Area Stacked Chart

## 🤝 협업 방식

### Git Workflow

- **이슈 생성** → **브랜치 생성** → **개발** → **PR 생성** → **리뷰** → **머지**

### 네이밍 규칙

- **이슈**: `[작업타입] 작업 간략 설명`
- **브랜치**: `#이슈번호-작업설명`
- **커밋**: `작업타입: [작업설명] #이슈번호`
- **PR**: `[작업타입] 작업 간략 설명 (#이슈번호)`

### 작업 타입

- `FEAT`: 새로운 기능 추가
- `FIX`: 버그 수정
- `DOCS`: 문서 작성 및 수정
- `REFACTOR`: 코드 리팩토링
- `ENHANCEMENT`: 기능/성능 개선
- `CHORE`: 설정/빌드 관련 작업
- `ETC`: 기타

## 🛠️ 기술 스택

- **언어**: `Python`
- **데이터 저장**: `AWS S3`
- **데이터웨어하우스**: `Snowflake`
- **시각화**: `Preset`
- **버전 관리**: `Git/GitHub` , `Git Pro

## 📝 주요 학습 내용

- 클라우드 기반 ETL 파이프라인의 전체 흐름 경험
- 프로젝트 관리 및 협업 도구 사용 (GitHub Project, Issue)
- 데이터 특성에 맞는 시각화 도구 선정 기준 확립

## 프로젝트 시각화 대시보드

<img width="1414" height="759" alt="스크린샷 2025-11-12 오후 5 01 29" src="https://github.com/user-attachments/assets/0fd0b5e9-9655-4585-982a-dbdcbd4af4fe" />
<img width="1402" height="840" alt="스크린샷 2025-11-12 오후 5 01 58" src="https://github.com/user-attachments/assets/15cadfc7-4a06-486e-892b-ed288451ae7f" />
<img width="1398" height="799" alt="스크린샷 2025-11-12 오후 5 02 46" src="https://github.com/user-attachments/assets/65a100e6-da70-4545-b18f-3a3fdf2a57ba" />
<img width="1402" height="572" alt="스크린샷 2025-11-12 오후 5 03 05" src="https://github.com/user-attachments/assets/658ab93f-b4ab-45a2-a639-f695a8e51825" />
<img width="1394" height="452" alt="스크린샷 2025-11-12 오후 5 03 18" src="https://github.com/user-attachments/assets/a0b58456-7e42-4a6a-a811-6d5008f91673" />
<img width="1300" alt="스크린샷 2025-11-12 오후 4 59 21" src="https://github.com/user-attachments/assets/d91f40ef-8218-4521-a2d8-7ca6abfa055b" />
