# 🚚 DEng2 Dashboard

화물차주 분포 데이터와 야간주차 식당 후보지를 지도 기반으로 시각화하고,  
식당과의 계약 상태를 관리하는 대시보드입니다.

## 📁 디렉토리 구조
```
dashboard/
├── 0_🍽️_식당_주차장_관리.py    # 메인 대시보드 진입점 (식당 주차장 관리 화면)
├── chajoo_map.py               # 차주 관련 지도 시각화 컴포넌트
├── restaurant_map.py           # 식당 위치 및 정보 지도 시각화 컴포넌트
├── shp_loader.py               # SHP (Parquet) 데이터 로드 유틸리티
├── core/                       # DB 세션 관리 및 시스템 환경 설정을 위한 공통 컴포넌트
│   ├── db.py                   # 데이터베이스 연결 및 세션 관리
│   ├── query.py                # SQL 쿼리 정의 및 데이터 추출 로직
│   └── settings.py             # 환경 변수 및 전역 설정값 관리
├── data/                       # 데이터 리소스 관리
│   └── shp.parquet             # 경량화된 SHP 지리 정보 데이터 (Parquet 형식)
└── pages/                      # Streamlit 멀티 페이지 구성
    └── 1_🚛_차주_수요_분석.py  # 서브 페이지: 차주 분포 분석 및 전략 거점 선정을 위한 페이지
```

## Prerequisite: 행정구역 경계 데이터 (SHP → Parquet)

본 대시보드는 **SGIS 센서스용 행정구역 경계 SHP 데이터를 Parquet 형식으로 변환한 파일**을 사용합니다.  
원본 SHP 파일을 그대로 사용하지 않으며, **사전에 Parquet로 변환하여 `dashboard/data/` 하위에 저장되어 있어야 합니다.**

### 1) 원본 SHP 데이터 다운로드
* **출처**: https://sgis.kostat.go.kr
* **데이터**: 시군구 행정구역 경계 (`bnd_sigungu_00`)
* **필요 파일**:
  - `bnd_sigungu_00.cpg`
  - `bnd_sigungu_00.dbf`
  - `bnd_sigungu_00.prj`
  - `bnd_sigungu_00.shp`
  - `bnd_sigungu_00.shx`

## ⚙️ 실행 방법

```bash
streamlit run dashboard/0_🍽️_식당_주차장_관리.py
```

## 🗄️ 데이터베이스 요구사항 (필수 테이블)

본 대시보드는 PostgreSQL 데이터베이스와 연동되며,  
아래 테이블들이 **사전에 생성되어 있어야 정상 동작**합니다.

### 1. `chajoo_dist`
화물차주(영업용 차량) 분포 통계 테이블  
히트맵 시각화에 사용됩니다.

| 컬럼명 | 타입 | 설명 |
|------|------|------|
| sido | text | 시도명 |
| sigungu | text | 시군구명 |
| cargo_count | bigint | 화물차주 수 |
| SHP_CD | text | 행정구역 코드 (SHP 매핑용) |
| 전략적_중요도 | double precision | 화물차주 인구수를 기반으로 산출한 사업 우선순위 지표 
| year | text | 연도 |
| month | text | 월 |

---

### 2. `restaurant`
식당 기본 정보 및 입지 분석 결과 테이블  
식당 지도 렌더링 및 계약 관리의 **기본 데이터 소스**입니다.

| 컬럼명 | 타입 | 설명 |
|--------|------------------|--------------------------------------------|
| sigungu | text | 시군구명 |
| 총점 | double precision | 입지 종합 평가 점수 |
| 영업_적합도 | double precision | 영업 지속 가능성 및 상권 적합도 점수 |
| 수익성 | double precision | 예상 수익성 평가 점수 |
| 업체명 | text | 식당명 (PK 구성 요소) |
| 도로명주소 | text | 도로명 주소 (PK 구성 요소) |
| 유휴부지_면적 | double precision | 추정 유휴 부지 면적 |
| longitude | double precision | 경도 |
| latitude | double precision | 위도 |
| year | integer | 데이터 수집 연도 |
| month | integer | 데이터 수집 월 |
| week | integer | 데이터 수집 주차 |
| region | text | 시도명 |
| 주차_적합도 | integer | 대형 차량 진입 및 회차 가능성 점수 |
| contract_status | text | 계약 진행 상태 ("후보", "접촉", "관심", "협의", "성공", "실패") |
| remarks | text | 비고 및 계약 메모 |

---

### 3. `truckhelper_parking_area`
트럭헬퍼의 기존 공영 차고지 정보 테이블  
식당 공략 지역을 정할 때 참고할 자료입니다.

| 컬럼명 | 타입 | 설명 |
|------|------|------|
| 공영차고지명 | text | 차고지명 |
| 주소 | text | 주소 |
| lat | double precision | 위도 |
| lon | double precision | 경도 |
