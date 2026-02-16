# 🚀 식당 주차장 야간 화물 차고지 매칭 대시보드

이 프로젝트는 식당의 유휴 주차 공간을 야간 화물차 차고지로 활용하기 위한 데이터 시각화 및 관리 도구입니다. 전국 시군구별 화물차주 분포(수요)와 식당별 계약 상태(공급)를 한눈에 파악하고 실시간으로 관리할 수 있습니다.

## 🛠 환경 설정 (Prerequisites)

### 1. 행정구역 경계 데이터 (SHP)
지도 렌더링을 위해 아래 경로에 센서스용 행정구역 경계 파일이 반드시 존재해야 합니다.
* **권장 경로**: `dashboard/data/sigungu_shp/`
* **필요 파일**: `bnd_sigungu_00` 관련 파일 (.shp, .shx, .dbf 등)
* **다운로드**: [SGIS 통계지리정보서비스](https://sgis.kostat.go.kr)

### 2. 데이터베이스 스키마 (PostgreSQL)
프로젝트 작동을 위해 아래 두 테이블이 데이터베이스에 생성되어 있어야 합니다.

#### A. 화물차주 분포 테이블 (`chajoo_dist_for_db`)
시군구별 화물차주 등록 현황을 관리합니다.
```sql
CREATE TABLE chajoo_dist_for_db (
    year_month        TEXT,      -- 기준 년월 (예: '2026-01')
    sido              TEXT,      -- 시도 명칭
    sigungu           TEXT,      -- 시군구 명칭
    cargo_sales_count BIGINT,    -- 화물차주 수
    "SHP_CD"          TEXT       -- 행정구역 코드 (지도 매칭용)
);
```

#### B. 식당 관리 테이블 (`restaurant_for_db`)
대상 식당의 상세 정보 및 계약 상태를 관리합니다.
```sql
CREATE TABLE restaurant_for_db (
    restaurant_name      TEXT,             -- 상호명
    road_address         TEXT,             -- 도로명 주소
    owner_name           TEXT,             -- 대표자명
    longitude            DOUBLE PRECISION, -- 경도
    latitude             DOUBLE PRECISION, -- 위도
    total_parking_area   DOUBLE PRECISION, -- 주차장 면적 (㎡)
    wds                  BIGINT,           -- WDS 등급
    large_vehicle_access BIGINT,           -- 대형차 접근성 (1~5)
    contract_status      TEXT,             -- 계약 상태
    remarks              TEXT              -- 비고 (특이사항)
);
```

## 대시보드 실행
(프로젝트 루트 폴더 혹은 dashboard 폴더 위치에 따라 경로 조정)
```
streamlit run dashboard/app.py
```