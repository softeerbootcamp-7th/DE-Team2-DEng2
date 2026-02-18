# 🚚 DEng2 Dashboard

화물차주 분포 데이터와 식당 후보지를 지도 기반으로 시각화하고,  
식당 단위 계약 상태를 관리하는 Streamlit 대시보드입니다.

## 📁 디렉토리 구조
```
dashboard/
├── app.py                 # Streamlit 메인 엔트리포인트
├── chajoo_heatmap.py      # 화물차주 분포 히트맵 페이지
├── restaurant_map.py      # 식당 지도 및 상태 관리 페이지
├── dashboard.md           # 대시보드 설명 문서
│
├── core/
│   ├── db.py              # DB 엔진/세션 생성
│   ├── query.py           # SQL 쿼리 모음 (데이터 조회 전용)
│   └── settings.py        # 공통 설정값
│
└── data/
    └── sigungu_shp/
        ├── bnd_sigungu_00.shp
        ├── bnd_sigungu_00.shx
        ├── bnd_sigungu_00.dbf
        └── bnd_sigungu_00.prj
```


## Prerequisite: 행정구역 경계 데이터 (SHP)
지도 렌더링을 위해 아래 경로에 센서스용 행정구역 경계 파일이 반드시 존재해야 합니다.
* **권장 경로**: `dashboard/data/sigungu_shp/`
* **필요 파일**: `bnd_sigungu_00` 관련 파일 (.shp, .shx, .dbf 등)
* **다운로드**: [SGIS 통계지리정보서비스](https://sgis.kostat.go.kr)


## 📌 파일별 역할 정리

### `app.py`
- Streamlit **진입점**
- 사이드바 메뉴 구성
- 페이지 라우팅 담당
- 실제 데이터 로직은 포함하지 않음

역할 요약:
- UI 진입
- 페이지 선택
- 전체 앱 흐름 제어

---

### `chajoo_heatmap.py`
- 화물차주(영업용 차량) 분포 히트맵 시각화
- 시도 / 시군구 단위 집계 데이터 사용
- PyDeck + Mapbox 기반 지도 렌더링

주요 기능:
- 지역 선택 필터
- 행정구역 폴리곤 색상 표현
- Tooltip을 통한 수치 정보 제공

---

### `restaurant_map.py`
- 식당 위치 기반 지도 시각화
- 개별 식당 클릭 시 상태 수정 UI 제공
- DB와 직접 연동하여 상태 즉시 반영

관리 상태 예시:
- 미입력
- 후보
- 연락 시도
- 계약 성공
- 계약 실패


## 🧩 core 모듈 설명

### `core/db.py`
- DB 연결 전담 모듈
- SQLAlchemy Engine 생성
- 앱 전체에서 공통으로 사용

책임:
- DB 연결 방식 통일
- 환경별(DB URL) 변경 대응

---

### `core/query.py`
- **모든 SQL 쿼리 집합**
- UI 코드에서 SQL 직접 작성하지 않기 위한 분리 계층

역할:
- 화물차주 통계 조회
- 식당 목록 조회
- 식당 상태 조회/업데이트용 쿼리 제공


---

### `core/settings.py`
- 설정값 중앙 관리
- 환경변수, 경로, 상수 정의

예시:
- SHP 파일 경로

## ⚙️ 실행 방법

```bash
streamlit run dashboard/app.py
```

## 🗄️ 데이터베이스 요구사항 (필수 테이블)

본 대시보드는 PostgreSQL 데이터베이스와 연동되며,  
아래 테이블들이 **사전에 생성되어 있어야 정상 동작**합니다.

### 1. `public.chajoo_dist`
화물차주(영업용 차량) 분포 통계 테이블  
히트맵 시각화에 사용됩니다.

| 컬럼명 | 타입 | 설명 |
|------|------|------|
| sido | text | 시도명 |
| sigungu | text | 시군구명 |
| cargo_sales_count | bigint | 화물차주 수 |
| SHP_CD | text | 행정구역 코드 (SHP 매핑용) |
| year | text | 연도 |
| month | text | 월 |

---

### 2. `public.restaurant_master`
식당 기본 정보 및 입지 분석 결과 테이블  
식당 지도 렌더링의 **기본 데이터 소스**입니다.

| 컬럼명 | 타입 | 설명 |
|------|------|------|
| 법정동명 | text | 법정동명 |
| 본번 | bigint | 지번 본번 |
| 지주 | text | 지목 |
| 부번 | double precision | 지번 부번 |
| PNU코드 | bigint | 토지 고유 식별자 |
| 도로명주소 | text | 도로명 주소 (PK 구성) |
| 업체명 | text | 식당명 (PK 구성) |
| 대표자 | text | 대표자명 |
| min_유휴부지면적 | double precision | 최소 유휴부지 면적 |
| 유휴부지면적 | double precision | 유휴부지 면적 |
| 신뢰도점수 | double precision | 입지 신뢰도 점수 |
| longitude | double precision | 경도 |
| latitude | double precision | 위도 |
| year | text | 연도 |
| month | text | 월 |
| region | text | 시도 |
| sigungu | text | 시군구 |

**Indexes**
- Primary Key: (`업체명`, `도로명주소`)

---

### 3. `public.restaurant_status`
식당 계약 및 접근성 상태 관리 테이블  
대시보드 UI에서 **사용자 입력으로 갱신**됩니다.

| 컬럼명 | 타입 | 설명 |
|------|------|------|
| 업체명 | text | 식당명 (PK 구성) |
| 도로명주소 | text | 도로명 주소 (PK 구성) |
| large_vehicle_access | integer | 대형차 진입 가능 여부 |
| contract_status | text | 계약 상태 |
| remarks | text | 비고 |
| created_at | timestamp | 생성 시각 |
| updated_at | timestamp | 수정 시각 |

**Indexes**
- Primary Key: (`업체명`, `도로명주소`)

---

### 4. `public.truckhelper_parking_area`
기존 공영 차고지 정보 테이블  
비교용 레퍼런스 데이터로 사용됩니다.

| 컬럼명 | 타입 | 설명 |
|------|------|------|
| 공영차고지명 | text | 차고지명 |
| 주소 | text | 주소 |
| lat | double precision | 위도 |
| lon | double precision | 경도 |

---

### 5. `public.cargo_zscore_hotspots`
화물차주 분포의 통계적 유의성을 분석한 **Z-Score 핫스팟** 테이블입니다.  
단순 수치를 넘어 밀집 지역의 통계적 특성을 시각화하는 데 사용됩니다.

| 컬럼명 | 타입 | 설명 |
|:---:|:---:|---|
| **sigungu_cd** | bigint | 시군구 코드 |
| **oa_code** | bigint | 집계구 코드 (Output Area) |
| **value** | double precision | 해당 구역의 화물차주 수 또는 밀도 |
| **z_score** | double precision | 통계적 유의성을 나타내는 Z-점수 |
| **lat** | double precision | 해당 구역 중심점 위도 |
| **lon** | double precision | 해당 구역 중심점 경도 |

## ⚠️ 주의사항

- 모든 테이블은 **동일한 데이터베이스 스키마 (`public`)**에 존재해야 합니다.
- `restaurant_master` ↔ `restaurant_status`는  
  (`업체명`, `도로명주소`) 기준으로 JOIN 됩니다.
- 컬럼명은 **한글 컬럼명을 그대로 사용**하므로  
  DB 스키마 변경 시 대시보드 코드도 함께 수정해야 합니다.