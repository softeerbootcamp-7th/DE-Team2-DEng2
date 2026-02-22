# api/models/

SQLAlchemy ORM 모델 정의 모듈입니다.

---

## 테이블 스키마

### 1. `restaurant` (restaurant.py)

식당 테이블로, Gold 레이어의 최종 분석 결과를 저장합니다.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| **업체명** | String (PK) | 음식점명 |
| **도로명주소** | String (PK) | 소재지 주소 |
| **year** | Integer (PK) | 연도 |
| **month** | Integer (PK) | 월 |
| **week** | Integer (PK) | 주차 |
| sigungu | String | 시군구 |
| region | String | 지역 |
| 총점 | Float | 종합 점수 |
| 영업_적합도 | Float | 영업 적합도 점수 |
| 수익성 | Float | 수익성 점수 |
| latitude | Float | 위도 |
| longitude | Float | 경도 |
| 유휴부지_면적 | Float | 주차장으로 활용 가능한 유휴부지 면적 |
| 주차_적합도 | Integer | 화물차 주차 적합도 (1-5) |
| contract_status | String | 계약 상태 (후보/접촉/관심/협의/성공/실패) |
| remarks | Text | 비고 |

---

### 2. `chajoo_dist` (chajoo.py)

시군구별 화물차 분포 및 전략적 중요도를 저장합니다.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| **id** | Integer (PK) |  |
| sido | String | 시도 |
| sigungu | String | 시군구 |
| year | Integer | 연도 |
| month | Integer | 월 |
| SHP_CD | String | 행정구역 코드 |
| 전략적_중요도 | Float | 전략적 중요도 점수 |
| cargo_count | Integer | 화물차 등록 대수 |

---

### 3. `truckhelper_parking_area` (truckhelper.py)

트럭헬퍼 차고지 참조 데이터입니다.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| **차고지명** | String (PK) | 차고지명 |
| **주소** | String (PK) | 소재지 |
| lat | Float | 위도 |
| lon | Float | 경도 |

---

### 4. `user_view_history` (user_view_history.py)

대시보드 사용자의 마지막 조회 지역을 저장합니다

| 컬럼 | 타입 | 설명 |
|------|------|------|
| **id** | Integer (PK) | 고정값 1 |
| sigungu | String | 마지막 조회 시군구 |
| updated_at | DateTime | 갱신 시각 |


