# airflow

Apache Airflow 기반 데이터 파이프라인 오케스트레이션 모듈입니다.



## 개요

데이터 파이프라인의 **Extract → Transform → Load** 전 과정을 Airflow DAG로 관리합니다.
총 **7개 DAG**가 실행되며, 성공/실패 시 Slack 알림이 발송됩니다.




## 공통 기능


- **Slack 알림**: 모든 DAG에 성공/실패 콜백 연동
- **재시도**: 1~2회, 10분 간격
- **Sensor 기반 의존성**: Parquet 파일 존재 여부 확인 후 후속 태스크 실행
- **S3 업로드**: 추출/변환 결과물을 S3에 업로드



## DAG 목록

### Extract DAGs (Bronze Layer)

#### 1. `weekly_extract_restaurant_workflow.py`

음식점 및 대표자 데이터를 크롤링합니다.

- **스케줄**: 주 배치
- **주요 태스크**: `extract_restaurant_owner`
- **특징**:
  - DAG 파라미터로 `광역시/도`, `시군구`, 페이지 범위 지정 가능
  - `auto_resume` 기능으로 중단된 크롤링 자동 재개


#### 2. `monthly_extract_building_workflow.py`

건축물 대장(표제부) 데이터를 hub.go.kr에서 수집합니다.

- **스케줄**: 월 배치
- **주요 태스크**: `extract_building_leader`

#### 3. `monthly_extract_chajoo_workflow.py`

화물차 등록 분포 데이터를 수집합니다.

- **스케줄**: 월 배치
- **주요 태스크**: `extract_chajoo_dist`

#### 4. `monthly_extract_tojiSoyuJeongbo_workflow.py`

토지 소유 정보를 VWorld 포털에서 수집합니다.

- **스케줄**: 월 배치
- **주요 태스크**: `extract_tojisoyu`

---

### Transform DAGs (Silver Layer)

#### 5. `monthly_transform_workflow.py`

월마다 수집되는 Bronze 데이터(토지, 건축물, 주소, 좌표)를 
Bronze → Clean → S0 과정을 거쳐 변환합니다.

- **스케줄**: 월 배치 (extract 시점 이후)
- **Phase 1 (Clean)**: raw data 정제
- **Phase 2 (S0)**: 주소+좌표 조인, 토지+건축물 조인
- **특징**:
  - `PythonSensor`로 input 데이터 존재 여부 확인 후 실행

#### 6. `weekly_transform_workflow.py`

주마다 수집되는 음식점 데이터 정제 후, 월 배치 데이터와 join 한후 S0 → S1 를 거쳐 토지대장 크롤링 리스트(후보 토지 리스트)를 생성합니다.

- **스케줄**: 주 배치
- **주요 태스크**: restaurant clean → S0 주소/토지 결합 → S1 후보 토지 리스트

---

### Load DAG 

#### 7. `transform_to_gold_workflow.py`

S1 → S2 → Gold 최종 변환 및 RDS에 저장을 수행합니다.

- **스케줄**: 토지대장 크롤링이 완료된 시점에 시작
- **주요 태스크**: S1→S2 토지 및 음식점 소유자 추론, S2→Gold 지표 계산, RDS 업로드
- **특징**: RDS/S3에 최종 데이터 업로드






## DAG 의존성 흐름

```
[Extract DAGs]                    [Transform DAGs]           [Load DAG]


monthly_extract_building  ──┐
monthly_extract_tojiSoyu ───┼──▶ monthly_transform ──┐
                                                     │
weekly_extract_restaurant ─────▶ weekly_transform ───┼──▶ transform_to_gold ──▶ final data
                                                      
                                  
```