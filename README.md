# Softeer Bootcamp 7th - DE Team2-DEng2

## 🚛 Teum: 화물차주를 위한 식당 야간 주차장 추천 서비스

식당이 소유한 유휴 부지를 분석하여, 신규 화물차 주차용 부지를 찾고 있는 사업자(트럭헬퍼) 영업부서에게 최적의 on-demand 주차 장소를 추천하는 데이터 엔지니어링 프로젝트입니다.

---


## 상황

- 우리나라의 대형 **화물트럭 운전사**들은 일이 끝난 후, 차량 주차할 곳을 찾지 못해 집 근처 갓길에 **불법으로 밤샘주차**(00:00 ~ 04:00)를 합니다.
    - 불법 밤샘주차를 하면 집근처에 댈 수 있다는 장점이 있지만, `화물자동차 운수사업법`상 불법 밤샘주차는 과태료를 최대 50만원(지역별/규정별 상이)까지 낼 수 있다는 위험이 있습니다.
    - 공무원들이 직접 단속을 하거나, 민원을 받아 단속을 하는데 단속이 심한 구역에 주차하는 차주는 한달에 3번 이상 단속에 걸리기도 합니다.
- 국가에서 전국 각지에 55개의 **공영 차고지를 운영 중이긴 하지만, 항상 만차 상태**이고 등록하기 위해서는 적어도 1년씩을 기다려야 합니다.
    - 전국에 등록된 사업용 화물차 대수: **472,175** 대
    - 전국 모든 공영차고지의 주차 면수: **12,738** 대
- “빅모빌리티”는 화물차주들의 이런 고통을 해결하고자, 직접 사설 주차장을 운영하는 서비스인 `트럭헬퍼`를 시작했습니다.

    **트럭헬퍼 (Target)** 

    - 현재 유일하게 화물차 주차 전용으로 운영되는 사설 주차장 관리 플랫폼
    - **도시 외곽 공장부지나 창고부지** 등의 유휴 부지를 화물차 주차장으로 개발하여 화물차 운전자들에게 합법적인 주차 공간을 제공
    - 현재 트럭헬퍼에서 제공하는 화물 주차장은 대부분 만차인 상태로, 지속적으로 신규 부지를 찾아 운영하는 주차장의 수를 늘리고 있음

## 어떤 문제

- 화물차 운전자 10명을 대상으로 인터뷰를 진행한 결과
    - 트럭헬퍼를 인지하고 있음에도 실제로 사용하지 않는다고 응답한 사례가 8건
    -  서비스가 도심 외곽에만 위치해 있어 **접근성이 낮다**는 점을 이유로 제시
    
    → 주차장에 대한 접근성이 잠재 고객의 유입을 저해할 가능성이 있음
    
- 트럭헬퍼 입장에서 **접근성 문제로 인한 잠재 고객 이탈**을 하나의 문제 상황으로 정의하고, 이를 완화할 수 있는 방안을 탐색하고자합니다.

## 어떻게 해결

**대형 화물 차주들이 편하게 사용할만한 새로운 주차 공간의 조건**을 갖춘 주차 공간을 찾습니다.

1. 기존 외곽 창고/공장 부지보다 화물차주들의 생활 구역에 근접한 거리에 있어야 합니다.
2. 화물차가 들어가고 회차하기 편한 크기의 땅이어야 합니다.
3. 화물차주들이 밤샘주차를 할 수 있어야 합니다.

→ 이러한 이유로 1) 생활 공간과 밀접해 있는 2) 노상 부설 주차장을 가진 3) 야간에는 운영을 하지 않는 **식당 주차장**을 사용하기로 결정했습니다.


## 데이터 소스

| 유형 | 출처 | 설명 |
|------|------|------|
| **음식점 데이터** | 식품안전나라 | 전국 음식점 목록 및 대표자 정보 (주 단위 갱신) |
| **건축물 대장** | 건축HUB | 필지 내 건축물 존재 여부 및 건물 구성 정보 |
| **토지 소유 정보** | VWorld | 필지 단위 면적·지목·소유권 변동 정보 |
| **도로명주소/좌표** | 주소정보누리집 | 주소-좌표 매핑을 통한 위치 정보 확보 |
| **토지 대장** | 정부24 | 지주 확인을 위한 행정 문서 |
| **화물차 분포** | 국토교통 통계누리 | 지역별 화물차 수요 파악용 통계 데이터 |

---


## Data Pipeline Architecture

Airflow 기반 ETL 파이프라인을 통해 데이터를 주기적으로 수집, 정제, 적재합니다.

![image alt][reference]


## Data Lake Architecture (Medallion Pattern)

  
### Bronze (Raw Data)

| 도메인 | 주기 | 설명 |
|--------|------|------|
| `address` | monthly | 도로명주소 & 지번주소 매칭  |
| `coord` | monthly | 건물 좌표 정보 |
| `buildingLeader` | monthly | 건축물대장 표제부 |
| `tojiSoyuJeongbo` | monthly | 토지소유정보 |
| `restaurant_owner` | weekly | 식당+대표자 정보 |

  

### Silver (정제 & 통합)

- **Silver Clean**: 각 도메인별 정제 (`address_clean`, `building_clean`, `coord_clean`, `restaurant_clean`, `toji_clean`)
- **Silver 0**: 정제된 데이터 통합 - `address`(주소 $\bowtie$ 좌표 후 WGS84 변환), `toji_building`(토지 $\bowtie$ 건물 후 filtering)
- **Silver 1**: 토지 후보 리스트 추출 - `restaurant_toji_list`, `crawling_list`
- **Silver 2**: 토지 리스트 $\bowtie$ 지주 정보 &rarr; 지주와 음식점 대표자 정보 매칭 - `ownership_inference`, `toji_owner_match`

  

### Gold 
- `restaurant`: 유휴부지 면적 및 지표 계산이 완료된 최종 테이블


### Airflow DAGs (7개 워크플로우)

| DAG | 주기 | 설명 |
|-----|------|------|
| `weekly_extract_restaurant_workflow` | weekly | 음식점+대표자 데이터 추출 → S3 |
| `monthly_extract_building_workflow` | monthly | 건축물 대장 추출 → S3 |
| `monthly_extract_chajoo_workflow` | monthly | 화물차 분포 데이터 추출 → S3 |
| `monthly_extract_tojiSoyuJeongbo_workflow` | monthly | 토지 소유 정보 추출 → S3 |
| `monthly_transform_workflow` | monthly | 월 배치 데이터 Bronze → Silver (Silver Clean + Silver 0) |
| `weekly_transform_workflow` | weekly | 주 배치 데이터(식당) 이용 후보 토지  리스트 추출 (Silver 0 → 1 → 2) |
| `transform_to_gold_workflow` | weekly | Silver 2 → Gold, RDS 적재 |

---

## 기술적 고려사항

### Airflow 구성 시 고려한 점

- **Sensor 기반 의존성 관리**: `PythonSensor`를 활용하여 input으로 사용되는 데이터 적재 완료 후 Transform 작업 실행
- **Slack 알림 연동**: DAG 성공/실패 시 Slack Webhook으로 실시간 알림
- **재시도 전략**: 1~2회 재시도, 10분 간격으로 설정하여 일시적 장애에 대응

### Extract 과정에서 고려한 점

- **오류 대응 로직 구현**: 토지 대장/ 식당 대표자 데이터 크롤링 과정을 logging하여 오류 발생 시 중단 지점부터 재수집 가능하도록 구현
- **단계 분리 및 크롤링 비용 최적화**: 데이터 소스별 수집 주기 차이를 반영해 크롤링 스테이지를 분리하고, 비용이 큰 크롤링 작업은 S1 단계에서 후보 식당을 선별한 이후에만 수행

### Transformation 과정에서 고려한 점

- **Spark 기반 분산 처리**: 대규모 토지/건축물 데이터 조인 및 정제에 PySpark 활용
- **파티션 전략**: `region` 기준 파티셔닝으로 지역별 독립적 처리 및 증분 업데이트 지원

### 데이터 레이크 설계 시 고려한 점

- **Medallion Architecture**: Bronze / Silver / Gold 로 저장하는 데이터를 관리
- **S3 버전 관리**: year/month/week 기반 파티셔닝으로 시점별 데이터 추적 가능
- **Parquet 포맷 통일**: 모든 계층에서 Parquet 포맷을 사용하여 효율적인 스토리지 활용



### Web 서비스(Dashboard) 구성 시 고려한 점

- **Streamlit 기반 대시보드**: 영업 부서 입장에서 필요한 정보를 우선적으로 배치, 영업 적합도와 수익성 같은 지표로 TOP15 후보 식당 리스트 제공
- **실시간 계약 관리**: 하단 테이블을 통해 계약 상태(후보→접촉→협의→성공/실패) 업데이트

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| **데이터 처리** | Apache Spark (PySpark) |
| **오케스트레이션** | Apache Airflow |
| **데이터 웨어하우스** | PostgreSQL (AWS RDS) |
| **데이터 레이크** | AWS S3 |

---

## 프로젝트 구조

```
DE-Team2-DEng2/
├── airflow/                          # Airflow 오케스트레이션
│   ├── dags/                         # 7개 DAG 정의
│   ├── Dockerfile
│   └── docker-compose.yaml
│
├── api/                              # DB 모델
│   ├── models/                      
│   ├── alembic/                      
│   └── session.py
│
├── data_pipeline/                    # ETL 파이프라인
│   ├── extract/                      # 데이터 추출 (7개 소스)
│   ├── transform/                    # 데이터 정제 (Spark job)
│   ├── load/                         # 데이터 적재
│   ├── utils/                        # 유틸리티 (Slack 알림, AWS)
│   └── docker/                       # Postgres, Spark 컨테이너
│
├── dashboard/                        # Streamlit 대시보드
│   ├── 0_🍽️_식당_주차장_관리.py           # 메인 페이지: 식당 주차장 관리
│   ├── pages/
│   │   └── 1_🚛_차주_수요_분석.py         # 서브 페이지: 화물차 수요분석
│   ├── core/                         
│   └── data/                         
│
├── scripts/                          
├── requirements.txt
├── .env.example                      
└── README.md
```
