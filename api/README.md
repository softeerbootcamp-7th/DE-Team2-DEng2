# api/

SQLAlchemy ORM 모델 및 데이터베이스 세션 관리 모듈입니다.

---

## 개요

PostgreSQL 데이터베이스의 테이블 스키마를 Python ORM으로 정의하고,
로컬(Docker) / AWS(RDS) 환경을 자동 전환하는 DB 연결 관리를 담당합니다.

---

## 디렉토리 구조

```
api/
├── models/                     # ORM 모델 정의 (상세는 models/README.md 참고)
│   ├── base.py                 
│   ├── restaurant.py           # 식당 테이블
│   ├── chajoo.py               # 화물차 분포 테이블
│   ├── truckhelper.py          # 트럭헬퍼 주차장 테이블
│   └── user_view_history.py    
├── alembic/                    # DB 마이그레이션
│   ├── versions/               
│   └── env.py                  # Alembic 설정
├── alembic.ini                 
├── session.py                  
└── README.md
```

---

## DB 연결 (session.py)

### 환경별 자동 전환

| 모드 | 환경변수 `DASHBOARD_DB_CONNECTION` | 접속 대상 |
|------|------|------|
| **local** | `local` | Docker PostgreSQL (localhost:5432) |
| **aws** | `aws` | AWS RDS (SSH 터널 경유) |

### 주요 함수

| 함수 | 설명 |
|------|------|
| `build_postgres_url()` | 환경변수 기반 접속 URL 생성 |
| `create_engine_for_mode(mode)` | 모드에 따른 SQLAlchemy 엔진 생성 |

---

## 마이그레이션 (Alembic)

```bash
# 마이그레이션 생성
alembic revision --autogenerate -m "description"

# 마이그레이션 적용
alembic upgrade head

# 현재 버전 확인
alembic current
```
