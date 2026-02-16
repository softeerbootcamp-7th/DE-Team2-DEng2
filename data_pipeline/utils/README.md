# Utils Guide

`data_pipeline/utils` 폴더의 S3 / Postgres 유틸 스크립트 사용법입니다.

## 1) 파일 구성

- `aws_auth.py`
  - AWS profile + MFA 기반 임시 세션 생성 함수
  - 함수: `authenticate_aws_session(...)`
- `upload_data_to_s3.py`
  - 로컬 폴더를 S3로 업로드
- `download_data_from_s3.py`
  - S3 prefix를 로컬로 다운로드
- `upload_data_to_local_db.py`
  - 로컬 CSV/Parquet를 Local Docker Postgres(`silver_stage_1_main`)에 적재
- `upload_data_to_rds.py`
  - 로컬 CSV/Parquet를 RDS Postgres(`silver_stage_1_main`)에 적재

## 2) 사전 준비

- AWS
  - 로컬 터미널에 `aws configure` 완료
  - MFA 필요한 경우 `--mfa-serial` + (`--mfa-token-code` 또는 `--prompt-mfa`) 사용
- Postgres 연결 정보
  - RDS mode: `RDS_*` 환경변수 또는 `--db-url`

## 3) AWS 인증 함수

`aws_auth.py`의 `authenticate_aws_session(...)`는 아래 순서로 동작합니다.

1. profile/region 기반 기본 세션 생성
2. `mfa_serial`이 있으면 STS `get_session_token` 호출
3. 임시 AccessKey/Secret/SessionToken으로 새 세션 반환

MFA 관련 인자:

- `--mfa-serial`: MFA 디바이스 ARN
- `--mfa-token-code`: 현재 MFA 코드
- `--prompt-mfa`: 코드 입력 프롬프트 표시
- `--mfa-duration-seconds`: 임시 세션 유효시간(기본 3600)

## 4) S3 업로드

스크립트: `upload_data_to_s3.py`

### 주요 동작

- `--local-dir` 하위 파일을 재귀 탐색
- 로컬 상대경로를 유지한 채 `s3://bucket/prefix/...`로 업로드
- S3에 동일 key가 이미 있으면 자동 skip
- `--dry-run`이면 실제 업로드 없이 계획만 출력

### 예시

```bash
python data_pipeline/utils/upload_data_to_s3.py \
  --local-dir data \
  --bucket your-bucket \
  --prefix data \
  --mfa-serial arn:aws:iam::123456789012:mfa/your-user \
  --prompt-mfa
```

## 5) S3 다운로드

스크립트: `download_data_from_s3.py`

### 주요 동작

- `s3://bucket/prefix` 하위 객체를 모두 조회
- S3 key 구조를 로컬 경로로 복원해서 저장
- 로컬에 동일 파일이 이미 있으면 자동 skip
- `--dry-run`이면 실제 다운로드 없이 계획만 출력

### 예시

```bash
python data_pipeline/utils/download_data_from_s3.py \
  --bucket your-bucket \
  --prefix data \
  --local-dir data \
  --mfa-serial arn:aws:iam::123456789012:mfa/your-user \
  --prompt-mfa
```

## 6) Local Postgres 업로드

스크립트: `upload_data_to_local_db.py`

### 주요 동작

- 입력: `--local-dir` 하위 `csv/parquet` (`--file-format auto|csv|parquet`)
- 적재 대상 테이블: 현재 `silver_stage_1_main`
- 테이블/스키마 변경 -> `upload_data_to_local_db.py` 상단 `TARGET_PRESET` 블록만 수정
- 파일 경로에서 snapshot을 추출합니다.
  - `year=YYYY`
  - `month=MM` 
  - `region=...`
  - `sigungu=...`
- 같은 snapshot으로 재실행 시 (`source_year`, `source_month`, `region`, `sigungu`) 기존 데이터 전체 삭제 후 다시 저장하는 방식으로 업데이트

### 예시

```bash
python data_pipeline/utils/upload_data_to_local_db.py \
  --local-dir data/output/silver_stage_1/main \
  --file-format parquet
```

Dry-run:

```bash
python data_pipeline/utils/upload_data_to_local_db.py \
  --local-dir data/output/silver_stage_1/main \
  --dry-run
```

## 7) RDS Postgres 업로드

스크립트: `upload_data_to_rds.py`

### 주요 동작

- 내부적으로 `upload_data_to_local_db.py`의 공통 업로드 함수를 재사용
- 차이점: DB mode가 `aws`, `--rds-sslmode` 옵션 사용 가능

### 예시

```bash
python data_pipeline/utils/upload_data_to_rds.py \
  --local-dir data/output/silver_stage_1/main \
  --file-format auto \
  --rds-sslmode require
```

## 8) 운영 시 주의사항

- Postgres 업로드는 동일 snapshot 데이터 삭제 후 적재합니다.
- CSV는 chunk 단위(`50,000`)로 읽지만, Parquet는 파일 단위로 읽습니다.
- snapshot은 파일 경로 파티션(`year/month/region/sigungu`) 기준입니다.
- 인코딩 이슈가 있는 CSV는 `utf-8`, `euc-kr`, `utf-8-sig`, `cp949` 순서로 읽습니다.
