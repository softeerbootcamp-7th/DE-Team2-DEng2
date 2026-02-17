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
- `init_mfa_session.py`
  - MFA 임시 세션을 먼저 발급/캐시
- `upload_data_to_local_db.py`
  - 로컬 CSV/Parquet를 Local Docker Postgres에 적재
- `upload_data_to_rds.py`
  - 로컬 CSV/Parquet를 RDS Postgres에 적재

## 2) 사전 준비

- AWS
  - 로컬 터미널에 `aws configure` 완료


## 3) AWS 인증 함수

세션 인증 스크립트 예시:

```bash
python data_pipeline/utils/init_mfa_session.py \
  --prompt-mfa
```

## 4) S3 업로드

스크립트: `upload_data_to_s3.py`

### 주요 동작

- `--local-dir` 하위 파일을 재귀 탐색
- 로컬 상대경로를 유지한 채 `s3://bucket/prefix/...`로 업로드
- S3에 동일 key가 이미 있으면 자동 skip
- `--exclude` 특정 경로/파일명을 제외 가능 (반복 지정 가능)
- `--dry-run`이면 실제 업로드 없이 계획만 출력

### 예시

```bash
python data_pipeline/utils/upload_data_to_s3.py \
  --local-dir data \
  --prefix data \
  --exclude _work \
  --exclude .DS_Store \
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
  --prefix data \
  --local-dir data \
```

## 6) Local Postgres 업로드

스크립트: `upload_data_to_local_db.py`


## 7) RDS Postgres 업로드

스크립트: `upload_data_to_rds.py`

