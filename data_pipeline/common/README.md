# Common S3 Utilities


##  Upload local `data/` directory to S3

```bash
python data_pipeline/common/upload_data_to_s3.py \
  --local-dir data \
  --bucket your-bucket-name \
  --prefix your_s3_folder_path
```

Optional flags:
- `--dry-run`: print upload targets only
- `--skip-existing`: skip objects that already exist
- `--profile`: use specific AWS profile

## Postgres ORM

Env vars required for RDS:
- `RDS_HOST`, `RDS_PORT`, `RDS_DB`, `RDS_USER`, `RDS_PASSWORD`

`data_pipeline/common/postgres_orm.py`는 Spark 적재(writer) 전용입니다.
테이블 스키마/초기화/조회 쿼리는 `api/` 하위 모듈에서 관리합니다.

