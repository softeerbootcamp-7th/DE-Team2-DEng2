# Extract + S3 Upload Wrapper Guide


## 1) 무엇을 위한 스크립트인가

각 래퍼는 아래 순서로 동작합니다.

1. 대상 extract 스크립트를 먼저 실행합니다.
2. extract가 예외 없이 정상 종료되면, extract가 생성한 출력 디렉터리를 찾습니다.
3. 찾은 디렉터리를 `data_pipeline/utils/upload_data_to_s3.py`로 업로드합니다.
4. 디렉터리가 없거나 파일이 없으면 해당 경로는 자동 skip 합니다.

즉, **extract 실패 시 업로드는 실행되지 않습니다.**

## 2) 파일별 설명

### `data_pipeline/script/_upload_to_s3_runner.py`

extract + 업로드 래퍼가 공통으로 쓰는 유틸입니다.

- `add_upload_args(parser)`
  - S3 업로드 공통 CLI 옵션(`--bucket`, `--prefix`, `--profile`, `--region`, `--mfa-*`, `--dry-run`)을 parser에 추가
- `ensure_upload_script()`
  - `upload_data_to_s3.py` 경로 존재 여부 확인
- `build_upload_prefix(base_prefix, local_dir)`
  - 업로드 대상 로컬 경로를 프로젝트 상대 경로로 바꿔 S3 prefix 생성
- `run_upload_for_dir(local_dir, args)`
  - 실제 `upload_data_to_s3.py` subprocess 실행
- `upload_existing_dirs(dirs, args)`
  - 후보 디렉터리를 순회하며 존재/파일 수 확인 후 업로드
- `run_extractor_script(extract_script, extractor_args)`
  - extractor subprocess 실행

### `data_pipeline/script/run_extract_tojiSoyuJeongbo_and_upload_s3.py`

`extract_tojiSoyuJeongbo.py` 전용 래퍼입니다.

- extractor를 subprocess로 실행
- `per_row_zips`/`unzipped`의 run 디렉터리(`YYYY-MM-DD_to_YYYY-MM-DD`) 중 최신 1개를 자동 탐색해 업로드
- `data/tojiSoyuJeongbo/parquet/year=*/month=*` 중 최신 월 파티션을 자동 탐색해 업로드
- 업로드 대상 경로:
  - 최신 `data/tojiSoyuJeongbo/_work/per_row_zips/YYYY-MM-DD_to_YYYY-MM-DD`
  - 최신 `data/tojiSoyuJeongbo/_work/unzipped/YYYY-MM-DD_to_YYYY-MM-DD`
  - 최신 `data/tojiSoyuJeongbo/parquet/year=*/month=*`

### `data_pipeline/script/run_extract_restaurant_and_upload_s3.py`

`extract_restaurant.py` 전용 래퍼입니다.

- extractor를 subprocess로 실행
- `data/restaurant/parquet/dataset=YYYYQn` 중 최신 분기를 자동 탐색해 업로드
- 업로드 대상 경로:
  - `data/restaurant/_work/zips`
  - `data/restaurant/_work/unzipped`
  - 최신 `data/restaurant/parquet/dataset=YYYYQn`

### `data_pipeline/script/run_extract_restaurant_owner_and_upload_s3.py`

`extract_restaurant_owner.py` 전용 래퍼입니다.

- extractor 실행 시 `--sido`, `--addr`를 그대로 전달
- region은 `--sido` 앞 2글자로 계산 (예: `경기도` -> `경기`)
- 최신 연월의 해당 region 파티션을 업로드
- 업로드 대상 경로:
  - `data/restaurant_owner/_work`
  - 최신 `data/restaurant_owner/parquet/year=*/month=*/region=<시도2글자>`

### `data_pipeline/script/run_extract_buildingLeader_and_upload_s3.py`

`extract_buildingLeader.py` 전용 래퍼입니다.

- `data/buildingLeader/parquet/year=*/month=*` 중 최신 월 폴더 업로드
- 업로드 대상 경로:
  - `data/buildingLeader/_work/zips`
  - `data/buildingLeader/_work/unzipped`
  - 최신 `data/buildingLeader/parquet/year=*/month=*`

## 3) 공통 CLI 옵션

아래 옵션은 모든 래퍼에서 동일하게 지원합니다.

- `--bucket` (필수): 대상 S3 버킷
- `--prefix` (기본 `data`): S3 기본 prefix
- `--profile`: AWS profile
- `--region` (기본 `ap-northeast-2`)
- `--mfa-serial`
- `--mfa-token-code`
- `--mfa-duration-seconds` (기본 `3600`)
- `--prompt-mfa`
- `--dry-run`: extract는 실제 실행, 업로드는 계획만 출력

## 4) 실행 예시

### 토지소유정보

```bash
python data_pipeline/script/run_extract_tojiSoyuJeongbo_and_upload_s3.py \
  --bucket your-bucket \
  --prefix data \
  --mfa-serial arn:aws:iam::123456789012:mfa/your-user \
  --prompt-mfa
```

### 식당정보

```bash
python data_pipeline/script/run_extract_restaurant_and_upload_s3.py \
  --bucket your-bucket \
  --prefix data \
  --mfa-serial arn:aws:iam::123456789012:mfa/your-user \
  --prompt-mfa
```

### 식당 대표자 정보

```bash
python data_pipeline/script/run_extract_restaurant_owner_and_upload_s3.py \
  --sido 경기도 \
  --addr "용인시 처인구" \
  --bucket your-bucket \
  --prefix data \
  --mfa-serial arn:aws:iam::123456789012:mfa/your-user \
  --prompt-mfa
```

### 건축물대장

```bash
python data_pipeline/script/run_extract_buildingLeader_and_upload_s3.py \
  --bucket your-bucket \
  --prefix data \
  --mfa-serial arn:aws:iam::123456789012:mfa/your-user \
  --prompt-mfa
```