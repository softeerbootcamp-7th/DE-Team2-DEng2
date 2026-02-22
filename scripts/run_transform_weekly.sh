#!/bin/bash
# ==========================================================
# 주배치: 식당 Clean → S1 (크롤링 리스트 생성)
# 식당 대표자 데이터 (주 1회 갱신)
# 월배치 스크립트 (run_transform_monthly.sh) 실행하여
# s0/address, s0/toji_building 업데이트 후 실행
#
# USE CASE:
#   cd <프로젝트 루트>
#   bash scripts/run_transform_weekly.sh
#   bash scripts/run_transform_weekly.sh --with-s3
# ==========================================================
set -euo pipefail

# 경로 설정
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$PROJECT_ROOT/data"
UTILS_DIR="$PROJECT_ROOT/data_pipeline/utils"
JOBS_DIR="/opt/spark/jobs"  # 컨테이너: data_pipeline/transform → /opt/spark/jobs

# .env 로드
ENV_FILE="$PROJECT_ROOT/.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

WITH_S3=false
for arg in "$@"; do
    case "$arg" in
        --with-s3) WITH_S3=true ;;
    esac
done


# spark-submit
run_spark() {
    local script="$1"
    echo "========================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SPARK: $script"
    echo "========================================"
    docker exec spark-master spark-submit "$JOBS_DIR/$script"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] DONE:  $script"
    echo ""
}

# S3 다운로드
s3_download() {
    local prefix="$1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] S3 DOWNLOAD: $prefix"
    python "$UTILS_DIR/download_data_from_s3.py" \
        --prefix "$prefix" --local-dir "$DATA_DIR"
}

# S3 업로드
s3_upload() {
    local local_dir="$1"
    local prefix="$2"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] S3 UPLOAD: $prefix"
    python "$UTILS_DIR/upload_data_to_s3.py" \
        --local-dir "$local_dir" --prefix "$prefix" \
        --exclude _work --exclude .DS_Store --exclude _SUCCESS
}

notify_slack() {
    python "$SCRIPT_DIR/notify_slack.py" "$@" || true
}

# ─── main ───
TOTAL_START=$(date +%s)
BATCH_NAME="주배치 Transform (Clean → S1)"

echo "========== $BATCH_NAME 시작 =========="
if $WITH_S3; then
    echo "[MODE] S3 연동 모드 (--with-s3)"
else
    echo "[MODE] 로컬 모드"
fi
echo ""

notify_slack info "$BATCH_NAME 시작" "모드: $(if $WITH_S3; then echo 'S3 연동'; else echo '로컬'; fi)"

if $WITH_S3; then
    echo "--- MFA 인증 ---"
    python "$UTILS_DIR/init_mfa_session.py" --prompt-mfa
    echo ""
fi

# ─── Phase 1: Bronze → Silver Clean (식당) ───
echo "--- Phase 3: Clean Restaurant ---"

if $WITH_S3; then s3_download "data/bronze/restaurant_owner"; fi
run_spark clean_restaurant.py
if $WITH_S3; then s3_upload "$DATA_DIR/silver/clean/restaurant" "data/silver/clean/restaurant"; fi

# ─── Phase 2: Silver Clean + S0 → Silver S1 ───
echo "--- Phase 4: S1 ---"

# chk_s0_to_s1
if $WITH_S3; then s3_download "data/silver/s0/address"; fi
run_spark transform_chk_s0_to_s1.py
if $WITH_S3; then s3_upload "$DATA_DIR/silver/s1/restaurant_coord" "data/silver/s1/restaurant_coord"; fi

# s0_to_s1
if $WITH_S3; then s3_download "data/silver/s0/toji_building"; fi
run_spark transform_s0_to_s1.py
if $WITH_S3; then
    s3_upload "$DATA_DIR/silver/s1/toji_list" "data/silver/s1/toji_list"
    s3_upload "$DATA_DIR/silver/s1/crawling_list" "data/silver/s1/crawling_list"
fi

TOTAL_END=$(date +%s)
ELAPSED=$(( TOTAL_END - TOTAL_START ))

echo "=========================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] $BATCH_NAME 완료"
echo "📋 다음 단계: crawling_list 기반 Gov24 크롤링 실행 후 run_transform_to_gold.sh 실행"
echo "총 소요시간: ${ELAPSED}초"
echo "=========================================="

notify_slack success "$BATCH_NAME 완료" "총 소요시간: ${ELAPSED}초\n다음 단계: crawling_list 기반 크롤링 실행 후 run_transform_to_gold.sh"
