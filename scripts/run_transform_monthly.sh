#!/bin/bash
# ==========================================================
# 월배치: Bronze → Silver Clean → Silver S0
# 주소/좌표/건축물/토지 데이터 (월 1회 갱신)
#
# USE CASE:
#   cd <프로젝트 루트>
#   bash scripts/run_transform_monthly.sh
#   bash scripts/run_transform_monthly.sh --with-s3
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
BATCH_NAME="월배치 Transform (Bronze → S0)"

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

# ─── Phase 1: Bronze → Silver Clean ───
echo "--- Phase 1: Clean ---"

# clean_address
if $WITH_S3; then s3_download "data/bronze/address"; fi
run_spark clean_address.py
if $WITH_S3; then s3_upload "$DATA_DIR/silver/clean/address" "data/silver/clean/address"; fi

# clean_coord
if $WITH_S3; then s3_download "data/bronze/coord"; fi
run_spark clean_coord.py
if $WITH_S3; then s3_upload "$DATA_DIR/silver/clean/coord" "data/silver/clean/coord"; fi

# clean_building
if $WITH_S3; then s3_download "data/bronze/buildingLeader"; fi
run_spark clean_building.py
if $WITH_S3; then s3_upload "$DATA_DIR/silver/clean/building" "data/silver/clean/building"; fi

# clean_toji
if $WITH_S3; then s3_download "data/bronze/tojiSoyuJeongbo"; fi
run_spark clean_toji.py
if $WITH_S3; then s3_upload "$DATA_DIR/silver/clean/toji" "data/silver/clean/toji"; fi

# ─── Phase 2: Silver Clean → Silver S0 ───
echo "--- Phase 2: S0 ---"

# s0_address
run_spark transform_clean_to_s0_address.py
if $WITH_S3; then s3_upload "$DATA_DIR/silver/s0/address" "data/silver/s0/address"; fi

# s0_toji_building
run_spark transform_clean_to_s0_toji_building.py
if $WITH_S3; then s3_upload "$DATA_DIR/silver/s0/toji_building" "data/silver/s0/toji_building"; fi

TOTAL_END=$(date +%s)
ELAPSED=$(( TOTAL_END - TOTAL_START ))

echo "=========================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] $BATCH_NAME 완료"
echo "총 소요시간: ${ELAPSED}초"
echo "=========================================="

notify_slack success "$BATCH_NAME 완료" "총 소요시간: ${ELAPSED}초"
