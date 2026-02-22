#!/bin/bash
# ==========================================================
# 수동 실행: S2 → Gold (소유자 매칭 + 스코어링)
# 전제: 주배치 완료 + ownership_inference 크롤링 데이터 존재
#
# USE CASE:
#   cd <프로젝트 루트>
#   bash scripts/run_transform_to_gold.sh
#   bash scripts/run_transform_to_gold.sh --with-s3
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
BATCH_NAME="Gold Transform (S2 → Gold)"

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

# ─── Phase 1: S1 + ownership_inference → S2 ───
echo "--- Phase 5: S2 (소유자 매칭) ---"

if $WITH_S3; then
    s3_download "data/silver/s1/toji_list"
    s3_download "data/silver/s2/ownership_inference"
fi
run_spark transform_s1_to_s2.py
if $WITH_S3; then s3_upload "$DATA_DIR/silver/s2/toji_owner_match" "data/silver/s2/toji_owner_match"; fi

# ─── Phase 2: S2 → Gold ───
echo "--- Phase 6: Gold (스코어링) ---"

run_spark transform_s2_to_gold.py
if $WITH_S3; then s3_upload "$DATA_DIR/gold/restaurant" "data/gold/restaurant"; fi

TOTAL_END=$(date +%s)
ELAPSED=$(( TOTAL_END - TOTAL_START ))

echo "=========================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] $BATCH_NAME 완료"
echo "총 소요시간: ${ELAPSED}초"
echo "=========================================="

notify_slack success "$BATCH_NAME 완료" "총 소요시간: ${ELAPSED}초"
