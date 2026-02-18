#!/bin/bash
# ==============================================
# SSH 터널링: 로컬 → EC2 → RDS (PostgreSQL)
# ==============================================
#
# 사용법:
#   ./scripts/ssh_tunnel_rds.sh          # 터널 열기 (백그라운드)
#   ./scripts/ssh_tunnel_rds.sh stop     # 터널 닫기
#   ./scripts/ssh_tunnel_rds.sh status   # 터널 상태 확인
#
# 사전 설정 (.env에 추가):
#   EC2_KEY=~/.ssh/your-keypair.pem
#   EC2_HOST=ec2-user@<EC2-PUBLIC-IP>
#   RDS_ENDPOINT=<RDS-ENDPOINT>
# ==============================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${PROJECT_ROOT}/.env"
PID_FILE="${SCRIPT_DIR}/.ssh_tunnel.pid"

# .env 파일 로드
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "[ERROR] .env 파일을 찾을 수 없습니다: $ENV_FILE"
    exit 1
fi

LOCAL_PORT="${RDS_PORT:-15432}"
RDS_PORT_REMOTE=5432

stop_tunnel() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID"
            rm -f "$PID_FILE"
            echo "[TUNNEL] 터널 종료 (PID: $PID)"
        else
            rm -f "$PID_FILE"
            echo "[TUNNEL] 프로세스가 이미 종료됨"
        fi
    else
        echo "[TUNNEL] 실행 중인 터널 없음"
    fi
}

check_status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "[TUNNEL] 실행 중 (PID: $PID, localhost:${LOCAL_PORT})"
            return 0
        else
            rm -f "$PID_FILE"
            echo "[TUNNEL] 터널 종료됨 (PID 파일 정리)"
            return 1
        fi
    else
        echo "[TUNNEL] 실행 중인 터널 없음"
        return 1
    fi
}

start_tunnel() {
    # 이미 실행 중인지 확인
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "[TUNNEL] 이미 실행 중 (PID: $PID, localhost:${LOCAL_PORT})"
            return 0
        fi
        rm -f "$PID_FILE"
    fi

    # 필수 변수 검증
    for var in EC2_KEY EC2_HOST RDS_ENDPOINT; do
        if [ -z "${!var:-}" ]; then
            echo "[ERROR] .env에 ${var}가 설정되지 않았습니다"
            exit 1
        fi
    done

    # keypair 경로 확장 (~/ 처리)
    EC2_KEY="${EC2_KEY/#\~/$HOME}"

    if [ ! -f "$EC2_KEY" ]; then
        echo "[ERROR] keypair 파일을 찾을 수 없습니다: $EC2_KEY"
        exit 1
    fi

    # 백그라운드로 터널 실행
    ssh -i "${EC2_KEY}" \
        -L "${LOCAL_PORT}:${RDS_ENDPOINT}:${RDS_PORT_REMOTE}" \
        -N -f \
        -o ServerAliveInterval=60 \
        -o ServerAliveCountMax=3 \
        -o ExitOnForwardFailure=yes \
        -o StrictHostKeyChecking=accept-new \
        "${EC2_HOST}"

    # 방금 생성된 ssh 프로세스 PID 저장
    PID=$(pgrep -f "ssh.*-L.*${LOCAL_PORT}:${RDS_ENDPOINT}" | tail -1)
    if [ -n "$PID" ]; then
        echo "$PID" > "$PID_FILE"
        echo "[TUNNEL] 시작 (PID: $PID)"
        echo "  localhost:${LOCAL_PORT} → ${RDS_ENDPOINT}:${RDS_PORT_REMOTE}"
        echo "  종료: ./scripts/ssh_tunnel_rds.sh stop"
    else
        echo "[ERROR] 터널 시작 실패"
        exit 1
    fi
}

# 명령어 분기
case "${1:-start}" in
    start)  start_tunnel ;;
    stop)   stop_tunnel ;;
    status) check_status ;;
    *)
        echo "사용법: $0 {start|stop|status}"
        exit 1
        ;;
esac
