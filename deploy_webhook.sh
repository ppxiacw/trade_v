#!/bin/bash
set -Eeuo pipefail

PROJECT_DIR="${PROJECT_DIR:-/www/wwwroot/trade_v}"
PROJECT_NAME="${PROJECT_NAME:-trade_v}"
BRANCH="${BRANCH:-master}"
LOG_DIR="${LOG_DIR:-/www/wwwlogs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/trade_v_deploy.log}"
LOCK_FILE="${LOCK_FILE:-/tmp/trade_v_deploy.lock}"

BT_PYTHON_BIN="${BT_PYTHON_BIN:-/www/server/panel/pyenv/bin/python3}"
BT_PYTHON_MODEL="${BT_PYTHON_MODEL:-/www/server/panel/class/projectModel/pythonModel.py}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:5000/api/health}"
VERSION_URL="${VERSION_URL:-http://127.0.0.1:5000/api/version}"
PORT="${PORT:-5000}"
MAX_HEALTH_RETRIES="${MAX_HEALTH_RETRIES:-15}"
HEALTH_RETRY_INTERVAL_SECONDS="${HEALTH_RETRY_INTERVAL_SECONDS:-3}"

mkdir -p "$LOG_DIR"
exec >> "$LOG_FILE" 2>&1

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

get_port_pids() {
    if command -v lsof >/dev/null 2>&1; then
        lsof -ti tcp:"$PORT" -sTCP:LISTEN 2>/dev/null || true
    else
        ss -ltnp "sport = :$PORT" 2>/dev/null \
            | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' \
            | sort -u || true
    fi
}

kill_project_backend_on_port() {
    local pids
    pids="$(get_port_pids)"
    [ -z "$pids" ] && return 0

    for pid in $pids; do
        local cmd
        cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
        if echo "$cmd" | grep -q "$PROJECT_DIR"; then
            log ">>> 兜底停止旧后端 PID=$pid CMD=$cmd"
            kill "$pid" 2>/dev/null || true
        else
            log ">>> 端口 $PORT 被非项目进程占用，跳过 PID=$pid CMD=$cmd"
        fi
    done

    sleep 2

    for pid in $pids; do
        if kill -0 "$pid" 2>/dev/null; then
            local cmd
            cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
            if echo "$cmd" | grep -q "$PROJECT_DIR"; then
                log ">>> 强制停止旧后端 PID=$pid"
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
    done
}

wait_port_release() {
    for i in $(seq 1 10); do
        [ -z "$(get_port_pids)" ] && return 0
        log ">>> 等待端口 $PORT 释放，第 ${i} 次"
        sleep 1
    done
    return 1
}

log "============================="
log "部署开始"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    log "检测到已有部署任务在运行，跳过本次执行"
    exit 0
fi

if [ ! -d "$PROJECT_DIR/.git" ]; then
    log "项目目录不存在或不是 git 仓库: $PROJECT_DIR"
    exit 1
fi

if [ ! -x "$BT_PYTHON_BIN" ]; then
    log "宝塔 Python 解释器不存在: $BT_PYTHON_BIN"
    exit 1
fi

if [ ! -f "$BT_PYTHON_MODEL" ]; then
    log "宝塔 Python 项目管理器脚本不存在: $BT_PYTHON_MODEL"
    exit 1
fi

cd "$PROJECT_DIR"

OLD_PIDS="$(get_port_pids | tr '\n' ' ' || true)"
log ">>> 重启前监听 PID: ${OLD_PIDS:-无}"

log ">>> 拉取最新代码..."
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"

CURRENT_SHA="$(git rev-parse --short HEAD)"
export GIT_COMMIT="$CURRENT_SHA"
export APP_VERSION="$CURRENT_SHA"
export APP_ENV="${APP_ENV:-production}"

log ">>> 当前版本: $(git log -1 --pretty=format:'%h - %s (%ci)')"
log ">>> 部署版本: $CURRENT_SHA"

log ">>> 调用宝塔重启项目..."
set +e
"$BT_PYTHON_BIN" "$BT_PYTHON_MODEL" restart "$PROJECT_NAME"
BT_RESTART_CODE=$?
set -e

if [ "$BT_RESTART_CODE" -ne 0 ]; then
    log ">>> 宝塔 restart 返回失败，exit_code=$BT_RESTART_CODE"
fi

sleep 2

NEW_PIDS="$(get_port_pids | tr '\n' ' ' || true)"
if [ -n "$OLD_PIDS" ] && [ "$NEW_PIDS" = "$OLD_PIDS" ]; then
    log ">>> 监听 PID 未变化，判断 restart 可能未真正重启，执行兜底清理"
    kill_project_backend_on_port
    wait_port_release || log ">>> 端口 $PORT 未完全释放，继续尝试重启"
    "$BT_PYTHON_BIN" "$BT_PYTHON_MODEL" restart "$PROJECT_NAME"
fi

log ">>> 执行健康检查: $HEALTH_URL"
health_ok=0
for i in $(seq 1 "$MAX_HEALTH_RETRIES"); do
    if curl -fsS "$HEALTH_URL" >/dev/null; then
        current_version="$(curl -fsS "$VERSION_URL" 2>/dev/null || true)"
        log ">>> 健康检查通过，第 ${i} 次成功，当前 PID: $(get_port_pids | tr '\n' ' ' || true)"
        log ">>> 版本信息: $current_version"
        health_ok=1
        break
    fi
    log ">>> 健康检查第 ${i} 次失败，${HEALTH_RETRY_INTERVAL_SECONDS}s 后重试"
    sleep "$HEALTH_RETRY_INTERVAL_SECONDS"
done

if [ "$health_ok" -ne 1 ]; then
    log ">>> 健康检查失败，部署中止"
    log ">>> 当前端口 PID: $(get_port_pids | tr '\n' ' ' || true)"
    exit 1
fi

log "部署完成"
log "============================="
