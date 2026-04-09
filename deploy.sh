#!/bin/bash
# trade_v 自动部署脚本
# 用于宝塔 WebHook 自动拉取并重启服务

set -e

# 配置区域 - 请根据实际情况修改
PROJECT_DIR="/www/wwwroot/trade_v"  # 项目目录，根据实际路径修改
BRANCH="master"                      # 部署的分支
LOG_FILE="/www/wwwlogs/trade_v_deploy.log"  # 部署日志
APP_LOG_FILE="/www/wwwlogs/trade_v_app.log"
PYTHON_BIN="${PYTHON_BIN:-$PROJECT_DIR/.venv/bin/python}"
APP_PORT="${APP_PORT:-5000}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:${APP_PORT}/api/health}"
MAX_HEALTH_RETRIES="${MAX_HEALTH_RETRIES:-8}"
HEALTH_RETRY_INTERVAL_SECONDS="${HEALTH_RETRY_INTERVAL_SECONDS:-2}"

# 记录时间
echo "=============================" >> $LOG_FILE
echo "部署开始: $(date '+%Y-%m-%d %H:%M:%S')" >> $LOG_FILE

# 进入项目目录
cd $PROJECT_DIR

# 拉取最新代码
echo "拉取最新代码..." >> $LOG_FILE
git fetch origin $BRANCH >> $LOG_FILE 2>&1
git reset --hard origin/$BRANCH >> $LOG_FILE 2>&1
CURRENT_SHA=$(git rev-parse --short HEAD)
echo "部署版本: ${CURRENT_SHA}" >> $LOG_FILE

# 安装/更新依赖（如果有新依赖）
echo "检查依赖更新..." >> $LOG_FILE
if [ -f "requirements.txt" ]; then
    "$PYTHON_BIN" -m pip install -r requirements.txt >> $LOG_FILE 2>&1
fi

# 重启服务（根据你的部署方式选择一种）

# 方式1: 使用 supervisor 管理
# supervisorctl restart trade_v >> $LOG_FILE 2>&1

# 方式2: 使用 systemd 管理
# systemctl restart trade_v >> $LOG_FILE 2>&1

# 方式3: 使用宝塔 Python 项目管理器
# 如果使用宝塔的 Python 项目管理器，需要通过宝塔面板重启

# 方式4: 直接 kill 并重启（简单粗暴）
echo "重启服务..." >> $LOG_FILE
pkill -f "python.*app.py" >> $LOG_FILE 2>&1
sleep 2
cd $PROJECT_DIR
APP_VERSION=$CURRENT_SHA nohup "$PYTHON_BIN" app.py >> "$APP_LOG_FILE" 2>&1 &

# 发布后健康检查，避免“进程已启动但接口不可用”
echo "执行健康检查: ${HEALTH_URL}" >> $LOG_FILE
health_ok=0
for i in $(seq 1 "$MAX_HEALTH_RETRIES"); do
    if curl -fsS "$HEALTH_URL" >> $LOG_FILE 2>&1; then
        health_ok=1
        break
    fi
    sleep "$HEALTH_RETRY_INTERVAL_SECONDS"
done

if [ "$health_ok" -ne 1 ]; then
    echo "健康检查失败，部署中止: $(date '+%Y-%m-%d %H:%M:%S')" >> $LOG_FILE
    exit 1
fi

echo "部署完成: $(date '+%Y-%m-%d %H:%M:%S')" >> $LOG_FILE
echo "=============================" >> $LOG_FILE









