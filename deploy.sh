#!/bin/bash
# trade_v 自动部署脚本
# 用于宝塔 WebHook 自动拉取并重启服务

# 配置区域 - 请根据实际情况修改
PROJECT_DIR="/www/wwwroot/trade_v"  # 项目目录，根据实际路径修改
BRANCH="master"                      # 部署的分支
LOG_FILE="/www/wwwlogs/trade_v_deploy.log"  # 部署日志

# 记录时间
echo "=============================" >> $LOG_FILE
echo "部署开始: $(date '+%Y-%m-%d %H:%M:%S')" >> $LOG_FILE

# 进入项目目录
cd $PROJECT_DIR

# 拉取最新代码
echo "拉取最新代码..." >> $LOG_FILE
git fetch origin $BRANCH >> $LOG_FILE 2>&1
git reset --hard origin/$BRANCH >> $LOG_FILE 2>&1

# 安装/更新依赖（如果有新依赖）
echo "检查依赖更新..." >> $LOG_FILE
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt >> $LOG_FILE 2>&1
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
nohup python app.py >> /www/wwwlogs/trade_v_app.log 2>&1 &

echo "部署完成: $(date '+%Y-%m-%d %H:%M:%S')" >> $LOG_FILE
echo "=============================" >> $LOG_FILE


