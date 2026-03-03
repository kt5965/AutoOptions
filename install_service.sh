#!/bin/bash
# systemd 서비스 등록 스크립트
# 사용법: sudo bash auto/install_service.sh

set -e

SERVICE_NAME="stock-auto"
WORK_DIR="/mnt/sub"
VENV_PYTHON="/mnt/sub/auto/.venv/bin/python"
USER="$(logname 2>/dev/null || echo $SUDO_USER || echo $USER)"

echo "=== ${SERVICE_NAME} 서비스 설치 ==="
echo "작업 디렉토리: ${WORK_DIR}"
echo "Python: ${VENV_PYTHON}"
echo "실행 사용자: ${USER}"

# 서비스 파일 생성
cat > /etc/systemd/system/${SERVICE_NAME}.service << EOF
[Unit]
Description=Stock Auto Trading Scheduler
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=${USER}
WorkingDirectory=${WORK_DIR}
ExecStart=${VENV_PYTHON} -m auto.scheduler
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

# 환경
Environment=PYTHONUNBUFFERED=1

# 보안
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=${WORK_DIR}

[Install]
WantedBy=multi-user.target
EOF

# 서비스 등록 및 시작
systemctl daemon-reload
systemctl enable ${SERVICE_NAME}
systemctl start ${SERVICE_NAME}

echo ""
echo "=== 설치 완료 ==="
echo "상태 확인:  sudo systemctl status ${SERVICE_NAME}"
echo "로그 확인:  tail -f ${WORK_DIR}/auto/logs/scheduler.log"
echo "중지:       sudo systemctl stop ${SERVICE_NAME}"
echo "재시작:     sudo systemctl restart ${SERVICE_NAME}"
echo "삭제:       sudo systemctl disable ${SERVICE_NAME} && sudo rm /etc/systemd/system/${SERVICE_NAME}.service"
