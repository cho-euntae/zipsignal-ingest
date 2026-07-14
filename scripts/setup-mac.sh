#!/bin/bash
# 맥에서 일일 자동 수집 설치 (launchd)
#
#   ./scripts/setup-mac.sh            # 설치 (키 등록 + 백필 큐 + launchd 등록)
#   ./scripts/setup-mac.sh uninstall  # 해제
#
# GitHub Actions 가 계정 잠금으로 막혀 있어 맥의 launchd 로 대신 돌린다.
# 맥이 꺼져 있으면 그날은 건너뛰고, 켜지면 launchd 가 밀린 실행을 한 번 보충한다.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="dev.zipsignal.ingest"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
STATE_DIR="$HOME/.local/state/zipsignal"
LOG_FILE="$HOME/Library/Logs/zipsignal-ingest.log"
KEYCHAIN_SERVICE="zipsignal-data-go-kr"

if [[ "${1:-}" == "uninstall" ]]; then
  launchctl bootout "gui/$UID/$LABEL" 2>/dev/null || true
  rm -f "$PLIST"
  echo "✅ 자동 수집 해제 완료 (키체인 키·백필 큐는 남겨둡니다)"
  exit 0
fi

# ---------- 1. API 키 (키체인) ----------
if security find-generic-password -s "$KEYCHAIN_SERVICE" -w >/dev/null 2>&1; then
  echo "✓ 키체인에 API 키가 이미 있습니다"
else
  echo "공공데이터포털 인증키(디코딩 키)를 입력하세요. 입력값은 화면에 안 보입니다."
  read -rsp "DATA_GO_KR_API_KEY: " key
  echo
  [[ -n "$key" ]] || { echo "❌ 키가 비어 있습니다"; exit 1; }
  security add-generic-password -s "$KEYCHAIN_SERVICE" -a "$USER" -w "$key" -U
  echo "✓ 키체인에 저장했습니다 (저장소에는 남지 않습니다)"
fi

# ---------- 2. 백필 큐 ----------
mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"

# YYYYMM 형식만 받는다 (오타가 그대로 큐에 박히면 엉뚱한 달을 긁는다)
ask_ym() {
  local prompt="$1" default="$2" value
  while true; do
    read -rp "$prompt (YYYYMM, 기본 $default): " value
    value="${value:-$default}"
    if [[ "$value" =~ ^[0-9]{6}$ ]]; then
      echo "$value"
      return
    fi
    echo "  ❌ YYYYMM 6자리 숫자로 입력하세요 (예: 202605)" >&2
  done
}

if [[ ! -f "$STATE_DIR/backfill-next" ]]; then
  # 현재 수집된 가장 오래된 달의 직전 달부터 거꾸로 내려간다
  ask_ym "백필 시작 월" "202605" > "$STATE_DIR/backfill-next"
fi
if [[ ! -f "$STATE_DIR/backfill-floor" ]]; then
  ask_ym "백필 하한 월 (여기까지만)" "202401" > "$STATE_DIR/backfill-floor"
fi
echo "✓ 백필: $(cat "$STATE_DIR/backfill-next") → $(cat "$STATE_DIR/backfill-floor") (하루 한 달씩)"

# ---------- 3. launchd ----------
mkdir -p "$(dirname "$PLIST")" "$(dirname "$LOG_FILE")"
cat > "$PLIST" <<PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$REPO_DIR/scripts/daily.sh</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key><integer>7</integer>
    <key>Minute</key><integer>30</integer>
  </dict>
  <!-- 맥이 꺼져 있어 걸렀으면 켜졌을 때 한 번 보충 실행 -->
  <key>RunAtLoad</key>
  <false/>
  <key>StandardOutPath</key>
  <string>$LOG_FILE</string>
  <key>StandardErrorPath</key>
  <string>$LOG_FILE</string>
  <key>WorkingDirectory</key>
  <string>$REPO_DIR</string>
</dict>
</plist>
PLIST_EOF

launchctl bootout "gui/$UID/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$UID" "$PLIST"
echo "✓ launchd 등록 완료 — 매일 07:30 실행"
echo
echo "로그:      tail -f $LOG_FILE"
echo "즉시 실행: launchctl kickstart -k gui/$UID/$LABEL"
echo "해제:      ./scripts/setup-mac.sh uninstall"
echo
echo "⚠️ wrangler 로그인 세션이 필요합니다 (한 번만): npx wrangler login"
