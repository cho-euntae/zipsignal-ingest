#!/bin/bash
# 집시그널 일일 수집 (macOS launchd 에서 실행)
#
#   1) 최신 수집 — 당월 + 전월 (신고 지연 반영)
#   2) 과거 백필 — 큐에서 한 달 꺼내 수집하고 큐를 한 달 뒤로 되감음
#
# 왜 하루 한 달씩인가: D1 무료 플랜은 쓰기 10만 행/일. 한 달치가 거래 ~6.5만 행이라
# 하루에 두 달을 넣으면 한도를 넘긴다. 열흘쯤 돌리면 1년치가 채워진다.
#
# 비밀값은 저장소가 아니라 macOS 키체인에 둔다 (이 저장소는 Public).
#   security add-generic-password -s zipsignal-data-go-kr -a "$USER" -w '<디코딩키>'
#   security add-generic-password -s zipsignal-cf-token   -a "$USER" -w '<CF API 토큰>'
#
# ⚠️ wrangler OAuth(로그인 세션)는 무인 배치에 부적합하다 — 토큰이 만료되면 launchd
#    실행이 code:7403 로 죽는다. 그래서 장수 API 토큰(CLOUDFLARE_API_TOKEN)을 쓴다.
#
# 설치: scripts/setup-mac.sh 참고

set -euo pipefail

SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"  # 순수 문자열 (실패 불가)
STATE_DIR="$HOME/.local/state/zipsignal"
QUEUE_FILE="$STATE_DIR/backfill-next"   # 다음에 백필할 YYYYMM
FLOOR_FILE="$STATE_DIR/backfill-floor"  # 여기까지만 백필 (그 이전은 멈춤)
KEYCHAIN_API="zipsignal-data-go-kr"     # 국토부 API 키
KEYCHAIN_CF_TOKEN="zipsignal-cf-token"  # Cloudflare API 토큰 (D1 Edit)
KEYCHAIN_WEBHOOK="zipsignal-notify-webhook"  # (선택) Slack 등 알림 webhook URL
CLOUDFLARE_ACCOUNT_ID="44db11dbbdd3cedbb78195406be3a6db"

# launchd 는 PATH 가 빈약하다 → node/npx 경로 확보
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# 배치 실패 알림. webhook 이 있으면 그리로(Slack 등), 없으면 맥 알림센터로 폴백.
# 실패는 로그만 남기면 사람이 봐야만 안다 — 오늘 아침 7403 도 그래서 늦게 발견했다.
#
# 메시지 이스케이프는 수제로 하지 않는다(백슬래시·따옴표 조합에서 문자열 경계가 깨진다).
# JSON 은 node(반드시 있음)로 인코딩, AppleScript 는 argv 로 넘겨 소스에 안 섞이게 한다.
notify() {
  local msg="$1"
  log "🔔 알림: $msg"

  local hook
  if hook="$(security find-generic-password -s "$KEYCHAIN_WEBHOOK" -w 2>/dev/null)" && [[ -n "$hook" ]]; then
    local payload
    payload="$(MSG="$msg" node -e 'process.stdout.write(JSON.stringify({text:"[집시그널 수집] "+process.env.MSG}))')"
    # 실패해도 배치를 막지 않는다. URL 이 새지 않도록 출력 전부 버린다.
    curl -fsS -m 10 -X POST -H 'Content-Type: application/json' \
      --data "$payload" "$hook" >/dev/null 2>&1 || log "⚠️ webhook 전송 실패 (URL 확인 필요)"
  fi

  # 맥 알림센터. msg 를 AppleScript 소스에 보간하지 않고 argv(item 1)로 전달.
  osascript - "$msg" >/dev/null 2>&1 <<'APPLESCRIPT' || true
on run argv
  display notification (item 1 of argv) with title "집시그널 수집 실패"
end run
APPLESCRIPT
}

# 구체적 실패는 fail() 로 (명확한 메시지), 예기치 못한 죽음은 trap 이 백스톱.
NOTIFIED=0
fail() { notify "$1"; NOTIFIED=1; exit 1; }
trap 'code=$?; [[ $code -ne 0 && $NOTIFIED -eq 0 ]] && notify "배치가 예기치 못하게 종료 (exit $code) — 로그 확인"' EXIT

# 이 시점부터 실패는 trap 이 알림을 보낸다 (앞은 순수 문자열 할당뿐이라 사각지대 없음)
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if ! DATA_GO_KR_API_KEY="$(security find-generic-password -s "$KEYCHAIN_API" -w 2>/dev/null)"; then
  fail "국토부 API 키를 키체인에서 못 찾음 — setup-mac.sh 재실행 필요"
fi
export DATA_GO_KR_API_KEY

# Cloudflare 인증: API 토큰이 있으면 OAuth 대신 그걸 쓴다(wrangler 규약).
# account_id 는 어느 경로든 필요하므로 무조건 export (OAuth 폴백도 비대화형이라 필요).
export CLOUDFLARE_ACCOUNT_ID
CF_TOKEN="$(security find-generic-password -s "$KEYCHAIN_CF_TOKEN" -w 2>&1)" && CF_TOKEN_OK=1 || CF_TOKEN_OK=0
if [[ "$CF_TOKEN_OK" == "1" ]]; then
  export CLOUDFLARE_API_TOKEN="$CF_TOKEN"
  unset CF_TOKEN
else
  # 실패 원인(항목 없음 vs 키체인 잠금)을 남겨야 진단이 된다
  log "⚠️ CF API 토큰($KEYCHAIN_CF_TOKEN) 못 읽음 → wrangler login 세션에 의존(만료 위험)."
  log "   원인: ${CF_TOKEN:-알수없음}. setup-mac.sh 재실행 또는 키체인 잠금 해제 확인."
  unset CF_TOKEN
fi

cd "$REPO_DIR"
mkdir -p "$STATE_DIR"

# ---------- 1. 최신 수집 (당월 + 전월) ----------
log "▶ 최신 수집 시작 (당월+전월)"
if node ingest.mjs --remote; then
  log "✅ 최신 수집 완료"
else
  fail "최신 수집 실패 — 백필 건너뜀. ~/Library/Logs/zipsignal-ingest.log 확인"
fi

# ---------- 2. 과거 백필 (하루 한 달) ----------
if [[ ! -f "$QUEUE_FILE" ]]; then
  log "ℹ️ 백필 큐 없음 → 최신 수집만 하고 종료"
  exit 0
fi

YM="$(tr -d '[:space:]' < "$QUEUE_FILE")"
FLOOR="$(tr -d '[:space:]' < "$FLOOR_FILE" 2>/dev/null || echo "")"

if [[ ! "$YM" =~ ^[0-9]{6}$ ]]; then
  fail "백필 큐 값이 이상함: '$YM' (YYYYMM 이어야 함)"
fi
# 하한이 깨져 있으면(빈 값·오타) 비교가 항상 거짓이 돼 과거로 무한히 내려간다 → 기본값으로
if [[ ! "$FLOOR" =~ ^[0-9]{6}$ ]]; then
  log "⚠️ 백필 하한 값이 이상함: '$FLOOR' → 기본값 202401 사용"
  FLOOR="202401"
fi
if [[ "$YM" < "$FLOOR" ]]; then
  log "🎉 백필 완료 — $FLOOR 까지 다 채웠습니다. 큐를 비웁니다."
  rm -f "$QUEUE_FILE"
  exit 0
fi

log "▶ 백필 시작 · $YM (하한 $FLOOR)"
if node ingest.mjs --remote --ym="$YM"; then
  # 한 달 되감기 (202501 → 202412)
  PREV="$(YM="$YM" node -e '
    const ym = process.env.YM;
    const y = Number(ym.slice(0, 4));
    const m = Number(ym.slice(4, 6));
    const d = new Date(Date.UTC(y, m - 2, 1)); // m-1 이 이번달 → m-2 가 전달
    console.log(`${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}`);
  ')"
  echo "$PREV" > "$QUEUE_FILE"
  log "✅ 백필 완료 · $YM → 다음 차례 $PREV"
else
  fail "백필 실패 · $YM (큐 유지, 내일 재시도)"
fi
