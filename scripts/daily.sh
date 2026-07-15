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

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$HOME/.local/state/zipsignal"
QUEUE_FILE="$STATE_DIR/backfill-next"   # 다음에 백필할 YYYYMM
FLOOR_FILE="$STATE_DIR/backfill-floor"  # 여기까지만 백필 (그 이전은 멈춤)
KEYCHAIN_API="zipsignal-data-go-kr"     # 국토부 API 키
KEYCHAIN_CF_TOKEN="zipsignal-cf-token"  # Cloudflare API 토큰 (D1 Edit)
CLOUDFLARE_ACCOUNT_ID="44db11dbbdd3cedbb78195406be3a6db"

# launchd 는 PATH 가 빈약하다 → node/npx 경로 확보
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

if ! DATA_GO_KR_API_KEY="$(security find-generic-password -s "$KEYCHAIN_API" -w 2>/dev/null)"; then
  log "❌ 키체인에서 국토부 API 키를 못 찾음 (서비스명: $KEYCHAIN_API). scripts/setup-mac.sh 를 먼저 실행하세요."
  exit 1
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
  log "❌ 최신 수집 실패 — 백필은 건너뜀 (D1 쓰기 한도를 아껴둔다)"
  exit 1
fi

# ---------- 2. 과거 백필 (하루 한 달) ----------
if [[ ! -f "$QUEUE_FILE" ]]; then
  log "ℹ️ 백필 큐 없음 → 최신 수집만 하고 종료"
  exit 0
fi

YM="$(tr -d '[:space:]' < "$QUEUE_FILE")"
FLOOR="$(tr -d '[:space:]' < "$FLOOR_FILE" 2>/dev/null || echo "")"

if [[ ! "$YM" =~ ^[0-9]{6}$ ]]; then
  log "❌ 백필 큐 값이 이상함: '$YM' (YYYYMM 이어야 함)"
  exit 1
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
  log "❌ 백필 실패 · $YM (큐 그대로 두고 내일 재시도)"
  exit 1
fi
