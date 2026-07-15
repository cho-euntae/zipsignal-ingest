# zipsignal-ingest

[집시그널(ZipSignal)](https://zipsignal.darksyains.workers.dev) 의 **실거래 수집 배치**.
국토교통부 아파트 매매 실거래가 공개 API(data.go.kr)를 긁어 Cloudflare D1(`zipsignal-db`)에 적재합니다.

> 웹앱(UI·SEO) 저장소와 **분리**되어 있습니다. 이 저장소는 공개 API를 D1에 넣는 배치만 담당하며,
> 웹앱은 별도 Private 저장소에서 같은 D1을 **읽기 전용**으로 사용합니다.

```
[이 저장소 · GitHub Actions cron, 매일]
  → 국토부 API (지역 × 년월) → XML 파싱 → dedup_key 생성
  → INSERT OR IGNORE 로 complexes/trades 적재 (ingest_log 기록)
                      │
                      ▼
              Cloudflare D1 (zipsignal-db)
                      ▲
                      │  읽기 (SSR)
        [웹앱 저장소 · Next.js on Workers]
```

## 동작 방식

- D1의 `regions` 테이블에서 수집 대상 시군구를 읽어, 지역 × 년월로 API를 호출합니다.
- 응답 XML을 파싱해 `complexes`(단지) / `trades`(거래) 로 upsert 하고, `dedup_key`(SHA-256)로 중복 적재를 막습니다.
- `regions` 시드는 **웹앱 저장소의 `seed.sql`** 로 먼저 넣어야 합니다(이 저장소는 지역 시드를 하지 않음).

## 맥에서 매일 자동 수집 (launchd) — 현재 운영 방식

GitHub Actions 는 **계정 잠금(카드 인증 실패)** 으로 못 씁니다. 대신 맥의 launchd 로 매일 돌립니다.

```bash
./scripts/setup-mac.sh      # 국토부 API 키 + Cloudflare API 토큰(키체인) + 백필 큐 + launchd
```

> **인증은 API 토큰**을 쓴다(키체인 `zipsignal-cf-token`). `wrangler login`(OAuth)은
> 토큰이 만료돼 launchd 무인 실행이 `code:7403` 로 죽는다 — 실제로 2026-07-15 에 겪음.
>
> **토큰 발급** ([dash → API Tokens](https://dash.cloudflare.com/profile/api-tokens) → Create Custom Token):
> - 권한: **Account · D1 · Edit**
> - **Account Resources: 이 계정 하나로 한정** (기본 "All accounts" 면 다른 프로젝트 D1 까지 편집 가능 → 반드시 좁힐 것). 잘못된 계정을 고르면 `code:7403`.
> - 개인 랩탑 + Public 저장소라 **만료일(TTL)** 과 가능하면 **IP 제한**을 걸어 유출 시 피해를 줄인다.
> - 토큰이 노출되면(로그·화면 공유 등) 즉시 dashboard 에서 **Roll(재발급)**.
>
> ⚠️ 키체인 항목은 CLI 로 만들면 앱 ACL 이 없어 같은 사용자의 아무 프로세스나 프롬프트 없이 읽는다.
> 만료일·IP 제한이 그나마의 보완책이다.

매일 07:30 에 [scripts/daily.sh](scripts/daily.sh) 가 두 가지를 합니다.

1. **최신 수집** — 당월 + 전월 (신고 지연 30일 반영)
2. **과거 백필** — 큐에서 한 달 꺼내 수집하고 한 달 되감기 (202605 → 202604 → …)

> **왜 하루 한 달씩인가**: D1 무료 플랜은 **쓰기 10만 행/일**. 한 달치가 거래 ~6.5만 행이라
> 하루에 두 달을 넣으면 한도를 넘깁니다. 열흘쯤 돌리면 1년치가 채워집니다.
> (data.go.kr 호출은 한 달 = 122지역 × 6유형 = 732회라 1만/일 한도에 여유가 많습니다.)

```bash
tail -f ~/Library/Logs/zipsignal-ingest.log     # 로그
launchctl kickstart -k gui/$UID/dev.zipsignal.ingest   # 즉시 실행
./scripts/setup-mac.sh uninstall                # 해제
```

- API 키는 **저장소가 아니라 macOS 키체인**에 둡니다 (이 저장소는 Public).
- 맥이 꺼져 있던 날은 건너뛰고, 켜지면 다음 스케줄에 이어서 진행합니다(큐 방식이라 진도가 밀리지 않음).

**실패 알림**: 수집·백필이 실패하면 알린다(로그만 보면 조용한 실패를 놓친다 — 실제로 2026-07-15 아침 7403 을 늦게 발견).
- 기본: **맥 알림센터**(설정 0).
- webhook(Slack 등)을 키체인 `zipsignal-notify-webhook` 에 넣으면 그리로도 보냄:
  ```bash
  security add-generic-password -s zipsignal-notify-webhook -a "$USER" -U -w   # 프롬프트에 URL 붙여넣기
  ```
  Slack Incoming Webhook URL 이면 `{"text":...}` 로 전송(Discord 호환).

## GitHub Actions Secrets

저장소 Settings → Secrets and variables → Actions:

| Secret | 값 |
|---|---|
| `DATA_GO_KR_API_KEY` | 공공데이터포털 **디코딩(Decoding)** 서비스 키 |
| `CLOUDFLARE_API_TOKEN` | D1 쓰기 권한 토큰 |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare 계정 ID |

등록 후 **Actions → "실거래 수집 배치" → Run workflow** 로 수동 실행(`ym=202606` 등)해 검증. 이후 매일 04:00 KST cron 자동 실행됩니다.

## 로컬 실행

```bash
npm install

# 로컬 D1 (웹앱 저장소에서 스키마/시드 적용 후)
DATA_GO_KR_API_KEY=<디코딩키> node ingest.mjs --local --ym=202606

# 원격 D1 (wrangler login 세션 또는 CF 토큰 필요)
DATA_GO_KR_API_KEY=<디코딩키> node ingest.mjs --remote --ym=202606
```

- `--ym` 생략 시 **당월 + 전월** 을 함께 수집(신고기한 30일 → 지연분 보정).
- API 키는 **디코딩 키** 사용(인코딩 키는 이중 인코딩됨). 순수 16진수 키는 인코딩/디코딩 동일.

## 운영 메모

- 국토부 API 성공코드는 `resultCode: 0`(+`OK`). `resultCode` 가 빈 값이면 장애로 간주해 실패 처리.
- 사용자 요청 경로에서는 국토부 API를 호출하지 않음 — 수집은 이 배치로만.
- 저장소 Public: **코드는 공개, 시크릿은 GitHub Secrets/`.env`(gitignore)에만** 존재.
