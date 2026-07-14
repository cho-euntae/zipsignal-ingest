// 집시그널 실거래 수집 배치 (GitHub Actions / 로컬 공용)
//
// 흐름: D1에서 지역 목록 조회 → 유형×거래별 국토부 API 호출(지역×년월) → XML 파싱
//       → dedup_key 생성 → complexes/trades upsert SQL 생성 → wrangler로 D1 적재
//
// 사용법:
//   node --env-file=.env ingest.mjs --local          # 로컬 D1
//   node ingest.mjs --remote                         # 원격 D1, 전체 유형·전 지역
//   node ingest.mjs --remote --ym=202606             # 특정 년월만
//   node ingest.mjs --remote --only=11680,41135      # 특정 시군구만
//   node ingest.mjs --remote --source=apt-sale,offi-sale  # 특정 유형만
//
// 필수 환경변수: DATA_GO_KR_API_KEY (디코딩 키)
// 원격(--remote) 추가: CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID (Actions Secrets)

import { XMLParser } from "fast-xml-parser";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { writeFileSync, rmSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const DB_NAME = "zipsignal-db";
const API_BASE = "https://apis.data.go.kr/1613000";
const NUM_OF_ROWS = 1000;
const FETCH_TIMEOUT_MS = 30_000;
const STMT_CHUNK = 500; // d1File 트랜잭션당 최대 SQL 문 수 (D1 CPU 한도 회피)

// ---------- 수집 소스 정의 (유형 × 거래구분) ----------
// kind: "trade"(매매, dealAmount→price) | "rent"(전월세, deposit→price, monthlyRent 있으면 월세)
// nameField: 단지/건물명 XML 필드 (유형마다 다름)
//
// ⚠️ 아파트 매매 외 5종의 nameField 는 문서 기준 추정값 — 게이트웨이 반영 후
//    실제 XML 로 검증·보정 필요(scripts 없이 API 직접 호출로 확인). #TODO-verify-fields
const SOURCES = [
  { id: "apt-sale",  propertyType: "APT",       kind: "trade", path: "RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev", nameField: "aptNm",    label: "아파트 매매" },
  { id: "apt-rent",  propertyType: "APT",       kind: "rent",  path: "RTMSDataSvcAptRent/getRTMSDataSvcAptRent",           nameField: "aptNm",    label: "아파트 전월세" },
  { id: "offi-sale", propertyType: "OFFICETEL", kind: "trade", path: "RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",       nameField: "offiNm",   label: "오피스텔 매매" },
  { id: "offi-rent", propertyType: "OFFICETEL", kind: "rent",  path: "RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",         nameField: "offiNm",   label: "오피스텔 전월세" },
  { id: "rh-sale",   propertyType: "ROWHOUSE",  kind: "trade", path: "RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",           nameField: "mhouseNm", label: "연립다세대 매매" },
  { id: "rh-rent",   propertyType: "ROWHOUSE",  kind: "rent",  path: "RTMSDataSvcRHRent/getRTMSDataSvcRHRent",             nameField: "mhouseNm", label: "연립다세대 전월세" },
];

// ---------- 인자 파싱 ----------
const args = process.argv.slice(2);
const isLocal = args.includes("--local");
const dbFlag = isLocal ? "--local" : "--remote";
const ymArg = args.find((a) => a.startsWith("--ym="))?.split("=")[1];
// --only=41192,41194 : 특정 시군구(lawd_cd)만. 생략 시 전체.
const onlyArg = args.find((a) => a.startsWith("--only="))?.split("=")[1];
const onlyCodes = onlyArg ? new Set(onlyArg.split(",").map((s) => s.trim())) : null;
// --source=apt-sale,offi-sale : 특정 수집 소스만. 생략 시 전체 6종.
const sourceArg = args.find((a) => a.startsWith("--source="))?.split("=")[1];
const sourceIds = sourceArg ? new Set(sourceArg.split(",").map((s) => s.trim())) : null;
// --backfill : API 호출 없이 complexes 비정규화 캐시(거래수 등)만 지역별로 재계산.
const backfillOnly = args.includes("--backfill");

const API_KEY = process.env.DATA_GO_KR_API_KEY;
if (!API_KEY) {
  console.error("[ingest] DATA_GO_KR_API_KEY 가 설정되지 않았습니다.");
  process.exit(1);
}
if (ymArg !== undefined && !/^\d{6}$/.test(ymArg)) {
  console.error(`[ingest] --ym 형식 오류: '${ymArg}' (YYYYMM 6자리 숫자여야 함)`);
  process.exit(1);
}
// 값이 빈 필터(--source= / --only=)는 "전체 실행"으로 조용히 폴백되면 위험 → 오류 처리
if (sourceArg === "") {
  console.error("[ingest] --source= 값이 비어 있습니다. 소스 id 를 지정하세요.");
  process.exit(1);
}
if (onlyArg === "") {
  console.error("[ingest] --only= 값이 비어 있습니다. lawd_cd 를 지정하세요.");
  process.exit(1);
}
if (sourceIds) {
  const valid = new Set(SOURCES.map((s) => s.id));
  const bad = [...sourceIds].filter((id) => !valid.has(id));
  if (bad.length) {
    console.error(`[ingest] --source 알 수 없는 값: ${bad.join(", ")} (가능: ${[...valid].join(", ")})`);
    process.exit(1);
  }
}
const activeSources = SOURCES.filter((s) => !sourceIds || sourceIds.has(s.id));

const parser = new XMLParser({ ignoreAttributes: true, trimValues: true });

// 로그에 API 키가 새지 않도록 마스킹 (에러 메시지에 요청 URL이 섞일 수 있음)
function redact(msg) {
  return String(msg).split(API_KEY).join("***");
}

// ---------- 유틸 ----------
function sqlStr(v) {
  if (v === null || v === undefined || v === "") return "''";
  return `'${String(v).replace(/'/g, "''")}'`;
}
/**
 * 외부(국토부) 텍스트에서 제어문자를 걷어낸다.
 * - 개행이 섞이면 "한 줄 = 한 문장" 전제가 깨져 SQL 문장 경계가 어긋난다.
 * - 0x1F 는 비정규화 갱신 키의 구분자라 데이터에 있으면 키가 충돌한다.
 */
function cleanText(v) {
  return String(v ?? "")
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .trim();
}
function sqlNum(v) {
  return v === null || v === undefined || Number.isNaN(v) ? "NULL" : String(v);
}
function toInt(v) {
  if (v === undefined || v === null) return null;
  const d = String(v).replace(/[^0-9-]/g, "");
  if (d === "" || d === "-") return null;
  const n = Number.parseInt(d, 10);
  return Number.isNaN(n) ? null : n;
}
function toFloat(v) {
  if (v === undefined || v === null) return null;
  const n = Number.parseFloat(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isNaN(n) ? null : n;
}
function pad2(n) {
  return String(n).padStart(2, "0");
}
function slugify(name, jibun) {
  const base = `${name} ${jibun ?? ""}`.trim();
  return base
    .replace(/[^가-힣a-zA-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
}
function ymOffset(offset) {
  const now = new Date(Date.now() + 9 * 60 * 60 * 1000); // KST
  const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + offset, 1));
  return `${d.getUTCFullYear()}${pad2(d.getUTCMonth() + 1)}`;
}

// ---------- wrangler ----------
function wrangler(execArgs) {
  try {
    return execFileSync("npx", ["wrangler", ...execArgs], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      maxBuffer: 64 * 1024 * 1024,
    });
  } catch (err) {
    // execFileSync 의 기본 메시지는 "Command failed: ..." 뿐이라 원인(D1 CPU 한도,
    // 쿼터 초과, SQL 오류)이 통째로 사라진다 → wrangler 가 stderr 로 뱉은 본문을 붙인다.
    const detail = [err.stderr, err.stdout]
      .map((s) => (s ? String(s).trim() : ""))
      .filter(Boolean)
      .join(" | ")
      .slice(0, 800);
    throw new Error(detail ? `${err.message}\n    ↳ ${detail}` : err.message);
  }
}
function d1Query(sql) {
  const out = wrangler(["d1", "execute", DB_NAME, dbFlag, "--json", "--command", sql]);
  const parsed = JSON.parse(out);
  const first = Array.isArray(parsed) ? parsed[0] : parsed;
  return first?.results ?? [];
}
/** 일시 오류(동시 요청·레이트리밋·네트워크)로 하루치 데이터가 비지 않도록 재시도 */
const D1_RETRIES = 3;
const D1_RETRY_BASE_MS = 2000;

function sleepSync(ms) {
  // 배치 스크립트라 동기 대기로 충분 (Atomics.wait 은 타이머 없이 정확히 멈춘다)
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function d1File(sql) {
  const dir = mkdtempSync(join(tmpdir(), "zipsignal-"));
  const file = join(dir, "batch.sql");
  try {
    writeFileSync(file, sql, "utf8");
    for (let attempt = 1; ; attempt += 1) {
      try {
        wrangler(["d1", "execute", DB_NAME, dbFlag, "--file", file]);
        return;
      } catch (err) {
        if (attempt >= D1_RETRIES) throw err;
        const waitMs = D1_RETRY_BASE_MS * 2 ** (attempt - 1);
        console.error(
          `    ↻ D1 적재 실패 (${attempt}/${D1_RETRIES}) — ${waitMs / 1000}초 후 재시도: ${redact(err.message).split("\n")[0]}`,
        );
        sleepSync(waitMs);
      }
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

// ---------- 국토부 API ----------
async function fetchPage(source, lawdCd, ym, pageNo) {
  const params = new URLSearchParams({
    serviceKey: API_KEY,
    LAWD_CD: lawdCd,
    DEAL_YMD: ym,
    numOfRows: String(NUM_OF_ROWS),
    pageNo: String(pageNo),
  });
  const res = await fetch(`${API_BASE}/${source.path}?${params}`, {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} (${source.id} lawd=${lawdCd} ym=${ym})`);
  const parsed = parser.parse(await res.text());

  // 성공코드는 게이트웨이/API에 따라 "0"·"00"·"000" 로 다양 → 숫자 0이면 성공.
  // 단, 빈 문자열/공백은 Number("")===0 이라 성공으로 오인되므로 명시적으로 실패 처리.
  const rawCode = parsed?.response?.header?.resultCode;
  const code =
    rawCode === undefined || rawCode === null ? undefined : String(rawCode).trim();
  if (code !== undefined && (code === "" || Number(code) !== 0)) {
    const msg = parsed?.response?.header?.resultMsg ?? "unknown";
    const err = new Error(`API ${rawCode}: ${msg}`);
    // data.go.kr 은 트래픽 초과를 HTTP 200 + resultCode 로 준다 (하루 1만 호출 한도).
    // 이건 잠깐 쉬면 풀리는 일시 오류라 재시도 대상으로 표시한다.
    err.rateLimited = /LIMITED_NUMBER_OF_SERVICE_REQUESTS|EXCEEDS/i.test(String(msg));
    throw err;
  }
  const body = parsed?.response?.body;
  const total = toInt(body?.totalCount) ?? 0;
  const raw = body?.items?.item;
  const items = Array.isArray(raw) ? raw : raw ? [raw] : [];
  return { items, total };
}

const FETCH_RETRIES = 3;
const FETCH_RETRY_BASE_MS = 2000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * 국토부 API 는 간헐적으로 끊긴다(fetch failed / 게이트웨이 5xx).
 * 무인 배치라 한 번 끊기면 그 지역·유형·달이 통째로 빈다 → 재시도.
 * (API 오류 응답(resultCode≠0)은 재시도해도 같으니 그대로 던진다)
 */
/**
 * 재시도할 오류인가.
 * - 네트워크 끊김·타임아웃·5xx: 다시 하면 될 수 있다
 * - 429 / 트래픽 초과(resultCode 22): 레이트리밋 → 잠깐 쉬면 풀린다
 *   (data.go.kr 은 트래픽 초과를 HTTP 200 + resultCode 로 주기도 한다)
 * - 401·키 오류·잘못된 파라미터: 다시 해도 같다 → 즉시 실패
 */
function isTransientApiError(err) {
  if (err.name === "TimeoutError") return true;
  if (err.rateLimited) return true;
  return /fetch failed|network|ECONN|socket|HTTP 429|HTTP 5\d\d/i.test(
    String(err.message),
  );
}

async function fetchPageWithRetry(source, lawdCd, ym, pageNo) {
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await fetchPage(source, lawdCd, ym, pageNo);
    } catch (err) {
      if (!isTransientApiError(err) || attempt >= FETCH_RETRIES) throw err;
      const waitMs = FETCH_RETRY_BASE_MS * 2 ** (attempt - 1);
      console.error(
        `    ↻ API 실패 (${attempt}/${FETCH_RETRIES}) ${source.id} lawd=${lawdCd} ym=${ym} — ` +
          `${waitMs / 1000}초 후 재시도: ${redact(err.message)}`,
      );
      await sleep(waitMs);
    }
  }
}

async function fetchAll(source, lawdCd, ym) {
  const { items, total } = await fetchPageWithRetry(source, lawdCd, ym, 1);
  const all = [...items];
  const pages = Math.ceil(total / NUM_OF_ROWS);
  for (let p = 2; p <= pages; p++) {
    const next = await fetchPageWithRetry(source, lawdCd, ym, p);
    all.push(...next.items);
  }
  return all;
}

// ---------- 매핑 ----------
// source.kind 에 따라 매매/전월세를 통합 처리해 표준 row 로 변환.
function mapItem(item, lawdCd, source) {
  const year = toInt(item.dealYear);
  const month = toInt(item.dealMonth);
  const day = toInt(item.dealDay);
  const area = toFloat(item.excluUseAr);
  // 면적은 양수만 유효 (0/음수/누락은 통계 왜곡 → 버림)
  if (year === null || month === null || day === null || area === null || area <= 0)
    return null;
  // 월/일 범위 방어 (toInt 는 "13"·"40" 도 통과시키므로 명시 검증)
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;

  // 가격·거래구분: 매매는 dealAmount→price(SALE); 전월세는 deposit→price + monthlyRent
  let price, monthlyRent, tradeType;
  if (source.kind === "rent") {
    price = toInt(item.deposit); // 보증금(만원)
    monthlyRent = toInt(item.monthlyRent) ?? 0; // 월세(만원, 0이면 전세)
    if (price === null || price < 0 || monthlyRent < 0) return null;
    tradeType = monthlyRent > 0 ? "MONTHLY" : "JEONSE";
  } else {
    price = toInt(item.dealAmount); // 거래금액(만원)
    monthlyRent = 0;
    if (price === null || price <= 0) return null; // 매매가는 양수만
    tradeType = "SALE";
  }

  const dealDate = `${year}-${pad2(month)}-${pad2(day)}`;
  const umd = cleanText(item.umdNm);
  const name = cleanText(item[source.nameField]);
  const jibun = cleanText(item.jibun);
  const floor = toInt(item.floor);
  // 해제(취소)는 매매에만 존재. 전월세 응답엔 cdealType 이 없어 자연히 false.
  const canceled = String(item.cdealType ?? "").trim().toUpperCase() === "O";
  const canceledDate = (() => {
    const d = String(item.cdealDay ?? "").replace(/[^0-9]/g, "");
    if (d.length === 8) return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`;
    return null;
  })();

  const dedupKey = createHash("sha256")
    .update(
      [
        lawdCd, umd, name, jibun,
        source.propertyType, tradeType,
        dealDate, price, monthlyRent, area, floor ?? "",
      ].join("|"),
    )
    .digest("hex");

  return {
    lawdCd, umd, name, jibun,
    buildYear: toInt(item.buildYear),
    propertyType: source.propertyType,
    tradeType,
    dealDate, price, monthlyRent, area, floor,
    canceled, canceledDate,
    slug: slugify(name, jibun),
    dedupKey,
  };
}

// ---------- SQL 생성 ----------
/**
 * 문장 "배열"을 반환한다 (문자열로 합쳤다가 개행으로 다시 쪼개지 않는다).
 * 값에 개행이 섞이면 "한 줄 = 한 문장" 전제가 깨져 청크 경계에서 문자열 리터럴이
 * 잘린다 — cleanText 로도 막지만, 애초에 경계를 데이터에 의존하지 않게 한다.
 */
function rowsToSql(rows, lawdCd, ym, source) {
  const lines = [];
  for (const r of rows) {
    lines.push(
      `INSERT OR IGNORE INTO complexes (lawd_cd, umd_nm, name, jibun, build_year, property_type, slug) ` +
        `VALUES (${sqlStr(r.lawdCd)}, ${sqlStr(r.umd)}, ${sqlStr(r.name)}, ${sqlStr(r.jibun)}, ${sqlNum(r.buildYear)}, ${sqlStr(r.propertyType)}, ${sqlStr(r.slug)});`,
    );
    lines.push(
      `INSERT OR IGNORE INTO trades (complex_id, trade_type, deal_date, price, monthly_rent, area, floor, is_canceled, canceled_date, dedup_key) ` +
        `SELECT id, ${sqlStr(r.tradeType)}, ${sqlStr(r.dealDate)}, ${sqlNum(r.price)}, ${sqlNum(r.monthlyRent)}, ${sqlNum(r.area)}, ${sqlNum(r.floor)}, ${r.canceled ? 1 : 0}, ${r.canceledDate ? sqlStr(r.canceledDate) : "NULL"}, ${sqlStr(r.dedupKey)} ` +
        `FROM complexes WHERE lawd_cd = ${sqlStr(r.lawdCd)} AND umd_nm = ${sqlStr(r.umd)} AND name = ${sqlStr(r.name)} AND jibun = ${sqlStr(r.jibun)} AND property_type = ${sqlStr(r.propertyType)};`,
    );
    // dedup_key 는 해제여부를 제외 → 나중에 해제된 거래는 INSERT OR IGNORE 로 갱신 안 됨.
    // 해제건(소수)만 별도 UPDATE 로 반영. (전 행 upsert 는 큰 구에서 D1 CPU 초과라 지양)
    if (r.canceled) {
      lines.push(
        `UPDATE trades SET is_canceled = 1, canceled_date = ${r.canceledDate ? sqlStr(r.canceledDate) : "NULL"} WHERE dedup_key = ${sqlStr(r.dedupKey)};`,
      );
    }
  }
  // ingest_log: trade_type 은 소스 단위 마커(SALE / RENT)로 기록해 커버리지 추적.
  const logTradeType = source.kind === "rent" ? "RENT" : "SALE";
  lines.push(
    `INSERT INTO ingest_log (lawd_cd, property_type, trade_type, deal_ymd, row_count) ` +
      `VALUES (${sqlStr(lawdCd)}, ${sqlStr(source.propertyType)}, ${sqlStr(logTradeType)}, ${sqlStr(ym)}, ${sqlNum(rows.length)});`,
  );
  return lines;
}

// 비정규화 캐시 갱신 (complexes.trade_count/last_deal_date/last_sale_price).
// 지역 목록 쿼리가 JOIN/집계 없이 인덱스 조회하도록.

const DENORM_SET =
  `trade_count = (SELECT COUNT(*) FROM trades WHERE complex_id = complexes.id AND is_canceled = 0), ` +
  `last_deal_date = (SELECT MAX(deal_date) FROM trades WHERE complex_id = complexes.id AND is_canceled = 0), ` +
  `last_sale_price = (SELECT price FROM trades WHERE complex_id = complexes.id AND is_canceled = 0 AND trade_type = 'SALE' ORDER BY deal_date DESC, id DESC LIMIT 1)`;

/** 지역 전체 재계산 (--backfill: 수동 D1 조작 후 캐시 복구용) */
function denormSql(lawdCd) {
  return `UPDATE complexes SET ${DENORM_SET} WHERE lawd_cd = ${sqlStr(lawdCd)};`;
}

// complexes 의 자연키(UNIQUE lawd_cd,umd_nm,name,jibun,property_type)를 한 문자열로.
// 구분자는 데이터에 나올 수 없는 US(0x1F).
const KEY_SEP = "\u001f";
const complexKey = (r) =>
  [r.umd, r.name, r.jibun, r.propertyType].join(KEY_SEP);

const DENORM_KEY_CHUNK = 200; // UPDATE 한 문장이 건드리는 단지 수 (D1 CPU 한도 여유)

/**
 * 이번 배치가 실제로 건드린 단지만 갱신한다.
 *
 * 지역 전체를 갱신하면 새 거래가 한 건도 없어도 그 지역 단지 전부를 다시 쓴다
 * (전국 4만 행). D1 무료 쓰기 한도가 10만 행/일이라, 매일 수집만 돌려도 한도의
 * 40%가 캐시 갱신으로 날아가고 과거 백필을 같은 날 돌릴 여유가 없어진다.
 */
function denormSqlForKeys(lawdCd, keys) {
  const stmts = [];
  for (let i = 0; i < keys.length; i += DENORM_KEY_CHUNK) {
    const inList = keys
      .slice(i, i + DENORM_KEY_CHUNK)
      .map((k) => sqlStr(k))
      .join(", ");
    stmts.push(
      `UPDATE complexes SET ${DENORM_SET} ` +
        `WHERE lawd_cd = ${sqlStr(lawdCd)} ` +
        `AND (umd_nm || char(31) || name || char(31) || COALESCE(jibun, '') || char(31) || property_type) IN (${inList});`,
    );
  }
  return stmts;
}

/**
 * 홈 화면 롤업 갱신 (웹앱의 region_stats / site_stats).
 *
 * 홈이 실시간 집계였을 때 1뷰당 D1 68만 행을 읽었다 — 무료 한도 500만 행/일이라
 * 홈 7뷰면 그날 D1 이 잠긴다. 배치가 하루 한 번 계산해 두고 웹앱은 123행만 읽는다.
 *
 * 90일 거래수는 여기서 정확히 센다(trades 스캔은 배치라 괜찮다).
 * 전국을 한 문장으로 돌리면 지금(거래 18만)은 통과하지만, 백필로 1년치가 쌓이면
 * 스캔이 몇 배가 돼 D1 CPU 한도에 걸린다(5206e26·9b6061a 와 같은 사고) → 지역을 쪼갠다.
 */
const ROLLUP_REGION_CHUNK = 10;

function rollupStats(regions) {
  const codes = regions.map((r) => r.lawd_cd);

  for (let i = 0; i < codes.length; i += ROLLUP_REGION_CHUNK) {
    const inList = codes
      .slice(i, i + ROLLUP_REGION_CHUNK)
      .map((c) => sqlStr(c))
      .join(", ");
    d1File(
      `INSERT OR REPLACE INTO region_stats
         (lawd_cd, complex_count, trade_count_90d, latest_deal_date, updated_at)
       SELECT c.lawd_cd,
              COUNT(DISTINCT c.id),
              COUNT(CASE WHEN t.is_canceled = 0
                          AND t.deal_date >= date('now','-90 day') THEN 1 END),
              MAX(CASE WHEN t.is_canceled = 0 THEN t.deal_date END),
              datetime('now')
       FROM complexes c
       LEFT JOIN trades t ON t.complex_id = c.id
       WHERE c.lawd_cd IN (${inList})
       GROUP BY c.lawd_cd;`,
    );
  }

  // 단지가 사라진 지역이 옛 숫자로 남지 않도록 (지금은 삭제 경로가 없지만 방어)
  d1File(
    `DELETE FROM region_stats
     WHERE lawd_cd NOT IN (SELECT DISTINCT lawd_cd FROM complexes);`,
  );

  // site_stats 는 region_stats 를 참조(region_count) → 반드시 뒤에
  d1File(
    `INSERT OR REPLACE INTO site_stats
       (id, total_trades, total_complexes, region_count, latest_deal_date, updated_at)
     SELECT 1,
       (SELECT COUNT(*) FROM trades WHERE is_canceled = 0),
       (SELECT COUNT(*) FROM complexes),
       (SELECT COUNT(*) FROM region_stats WHERE complex_count > 0),
       (SELECT MAX(deal_date) FROM trades WHERE is_canceled = 0),
       datetime('now');`,
  );
}

// ---------- 메인 ----------
async function main() {
  const months = ymArg ? [ymArg] : [ymOffset(0), ymOffset(-1)];
  let regions = d1Query("SELECT lawd_cd, sigungu FROM regions ORDER BY lawd_cd");
  if (regions.length === 0) {
    console.error("[ingest] regions 가 비어 있습니다. 웹앱 저장소의 seed.sql 을 먼저 적용하세요.");
    process.exit(1);
  }
  if (onlyCodes) {
    regions = regions.filter((r) => onlyCodes.has(r.lawd_cd));
    if (regions.length === 0) {
      console.error(`[ingest] --only 로 지정한 지역이 regions 에 없습니다: ${onlyArg}`);
      process.exit(1);
    }
  }

  // --backfill: API 없이 지역별 비정규화 캐시만 재계산 (수동 D1 조작 후 복구용)
  if (backfillOnly) {
    console.log(`[backfill] ${dbFlag} · 지역 ${regions.length}곳 비정규화 캐시 갱신`);
    for (const region of regions) {
      try {
        d1File(denormSql(region.lawd_cd));
        console.log(`  ✓ ${region.sigungu}`);
      } catch (err) {
        console.error(`  ✗ ${region.sigungu}: ${redact(err.message)}`);
      }
    }
    // 캐시를 고쳐놓고 홈 롤업을 안 고치면, 복구 후에도 홈은 옛 숫자를 계속 보여준다
    // (--only 로 일부만 돌렸어도 롤업은 전 지역 기준으로 다시 계산해야 정합이 맞는다)
    const allRegions = d1Query("SELECT lawd_cd FROM regions ORDER BY lawd_cd");
    rollupStats(allRegions);
    console.log("[backfill] 완료 · 홈 롤업 갱신 완료");
    return;
  }

  console.log(
    `[ingest] ${dbFlag} · 지역 ${regions.length}곳 · 월 [${months.join(", ")}] · ` +
      `소스 [${activeSources.map((s) => s.id).join(", ")}]`,
  );

  let grandTotal = 0;
  let failures = 0; // 실패가 하나라도 있으면 exit 1 → 스케줄러가 백필 큐를 넘기지 않는다
  for (const region of regions) {
    const lawdCd = region.lawd_cd;
    const stmts = [];
    let regionCount = 0;
    const touchedKeys = new Set(); // 이번 배치에 거래가 있던 단지 (비정규화 갱신 대상)

    for (const source of activeSources) {
      for (const ym of months) {
        try {
          const items = await fetchAll(source, lawdCd, ym);
          const rows = items
            .map((it) => mapItem(it, lawdCd, source))
            .filter((r) => r !== null);
          stmts.push(...rowsToSql(rows, lawdCd, ym, source));
          regionCount += rows.length;
          for (const r of rows) touchedKeys.add(complexKey(r));
        } catch (err) {
          failures += 1;
          console.error(`  ✗ ${region.sigungu} ${source.id} ${ym}: ${redact(err.message)}`);
        }
      }
    }

    if (stmts.length > 0) {
      // 한 지역 적재 실패(D1 CPU 한도 등)가 이후 지역을 막지 않도록 격리
      try {
        // 대량 INSERT + denorm 을 한 트랜잭션에 몰면 큰 구에서 D1 CPU 한도 초과.
        // → INSERT 를 청크로 쪼개 각각 별도 트랜잭션으로, denorm 도 분리.
        for (let i = 0; i < stmts.length; i += STMT_CHUNK) {
          d1File(stmts.slice(i, i + STMT_CHUNK).join("\n"));
        }
        // 이번 배치가 건드린 단지만 갱신 (지역 전체 갱신은 D1 쓰기 한도를 태운다)
        for (const stmt of denormSqlForKeys(lawdCd, [...touchedKeys])) {
          d1File(stmt);
        }
        grandTotal += regionCount;
        console.log(`  ✓ ${region.sigungu}: ${regionCount}건 (단지 ${touchedKeys.size}곳 갱신)`);
      } catch (err) {
        failures += 1;
        console.error(`  ✗ ${region.sigungu} 적재 실패: ${redact(err.message)}`);
      }
    }
  }

  // 홈 롤업 갱신 (실시간 집계면 홈 1뷰에 D1 68만 행을 읽는다).
  // --only 로 일부 지역만 돌렸어도 site_stats 는 전체 기준이라 전 지역으로 계산한다.
  try {
    const allRegions = onlyCodes
      ? d1Query("SELECT lawd_cd FROM regions ORDER BY lawd_cd")
      : regions;
    rollupStats(allRegions);
    console.log("[ingest] 홈 롤업(region_stats/site_stats) 갱신 완료");
  } catch (err) {
    failures += 1;
    console.error(`[ingest] 홈 롤업 갱신 실패: ${redact(err.message)}`);
  }

  console.log(`[ingest] 완료 · 총 ${grandTotal}건 처리 · 실패 ${failures}건`);

  // 실패를 삼키고 exit 0 을 내면, 스케줄러(daily.sh)가 "성공"으로 보고 백필 큐를
  // 다음 달로 넘겨버린다 → 그 달은 영영 빈 채로 남는다. 실패는 그대로 알린다.
  if (failures > 0) {
    console.error(
      `[ingest] ${failures}건 실패 — 종료코드 1 (같은 달을 다시 돌리면 됩니다. INSERT OR IGNORE 라 중복 적재 안 됨)`,
    );
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[ingest] 실패:", redact(err.message ?? err));
  process.exit(1);
});
