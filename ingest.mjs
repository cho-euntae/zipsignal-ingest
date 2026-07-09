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
  return execFileSync("npx", ["wrangler", ...execArgs], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    maxBuffer: 64 * 1024 * 1024,
  });
}
function d1Query(sql) {
  const out = wrangler(["d1", "execute", DB_NAME, dbFlag, "--json", "--command", sql]);
  const parsed = JSON.parse(out);
  const first = Array.isArray(parsed) ? parsed[0] : parsed;
  return first?.results ?? [];
}
function d1File(sql) {
  const dir = mkdtempSync(join(tmpdir(), "zipsignal-"));
  const file = join(dir, "batch.sql");
  try {
    writeFileSync(file, sql, "utf8");
    wrangler(["d1", "execute", DB_NAME, dbFlag, "--file", file]);
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
    throw new Error(`API ${rawCode}: ${parsed?.response?.header?.resultMsg ?? "unknown"}`);
  }
  const body = parsed?.response?.body;
  const total = toInt(body?.totalCount) ?? 0;
  const raw = body?.items?.item;
  const items = Array.isArray(raw) ? raw : raw ? [raw] : [];
  return { items, total };
}

async function fetchAll(source, lawdCd, ym) {
  const { items, total } = await fetchPage(source, lawdCd, ym, 1);
  const all = [...items];
  const pages = Math.ceil(total / NUM_OF_ROWS);
  for (let p = 2; p <= pages; p++) {
    const next = await fetchPage(source, lawdCd, ym, p);
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
  const umd = String(item.umdNm ?? "").trim();
  const name = String(item[source.nameField] ?? "").trim();
  const jibun = String(item.jibun ?? "").trim();
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
  }
  // ingest_log: trade_type 은 소스 단위 마커(SALE / RENT)로 기록해 커버리지 추적.
  const logTradeType = source.kind === "rent" ? "RENT" : "SALE";
  lines.push(
    `INSERT INTO ingest_log (lawd_cd, property_type, trade_type, deal_ymd, row_count) ` +
      `VALUES (${sqlStr(lawdCd)}, ${sqlStr(source.propertyType)}, ${sqlStr(logTradeType)}, ${sqlStr(ym)}, ${sqlNum(rows.length)});`,
  );
  return lines.join("\n");
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

  console.log(
    `[ingest] ${dbFlag} · 지역 ${regions.length}곳 · 월 [${months.join(", ")}] · ` +
      `소스 [${activeSources.map((s) => s.id).join(", ")}]`,
  );

  let grandTotal = 0;
  for (const region of regions) {
    const lawdCd = region.lawd_cd;
    let regionSql = "";
    let regionCount = 0;

    for (const source of activeSources) {
      for (const ym of months) {
        try {
          const items = await fetchAll(source, lawdCd, ym);
          const rows = items
            .map((it) => mapItem(it, lawdCd, source))
            .filter((r) => r !== null);
          regionSql += rowsToSql(rows, lawdCd, ym, source) + "\n";
          regionCount += rows.length;
        } catch (err) {
          console.error(`  ✗ ${region.sigungu} ${source.id} ${ym}: ${redact(err.message)}`);
        }
      }
    }

    if (regionSql.trim()) {
      d1File(regionSql);
      grandTotal += regionCount;
      console.log(`  ✓ ${region.sigungu}: ${regionCount}건`);
    }
  }

  console.log(`[ingest] 완료 · 총 ${grandTotal}건 처리`);
}

main().catch((err) => {
  console.error("[ingest] 실패:", redact(err.message ?? err));
  process.exit(1);
});
