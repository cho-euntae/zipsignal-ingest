// 집시그널 실거래 수집 배치 (GitHub Actions / 로컬 공용)
//
// 흐름: D1에서 지역 목록 조회 → 국토부 API 호출(지역×년월) → XML 파싱
//       → dedup_key 생성 → complexes/trades upsert SQL 생성 → wrangler로 D1 적재
//
// 사용법:
//   node --env-file=.env ingest.mjs --local      # 로컬 D1
//   node ingest.mjs --remote                     # 원격 D1 (Actions)
//   node ingest.mjs --remote --ym=202606         # 특정 년월만
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
const ENDPOINT =
  "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev";
const NUM_OF_ROWS = 1000;
const PROPERTY_TYPE = "APT";
const TRADE_TYPE = "SALE";
const FETCH_TIMEOUT_MS = 30_000;

const args = process.argv.slice(2);
const isLocal = args.includes("--local");
const dbFlag = isLocal ? "--local" : "--remote";
const ymArg = args.find((a) => a.startsWith("--ym="))?.split("=")[1];
// --only=41192,41194 : 특정 시군구(lawd_cd)만 수집 (재수집/보정용). 생략 시 전체.
const onlyArg = args.find((a) => a.startsWith("--only="))?.split("=")[1];
const onlyCodes = onlyArg ? new Set(onlyArg.split(",").map((s) => s.trim())) : null;

const API_KEY = process.env.DATA_GO_KR_API_KEY;
if (!API_KEY) {
  console.error("[ingest] DATA_GO_KR_API_KEY 가 설정되지 않았습니다.");
  process.exit(1);
}

// --ym 은 YYYYMM 6자리 숫자만 허용 (형식 오류 조기 차단)
if (ymArg !== undefined && !/^\d{6}$/.test(ymArg)) {
  console.error(`[ingest] --ym 형식 오류: '${ymArg}' (YYYYMM 6자리 숫자여야 함)`);
  process.exit(1);
}

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
  const out = wrangler([
    "d1",
    "execute",
    DB_NAME,
    dbFlag,
    "--json",
    "--command",
    sql,
  ]);
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
async function fetchPage(lawdCd, ym, pageNo) {
  const params = new URLSearchParams({
    serviceKey: API_KEY,
    LAWD_CD: lawdCd,
    DEAL_YMD: ym,
    numOfRows: String(NUM_OF_ROWS),
    pageNo: String(pageNo),
  });
  const res = await fetch(`${ENDPOINT}?${params}`, {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} (lawd=${lawdCd}, ym=${ym})`);
  const parsed = parser.parse(await res.text());

  // 성공코드는 게이트웨이/API에 따라 "0"·"00"·"000" 로 다양 → 숫자 0이면 성공.
  // 단, 빈 문자열/공백은 Number("")===0 이라 성공으로 오인되므로 명시적으로 실패 처리.
  const rawCode = parsed?.response?.header?.resultCode;
  const code =
    rawCode === undefined || rawCode === null ? undefined : String(rawCode).trim();
  if (code !== undefined && (code === "" || Number(code) !== 0)) {
    throw new Error(
      `API ${rawCode}: ${parsed?.response?.header?.resultMsg ?? "unknown"}`,
    );
  }
  const body = parsed?.response?.body;
  const total = toInt(body?.totalCount) ?? 0;
  const raw = body?.items?.item;
  const items = Array.isArray(raw) ? raw : raw ? [raw] : [];
  return { items, total };
}

async function fetchTrades(lawdCd, ym) {
  const { items, total } = await fetchPage(lawdCd, ym, 1);
  const all = [...items];
  const pages = Math.ceil(total / NUM_OF_ROWS);
  for (let p = 2; p <= pages; p++) {
    const next = await fetchPage(lawdCd, ym, p);
    all.push(...next.items);
  }
  return all;
}

// ---------- 매핑 ----------
function mapItem(item, lawdCd) {
  const year = toInt(item.dealYear);
  const month = toInt(item.dealMonth);
  const day = toInt(item.dealDay);
  const price = toInt(item.dealAmount);
  const area = toFloat(item.excluUseAr);
  if (year === null || month === null || day === null) return null;
  if (price === null || area === null) return null;

  const dealDate = `${year}-${pad2(month)}-${pad2(day)}`;
  const umd = String(item.umdNm ?? "").trim();
  const name = String(item.aptNm ?? "").trim();
  const jibun = String(item.jibun ?? "").trim();
  const floor = toInt(item.floor);
  const canceled = String(item.cdealType ?? "").trim().toUpperCase() === "O";
  const canceledDate = (() => {
    const d = String(item.cdealDay ?? "").replace(/[^0-9]/g, "");
    if (d.length === 8) return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`;
    return null;
  })();

  const dedupKey = createHash("sha256")
    .update(
      [
        lawdCd,
        umd,
        name,
        jibun,
        PROPERTY_TYPE,
        TRADE_TYPE,
        dealDate,
        price,
        area,
        floor ?? "",
      ].join("|"),
    )
    .digest("hex");

  return {
    lawdCd,
    umd,
    name,
    jibun,
    buildYear: toInt(item.buildYear),
    dealDate,
    price,
    area,
    floor,
    canceled,
    canceledDate,
    slug: slugify(name, jibun),
    dedupKey,
  };
}

// ---------- SQL 생성 ----------
function rowsToSql(rows, lawdCd, ym) {
  const lines = [];
  for (const r of rows) {
    lines.push(
      `INSERT OR IGNORE INTO complexes (lawd_cd, umd_nm, name, jibun, build_year, property_type, slug) ` +
        `VALUES (${sqlStr(r.lawdCd)}, ${sqlStr(r.umd)}, ${sqlStr(r.name)}, ${sqlStr(r.jibun)}, ${sqlNum(r.buildYear)}, ${sqlStr(PROPERTY_TYPE)}, ${sqlStr(r.slug)});`,
    );
    lines.push(
      `INSERT OR IGNORE INTO trades (complex_id, trade_type, deal_date, price, monthly_rent, area, floor, is_canceled, canceled_date, dedup_key) ` +
        `SELECT id, ${sqlStr(TRADE_TYPE)}, ${sqlStr(r.dealDate)}, ${sqlNum(r.price)}, 0, ${sqlNum(r.area)}, ${sqlNum(r.floor)}, ${r.canceled ? 1 : 0}, ${r.canceledDate ? sqlStr(r.canceledDate) : "NULL"}, ${sqlStr(r.dedupKey)} ` +
        `FROM complexes WHERE lawd_cd = ${sqlStr(r.lawdCd)} AND umd_nm = ${sqlStr(r.umd)} AND name = ${sqlStr(r.name)} AND jibun = ${sqlStr(r.jibun)} AND property_type = ${sqlStr(PROPERTY_TYPE)};`,
    );
  }
  lines.push(
    `INSERT INTO ingest_log (lawd_cd, property_type, trade_type, deal_ymd, row_count) ` +
      `VALUES (${sqlStr(lawdCd)}, ${sqlStr(PROPERTY_TYPE)}, ${sqlStr(TRADE_TYPE)}, ${sqlStr(ym)}, ${rows.length});`,
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
    `[ingest] ${dbFlag} · 지역 ${regions.length}곳 · 월 [${months.join(", ")}]`,
  );

  let grandTotal = 0;
  for (const region of regions) {
    const lawdCd = region.lawd_cd;
    let regionSql = "";
    let regionCount = 0;

    for (const ym of months) {
      try {
        const items = await fetchTrades(lawdCd, ym);
        const rows = items
          .map((it) => mapItem(it, lawdCd))
          .filter((r) => r !== null);
        regionSql += rowsToSql(rows, lawdCd, ym) + "\n";
        regionCount += rows.length;
      } catch (err) {
        console.error(`  ✗ ${region.sigungu} ${ym}: ${redact(err.message)}`);
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
