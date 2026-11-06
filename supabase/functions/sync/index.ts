// Синхронизация сделок Binance → БД.
//
// POST { mode?: "auto" | "quick" | "full", restart?: boolean }
//
// Работает порциями: каждый вызов обрабатывает очередь задач ~45 секунд и сохраняет прогресс
// в sync_jobs. Клиент вызывает функцию повторно, пока не придёт status = "done".
//
//  * Spot:    /api/v3/myTrades (fromId-пагинация, вся история). У Binance нет общего эндпоинта
//             сделок, поэтому в режиме "full" проверяется КАЖДАЯ пара биржи, в "quick" —
//             уже торговавшиеся пары + пары по монетам на балансе.
//  * USDⓈ-M:  /fapi/v1/income → находим торговавшиеся символы, затем /fapi/v1/userTrades
//             окнами по 7 дней. API Binance отдаёт фьючерсные сделки только за ~6 месяцев —
//             более старые загружаются через импорт CSV/XLSX в интерфейсе.
import { corsHeaders, json } from "../_shared/cors.ts";
import { adminClient, getUser } from "../_shared/auth.ts";
import { decrypt } from "../_shared/crypto.ts";
import { Binance, BinanceError, FUT, RateLimited, SPOT } from "../_shared/binance.ts";
import type { SupabaseClient } from "npm:@supabase/supabase-js@2.45.4";

const CALL_BUDGET_MS = 45_000;
const DAY = 86_400_000;
const FUT_WINDOW = 7 * DAY;
const FUT_HISTORY = 180 * DAY;
const FULLSCAN_MARK = "*fullscan*"; // отметка в sync_cursors: полная проверка всех пар уже была

type Task =
  | { t: "spot"; s: string; b: string; q: string }
  | { t: "fdisc" }
  | { t: "fsym" } // разворачивается в задачи fut после fdisc
  | { t: "fut"; s: string };

interface Stats {
  inserted: number;
  spotSymbols: number;
  futSymbols: number;
  income: number;
  warnings: string[];
  [k: string]: unknown;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  const started = Date.now();
  const admin = adminClient();
  const user = await getUser(req, admin);
  if (!user) return json({ error: "Не авторизован" }, 401);
  const uid = user.id;

  const body = await req.json().catch(() => ({}));

  const { data: keyRow } = await admin.from("exchange_keys").select("api_key, secret_enc").eq("user_id", uid).maybeSingle();
  if (!keyRow) return json({ error: "Сначала подключи API-ключ Binance" }, 400);
  const bn = new Binance(keyRow.api_key, await decrypt(keyRow.secret_enc), started + CALL_BUDGET_MS);

  let { data: job } = await admin.from("sync_jobs").select("*").eq("user_id", uid).maybeSingle();

  try {
    // Незавершённую (или упавшую) загрузку продолжаем с того же места, если не просили начать заново
    const resumable = job && (job.status === "running" || job.status === "error") && job.pos < job.queue.length;
    if (!resumable || body.restart) {
      job = await createJob(admin, bn, uid, body.mode ?? "auto");
    }

    const queue: Task[] = job.queue;
    const stats: Stats = job.stats;
    let pos: number = job.pos;
    let waitMs = 0;
    let lastSave = Date.now();

    const save = async (status = "running", extra: Record<string, unknown> = {}) => {
      await admin.from("sync_jobs").update({
        queue, pos, stats, status, updated_at: new Date().toISOString(), ...extra,
      }).eq("user_id", uid);
      lastSave = Date.now();
    };

    try {
      while (pos < queue.length && bn.timeLeft() > 4_000) {
        const task = queue[pos];
        const finished = await runTask(admin, bn, uid, task, queue, stats, job.mode);
        if (!finished) break; // упёрлись во время — продолжим с этой же задачи
        pos++;
        if (Date.now() - lastSave > 5_000) await save();
      }
    } catch (e) {
      if (e instanceof RateLimited) waitMs = e.waitMs;
      else throw e;
    }

    const done = pos >= queue.length;
    if (done) {
      if (job.mode === "full") await setCursor(admin, uid, "spot", FULLSCAN_MARK, { last_time: Date.now() });
      await save("done", { finished_at: new Date().toISOString(), error: null });
    } else {
      await save();
    }

    return json({
      status: done ? "done" : "running",
      mode: job.mode,
      pos,
      total: queue.length,
      current: queue[pos] ? describe(queue[pos]) : null,
      stats,
      waitMs,
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (job) await admin.from("sync_jobs").update({ status: "error", error: msg, updated_at: new Date().toISOString() }).eq("user_id", uid);
    return json({ error: msg }, e instanceof BinanceError ? 400 : 500);
  }
});

function describe(t: Task): string {
  if (t.t === "spot") return `Spot ${t.s}`;
  if (t.t === "fut") return `Futures ${t.s}`;
  if (t.t === "fdisc") return "Futures: поиск торгуемых контрактов";
  return "Futures: подготовка";
}

// ---------------------------------------------------------------------------
async function createJob(admin: SupabaseClient, bn: Binance, uid: string, requested: string) {
  const { data: cursors } = await admin.from("sync_cursors").select("market, symbol").eq("user_id", uid);
  const spotTraded = new Set((cursors ?? []).filter((c) => c.market === "spot" && c.symbol !== FULLSCAN_MARK).map((c) => c.symbol));
  const hadFullScan = (cursors ?? []).some((c) => c.market === "spot" && c.symbol === FULLSCAN_MARK);
  // Первый запуск — всегда полная проверка всех пар, дальше — быстрая
  const mode = requested === "auto" ? (hadFullScan ? "quick" : "full") : requested;

  const info = await bn.publicGet(SPOT, "/api/v3/exchangeInfo", { permissions: undefined });
  const symbols: { symbol: string; baseAsset: string; quoteAsset: string; status: string }[] = info.symbols;

  let spotList = symbols;
  if (mode === "quick") {
    const acc = await bn.signedGet(SPOT, "/api/v3/account", { omitZeroBalances: true });
    const held = new Set<string>(acc.balances.map((b: any) => b.asset));
    spotList = symbols.filter((s) => spotTraded.has(s.symbol) || (held.has(s.baseAsset) && s.status === "TRADING"));
  }
  // Сначала популярные котировки — пользователь быстрее увидит основные сделки
  const prio = (q: string) => ["USDT", "USDC", "FDUSD", "BTC", "ETH", "BNB"].indexOf(q) + 1 || 99;
  spotList.sort((a, b) => prio(a.quoteAsset) - prio(b.quoteAsset) || a.symbol.localeCompare(b.symbol));

  const queue: Task[] = [
    { t: "fdisc" },
    { t: "fsym" },
    ...spotList.map((s) => ({ t: "spot" as const, s: s.symbol, b: s.baseAsset, q: s.quoteAsset })),
  ];
  const stats: Stats = { inserted: 0, spotSymbols: spotList.length, futSymbols: 0, income: 0, warnings: [] };

  const row = {
    user_id: uid, mode, status: "running", queue, pos: 0, stats, error: null,
    started_at: new Date().toISOString(), updated_at: new Date().toISOString(), finished_at: null,
  };
  const { error } = await admin.from("sync_jobs").upsert(row);
  if (error) throw error;
  return row;
}

// ---------------------------------------------------------------------------
async function getCursor(admin: SupabaseClient, uid: string, market: string, symbol: string) {
  const { data } = await admin.from("sync_cursors").select("last_trade_id, last_time")
    .eq("user_id", uid).eq("market", market).eq("symbol", symbol).maybeSingle();
  return data as { last_trade_id: number | null; last_time: number | null } | null;
}

async function setCursor(admin: SupabaseClient, uid: string, market: string, symbol: string, v: { last_trade_id?: number | null; last_time?: number | null }) {
  const { error } = await admin.from("sync_cursors").upsert({
    user_id: uid, market, symbol, ...v, updated_at: new Date().toISOString(),
  });
  if (error) throw error;
}

async function insertTrades(admin: SupabaseClient, rows: Record<string, unknown>[]): Promise<number> {
  if (!rows.length) return 0;
  const { data, error } = await admin.from("trades")
    .upsert(rows, { onConflict: "user_id,market,symbol,trade_id", ignoreDuplicates: true })
    .select("id");
  if (error) throw error;
  return data?.length ?? 0;
}

/** Возвращает true, если задача завершена; false — если не хватило времени (продолжим позже). */
async function runTask(admin: SupabaseClient, bn: Binance, uid: string, task: Task, queue: Task[], stats: Stats, mode: string): Promise<boolean> {
  switch (task.t) {
    case "spot": return await syncSpot(admin, bn, uid, task, stats);
    case "fdisc": return await discoverFutures(admin, bn, uid, stats);
    case "fsym": {
      const disc = await getCursor(admin, uid, "usdm", "*income*");
      if (!disc) return true; // фьючерсы недоступны
      const since = mode === "full" ? Date.now() - FUT_HISTORY : (Number((stats as any).futSince) || Date.now() - FUT_HISTORY);
      const { data, error } = await admin.rpc("income_symbols", { p_user: uid, p_since: new Date(since).toISOString() });
      if (error) throw error;
      const { data: fc } = await admin.from("sync_cursors").select("symbol").eq("user_id", uid).eq("market", "usdm").neq("symbol", "*income*");
      const set = new Set<string>([...(data ?? []).map((r: any) => r.symbol)]);
      if (mode === "full") for (const c of fc ?? []) set.add(c.symbol);
      const fut = [...set].sort().map((s) => ({ t: "fut" as const, s }));
      const idx = queue.indexOf(task);
      queue.splice(idx + 1, 0, ...fut);
      stats.futSymbols = fut.length;
      return true;
    }
    case "fut": return await syncFutures(admin, bn, uid, task.s, stats);
  }
}

// --- Spot ------------------------------------------------------------------
async function syncSpot(admin: SupabaseClient, bn: Binance, uid: string, task: { s: string; b: string; q: string }, stats: Stats) {
  const cur = await getCursor(admin, uid, "spot", task.s);
  let fromId = cur?.last_trade_id != null ? Number(cur.last_trade_id) + 1 : 0;

  while (true) {
    if (bn.timeLeft() < 4_000) return false;
    let list: any[];
    try {
      list = await bn.signedGet(SPOT, "/api/v3/myTrades", { symbol: task.s, fromId, limit: 1000 });
    } catch (e) {
      // -1121 Invalid symbol (пара делистнута между запросами) — просто пропускаем
      if (e instanceof BinanceError && e.code === -1121) return true;
      throw e;
    }
    if (!list.length) return true;

    stats.inserted += await insertTrades(admin, list.map((t) => ({
      user_id: uid,
      market: "spot",
      symbol: t.symbol,
      base_asset: task.b,
      quote_asset: task.q,
      trade_id: String(t.id),
      order_id: String(t.orderId),
      side: t.isBuyer ? "BUY" : "SELL",
      price: t.price,
      qty: t.qty,
      quote_qty: t.quoteQty,
      commission: t.commission,
      commission_asset: t.commissionAsset,
      is_maker: t.isMaker,
      executed_at: new Date(t.time).toISOString(),
      source: "api",
    })));

    const last = list[list.length - 1];
    await setCursor(admin, uid, "spot", task.s, { last_trade_id: last.id, last_time: last.time });
    if (list.length < 1000) return true;
    fromId = last.id + 1;
  }
}

// --- Futures: поиск символов через income history -------------------------
async function discoverFutures(admin: SupabaseClient, bn: Binance, uid: string, stats: Stats) {
  const now = Date.now();
  const cur = await getCursor(admin, uid, "usdm", "*income*");
  let start = Math.max(cur?.last_time != null ? Number(cur.last_time) + 1 : 0, now - FUT_HISTORY);
  if ((stats as any).futSince == null) (stats as any).futSince = start;

  while (start < now) {
    if (bn.timeLeft() < 4_000) return false;
    const end = Math.min(start + FUT_WINDOW - 1, now);
    let pageStart = start;
    // Пагинация внутри окна
    while (true) {
      let rows: any[];
      try {
        rows = await bn.signedGet(FUT, "/fapi/v1/income", { startTime: pageStart, endTime: end, limit: 1000 });
      } catch (e) {
        if (e instanceof BinanceError && !cur) {
          // Фьючерсный аккаунт не открыт / нет прав на фьючерсы — пропускаем фьючерсы целиком
          stats.warnings.push("Фьючерсы пропущены: " + e.message);
          return true;
        }
        throw e;
      }
      if (rows.length) {
        const mapped = rows.map((r) => ({
          user_id: uid,
          tran_id: String(r.tranId),
          income_type: r.incomeType,
          symbol: r.symbol || null,
          income: r.income,
          asset: r.asset,
          time: new Date(r.time).toISOString(),
        }));
        const { error } = await admin.from("futures_income").upsert(mapped, { onConflict: "user_id,tran_id,income_type", ignoreDuplicates: true });
        if (error) throw error;
        stats.income += rows.length;
      }
      if (rows.length < 1000) break;
      const lastT = rows[rows.length - 1].time;
      pageStart = lastT > pageStart ? lastT : pageStart + 1; // дубликаты отсеет upsert
      if (bn.timeLeft() < 4_000) {
        await setCursor(admin, uid, "usdm", "*income*", { last_time: pageStart - 1 });
        return false;
      }
    }
    await setCursor(admin, uid, "usdm", "*income*", { last_time: end });
    start = end + 1;
  }
  if (!cur) await setCursor(admin, uid, "usdm", "*income*", { last_time: now });
  return true;
}

// --- Futures: сделки по символу --------------------------------------------
async function syncFutures(admin: SupabaseClient, bn: Binance, uid: string, symbol: string, stats: Stats) {
  const now = Date.now() - 5_000;
  const cur = await getCursor(admin, uid, "usdm", symbol);
  let start = Math.max(cur?.last_time != null ? Number(cur.last_time) + 1 : 0, now - FUT_HISTORY + DAY);

  while (start < now) {
    if (bn.timeLeft() < 4_000) return false;
    const end = Math.min(start + FUT_WINDOW - 1, now);
    let pageStart = start;
    while (true) {
      let list: any[];
      try {
        list = await bn.signedGet(FUT, "/fapi/v1/userTrades", { symbol, startTime: pageStart, endTime: end, limit: 1000 });
      } catch (e) {
        if (e instanceof BinanceError && e.code === -1121) return true; // символ больше не существует
        throw e;
      }
      if (list.length) {
        stats.inserted += await insertTrades(admin, list.map((t) => ({
          user_id: uid,
          market: "usdm",
          symbol: t.symbol,
          base_asset: null,
          quote_asset: t.marginAsset ?? t.commissionAsset ?? null,
          trade_id: String(t.id),
          order_id: String(t.orderId),
          side: t.side,
          position_side: t.positionSide,
          price: t.price,
          qty: t.qty,
          quote_qty: t.quoteQty,
          commission: t.commission,
          commission_asset: t.commissionAsset,
          realized_pnl: t.realizedPnl,
          is_maker: t.maker,
          executed_at: new Date(t.time).toISOString(),
          source: "api",
        })));
      }
      if (list.length < 1000) break;
      const lastT = list[list.length - 1].time;
      pageStart = lastT > pageStart ? lastT : pageStart + 1;
      if (bn.timeLeft() < 4_000) {
        await setCursor(admin, uid, "usdm", symbol, { last_time: pageStart - 1 });
        return false;
      }
    }
    await setCursor(admin, uid, "usdm", symbol, { last_time: end });
    start = end + 1;
  }
  return true;
}
