import { hmacHex } from "./crypto.ts";

export const SPOT = "https://api.binance.com";
export const FUT = "https://fapi.binance.com";

// Лимиты веса в минуту (с запасом от реальных 6000 / 2400)
const WEIGHT_SOFT_LIMIT: Record<string, number> = { [SPOT]: 5000, [FUT]: 2000 };

export class BinanceError extends Error {
  constructor(public status: number, public code: number | undefined, message: string) {
    super(message);
  }
}

export class RateLimited extends Error {
  constructor(public waitMs: number) {
    super(`Лимит запросов Binance, ждём ${Math.ceil(waitMs / 1000)} c`);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export class Binance {
  private offset: Record<string, number> = {};
  private weight: Record<string, number> = {};

  constructor(private apiKey: string, private secret: string, private deadline = Date.now() + 50_000) {}

  setDeadline(d: number) { this.deadline = d; }
  timeLeft() { return this.deadline - Date.now(); }

  private async syncTime(base: string) {
    if (this.offset[base] !== undefined) return;
    const path = base === SPOT ? "/api/v3/time" : "/fapi/v1/time";
    const t0 = Date.now();
    const r = await fetch(base + path);
    const { serverTime } = await r.json();
    this.offset[base] = serverTime - Math.round((t0 + Date.now()) / 2);
  }

  /** Если вес почти выбран — подождать до следующей минуты, либо прервать вызов (продолжим в следующем). */
  private async throttle(base: string) {
    const used = this.weight[base] ?? 0;
    if (used < WEIGHT_SOFT_LIMIT[base]) return;
    const wait = 60_000 - (Date.now() % 60_000) + 1_000;
    if (wait > this.timeLeft() - 5_000) throw new RateLimited(wait);
    await sleep(wait);
    this.weight[base] = 0;
  }

  async publicGet<T = any>(base: string, path: string, params: Record<string, unknown> = {}): Promise<T> {
    return this.request<T>(base, path, params, false);
  }

  async signedGet<T = any>(base: string, path: string, params: Record<string, unknown> = {}): Promise<T> {
    return this.request<T>(base, path, params, true);
  }

  private async request<T>(base: string, path: string, params: Record<string, unknown>, signed: boolean): Promise<T> {
    await this.throttle(base);
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null) qs.set(k, String(v));
    if (signed) {
      await this.syncTime(base);
      qs.set("recvWindow", "20000");
      qs.set("timestamp", String(Date.now() + this.offset[base]));
      qs.set("signature", await hmacHex(this.secret, qs.toString()));
    }
    const res = await fetch(`${base}${path}?${qs}`, { headers: signed ? { "X-MBX-APIKEY": this.apiKey } : {} });
    const w = res.headers.get("x-mbx-used-weight-1m");
    if (w) this.weight[base] = Number(w);

    if (res.status === 429 || res.status === 418) {
      const ra = Number(res.headers.get("retry-after") ?? 60);
      throw new RateLimited(ra * 1000);
    }
    const text = await res.text();
    let body: any;
    try { body = JSON.parse(text); } catch { body = { msg: text.slice(0, 300) }; }
    if (!res.ok) {
      if (res.status === 451 || res.status === 403) {
        throw new BinanceError(res.status, body?.code,
          "Binance блокирует регион сервера edge-функции (HTTP " + res.status + "). Вызывай функции с заголовком x-region: eu-central-1.");
      }
      throw new BinanceError(res.status, body?.code, `Binance ${path}: ${body?.msg ?? res.statusText} (code ${body?.code ?? res.status})`);
    }
    return body as T;
  }
}
