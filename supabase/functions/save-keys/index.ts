// POST { apiKey, apiSecret }  — проверяет ключ на Binance и сохраняет (secret шифруется)
// POST { delete: true }       — удаляет ключ, курсоры и состояние синхронизации
// POST { resetSync: true }    — сбрасывает курсоры синхронизации (после удаления сделок)
import { corsHeaders, json } from "../_shared/cors.ts";
import { adminClient, getUser } from "../_shared/auth.ts";
import { encrypt } from "../_shared/crypto.ts";
import { Binance, BinanceError, SPOT } from "../_shared/binance.ts";

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  const admin = adminClient();
  const user = await getUser(req, admin);
  if (!user) return json({ error: "Не авторизован" }, 401);

  try {
    const body = await req.json().catch(() => ({}));

    if (body.resetSync) {
      // после удаления сделок — сбрасываем курсоры, чтобы следующая синхронизация загрузила всё заново
      await admin.from("sync_jobs").delete().eq("user_id", user.id);
      await admin.from("sync_cursors").delete().eq("user_id", user.id);
      return json({ ok: true, reset: true });
    }

    if (body.delete) {
      await admin.from("exchange_keys").delete().eq("user_id", user.id);
      await admin.from("sync_jobs").delete().eq("user_id", user.id);
      await admin.from("sync_cursors").delete().eq("user_id", user.id);
      return json({ ok: true, deleted: true });
    }

    const apiKey = String(body.apiKey ?? "").trim();
    const apiSecret = String(body.apiSecret ?? "").trim();
    if (!/^[A-Za-z0-9]{32,128}$/.test(apiKey) || !/^[A-Za-z0-9]{32,128}$/.test(apiSecret)) {
      return json({ error: "Неверный формат API Key / Secret" }, 400);
    }

    const bn = new Binance(apiKey, apiSecret);
    // Проверяем, что ключ рабочий
    const account = await bn.signedGet(SPOT, "/api/v3/account", { omitZeroBalances: true });

    // Проверяем права ключа — с правом вывода средств не принимаем
    let restrictions: any = null;
    try {
      restrictions = await bn.signedGet(SPOT, "/sapi/v1/account/apiRestrictions");
    } catch (_) { /* не критично */ }
    if (restrictions?.enableWithdrawals) {
      return json({
        error: "У ключа включён вывод средств (Enable Withdrawals). Создай ключ только с правом чтения (Enable Reading) — так безопасно.",
      }, 400);
    }

    const permissions = {
      canTrade: account.canTrade,
      reading: restrictions?.enableReading ?? true,
      spotTrading: restrictions?.enableSpotAndMarginTrading ?? null,
      futures: restrictions?.enableFutures ?? null,
      ipRestrict: restrictions?.ipRestrict ?? null,
      accountType: account.accountType,
    };

    const { error } = await admin.from("exchange_keys").upsert({
      user_id: user.id,
      api_key: apiKey,
      secret_enc: await encrypt(apiSecret),
      permissions,
      updated_at: new Date().toISOString(),
    });
    if (error) throw error;

    const warnings: string[] = [];
    if (restrictions?.enableSpotAndMarginTrading || restrictions?.enableFutures) {
      warnings.push("У ключа есть право торговли — для дневника достаточно только чтения.");
    }
    if (restrictions?.ipRestrict) {
      warnings.push("Ключ ограничен по IP — запросы из облака Supabase могут не проходить.");
    }
    return json({ ok: true, permissions, warnings });
  } catch (e) {
    const status = e instanceof BinanceError ? 400 : 500;
    return json({ error: e instanceof Error ? e.message : String(e) }, status);
  }
});
