// AES-GCM шифрование секретов. Ключ берётся из секрета KEYS_ENCRYPTION_SECRET.
const enc = new TextEncoder();
const dec = new TextDecoder();

async function aesKey(): Promise<CryptoKey> {
  const secret = Deno.env.get("KEYS_ENCRYPTION_SECRET");
  if (!secret || secret.length < 16) {
    throw new Error("KEYS_ENCRYPTION_SECRET не задан (supabase secrets set KEYS_ENCRYPTION_SECRET=...)");
  }
  const hash = await crypto.subtle.digest("SHA-256", enc.encode(secret));
  return crypto.subtle.importKey("raw", hash, "AES-GCM", false, ["encrypt", "decrypt"]);
}

const b64 = (u: Uint8Array) => btoa(String.fromCharCode(...u));
const unb64 = (s: string) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));

export async function encrypt(plain: string): Promise<string> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ct = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv }, await aesKey(), enc.encode(plain)));
  const out = new Uint8Array(iv.length + ct.length);
  out.set(iv);
  out.set(ct, iv.length);
  return b64(out);
}

export async function decrypt(payload: string): Promise<string> {
  const raw = unb64(payload);
  const pt = await crypto.subtle.decrypt({ name: "AES-GCM", iv: raw.slice(0, 12) }, await aesKey(), raw.slice(12));
  return dec.decode(pt);
}

export async function hmacHex(secret: string, msg: string): Promise<string> {
  const key = await crypto.subtle.importKey("raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", key, enc.encode(msg)));
  return Array.from(sig, (b) => b.toString(16).padStart(2, "0")).join("");
}
