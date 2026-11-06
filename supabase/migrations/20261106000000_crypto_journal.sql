-- ============================================================
--  Crypto Journal — схема БД
-- ============================================================

-- API-ключи Binance (secret хранится зашифрованным AES-GCM, расшифровывается только в edge-функциях)
create table if not exists public.exchange_keys (
  user_id     uuid primary key references auth.users on delete cascade,
  api_key     text not null,
  secret_enc  text not null,
  permissions jsonb,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);
alter table public.exchange_keys enable row level security;
drop policy if exists "keys: read own" on public.exchange_keys;
create policy "keys: read own"   on public.exchange_keys for select using (auth.uid() = user_id);
drop policy if exists "keys: delete own" on public.exchange_keys;
create policy "keys: delete own" on public.exchange_keys for delete using (auth.uid() = user_id);
-- secret_enc клиенту не отдаём: оставляем доступ только к безопасным колонкам
revoke all on public.exchange_keys from anon, authenticated;
grant select (user_id, api_key, permissions, created_at, updated_at) on public.exchange_keys to authenticated;
grant delete on public.exchange_keys to authenticated;

-- Сделки (fills). market: spot | usdm
create table if not exists public.trades (
  id               bigserial primary key,
  user_id          uuid not null references auth.users on delete cascade,
  market           text not null check (market in ('spot','usdm')),
  symbol           text not null,
  base_asset       text,
  quote_asset      text,
  trade_id         text not null,
  order_id         text,
  side             text not null check (side in ('BUY','SELL')),
  position_side    text,
  price            numeric not null,
  qty              numeric not null,
  quote_qty        numeric not null,
  commission       numeric not null default 0,
  commission_asset text,
  realized_pnl     numeric,
  is_maker         boolean,
  executed_at      timestamptz not null,
  source           text not null default 'api',
  created_at       timestamptz not null default now(),
  unique (user_id, market, symbol, trade_id)
);
create index if not exists trades_user_time on public.trades (user_id, executed_at);
alter table public.trades enable row level security;
drop policy if exists "trades: read own" on public.trades;
create policy "trades: read own"   on public.trades for select using (auth.uid() = user_id);
drop policy if exists "trades: insert csv" on public.trades;
create policy "trades: insert csv" on public.trades for insert with check (auth.uid() = user_id and source = 'csv');
drop policy if exists "trades: delete own" on public.trades;
create policy "trades: delete own" on public.trades for delete using (auth.uid() = user_id);

-- Доходы/расходы фьючерсов (funding, комиссии, реализованный PnL)
create table if not exists public.futures_income (
  user_id     uuid not null references auth.users on delete cascade,
  tran_id     text not null,
  income_type text not null,
  symbol      text,
  income      numeric not null,
  asset       text,
  time        timestamptz not null,
  primary key (user_id, tran_id, income_type)
);
create index if not exists futures_income_user_time on public.futures_income (user_id, time);
alter table public.futures_income enable row level security;
drop policy if exists "income: read own" on public.futures_income;
create policy "income: read own" on public.futures_income for select using (auth.uid() = user_id);

-- Курсоры синхронизации по символам (используются только edge-функцией)
create table if not exists public.sync_cursors (
  user_id       uuid not null references auth.users on delete cascade,
  market        text not null,
  symbol        text not null,
  last_trade_id bigint,
  last_time     bigint,
  updated_at    timestamptz not null default now(),
  primary key (user_id, market, symbol)
);
alter table public.sync_cursors enable row level security;

-- Состояние текущей синхронизации (очередь задач, прогресс)
create table if not exists public.sync_jobs (
  user_id     uuid primary key references auth.users on delete cascade,
  mode        text not null,
  status      text not null,
  queue       jsonb not null default '[]',
  pos         int  not null default 0,
  stats       jsonb not null default '{}',
  error       text,
  started_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  finished_at timestamptz
);
alter table public.sync_jobs enable row level security;
drop policy if exists "jobs: read own" on public.sync_jobs;
create policy "jobs: read own" on public.sync_jobs for select using (auth.uid() = user_id);

-- Журнал: заметки к ордерам
create table if not exists public.trade_notes (
  user_id        uuid not null references auth.users on delete cascade,
  order_key      text not null,
  setup          text,
  tags           text[] not null default '{}',
  emotion        text,
  rating         int check (rating between 1 and 5),
  mistakes       text[] not null default '{}',
  notes          text,
  screenshot_url text,
  updated_at     timestamptz not null default now(),
  primary key (user_id, order_key)
);
alter table public.trade_notes enable row level security;
drop policy if exists "notes: all own" on public.trade_notes;
create policy "notes: all own" on public.trade_notes for all using (auth.uid() = user_id) with check (auth.uid() = user_id);

-- Символы фьючерсов, по которым были комиссии/PnL (для поиска торгуемых пар)
create or replace function public.income_symbols(p_user uuid, p_since timestamptz)
returns table(symbol text) language sql stable security definer set search_path = public as $$
  select distinct symbol from public.futures_income
  where user_id = p_user and time >= p_since and symbol is not null and symbol <> ''
    and income_type in ('COMMISSION','REALIZED_PNL');
$$;
revoke execute on function public.income_symbols(uuid, timestamptz) from public, anon, authenticated;
