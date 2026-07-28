#!/usr/bin/env python3
"""Інкрементальна докачка даних з публічного архіву data.binance.vision.

Працює ВИКЛЮЧНО через CDN-архів (без fapi REST) — тому запускається і з
GitHub Actions (американські IP, які fapi блокує), і локально. Тільки stdlib.

Що оновлює:
  datasets/perp_meta/<SYM>.csv   — 8h: premium, close, volume, taker_buy (щоденні файли, свіже)
  datasets/funding/<SYM>.csv     — funding-ставки (місячні архіви; лаг до 1 місяця — властивість архіву)
  datasets/daily/<SYM>.csv       — денні klines кошика TSMOM (щоденні файли)
  datasets/metrics/<SYM>.csv     — OI + long/short (агрегація 5m → 8h):
                                   вперед — до вчора, назад — прогресивний бекфіл
                                   по BACKFILL_DAYS днів за запуск (до BACKFILL_TARGET)

Ідемпотентний: повторний запуск не дублює рядків.
"""
import csv
import io
import json
import os
from decimal import Decimal as D
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

BASE = "https://data.binance.vision/data/futures/um"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "30"))
BACKFILL_TARGET = os.environ.get("BACKFILL_TARGET", "2024-01-01")
UTC = timezone.utc


def http_zip_csv(url):
    """Повертає список рядків CSV із zip за URL або None при 404."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "research-topup/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        name = z.namelist()[0]
        text = z.read(name).decode()
    rows = list(csv.reader(io.StringIO(text)))
    # Частина архівів має header-рядок, частина ні.
    if rows and rows[0] and not rows[0][0].strip().isdigit():
        rows = rows[1:]
    return rows


def fixed(x):
    """Число з архіву → фіксована крапка (архів інколи дає 5.7e-05)."""
    return format(D(str(x)), "f")


def iso(ms):
    """ms (іноді з +1ms квірком архіву) → ISO-час, округлений до хвилини."""
    sec = round(int(ms) / 1000 / 60) * 60
    return datetime.fromtimestamp(sec, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def last_ts(path):
    if not os.path.exists(path):
        return None
    last = None
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("timestamp"):
                last = line.split(",")[0]
    return datetime.strptime(last, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC) if last else None


def month_range(start, end):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m == 13:
            y, m = y + 1, 1


def day_range(start, end):
    d = start
    while d <= end:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)


def append_rows(path, rows):
    if not rows:
        return 0
    with open(path, "a") as f:
        for r in rows:
            f.write(r + "\n")
    return len(rows)


def symbols():
    d = os.path.join(ROOT, "datasets", "funding")
    return sorted(os.path.splitext(f)[0] for f in os.listdir(d) if f.endswith(".csv"))


# ── 1. perp_meta: 8h premium + klines ────────────────────────────────────────
def topup_perp_meta(sym, now):
    path = os.path.join(ROOT, "datasets", "perp_meta", f"{sym}.csv")
    if not os.path.exists(path):
        with open(path, "w") as f:
            f.write("timestamp,premium,close,volume,taker_buy_volume\n")
    last = last_ts(path)
    start_day = (last + timedelta(hours=8)).date() if last else datetime(2024, 1, 1, tzinfo=UTC).date()
    added, kl_cache = [], {}
    from concurrent.futures import ThreadPoolExecutor
    days = list(day_range(start_day, (now - timedelta(days=1)).date()))
    def fetch_pair(day):
        return (
            http_zip_csv(f"{BASE}/daily/premiumIndexKlines/{sym}/8h/{sym}-8h-{day}.zip"),
            http_zip_csv(f"{BASE}/daily/klines/{sym}/8h/{sym}-8h-{day}.zip"),
        )
    with ThreadPoolExecutor(max_workers=12) as ex:
        pairs = list(ex.map(fetch_pair, days))
    for prem, kl in pairs:
        if prem is None or kl is None:
            continue
        for r in kl:
            kl_cache[iso(r[0])] = (r[4], r[5], r[9])
        for r in prem:
            ts = iso(r[0])
            if last and datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC) <= last:
                continue
            k = kl_cache.get(ts)
            if k:
                added.append(f"{ts},{fixed(r[4])},{k[0]},{k[1]},{k[2]}")
    return append_rows(path, added)


# ── 2. funding: місячні архіви + mark-ціна з perp_meta ───────────────────────
def topup_funding(sym, now):
    path = os.path.join(ROOT, "datasets", "funding", f"{sym}.csv")
    last = last_ts(path)
    if last is None:
        return 0
    meta_close = {}
    meta_path = os.path.join(ROOT, "datasets", "perp_meta", f"{sym}.csv")
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            for row in csv.reader(f):
                if row and row[0] != "timestamp":
                    meta_close[row[0]] = row[2]
    added = []
    for month in month_range(last.date().replace(day=1), now.date()):
        rows = http_zip_csv(f"{BASE}/monthly/fundingRate/{sym}/{sym}-fundingRate-{month}.zip")
        if rows is None:
            continue  # неповний місяць ще не опублікований
        for r in rows:
            ts = iso(r[0])
            t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
            if t <= last:
                continue
            mark = meta_close.get(ts, "")
            if not mark:
                continue  # без ціни рядок не пишемо (движку потрібен mark)
            added.append(f"{ts},{sym},{fixed(r[2])},{mark},{mark}")
    return append_rows(path, added)


# ── 2b. funding: хвіст поточного місяця через REST ───────────────────────────
#
# Архів публікує fundingRate ТІЛЬКИ місячними файлами (перевірено: у
# data/futures/um/daily/ такої категорії немає взагалі), тому після кроку 2
# дані завжди відстають від 1 до 31 дня. Саме цей лаг зробив три рядки
# RESEARCH_LOG копіями одного прогону: вхід не мінявся, а таблиця виглядала
# як щотижневе підтвердження.
#
# REST закриває хвіст, але доступний не звідусіль (з US-раннерів GitHub
# fapi заблокований). Тому: перша ж відмова вимикає крок цілком і тихо —
# це ДОЗБІР, а не джерело правди. Архів лишається авторитетним, REST лише
# додає те, чого архів ще не опублікував.
FAPI = "https://fapi.binance.com/fapi/v1/fundingRate"
_rest_blocked = False


def http_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "research-topup/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def topup_funding_rest(sym, now):
    global _rest_blocked
    if _rest_blocked:
        return 0
    path = os.path.join(ROOT, "datasets", "funding", f"{sym}.csv")
    last = last_ts(path)
    if last is None:
        return 0

    start_ms = int(last.timestamp() * 1000) + 1
    try:
        payload = http_json(f"{FAPI}?symbol={sym}&startTime={start_ms}&limit=1000")
    except Exception as e:  # гео-блок, мережа, ліміт — усе однаково фатально
        _rest_blocked = True
        print(f"  REST-дозбір недоступний ({e}) — лишаємось на архіві")
        return 0

    if not isinstance(payload, list):
        _rest_blocked = True
        print(f"  REST повернув не список ({type(payload).__name__}) — крок вимкнено")
        return 0

    added = []
    for item in payload:
        try:
            ts = iso(item["fundingTime"])
            t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
            rate = fixed(item["fundingRate"])
            mark = fixed(item["markPrice"])
        except (KeyError, TypeError, ValueError):
            continue  # кривий запис пропускаємо, але файл не псуємо
        if t <= last or D(mark) <= 0:
            continue
        # mark у обидві колонки — так само, як це робить архівний шлях вище.
        added.append(f"{ts},{sym},{rate},{mark},{mark}")

    time.sleep(0.2)  # ліміт ваги fapi
    return append_rows(path, added)


# ── 3. daily: 1d klines кошика TSMOM ─────────────────────────────────────────
def topup_daily(sym, now):
    path = os.path.join(ROOT, "datasets", "daily", f"{sym}.csv")
    if not os.path.exists(path):
        return 0
    last = last_ts(path)
    added = []
    for day in day_range((last + timedelta(days=1)).date(), (now - timedelta(days=1)).date()):
        rows = http_zip_csv(f"{BASE}/daily/klines/{sym}/1d/{sym}-1d-{day}.zip")
        if rows is None:
            continue
        for r in rows:
            added.append(f"{iso(r[0])},{r[4]},{r[5]}")
    return append_rows(path, added)


# ── 4. metrics: OI + long/short, 5m → 8h, вперед + прогресивний бекфіл ───────
def read_metric_days(path):
    days = set()
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                if line and not line.startswith("timestamp"):
                    days.add(line[:10])
    return days


def fetch_metric_day(sym, day):
    rows = http_zip_csv(f"{BASE}/daily/metrics/{sym}/{sym}-metrics-{day}.zip")
    if rows is None:
        return []
    buckets = {}
    for r in rows:
        # create_time: "2024-01-15 00:05:00"
        t = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        bucket = t.replace(hour=(t.hour // 8) * 8, minute=0, second=0)
        buckets[bucket] = r  # останній запис у 8h-вікні = снепшот
    out = []
    for b in sorted(buckets):
        r = buckets[b]
        ts = b.strftime("%Y-%m-%dT%H:%M:%SZ")
        out.append(f"{ts},{r[2]},{r[3]},{r[4]},{r[5]},{r[6]},{r[7]}")
    return out


def topup_metrics(sym, now):
    path = os.path.join(ROOT, "datasets", "metrics", f"{sym}.csv")
    header = "timestamp,oi,oi_value,top_ls_accounts,top_ls_positions,global_ls_accounts,taker_ls_vol\n"
    if not os.path.exists(path):
        with open(path, "w") as f:
            f.write(header)
    have = read_metric_days(path)
    target = datetime.strptime(BACKFILL_TARGET, "%Y-%m-%d").replace(tzinfo=UTC).date()
    yesterday = (now - timedelta(days=1)).date()

    # Вперед: усі відсутні дні за останні 40 днів; назад: BACKFILL_DAYS днів.
    forward = [d for d in day_range(max(target, yesterday - timedelta(days=40)), yesterday) if d not in have]
    missing_hist = [d for d in day_range(target, yesterday) if d not in have and d not in forward]
    todo = forward + missing_hist[-BACKFILL_DAYS:]  # найсвіжіші з дірок історії

    from concurrent.futures import ThreadPoolExecutor
    new_rows = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        for rows in ex.map(lambda d: fetch_metric_day(sym, d), todo):
            new_rows.extend(rows)
    if not new_rows:
        return 0
    # Пересортувати весь файл (бекфіл пише «в минуле»).
    with open(path) as f:
        existing = [l.strip() for l in f if l.strip() and not l.startswith("timestamp")]
    merged = sorted(set(existing + new_rows))
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(header)
        for l in merged:
            f.write(l + "\n")
    os.replace(tmp, path)  # атомарно: обрив не лишає битого файла
    return len(new_rows)


def coverage(name):
    """(найстаріший «останній бар» по набору, вік у днях, к-сть файлів).

    Береться МІНІМУМ по символах, а не максимум: набір свіжий рівно настільки,
    наскільки свіжий найвідсталіший інструмент. Максимум приховав би, що
    13 із 45 символів застрягли на місяць раніше за решту.
    """
    d = os.path.join(ROOT, "datasets", name)
    if not os.path.isdir(d):
        return None, None, 0
    stamps = []
    for f in sorted(os.listdir(d)):
        if not f.endswith(".csv"):
            continue
        t = last_ts(os.path.join(d, f))
        if t:
            stamps.append(t)
    if not stamps:
        return None, None, 0
    oldest = min(stamps)
    age = (datetime.now(UTC) - oldest).days
    return oldest, age, len(stamps)


def report_coverage(now):
    """Друкує вік кожного набору. Три тижні поспіль RESEARCH_LOG показував
    однакові числа як щотижневі спостереження — саме тому, що ніде не було
    видно, що вхід не змінюється."""
    print("\nпокриття даних (за найвідсталішим символом):")
    for name in ("funding", "perp_meta", "metrics", "daily", "funding_htx"):
        oldest, age, n = coverage(name)
        if oldest is None:
            continue
        print(f"  {name:<12} {oldest:%Y-%m-%d} — {age:>3} дн. тому, {n} символів")


MAX_FUNDING_AGE_DAYS = int(os.environ.get("MAX_FUNDING_AGE_DAYS", "40"))


def check_freshness():
    """Валить прогін, якщо funding застояний або побитий.

    Поріг ловить не звичайний лаг (архів місячний — до 31 дня це норма),
    а поломку: джерело перестало публікуватись, скрипт відвалився, або
    локальні REST-дозбори припинились.
    """
    report_coverage(datetime.now(UTC))
    problems = []

    oldest, age, n = coverage("funding")
    if oldest is None:
        problems.append("набір funding порожній або відсутній")
    elif age > MAX_FUNDING_AGE_DAYS:
        problems.append(
            f"funding застояний: найвідсталіший символ на {oldest:%Y-%m-%d}, "
            f"{age} дн. тому (поріг {MAX_FUNDING_AGE_DAYS}). Архів місячний, "
            f"тож або джерело зупинилось, або потрібен локальний прогін "
            f"`python3 scripts/topup_data.py` (REST з US-раннерів заблокований)."
        )

    # Структурні дефекти. Розділені на фатальні й на шум від біржі —
    # сигнал, який завжди червоний, перестають читати.
    warnings = []
    d = os.path.join(ROOT, "datasets", "funding")
    if os.path.isdir(d):
        for name in sorted(os.listdir(d)):
            if not name.endswith(".csv"):
                continue
            rows = []
            with open(os.path.join(d, name)) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("timestamp"):
                        rows.append(line.split(","))
            stamps = [r[0] for r in rows]
            if stamps != sorted(stamps):
                problems.append(f"{name}: таймстемпи не відсортовані")
            # Нульова марк-ціна ламає дохідності діленням на нуль. Правило
            # пайплайну («без ціни рядок не пишемо») діє і в архівному шляху,
            # і в REST-дозборі — тут воно лише перевіряється.
            bad_px = sum(1 for r in rows if len(r) < 4 or D(r[3]) <= 0)
            if bad_px:
                problems.append(f"{name}: {bad_px} рядків із недодатною марк-ціною")
            # А от дубльований час — НЕ дефект наших даних: біржа справді
            # віддає дві події в одну мить (перевірено на NVDAUSDT
            # 2026-06-04T00:00 — два розрахунки при зміні інтервалу фандингу).
            # Видаляти було б підробкою запису; попереджаємо, бо завантажувачі
            # ключують за часом і мовчки лишать один із них.
            dups = len(stamps) - len(set(stamps))
            if dups:
                warnings.append(f"{name}: {dups} дубльованих таймстемпів (так віддає біржа)")

    if warnings:
        print("\nпопередження (не валять прогін):")
        for w in warnings:
            print(f"  ! {w}")

    if problems:
        print("\nпроблеми з даними:", file=sys.stderr)
        for p in problems:
            print(f"  ✗ {p}", file=sys.stderr)
        return 1
    print("\nдані свіжі й цілісні")
    return 0


def main():
    if "--check-freshness" in sys.argv:
        return check_freshness()
    now = datetime.now(UTC)
    os.makedirs(os.path.join(ROOT, "datasets", "metrics"), exist_ok=True)
    syms = symbols()
    daily_syms = sorted(
        os.path.splitext(f)[0]
        for f in os.listdir(os.path.join(ROOT, "datasets", "daily"))
        if f.endswith(".csv")
    )
    print(f"topup: {len(syms)} perp symbols, {len(daily_syms)} daily symbols")
    totals = {"perp_meta": 0, "funding": 0, "funding_rest": 0, "daily": 0, "metrics": 0}
    for sym in syms:
        totals["perp_meta"] += topup_perp_meta(sym, now)
        totals["funding"] += topup_funding(sym, now)
        # Порядок важливий: спершу архів (авторитетний), потім REST на хвіст.
        totals["funding_rest"] += topup_funding_rest(sym, now)
        totals["metrics"] += topup_metrics(sym, now)
        time.sleep(0.05)
    for sym in daily_syms:
        totals["daily"] += topup_daily(sym, now)
    print(f"appended rows: {totals}")
    report_coverage(now)
    return 0


if __name__ == "__main__":
    sys.exit(main())
