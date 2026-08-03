#!/usr/bin/env python3
"""Будує datasets/spot_pairs.csv — маніфест того, які перпи взагалі можна
захеджувати спотом на Binance.

НАВІЩО. Carry-стратегія за визначенням тримає лонг спота проти шорта перпа.
Якщо спот-ринку не існує, другої ноги немає і угода неможлива — не «дорожча»,
не «ризикованіша», а неможлива. Юніверс datasets/funding цього не розрізняв,
і наслідок виявився не крайовим випадком, а системним: у кошику тіньового
журналу за 2026-08-02 спот-пару мали 0 із 10 символів В ОБОХ кошиках
(baseline_est і rf).

Причина економічна, і вона неприємна. Топ за фандингом — це KORUUSDT (193%
річних), SKHYNIXUSDT (161%), SOXLUSDT (104%), MUUSDT, EWYUSDT, SNDKUSDT,
NVDAUSDT, MSTRUSDT, XAUUSDT: токенізовані акції, ETF і товари. Фандинг там
величезний САМЕ ТОМУ, що його нікому арбітражити — спот-ноги немає, канал
закритий, ставка нікуди не збігається. А ранжування за величиною фандингу
систематично відбирає рівно ці контракти. Високий фандинг тут — не сигнал
можливості, а підпис неарбітражованого перпа.

ДЖЕРЕЛА. Основне — exchangeInfo (точне: дає status=TRADING, а не лише
факт існування історії). Запасне — проба архіву spot/monthly/klines на
data.binance.vision. Обидва перевірені на юніверсі з 45 перпів і дали
ТОЧНО той самий набір із 19 символів, тож запасне джерело не гірше, просто
грубіше (не бачить призупинених пар).

api.binance.com, як і fapi, може бути заблокований з US-раннерів GitHub —
тому маніфест КОМІТИТЬСЯ, а прогін бектесту читає файл і в мережу не
ходить узагалі. Це вимога детермінізму (інваріант 4) і провенансу
(інваріант 5): юніверс мусить бути частиною входу, а не залежати від того,
що біржа відповіла в мить прогону. У CI стоїть лише --verify.

Формат: symbol,has_spot,source,checked_at
  has_spot ∈ {1,0}; source ∈ {exchangeInfo,archive}; checked_at — дата UTC.

Використання:
    python3 scripts/fetch_spot_pairs.py            # оновити маніфест
    python3 scripts/fetch_spot_pairs.py --verify   # перевірити закомічене
"""
import csv
import json
import os
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

UTC = timezone.utc
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FUNDING_DIR = os.path.join(ROOT, "datasets", "funding")
MANIFEST = os.path.join(ROOT, "datasets", "spot_pairs.csv")
HEADER = "symbol,has_spot,source,checked_at"

EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT"
ARCHIVE = "https://data.binance.vision/data/spot/monthly/klines/{s}/8h/{s}-8h-{m}.zip"
UA = {"User-Agent": "db-con-research/1.0"}


def universe():
    """Символи, для яких у нас є funding-історія — саме їх і ранжують."""
    if not os.path.isdir(FUNDING_DIR):
        sys.exit(f"нема {FUNDING_DIR}")
    return sorted(
        f[:-4].upper() for f in os.listdir(FUNDING_DIR) if f.endswith(".csv")
    )


def from_exchange_info():
    """Точне джерело. None (а не виняток) — щоб перемкнутись на архів."""
    try:
        req = urllib.request.Request(EXCHANGE_INFO, headers=UA)
        with urllib.request.urlopen(req, timeout=90) as r:
            raw = r.read()
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"  exchangeInfo недоступний ({e}) — переходжу на архівну пробу")
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        # Обірвана або підмінена відповідь — саме те, на чому сім місяців
        # горів datasets/etf. Мовчки не ковтаємо.
        print(f"  exchangeInfo віддав не-JSON ({e}) — переходжу на архівну пробу")
        return None
    symbols = data.get("symbols")
    if not isinstance(symbols, list) or len(symbols) < 100:
        print(f"  exchangeInfo віддав підозрілий список ({type(symbols)}) — на архів")
        return None
    return {s["symbol"] for s in symbols if s.get("status") == "TRADING"}


def archive_has_spot(symbol):
    """Запасна проба: чи існує місячний спот-архів за минулий повний місяць."""
    first = datetime.now(UTC).replace(day=1)
    month = (first - timedelta(days=1)).strftime("%Y-%m")
    req = urllib.request.Request(
        ARCHIVE.format(s=symbol, m=month), headers=UA, method="HEAD"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status == 200
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise
    except (urllib.error.URLError, TimeoutError, OSError):
        raise


def write_atomic(rows):
    tmp = MANIFEST + ".tmp"
    with open(tmp, "w", newline="") as f:
        f.write(HEADER + "\n")
        for r in rows:
            f.write(",".join(r) + "\n")
    os.replace(tmp, MANIFEST)


def verify():
    """Офлайн-перевірка закоміченого маніфесту. Це те, що стоїть у CI."""
    if not os.path.exists(MANIFEST):
        sys.exit(f"нема маніфесту {MANIFEST} — запусти без --verify")
    with open(MANIFEST) as f:
        header = f.readline().strip()
        if header != HEADER:
            sys.exit(f"шапка {header!r}, очікувалась {HEADER!r}")
        rows = list(csv.reader(f))
    if not rows:
        sys.exit("маніфест порожній")

    seen, hedgeable = set(), 0
    for i, row in enumerate(rows, start=2):
        if len(row) != 4:
            sys.exit(f"рядок {i}: {len(row)} полів замість 4")
        sym, has, source, checked = row
        if sym in seen:
            sys.exit(f"рядок {i}: дубль символа {sym}")
        seen.add(sym)
        if has not in ("0", "1"):
            sys.exit(f"рядок {i}: has_spot={has!r}, очікувалось 0 або 1")
        if source not in ("exchangeInfo", "archive"):
            sys.exit(f"рядок {i}: невідоме джерело {source!r}")
        try:
            datetime.strptime(checked, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"рядок {i}: погана дата {checked!r}")
        hedgeable += has == "1"

    missing = set(universe()) - seen
    if missing:
        sys.exit(
            f"у funding є символи без запису в маніфесті: {sorted(missing)}\n"
            "запусти scripts/fetch_spot_pairs.py — інакше вони мовчки випадуть з юніверсу"
        )
    if hedgeable == 0:
        sys.exit("жоден символ не хеджований — маніфест зіпсований")

    print(f"OK: {len(seen)} символів, хеджованих {hedgeable}")


def main():
    syms = universe()
    print(f"юніверс funding: {len(syms)} символів")
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    spot = from_exchange_info()
    if spot is not None:
        print(f"exchangeInfo: {len(spot)} спот-символів TRADING")
        rows = [(s, "1" if s in spot else "0", "exchangeInfo", today) for s in syms]
    else:
        with ThreadPoolExecutor(max_workers=12) as ex:
            flags = list(ex.map(archive_has_spot, syms))
        rows = [(s, "1" if ok else "0", "archive", today) for s, ok in zip(syms, flags)]

    hedgeable = [r[0] for r in rows if r[1] == "1"]
    write_atomic(rows)
    print(f"\nзаписано {MANIFEST}")
    print(f"хеджованих: {len(hedgeable)}/{len(rows)}")
    print("  " + " ".join(hedgeable))
    print("без спот-пари: " + " ".join(r[0] for r in rows if r[1] == "0"))


if __name__ == "__main__":
    if "--verify" in sys.argv:
        verify()
    else:
        main()
