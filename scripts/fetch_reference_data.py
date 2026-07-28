#!/usr/bin/env python3
"""Качає денні ряди диверсифікованого кошика в datasets/<набір>/<SYMBOL>.csv.

Ці ряди НЕ торгуються — вони потрібні лише для валідації движка на TSMOM
(гейт M2.1). Крипта в кошику M2.1 слабка тим, що вісім монет ходять як одна
річ: один ведмежий рік б'є по всіх відрізках одразу. Акції/облігації/золото/
товари падають не синхронно, і саме на такому кошику виміряний еталонний
Sharpe 0.4–0.8.

Формат: timestamp,adj_close,close,volume. Adjusted стоїть ПЕРШИМ навмисно —
завантажувач бере adj_close пріоритетно, а бектест мусить рахувати total
return: для TLT за 20 років дивідендів raw close занижує ціну більш ніж
удвічі (81.52 проти 35.84 на старті). Raw лишається поруч, як вимагає M0.6.

ГОЛОВНЕ ТУТ — ВАЛІДАЦІЯ. Попередній набір datasets/etf/ залили руками, і в
усіх сімох файлах опинилась HTML-сторінка анти-бот перевірки замість цін.
Сміття пролежало в репо непоміченим із 2026-07-11, бо ці файли не читає
жоден тест — тиха поломка даних не має де спрацювати. Тому тут:
  * нічого не пишеться на диск, поки відповідь не пройшла ВСІ перевірки;
  * запис атомарний (tmp + rename) — обірваний прогін не лишає півфайла;
  * будь-яка невдача валить скрипт ненульовим кодом, а не пропускається.

Використання:
    python3 scripts/fetch_reference_data.py etf        # завантажити
    python3 scripts/fetch_reference_data.py --verify   # перевірити закомічене
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"

# Мінімум барів, щоб ряд узагалі вважався даними. Один торговий рік: нижче
# цього інструмент не дасть ані 252-денного momentum-сигналу, ані фолдів.
MIN_ROWS = int(os.environ.get("MIN_ROWS", "250"))

# Набори: тека → {символ у репо: тікер у джерела}. Розділені за класами
# активів, щоб кошик був справді диверсифікований, а не сім способів
# купити S&P.
BASKETS = {
    "etf": {
        "SPY": "SPY",   # акції США
        "EFA": "EFA",   # акції розвинених ринків поза США
        "TLT": "TLT",   # довгі держоблігації США
        "IEF": "IEF",   # середні держоблігації США
        "GLD": "GLD",   # золото
        "DBC": "DBC",   # широкий товарний кошик
        "USO": "USO",   # нафта
    },
}


class DataError(Exception):
    """Відповідь не є цінами. Ніколи не веде до запису на диск."""


def fetch(ticker):
    """Сира відповідь чарт-API за весь доступний період."""
    url = CHART.format(ticker=ticker) + "?period1=0&period2=9999999999&interval=1d"
    req = urllib.request.Request(url, headers={"User-Agent": "research-fetch/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            if r.status != 200:
                raise DataError(f"HTTP {r.status}")
            return r.read()
    except urllib.error.URLError as e:
        raise DataError(f"мережа: {e}") from e


def parse_and_validate(raw, symbol):
    """Байти → рядки CSV. Кидає DataError на будь-що, що не є цінами.

    Саме тут ловиться випадок, який нас підвів: HTML-сторінка замість даних
    не пройде навіть json.loads, і файл не буде створено.
    """
    head = raw[:200].lstrip().lower()
    if head.startswith(b"<"):
        raise DataError("відповідь — HTML/XML, а не дані (анти-бот сторінка?)")

    try:
        doc = json.loads(raw)
    except json.JSONDecodeError as e:
        raise DataError(f"не JSON: {e}") from e

    chart = doc.get("chart") or {}
    if chart.get("error"):
        raise DataError(f"джерело повернуло помилку: {chart['error']}")
    results = chart.get("result") or []
    if not results:
        raise DataError("порожній result — символу не існує?")

    res = results[0]
    stamps = res.get("timestamp") or []
    indicators = res.get("indicators") or {}
    quote = (indicators.get("quote") or [{}])[0]
    adj_block = (indicators.get("adjclose") or [{}])[0]

    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    adj = adj_block.get("adjclose") or []
    if not closes or not adj:
        raise DataError("немає close/adjclose — структура відповіді не та")
    if not (len(stamps) == len(closes) == len(adj)):
        raise DataError(
            f"довжини не збігаються: ts={len(stamps)} close={len(closes)} adj={len(adj)}"
        )

    rows = []
    for i, ts in enumerate(stamps):
        c, a = closes[i], adj[i]
        # Дірки в ряду (свята, халти) джерело віддає як null — пропускаємо
        # бар, а не підставляємо нуль: нульова ціна зламала б дохідності.
        if c is None or a is None or c <= 0 or a <= 0:
            continue
        v = volumes[i] if i < len(volumes) and volumes[i] is not None else 0
        day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        rows.append(f"{day},{a:.8f},{c:.8f},{v}")

    if len(rows) < MIN_ROWS:
        raise DataError(f"лише {len(rows)} придатних барів, треба ≥ {MIN_ROWS}")

    # Ряд мусить бути строго зростаючим за датою: інакше вирівнювання панелі
    # мовчки перемішає бари й зламає point-in-time (інваріант 3).
    days = [r.split(",", 1)[0] for r in rows]
    if days != sorted(days) or len(set(days)) != len(days):
        raise DataError("дати не строго зростають (дублікати або розсортовано)")

    print(f"  {symbol}: {len(rows)} барів, {days[0]} .. {days[-1]}")
    return rows


def write_atomic(path, rows):
    """Спершу tmp, потім rename — переривання не лишає півфайла."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("timestamp,adj_close,close,volume\n")
        f.write("\n".join(rows))
        f.write("\n")
    os.replace(tmp, path)


# Теки в канонічному форматі timestamp,<close>,volume — ті, що живлять
# гейт M2.1 і бенчмарки. Саме їх перевіряє --verify.
VERIFIED_DIRS = ["etf", "fx", "daily"]


def verify_file(path):
    """Перевіряє вже наявний CSV. Повертає опис проблеми або None."""
    with open(path, encoding="utf-8", errors="replace") as f:
        lines = f.read().splitlines()
    if not lines:
        return "порожній файл"
    header = [c.strip().lower() for c in lines[0].split(",")]
    if not any(c in ("timestamp", "date") for c in header):
        # Точка, де ловиться HTML: у нього немає жодного нашого стовпця.
        return f"немає стовпця timestamp/date (заголовок: {lines[0][:60]!r})"
    if not any(c in ("close", "adj_close") for c in header):
        return f"немає стовпця close/adj_close (заголовок: {lines[0][:60]!r})"

    ts_i = next(i for i, c in enumerate(header) if c in ("timestamp", "date"))
    px_i = next(i for i, c in enumerate(header) if c == "adj_close") \
        if "adj_close" in header \
        else next(i for i, c in enumerate(header) if c == "close")

    days, bad_price = [], 0
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) <= max(ts_i, px_i):
            continue
        days.append(parts[ts_i].strip())
        try:
            if float(parts[px_i]) <= 0:
                bad_price += 1
        except ValueError:
            bad_price += 1

    if len(days) < MIN_ROWS:
        return f"лише {len(days)} рядків, треба ≥ {MIN_ROWS}"
    if bad_price:
        return f"{bad_price} рядків із недодатною/нечисловою ціною"
    if days != sorted(days):
        return "дати не відсортовані"
    if len(set(days)) != len(days):
        return f"{len(days) - len(set(days))} дублікатів дати"
    return None


def verify_all():
    """Страж проти тихої поломки даних: рівно те, чого бракувало, коли в
    datasets/etf/ сім місяців пролежала HTML-сторінка замість цін."""
    problems = 0
    for name in VERIFIED_DIRS:
        d = os.path.join(ROOT, "datasets", name)
        if not os.path.isdir(d):
            continue
        files = sorted(f for f in os.listdir(d) if f.endswith(".csv"))
        print(f"datasets/{name}: {len(files)} файлів")
        for fname in files:
            problem = verify_file(os.path.join(d, fname))
            if problem:
                print(f"  ✗ {fname}: {problem}", file=sys.stderr)
                problems += 1
    if problems:
        print(f"\nбитих файлів: {problems}", file=sys.stderr)
        return 1
    print("\nусі набори валідні")
    return 0


def main(basket):
    if basket == "--verify":
        return verify_all()
    if basket not in BASKETS:
        print(f"невідомий набір {basket!r}; є: {', '.join(BASKETS)}", file=sys.stderr)
        return 2

    out_dir = os.path.join(ROOT, "datasets", basket)
    os.makedirs(out_dir, exist_ok=True)
    print(f"набір {basket}: {len(BASKETS[basket])} інструментів → {out_dir}")

    failures = []
    for symbol, ticker in BASKETS[basket].items():
        try:
            rows = parse_and_validate(fetch(ticker), symbol)
        except DataError as e:
            # Наявний файл НЕ чіпаємо: старі валідні дані кращі за їх втрату.
            print(f"  {symbol}: ПОМИЛКА — {e}", file=sys.stderr)
            failures.append(symbol)
            continue
        write_atomic(os.path.join(out_dir, f"{symbol}.csv"), rows)
        time.sleep(1)  # не довбати джерело

    if failures:
        print(f"\nне вдалося: {', '.join(failures)}", file=sys.stderr)
        return 1
    print(f"\nготово: {len(BASKETS[basket])} інструментів")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "etf"))
