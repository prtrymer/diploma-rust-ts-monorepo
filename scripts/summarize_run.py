#!/usr/bin/env python3
"""Дописує підсумковий рядок тижневого прогону в RESEARCH_LOG.md.

Витягує ключові числа з текстових виводів research-сьюти. Свідомо
терпимий до відсутніх файлів/патернів: журнал не має валити воркфлоу.
"""
import os
import re
import sys
from datetime import datetime, timezone


def grab(path, pattern, group=1, default="—"):
    if not os.path.exists(path):
        return default
    text = open(path, encoding="utf-8").read()
    m = re.search(pattern, text)
    return m.group(group).strip() if m else default


def main(out_dir, log_path):
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    fml = os.path.join(out_dir, "funding_ml.txt")
    xsc = os.path.join(out_dir, "xs_carry.txt")
    tsm = os.path.join(out_dir, "tsmom.txt")

    # funding_ml: кошики базлайн/RF і стеля (ОСТАННЄ входження = економічна таблиця).
    base_all = re.findall(r"базлайн mean21\s+([\-\d.]+)", open(fml).read()) if os.path.exists(fml) else []
    base_pct = base_all[-1] if base_all else "—"
    rf_pct = grab(fml, r"RF \+базис/потік \(12 фіч\)\s+([\-\d.]+)\n", 1)
    # У таблиці якості теж є цей рядок — беремо останнє входження (економічний тест).
    m_all = re.findall(r"RF \+базис/потік \(12 фіч\)\s+([\-\d.]+)", open(fml).read()) if os.path.exists(fml) else []
    if m_all:
        rf_pct = m_all[-1]
    perfect_pct = grab(fml, r"ідеальне передбачення\s+([\-\d.]+)")

    # xs_carry: OOS-рядок maker-сценарію.
    xs_text = open(xsc, encoding="utf-8").read() if os.path.exists(xsc) else ""
    xs_oos = "—"
    maker_block = xs_text.split("maker 0.018%")[-1] if "maker 0.018%" in xs_text else ""
    m = re.search(r"OOS: net=([\-\d.]+) .*?sharpe=([\-\d.]+)", maker_block)
    if m:
        xs_oos = f"net {float(m.group(1)):.0f} / Sharpe {float(m.group(2)):.2f}"

    # tsmom: медіанний OOS Sharpe.
    tsmom_med = grab(tsm, r"Median OOS Sharpe: ([\-\d.]+)")

    header = (
        "# Дослідницький журнал (автогенерований щотижня)\n\n"
        "| Дата | carry-кошик базлайн ≈%/рік | RF-ранжування ≈%/рік | стеля ≈%/рік "
        "| xs_carry OOS (maker) | TSMOM медіанний OOS Sharpe |\n"
        "|---|---|---|---|---|---|\n"
    )
    row = f"| {date} | {base_pct} | {rf_pct} | {perfect_pct} | {xs_oos} | {tsmom_med} |\n"

    if os.path.exists(log_path):
        content = open(log_path, encoding="utf-8").read()
        if date in content:
            print(f"{date} вже в журналі — пропускаю.")
            return 0
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(row)
    else:
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(header + row)
    print(f"RESEARCH_LOG.md += {row.strip()}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
