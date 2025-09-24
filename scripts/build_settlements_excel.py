#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Полная выгрузка населённых пунктов:
 - Краснодарский край
 - Республика Адыгея

Выход: Excel с колонками: Регион | Район | Населенный пункт

Источник: Wikipedia
  - https://ru.wikipedia.org/wiki/Населённые_пункты_Краснодарского_края
  - https://ru.wikipedia.org/wiki/Населённые_пункты_Адыгеи
"""

import argparse
import os
import re
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import pandas as pd

WIKI_URLS = [
    ("Краснодарский край", "https://ru.wikipedia.org/wiki/%D0%9D%D0%B0%D1%81%D0%B5%D0%BB%D1%91%D0%BD%D0%BD%D1%8B%D0%B5_%D0%BF%D1%83%D0%BD%D0%BA%D1%82%D1%8B_%D0%9A%D1%80%D0%B0%D1%81%D0%BD%D0%BE%D0%B4%D0%B0%D1%80%D1%81%D0%BA%D0%BE%D0%B3%D0%BE_%D0%BA%D1%80%D0%B0%D1%8F"),
    ("Республика Адыгея", "https://ru.wikipedia.org/wiki/%D0%9D%D0%B0%D1%81%D0%B5%D0%BB%D1%91%D0%BD%D0%BD%D1%8B%D0%B5_%D0%BF%D1%83%D0%BD%D0%BA%D1%82%D1%8B_%D0%90%D0%B4%D1%8B%D0%B3%D0%B5%D0%B8"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

def load_html(url: str, html_dir: str | None) -> str:
    if html_dir:
        parsed = urlparse(url)
        fname = re.sub(r'[^a-zA-Z0-9._-]+', '_', parsed.path) + ".html"
        path = os.path.join(html_dir, fname)
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text

def headline_for(node) -> str | None:
    """Берёт ближайший предыдущий H3/H2; если есть span.mw-headline — используем его текст."""
    h = node.find_previous(["h3", "h2"])
    if not h:
        return None
    span = h.find("span", class_="mw-headline")
    if span:
        return span.get_text(strip=True)
    return h.get_text(" ", strip=True)

def choose_name_column(df: pd.DataFrame) -> str | None:
    # Ищем колонку с названием НП
    for c in map(str, df.columns):
        lc = c.lower()
        if ("насел" in lc and "пункт" in lc) or lc in ("населённый пункт", "населенный пункт"):
            return c
        if lc in ("название", "наименование"):
            return c
    for c in map(str, df.columns):
        if "пункт" in c.lower() or "назв" in c.lower():
            return c
    # fallback: если вторая колонка похожа на имя (часто так)
    if df.shape[1] >= 2:
        return str(df.columns[1])
    return None

def harvest_region(region_name: str, url: str, html_dir: str | None) -> pd.DataFrame:
    html = load_html(url, html_dir)
    soup = BeautifulSoup(html, "lxml")

    rows = []

    # 1) Проходим все таблицы страницы; для каждой ищем колонку "населенный пункт"
    for table in soup.select("table"):
        try:
            df = pd.read_html(str(table), flavor="lxml")[0]
        except Exception:
            continue

        name_col = choose_name_column(df)
        if not name_col:
            continue  # это не "наша" таблица

        unit = headline_for(table)
        if not unit:
            continue

        # чистим и складываем строки
        for raw in df[name_col].astype(str).tolist():
            name = re.sub(r"\[[^\]]*\]", "", raw).strip()
            if not name or name == "—":
                continue
            rows.append({"Регион": region_name, "Район": unit, "Населенный пункт": name})

    # 2) На всякий — некоторые секции могут быть списками UL (редко)
    for h in soup.select("h2, h3"):
        unit = headline_for(h)  # по сути текст самого заголовка
        ul = h.find_next_sibling("ul")
        if not ul:
            continue
        items = [li.get_text(" ", strip=True) for li in ul.select(":scope > li")]
        for raw in items:
            name = re.sub(r"\[[^\]]*\]", "", raw).strip()
            if not name or len(name) > 100:
                continue
            rows.append({"Регион": region_name, "Район": unit, "Населенный пункт": name})

    out = pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/settlements.xlsx")
    ap.add_argument("--html-dir", default=None)
    args = ap.parse_args()

    frames = []
    for region, url in WIKI_URLS:
        df = harvest_region(region, url, args.html_dir)
        frames.append(df)

    if not frames or all(df.empty for df in frames):
        raise RuntimeError("Парсер не нашёл таблиц с населёнными пунктами. Проверьте структуру страниц Wikipedia.")

    result = pd.concat(frames, ignore_index=True)

    # Нормализация «района/ГО»
    result["Район"] = (
        result["Район"]
        .str.replace(r"^\s*город-?курорт\s+", "", regex=True)
        .str.replace(r"^\s*город\s+", "", regex=True)
        .str.replace(r"\s*\[.*?\]\s*", "", regex=True)
        .str.strip()
    )
    # Уберём хвосты типа "(ГО)" в названии единицы
    result["Район"] = result["Район"].str.replace(r"\s*\(.*?\)\s*$", "", regex=True).str.strip()

    # Имя — без скобок с примечаниями
    result["Населенный пункт"] = result["Населенный пункт"].str.replace(r"\s+\(.*?\)$", "", regex=True).str.strip()

    # Сортировка
    result.sort_values(by=["Регион", "Район", "Населенный пункт"], inplace=True, key=lambda s: s.str.lower())

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with pd.ExcelWriter(args.out, engine="openpyxl") as xw:
        result.to_excel(xw, sheet_name="Населенные пункты", index=False)

    print(f"Готово: {args.out} (строк: {len(result)})")

if __name__ == "__main__":
    main()
