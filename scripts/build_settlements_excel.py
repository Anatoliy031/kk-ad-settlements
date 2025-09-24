#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Собирает полный перечень населенных пунктов для:
 - Краснодарского края
 - Республики Адыгея
и формирует Excel: Регион | Район | Населенный пункт.

Требования:
  Python 3.9+
  pip install -r requirements.txt

Запуск локально:
  python scripts/build_settlements_excel.py --out data/settlements.xlsx
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

def normalize_unit_title(h_tag_text: str) -> str:
    t = h_tag_text.strip()
    t = re.sub(r'^[#\s]+', '', t)
    return t

def extract_tables_by_h3(soup: BeautifulSoup):
    """Возвращает список: (заголовок, [DataFrames], [ul_lists])."""
    out = []
    for h3 in soup.select("h3"):
        title = h3.get_text(" ", strip=True)
        dfs = []
        ul_lists = []
        node = h3
        while True:
            node = node.find_next_sibling()
            if node is None:
                break
            if node.name in ("h2", "h3"):
                break
            if node.name == "table":
                caption = ""
                cap_el = node.find("caption")
                if cap_el:
                    caption = cap_el.get_text(" ", strip=True)
                cond = "Список насел" in caption
                if not cond:
                    th = node.find("th")
                    if th and ("Насел" in th.get_text() or "Название" in th.get_text() or "Наименование" in th.get_text()):
                        cond = True
                if cond:
                    try:
                        df = pd.read_html(str(node), flavor="lxml")[0]
                        dfs.append(df)
                    except Exception:
                        pass
            if node.name == "ul":
                items = [li.get_text(" ", strip=True) for li in node.select(":scope > li")]
                items = [re.sub(r"\[[^\]]*\]", "", it).strip() for it in items if len(it.strip()) > 0]
                if items:
                    ul_lists.append(items)
        if dfs or ul_lists:
            out.append((title, dfs, ul_lists))
    return out

def settle_column_name(candidates):
    for c in candidates:
        c_norm = str(c).lower().strip()
        if "насел" in c_norm and "пункт" in c_norm:
            return c
        if c_norm in ("населённый пункт", "населенный пункт", "населённые пункты"):
            return c
        if c_norm in ("название", "наименование"):
            return c
    for c in candidates:
        if "пункт" in str(c).lower() or "назв" in str(c).lower():
            return c
    return None

def harvest_region(region_name: str, url: str, html_dir: str | None) -> pd.DataFrame:
    html = load_html(url, html_dir)
    soup = BeautifulSoup(html, "lxml")
    pairs = extract_tables_by_h3(soup)

    rows = []
    for h3_title, tables, ul_lists in pairs:
        unit = normalize_unit_title(h3_title)
        for df in tables:
            col = settle_column_name(list(df.columns.astype(str)))
            if col is None:
                if df.shape[1] >= 2:
                    col = df.columns[1]
                else:
                    continue
            for val in df[col].astype(str).tolist():
                name = val.strip()
                if not name or name == "—":
                    continue
                name = re.sub(r"\[[^\]]*\]", "", name).strip()
                rows.append({"Регион": region_name, "Район": unit, "Населенный пункт": name})
        for items in ul_lists:
            for name in items:
                if not name or name == "—":
                    continue
                if len(name) > 100:
                    continue
                rows.append({"Регион": region_name, "Район": unit, "Населенный пункт": name})

    out = pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/settlements.xlsx", help="Путь к итоговому Excel")
    ap.add_argument("--html-dir", default=None, help="Каталог с локально сохраненными HTML (офлайн режим)")
    args = ap.parse_args()

    frames = []
    for region, url in WIKI_URLS:
        df = harvest_region(region, url, args.html_dir)
        frames.append(df)

    result = pd.concat(frames, ignore_index=True)
    result["Район"] = result["Район"].str.replace(r"^\s*город-?курорт\s+", "", regex=True)
    result["Район"] = result["Район"].str.replace(r"^\s*город\s+", "", regex=True)
    result["Населенный пункт"] = result["Населенный пункт"].str.replace(r"\s+\(.*?\)$", "", regex=True)

    result.sort_values(by=["Регион", "Район", "Населенный пункт"], inplace=True, key=lambda s: s.str.lower())

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with pd.ExcelWriter(args.out, engine="openpyxl") as xw:
        result.to_excel(xw, sheet_name="Населенные пункты", index=False)

    print(f"Готово: {args.out} (строк: {len(result)})")

if __name__ == "__main__":
    main()
