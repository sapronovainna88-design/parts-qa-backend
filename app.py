# app.py — FastAPI backend for Railway (robust version)
import os, io, uuid, re
import requests
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Form, Body, Header



from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
from difflib import get_close_matches, SequenceMatcher
from tabulate import tabulate
import openpyxl
from openpyxl.styles import PatternFill

# ----------------------------- Config -----------------------------
APP_PORT = int(os.getenv("PORT", "8000"))
UNIFICATION_PATH = os.getenv("UNIFICATION_PATH", "unifikatsiya.xlsx")
TMP_DIR = os.getenv("TMP_DIR", "/tmp")
API_KEY = os.getenv("X_API_KEY")  # optional simple auth

ALL_BRAND_ALIASES = {"всі", "все", "all", "any", "*", "всі доступні моделі"}

app = FastAPI(title="Parts QA Backend", version="1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------- Helpers: columns & norms -----------------
def _norm_text(s: str) -> str:
    x = str(s or "").strip().lower()
    x = x.replace("ё", "е").replace("’", "'")
    x = re.sub(r"\s+", " ", x)
    return x

# эквиваленты названий колонок (укр/рус/англ, популярные варианты)
_COL_CANDIDATES = {
    "cat": [
        "вид техніки", "вид техники", "категорія", "категория",
        "тип техніки", "тип техники", "вид"
    ],
    "brand": [
        "бренд", "виробник", "производитель",
        "виробник/бренд постачальника", "бренд постачальника", "brand"
    ],
    "name": [
        "найменування", "наименование", "назва", "название", "name"
    ],
    "pn": [
        "каталожний номер", "каталожный номер", "кат. номер",
        "артикул", "part number", "pn", "код"
    ],
    "analogs": [
        "допустимі аналоги", "допустимые аналоги", "аналоги",
        "equivalents", "альтернативи", "alternative", "analog"
    ],
}

def _pick_col(df: pd.DataFrame, key: str) -> str:
    want = [_norm_text(x) for x in _COL_CANDIDATES[key]]
    colmap = {orig: _norm_text(orig) for orig in df.columns}
    # точные совпадения
    for w in want:
        for orig, normed in colmap.items():
            if normed == w:
                return orig
    # частичные совпадения (содержит)
    for w in want:
        for orig, normed in colmap.items():
            if w in normed:
                return orig
    raise ValueError(f"Колонку для «{key}» не знайдено. Є колонки: {list(df.columns)}")

def load_unification() -> pd.DataFrame:
    # читаем первый лист; первый ряд — заголовки
    df = pd.read_excel(UNIFICATION_PATH, header=0)

    # выбираем реальные колонки по синонимам
    cat_col     = _pick_col(df, "cat")
    brand_col   = _pick_col(df, "brand")
    name_col    = _pick_col(df, "name")
    pn_col      = _pick_col(df, "pn")
    analogs_col = _pick_col(df, "analogs")

    # приводим к каноническим именам
    df = df.rename(columns={
        cat_col: "Вид техніки",
        brand_col: "Бренд",
        name_col: "Найменування",
        pn_col: "каталожний номер",
        analogs_col: "Допустимі аналоги",
    })

    # нормализованные поля для поиска
    df["cat_norm"] = df["Вид техніки"].astype(str).map(_norm_text)
    df["brand_norm"] = df["Бренд"].astype(str).map(_norm_text)

    return df

def _require_api_key(x_api_key: Optional[str]):
    if API_KEY and x_api_key != API_KEY:
        raise PermissionError("unauthorized")

# ===== META: списки для UI GPT =================================================

@app.get("/meta/types")
def meta_types(x_api_key: Optional[str] = Header(default=None, convert_underscores=False)):
    # опциональный ключ
    try:
        _require_api_key(x_api_key)
    except PermissionError:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        df = load_unification()
    except Exception:
        # чтобы GPT не падал — возвращаем пустой список, а не 500
        return {"items": []}

    # уникальные виды по cat_norm, с «человеческим» названием
    out = (
        df[["cat_norm", "Вид техніки"]]
        .dropna()
        .drop_duplicates(subset=["cat_norm"])
        .sort_values("Вид техніки")
        ["Вид техніки"]
        .tolist()
    )
    return {"items": out}


@app.get("/meta/brands")
def meta_brands(
    type: Optional[str] = None,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
):
    # опциональный ключ
    try:
        _require_api_key(x_api_key)
    except PermissionError:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        df = load_unification()
    except Exception:
        return {"items": []}

    if type:
        st = _norm_text(type)
        df = df[df["cat_norm"] == st]

    # берём «человеческие» лейблы брендов
    items = sorted(set(df["Бренд"].dropna().astype(str).str.strip().tolist()))

    # пункт «Всі доступні моделі» показываем всегда (удобно для выбора)
    if "Всі доступні моделі" not in items:
        items = ["Всі доступні моделі"] + items

    return {"items": items}


# ------------------------------- Routes ---------------------------
@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/preview")
async def preview(
    selected_type: str = Form(...),
    selected_brand: str = Form(...),
    main_file: UploadFile = File(...),
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False)
):
    # optional API key
    try:
        _require_api_key(x_api_key)
    except PermissionError:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    # сохраняем входной файл под токен
    token = str(uuid.uuid4())
    os.makedirs(TMP_DIR, exist_ok=True)
    tmp_main = os.path.join(TMP_DIR, f"{token}.xlsx")
    content = await main_file.read()
    with open(tmp_main, "wb") as f:
        f.write(content)

    # читаем уніфікацію
    try:
        df = load_unification()
    except Exception as e:
        return JSONResponse({"error": f"Помилка уніфікації: {e}"}, status_code=400)

    # --- 1) Фиксируем тип (как в твоём фрагменте: строго по cat_norm) ---
    st_raw = (selected_type or "").strip()
    sb_raw = (selected_brand or "").strip()
    st = _norm_text(st_raw)
    sb = _norm_text(sb_raw)

    df_cat = df[df["cat_norm"] == st]
    if df_cat.empty:
        # категория не найдена — подсказываем и выходим (здесь логично вернуть 400)
        choices = df["cat_norm"].dropna().unique().tolist()
        suggestions = get_close_matches(st, choices, n=3, cutoff=0.6)
        return JSONResponse(
            {"error": f"Категорію «{st_raw}» не знайдено", "suggestions": suggestions},
            status_code=400
        )
    normalized_type = df_cat["Вид техніки"].iloc[0]

    # --- 2) Бренд: подправляем к ближайшему и добавляем 'всі доступні моделі' в пул ---
    norm_all = _norm_text("всі доступні моделі")
    brands_pool = df_cat["brand_norm"].dropna().unique().tolist()
    pool_with_all = brands_pool + [norm_all]

    best = get_close_matches(sb, pool_with_all, n=1, cutoff=0.6)
    if best:
        sb = best[0]

    # --- 3) Берём строки БРЕНДА ИЛИ 'всі доступні моделі' ---
    df_brand = df_cat[df_cat["brand_norm"].isin([sb, norm_all])]

    # ⬇️ ВАЖНО: ЕСЛИ НИЧЕГО НЕ НАШЛИ — НЕ ОШИБКА!
    # Делаем "пустую" уніфікацію: вернём token и продолжим обработку без маппинга.
    if df_brand.empty:
        temp_df = pd.DataFrame(columns=["C (Найменування)", "D (Каталожний номер)", "E (Допустимі аналоги)"])
        brand_mode = "none_found_unification_skipped"
        normalized_brand = sb_raw or "—"
    else:
        temp_df = (
            df_brand[["Найменування", "каталожний номер", "Допустимі аналоги"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        temp_df.columns = ["C (Найменування)", "D (Каталожний номер)", "E (Допустимі аналоги)"]
        if sb == norm_all:
            brand_mode = "all_manual_or_corrected"
            normalized_brand = "Всі доступні моделі"
        elif sb in brands_pool:
            brand_mode = "exact_plus_all" if norm_all in brands_pool else "exact"
            normalized_brand = df_cat.loc[df_cat["brand_norm"] == sb, "Бренд"].iloc[0]
        else:
            brand_mode = "exact_or_all"
            normalized_brand = sb_raw

    # сохраняем «превью-таблицу» (пусть даже пустую) рядом с токеном
    tmp_temp = os.path.join(TMP_DIR, f"{token}_temp.parquet")
    temp_df.to_parquet(tmp_temp, index=False)

    # markdown-превью: если таблица пустая — показываем заметку
    if temp_df.empty:
        preview_markdown = "_Уніфікація для цієї категорії/бренду не знайдена — обробка буде без уніфікації._"
    else:
        preview_markdown = tabulate(temp_df, headers=temp_df.columns, tablefmt="github", showindex=False)

    return {
        "normalized_type": normalized_type,
        "normalized_brand": normalized_brand,
        "brand_mode": brand_mode,   # exact | exact_plus_all | all_manual_or_corrected | none_found_unification_skipped
        "preview_markdown": preview_markdown,
        "token": token
    }

@app.post("/preview_url")
async def preview_url(
    payload: dict = Body(...),
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False)
):
    """
    Альтернатива multipart: принимает JSON с полями
    selected_type, selected_brand, file_url (URI),
    скачивает файл и дальше делает то же, что /preview.
    """
    # optional API key
    try:
        _require_api_key(x_api_key)
    except PermissionError:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    selected_type = str(payload.get("selected_type", "")).strip()
    selected_brand = str(payload.get("selected_brand", "")).strip()
    file_url = str(payload.get("file_url", "")).strip()
    if not selected_type or not selected_brand or not file_url:
        return JSONResponse({"error": "selected_type, selected_brand и file_url обязательны"}, status_code=400)

    # 1) Скачиваем файл по URL и кладём в tmp под токен
    token = str(uuid.uuid4())
    os.makedirs(TMP_DIR, exist_ok=True)
    tmp_main = os.path.join(TMP_DIR, f"{token}.xlsx")
    try:
        r = requests.get(file_url, timeout=60)
        r.raise_for_status()
        with open(tmp_main, "wb") as f:
            f.write(r.content)
    except Exception as e:
        return JSONResponse({"error": f"Не удалось скачать файл по URL: {e}"}, status_code=400)

    # 2) Загружаем уніфікацію
    try:
        df = load_unification()
    except Exception as e:
        return JSONResponse({"error": f"Помилка уніфікації: {e}"}, status_code=400)

    # ==== ЛОГИКА ПРЕВЬЮ КАК В ТВОЕЙ ВЕРСИИ ====
    # строгое сопоставление по cat_norm
    st_raw = selected_type
    sb_raw = selected_brand
    st = _norm_text(st_raw)
    sb = _norm_text(sb_raw)

    df_cat = df[df["cat_norm"] == st]
    if df_cat.empty:
        choices = df["cat_norm"].dropna().unique().tolist()
        suggestions = get_close_matches(st, choices, n=3, cutoff=0.6)
        return JSONResponse(
            {"error": f"Категорію «{st_raw}» не знайдено", "suggestions": suggestions},
            status_code=400
        )
    normalized_type = df_cat["Вид техніки"].iloc[0]

    norm_all = _norm_text("всі доступні моделі")
    brands_pool = df_cat["brand_norm"].dropna().unique().tolist()
    pool_with_all = brands_pool + [norm_all]

    best = get_close_matches(sb, pool_with_all, n=1, cutoff=0.6)
    if best:
        sb = best[0]

    df_brand = df_cat[df_cat["brand_norm"].isin([sb, norm_all])]

    # пустая уніфікація — это НЕ ошибка
    if df_brand.empty:
        temp_df = pd.DataFrame(columns=["C (Найменування)", "D (Каталожний номер)", "E (Допустимі аналоги)"])
        brand_mode = "none_found_unification_skipped"
        normalized_brand = sb_raw or "—"
    else:
        temp_df = (
            df_brand[["Найменування", "каталожний номер", "Допустимі аналоги"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        temp_df.columns = ["C (Найменування)", "D (Каталожний номер)", "E (Допустимі аналоги)"]
        if sb == norm_all:
            brand_mode = "all_manual_or_corrected"
            normalized_brand = "Всі доступні моделі"
        elif sb in brands_pool:
            brand_mode = "exact_plus_all" if norm_all in brands_pool else "exact"
            normalized_brand = df_cat.loc[df_cat["brand_norm"] == sb, "Бренд"].iloc[0]
        else:
            brand_mode = "exact_or_all"
            normalized_brand = sb_raw

    tmp_temp = os.path.join(TMP_DIR, f"{token}_temp.parquet")
    temp_df.to_parquet(tmp_temp, index=False)

    if temp_df.empty:
        preview_markdown = "_Уніфікацію не знайдено — обробка піде без уніфікації._"
    else:
        preview_markdown = tabulate(temp_df, headers=temp_df.columns, tablefmt="github", showindex=False)

    return {
        "normalized_type": normalized_type,
        "normalized_brand": normalized_brand,
        "brand_mode": brand_mode,
        "preview_markdown": preview_markdown,
        "token": token
    }


@app.post("/process")
async def process(
    token: Optional[str] = Form(None),
    payload: Optional[dict] = Body(None),
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False)
):
    # optional API key
    try:
        _require_api_key(x_api_key)
    except PermissionError:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    # поддерживаем и form-data, и JSON
    if not token and isinstance(payload, dict):
        token = payload.get("token")
    if not token:
        return JSONResponse({"error": "token is required"}, status_code=400)

    tmp_main = os.path.join(TMP_DIR, f"{token}.xlsx")
    tmp_temp = os.path.join(TMP_DIR, f"{token}_temp.parquet")
    if not os.path.exists(tmp_main):
        return JSONResponse({"error": "invalid token"}, status_code=400)

    # читаем превью-таблицу (может быть пустой или отсутствовать)
    temp_df = pd.DataFrame()
    if os.path.exists(tmp_temp):
        try:
            temp_df = pd.read_parquet(tmp_temp)
        except Exception:
            temp_df = pd.DataFrame()

    # ← вот тот самый «ранний флаг»
    unification_enabled = not temp_df.empty

    wb = openpyxl.load_workbook(tmp_main)
    ws = wb.active

    red    = PatternFill(start_color="FFFF0000", end_color="FFFF0000", fill_type="solid")
    yellow = PatternFill(start_color="FFFFFF00", end_color="FFFFFF00", fill_type="solid")
    none   = PatternFill()

    def mark(cell, color, flags):
        """color: 'red' | 'yellow' | 'none'; flags tracks итог для колонки A"""
        if color == "red":
            cell.fill = red
            flags["had_red"] = True
        elif color == "yellow":
            cell.fill = yellow
            flags["had_yellow"] = True
        else:
            cell.fill = none

    from difflib import SequenceMatcher

    for row in ws.iter_rows(min_row=2):
        # сброс заливки
        for cell in row:
            cell.fill = none

        # A=0, D=3, J=9, L=11, P=15
        A = str(row[0].value or "")
        D = str(row[3].value or "")
        J = str(row[9].value or "")
        L = str(row[11].value or "")
        P = str(row[15].value or "").lower()

        flags = {"had_red": False, "had_yellow": False}

        if P == "аналог":
            if not unification_enabled:
                # Уніфікації нет — ничего не подсвечиваем для «аналог» (просто пропускаем)
                pass
            else:
                matched = False
                allowed = []

                # 1) Совпадение по каталожному номеру
                for _, r in temp_df.iterrows():
                    pn = str(r["D (Каталожний номер)"])
                    nums = [pn, pn.replace(".", "")]
                    if any(n and n in D for n in nums):
                        matched = True
                        allowed = [x.strip() for x in str(r["E (Допустимі аналоги)"]).split(",") if x.strip()]
                        break

                # 2) Если PN не нашли — сравнение по названию
                if not matched:
                    base = A.split(",")[0].split("|")[0].strip().lower()
                    sims = temp_df["C (Найменування)"].apply(
                        lambda x: SequenceMatcher(None, base, str(x).lower()).ratio()
                    )
                    if not sims.empty:
                        idx = sims.idxmax()
                        ratio = float(sims.max())
                        allowed = [x.strip() for x in str(temp_df.at[idx, "E (Допустимі аналоги)"]).split(",") if x.strip()]

                        if ratio < 0.6:
                            mark(row[15], "red", flags)   # P
                        elif ratio < 0.8:
                            # неуверенно — подсветим мягко
                            mark(row[0], "yellow", flags)  # A
                            mark(row[15], "yellow", flags)

                # 3) Проверка бренда J против допустимых аналогов
                if allowed and J.strip():
                    ok = any(SequenceMatcher(None, J.lower(), a.lower()).ratio() >= 0.8 for a in allowed)
                    if not ok:
                        close = any(SequenceMatcher(None, J.lower(), a.lower()).ratio() >= 0.6 for a in allowed)
                        mark(row[9], "yellow" if close else "red", flags)  # J
                        mark(row[15], "yellow" if close else "red", flags) # P
        else:
            # базовая проверка: если в L указан код, он должен содержаться в D
            if L and L not in D:
                mark(row[11], "red", flags)

        # сводная подсветка для A: красный > жёлтый > нет
        if flags["had_red"]:
            row[0].fill = red
        elif flags["had_yellow"]:
            row[0].fill = yellow
        else:
            row[0].fill = none

    original_name = os.path.splitext(os.path.basename(tmp_main))[0]
    out_name = f"{original_name}_processed.xlsx"
    out_path = os.path.join(TMP_DIR, out_name)
    wb.save(out_path)

    return {"download_url": f"/download/{token}"}

@app.get("/download/{token}")
async def download(token: str):
    out_name = None
    for fname in os.listdir(TMP_DIR):
        if fname.startswith(token) and fname.endswith("_processed.xlsx"):
            out_name = fname
            break
    if not out_name:
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(
        os.path.join(TMP_DIR, out_name),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=out_name
    )
