# app.py — FastAPI backend for Railway (robust version)
import os, io, uuid, re
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

    # сохраняем входной файл
    token = str(uuid.uuid4())
    os.makedirs(TMP_DIR, exist_ok=True)
    tmp_main = os.path.join(TMP_DIR, f"{token}.xlsx")
    content = await main_file.read()
    with open(tmp_main, "wb") as f:
        f.write(content)

    # читаем унификацию с понятными ошибками
    try:
        df = load_unification()
    except Exception as e:
        return JSONResponse({"error": f"Помилка уніфікації: {e}"}, status_code=400)

    # ---- подбор категории (фаззи) ----
    st_raw = (selected_type or "").strip()
    sb_raw = (selected_brand or "").strip()
    st = _norm_text(st_raw)
    sb = _norm_text(sb_raw)

    cats = sorted(df["cat_norm"].dropna().unique().tolist())
    if st not in cats:
        cand = get_close_matches(st, cats, n=1, cutoff=0.45)
        if cand:
            st = cand[0]
        else:
            suggestions = get_close_matches(st, cats, n=5, cutoff=0.3)
            return JSONResponse(
                {"error": f"Категорію «{selected_type}» не знайдено",
                 "suggestions": suggestions},
                status_code=400
            )

    df_cat = df[df["cat_norm"] == st]

    # ---- подбор бренда + fallback на "всі" ----
    brands_pool = df_cat["brand_norm"].dropna().unique().tolist()
    has_all_row = _norm_text("всі доступні моделі") in brands_pool

    brand_mode = "exact"
    used_brand_norm = None

    if sb in ALL_BRAND_ALIASES:
        df_brand = df_cat
        brand_mode = "all"
        used_brand_norm = "всі доступні моделі"
    else:
        best = get_close_matches(sb, brands_pool, n=1, cutoff=0.6)
        if best:
            used_brand_norm = best[0]
            df_brand = df_cat[df_cat["brand_norm"] == used_brand_norm]
        else:
            if has_all_row:
                df_brand = df_cat
                brand_mode = "fallback_all"
                used_brand_norm = "всі доступні моделі"
            else:
                suggestions = get_close_matches(sb, brands_pool, n=5, cutoff=0.4)
                if "всі доступні моделі" not in suggestions:
                    suggestions.append("всі доступні моделі")
                return JSONResponse(
                    {"error": f"Бренд «{selected_brand}» не знайдено для цієї категорії",
                     "suggestions": suggestions},
                    status_code=400
                )

    # ---- формируем превью таблицу ----
    temp_df = (
        df_brand[["Найменування", "каталожний номер", "Допустимі аналоги"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    temp_df.columns = ["C (Найменування)", "D (Каталожний номер)", "E (Допустимі аналоги)"]

    # кладём временный preview рядом с токеном
    tmp_temp = os.path.join(TMP_DIR, f"{token}_temp.parquet")
    temp_df.to_parquet(tmp_temp, index=False)

    preview_markdown = tabulate(temp_df, headers=temp_df.columns, tablefmt="github", showindex=False)

    # восстановим красивое имя категории из исходных данных
    normalized_type = df_cat["Вид техніки"].iloc[0]
    normalized_brand = used_brand_norm or sb

    return {
        "normalized_type": normalized_type,
        "normalized_brand": normalized_brand,
        "brand_mode": brand_mode,  # 'exact' | 'all' | 'fallback_all'
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
    if not (os.path.exists(tmp_main) and os.path.exists(tmp_temp)):
        return JSONResponse({"error": "invalid token"}, status_code=400)

    temp_df = pd.read_parquet(tmp_temp)

    wb = openpyxl.load_workbook(tmp_main)
    ws = wb.active

    red    = PatternFill(start_color='FFFF0000', end_color='FFFF0000', fill_type='solid')
    yellow = PatternFill(start_color='FFFFFF00', end_color='FFFFFF00', fill_type='solid')
    none   = PatternFill()

    # простая логика подсветки (как в ранней версии)
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.fill = none
        A = str(row[0].value or '')
        D = str(row[3].value or '')
        J = str(row[9].value or '')
        L = str(row[11].value or '')
        P = str(row[15].value or '').lower()
        red_flag = False

        if P == 'аналог':
            matched = False
            allowed = []
            for _, r in temp_df.iterrows():
                pn = str(r['D (Каталожний номер)'])
                nums = [pn, pn.replace('.', '')]
                if any(n and n in D for n in nums):
                    matched = True
                    allowed = [x.strip() for x in str(r['E (Допустимі аналоги)']).split(',') if x.strip()]
                    break
            if not matched:
                base = A.split(',')[0].split('|')[0].strip().lower()
                sims = temp_df['C (Найменування)'].apply(lambda x: SequenceMatcher(None, base, str(x).lower()).ratio())
                idx = sims.idxmax()
                ratio = float(sims.max())
                allowed = [x.strip() for x in str(temp_df.at[idx, 'E (Допустимі аналоги)']).split(',') if x.strip()]
                if ratio < 0.6:
                    row[15].fill = red; red_flag = True
                elif ratio < 0.8:
                    if any(SequenceMatcher(None, J.lower(), a.lower()).ratio() >= 0.8 for a in allowed):
                        row[0].fill = yellow; row[15].fill = yellow
                    else:
                        row[9].fill = red; row[15].fill = red; red_flag = True
            # проверка J против допустимых аналогов
            if (J.strip() and allowed) and not any(SequenceMatcher(None, J.lower(), a.lower()).ratio() >= 0.8 for a in allowed):
                row[9].fill = yellow; row[15].fill = yellow
        else:
            if L and L not in D:
                row[11].fill = red; red_flag = True

        if red_flag:
            row[0].fill = red

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
