# app.py — FastAPI бекенд для Railway
# Мета: реалізувати /preview та /process для роботи з інструкцією «Запасні частини».

import os, io, uuid, re, json
from typing import Optional
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from difflib import get_close_matches, SequenceMatcher
from tabulate import tabulate
import openpyxl
from openpyxl.styles import PatternFill

APP_PORT = int(os.getenv("PORT", "8000"))
UNIFICATION_PATH = os.getenv("UNIFICATION_PATH", "unifikatsiya.xlsx")  # завантажте ваш файл у репозиторій
TMP_DIR = os.getenv("TMP_DIR", "/tmp")

app = FastAPI(title="Parts QA Backend", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def fuzzy(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def has_cyr(s: str) -> bool:
    return bool(re.search('[\\u0400-\\u04FF]', s))

def load_unification() -> pd.DataFrame:
    df = pd.read_excel(UNIFICATION_PATH)
    df['cat_norm'] = df['Вид техніки'].astype(str).str.lower().str.strip()
    df['brand_norm'] = df['Бренд'].astype(str).str.lower().str.strip()
    return df

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/preview")
async def preview(
    selected_type: str = Form(...),
    selected_brand: str = Form(...),
    main_file: UploadFile = File(...)
):
    # 1) Зберегти завантажений файл тимчасово
    token = str(uuid.uuid4())
    tmp_main = os.path.join(TMP_DIR, f"{token}.xlsx")
    content = await main_file.read()
    with open(tmp_main, "wb") as f:
        f.write(content)

    # 2) Завантажити уніфікацію і відфільтрувати
    df = load_unification()
    st = (selected_type or "").lower().strip()
    sb = (selected_brand or "").lower().strip()

    df_cat = df[df['cat_norm'] == st]
    if df_cat.empty:
        choices = df['cat_norm'].dropna().unique().tolist()
        suggestions = get_close_matches(st, choices, n=3, cutoff=0.6)
        return JSONResponse(
            {"error": f"Категорію «{selected_type}» не знайдено", "suggestions": suggestions}, status_code=400
        )

    brands_pool = df_cat['brand_norm'].dropna().unique().tolist() + ['всі доступні моделі']
    best = get_close_matches(sb, brands_pool, n=1, cutoff=0.6)
    if best:
        sb = best[0]

    df_brand = df_cat[df_cat['brand_norm'].isin([sb, 'всі доступні моделі'])]
    if df_brand.empty:
        suggestions = get_close_matches(sb, brands_pool, n=3, cutoff=0.6)
        return JSONResponse(
            {"error": f"Бренд «{selected_brand}» не знайдено", "suggestions": suggestions}, status_code=400
        )

    temp_df = (
        df_brand[['Найменування', 'каталожний номер', 'Допустимі аналоги']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    temp_df.columns = ['C (Найменування)', 'D (Каталожний номер)', 'E (Допустимі аналоги)']

    # 3) Зберегти temp_df у /tmp щоб використати на /process
    tmp_temp = os.path.join(TMP_DIR, f"{token}_temp.parquet")
    temp_df.to_parquet(tmp_temp, index=False)

    # 4) Підготувати markdown-таблицю
    preview_markdown = tabulate(temp_df, headers=temp_df.columns, tablefmt='github', showindex=False)

    return {
        "normalized_type": st,
        "normalized_brand": sb,
        "preview_markdown": preview_markdown,
        "token": token
    }

@app.post("/process")
async def process(token: str = Form(...)):
    # відкрити тимчасові файли
    tmp_main = os.path.join(TMP_DIR, f"{token}.xlsx")
    tmp_temp = os.path.join(TMP_DIR, f"{token}_temp.parquet")
    if not (os.path.exists(tmp_main) and os.path.exists(tmp_temp)):
        return JSONResponse({"error": "invalid token"}, status_code=400)

    temp_df = pd.read_parquet(tmp_temp)

    # зчитати книгу
    wb = openpyxl.load_workbook(tmp_main)
    ws = wb.active

    red    = PatternFill(start_color='FFFF0000', end_color='FFFF0000', fill_type='solid')
    yellow = PatternFill(start_color='FFFFFF00', end_color='FFFFFF00', fill_type='solid')
    none   = PatternFill()

    # припустимо selected_brand зберігався у parquet? для простоти не використовуємо тут.
    # у реальному варіанті додайте selected_brand/selected_type у parquet (в/з /preview)
    main_brand = ""  # опціонально: додайте у /preview і збережіть біля token

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
                nums = [str(r['D (Каталожний номер)']), str(r['D (Каталожний номер)']).replace('.', '')]
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
            else:
                ratio = 1.0

            if (J.strip() and allowed) and (not any(SequenceMatcher(None, J.lower(), a.lower()).ratio() >= 0.8 for a in allowed)):
                if has_cyr(J) or (allowed and SequenceMatcher(None, J.lower(), allowed[0].lower()).ratio() >= 0.6):
                    row[9].fill = yellow; row[15].fill = yellow
                else:
                    row[9].fill = red; row[15].fill = red; red_flag = True
        else:
            if main_brand:
                ratio = SequenceMatcher(None, J.lower(), main_brand.lower()).ratio()
                if ratio < 0.6: row[9].fill = red; red_flag = True
                elif ratio < 0.8 or has_cyr(J): row[9].fill = yellow
            if L and L not in D: row[11].fill = red; red_flag = True

        if red_flag:
            row[0].fill = red

    # згенерувати ім'я файлу з суфіксом _processed.xlsx
    original_name = os.path.splitext(os.path.basename(tmp_main))[0]
    out_name = f"{original_name}_processed.xlsx"
    out_path = os.path.join(TMP_DIR, out_name)
    wb.save(out_path)

    # Сервінг: зробимо окремий ендпоінт /download/{token}
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
    return FileResponse(os.path.join(TMP_DIR, out_name), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=out_name)

# Запуск локально: uvicorn app:app --host 0.0.0.0 --port 8000