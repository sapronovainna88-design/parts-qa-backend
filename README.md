# Parts QA Backend (Railway)

Цей бекенд реалізує два ендпоінти:
- `POST /preview` — приймає `selected_type`, `selected_brand`, `main_file` (.xlsx), повертає `preview_markdown` та `token`.
- `POST /process` — приймає `token`, повертає `download_url` для файлу `<original>_processed.xlsx`.

## Деплой на Railway
1. Створіть новий проєкт на [railway.app] і підключіть GitHub репозиторій з цим кодом.
2. Додайте змінну середовища `UNIFICATION_PATH = unifikatsiya.xlsx`.
3. Завантажте файл уніфікації до кореня репозиторію під назвою `unifikatsiya.xlsx` (це ваш «уніфікаувція для ШІ.xlsx»).
4. Зазвичай Railway підставляє порт у `PORT`; `Procfile` вже налаштований.
5. Після деплою перевірте `/healthz`.

## Приклад використання
- `POST /preview` (multipart/form-data):
  - `selected_type` = "комбайни"
  - `selected_brand` = "john deere"
  - `main_file` = ваш .xlsx
- Відповідь: `{ "normalized_type": "...", "normalized_brand": "...", "preview_markdown": "...", "token": "..." }`
- `POST /process` (form-data або JSON з `token`) → `{ "download_url": "/download/<token>" }`

## Безпека
- Додайте просту перевірку походження (CORS), або API ключ у заголовку (наприклад, `X-API-Key`) і перевіряйте його в ендпоінтах.
- Тимчасові файли зберігаються у `/tmp` і очищуються при рестарті контейнера.