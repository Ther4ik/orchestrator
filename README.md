# GARAZ Risk Orchestrator — база backend (MVP)

```
app/catalog.py       процессы, источники, веса, группы (из легенды)
app/models.py        Task, Source, Decision, статусы
app/orchestrator.py  диспетчер + Decision Engine (К-01…К-07)  <- главный файл, только stdlib
app/runner.py        имитация исполнения (asyncio), заменяется реальными расчётами
app/main.py          REST API (FastAPI)
tests/               сценарии из описания конфликтов
demo.py              консольное демо с таймлайном
```

Запуск: `pip install -r requirements.txt && uvicorn app.main:app --reload` → http://localhost:8000/docs
Тесты: `python -m unittest -v` (зависимостей не нужно)  Демо: `python demo.py`

## Как принимается решение (Decision Engine)
Запрос → проверки → `run` / `queue` (с причинами) / `reject`:
* дубль того же процесса за ту же дату → reject (К-01)
* источник в статусе «Ошибка» → reject; «Обновляется/Неполные» → queue (К-05)
* тот же расчёт (группа, напр. CR-01/CR-07) за ту же дату уже идёт → queue (К-01)
* источнику нужна эксклюзивная запись → новые читатели ждут (К-02)
* квоты: параллельных задач / суммарный вес / тяжёлых → queue (К-07)

Ресурсы выдаются пакетом «всё или ничего» → deadlock невозможен (К-03).
На старте фиксируются версии источников (К-04). Сбой → ресурсы освобождены, staging очищена, retry = новый ID (К-06).
Очередь сортируется по (приоритет, порядок поступления); при освобождении ресурса диспетчер сам запускает ожидающие.

## Основные эндпоинты
POST /tasks
GET /tasks 
GET /tasks/{id}
POST /tasks/{id}/cancel 
POST /tasks/{id}/retry
GET /queue 
GET /system/load 
GET /processes 
GET /sources 
GET /results 
GET /audit
POST /sources/{id}/update {mode: versioned|exclusive} 
PUT /sources/{id}/status

## Допущения / что дальше
Хранение в памяти (при перезапуске всё сбрасывается) → SQLAlchemy/PostgreSQL; расписание (APScheduler/cron);
heartbeat для зависших задач; WebSocket/SSE для живого UI; aging приоритетов против «голодания».
