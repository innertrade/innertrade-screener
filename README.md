# Innertrade Screener

Этот репозиторий содержит HTTP-сервис c вычислением метрик по рынку деривативов Bybit и вспомогательные утилиты деплоя.

## Основные цели

* Единый эталонный источник кода — ветка `main` на GitHub.
* Любой сервер можно восстановить одной командой (`bash /root/its_wipe_sync_run.sh`).
* Автоматический контроль целостности `main.py` и ключевого блока `intervalTime`.

## Быстрый старт (локально)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m compileall main.py  # убедиться в корректном синтаксисе
FLASK_ENV=production python main.py
```

Приложение поднимется на `HTTP_PORT` (по умолчанию `8088`).

## Минимальный деплой на VPS

```bash
git reset --hard origin/main
git clean -fdx
bash scripts/deploy.sh
```

Скрипт автоматически:

1. Останавливает активные юниты `innertrade-api`, `tvoi_gateway`, `tvoi_consumer`, `pre_forwarder` и глушит легаси `screener.service`.
2. Переустанавливает unit-файлы из каталога `systemd/` в `/etc/systemd/system/`.
3. Создаёт очереди `inbox/`, `processed/`, `failed/` (и `inbox/.tmp`) с владельцем `deploy:deploy` и правами `0770`.
4. Перезапускает сервисы и выполняет смоук-проверку: `GET /health`, `GET /signals`, `POST /tvoi`.
5. Ожидает перемещения тестового файла из `inbox/` в `processed/`.

После успешного запуска проверьте окружение:

```bash
ss -ltnp | grep -E '127.0.0.1:(8088|8787)'
curl -fsS http://127.0.0.1:8088/health
curl -fsS http://127.0.0.1:8088/signals | head
```

Для ручной отправки тестового сигнала можно воспользоваться утилитой `tools/tvoi_gateway.py`:

```bash
./tools/tvoi_gateway.py send
```

## Полная переинициализация VPS

Скрипт `scripts/its_wipe_sync_run.sh` предназначен для запуска из `root` (или другого пользователя с sudo) и выполняет:

1. Остановку systemd-сервисов скринера.
2. Удаление каталога приложения и клонирование свежей копии из `origin/main`.
3. Сохранение (при наличии) и восстановление `.env` с токенами, обновление значений из переменных окружения `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`.
4. Создание нового виртуального окружения `.venv`, установка зависимостей и фиксация `cacert.pem` для TLS.
5. Проверку целостности `main.py` (`scripts/detect_main_drift.py`).
6. Перезапуск systemd-сервисов (скринер, push_signals, menu_bot) и smoke-проверку `/health` и `/signals` на порту `8088`.

Файл располагается в `/root/its_wipe_sync_run.sh` и может быть вызван одной командой:

```bash
bash /root/its_wipe_sync_run.sh
```

Все параметры (URL репозитория, каталог приложения, имя сервиса) можно переопределить переменными окружения. Подробности — в самом скрипте.

## Нормализация серверного окружения

Если на сервере остаются процессы со статусом `cwd -> (deleted)` или активны легаси-сервисы `screener.service` / `push_trend.service`, используйте сценарий `its_fix_stack.sh`.

**Двухшаговый запуск:**

1. Создайте (или обновите) файл и выставьте права:

   ```bash
   sudo install -Dm755 its_fix_stack.sh /root/its_fix_stack.sh
   ```

2. Выполните сценарий от `root`:

   ```bash
   sudo bash /root/its_fix_stack.sh
   ```

Скрипт сам:

* завершит процессы `python`/`gunicorn`/`push_trend` со сброшенным рабочим каталогом;
* отключит `screener.service` и `push_trend.service`;
* перезапустит `innertrade-screener.service`, `menu_bot.service`, `push_signals.service`;
* проверит, что `127.0.0.1:8088` слушает gunicorn и `/health` отвечает `status: "ok"`.

После выполнения убедитесь, что повторный запуск не приводит к ошибкам, а `systemctl list-unit-files | grep -E 'screener|push_trend'` показывает состояние `disabled`.

## Детектор дрейфа `main.py`

`scripts/detect_main_drift.py` сравнивает локальный `main.py` c версией из `origin/main` и дополнительно проверяет, что в коде присутствует строка `intervalTime` и отсутствуют артефакты вида `"interval": str(`.

```bash
./.venv/bin/python scripts/detect_main_drift.py --fetch
```

При обнаружении расхождений скрипт возвращает код выхода `1` и выводит контрольные суммы и diff. Скрипт используется как в ручном режиме, так и в `its_wipe_sync_run.sh`.

## Проверка работоспособности

После перезапуска выполните:

```bash
curl -s http://127.0.0.1:8088/health | jq
curl -s http://127.0.0.1:8088/signals | jq '.data[0]'
```

* `/health` должен вернуть `{"status": "ok", ...}`.
* `/signals` должен содержать ненулевые `oi_z` (если данные Bybit доступны).

## Быстрый фикс окружения (`its_fix_stack.sh`)

1. Создайте (или обновите) файл `/root/its_fix_stack.sh` c содержимым из `scripts/its_fix_stack.sh` и сделайте его исполняемым:

   ```bash
   install -m 750 scripts/its_fix_stack.sh /root/its_fix_stack.sh
   ```

2. Запустите фиксацию стека от имени `root`:

   ```bash
   bash /root/its_fix_stack.sh
   ```

Скрипт устранит процессы со статусом `cwd -> (deleted)`, выключит легаси-юниты `screener.service` и `push_trend.service`, перезапустит действующие сервисы (`innertrade-screener.service`, `menu_bot.service`, `push_signals.service`) и выполнит проверки: листенер на `127.0.0.1:8088`, `/health` и отсутствие удалённых рабочих каталогов.

## Автоматизация GitHub Actions

Workflow `.github/workflows/deploy.yml` обновлён: он копирует свежий `its_wipe_sync_run.sh` на сервер и запускает его по SSH, передавая токены через переменные окружения. Таким образом GitHub Actions и ручной запуск используют один и тот же сценарий.

## Полезные заметки

* `requirements.txt` включает `gunicorn`, который используется в systemd-сервисе.
* Все команды на сервере выполняются в «двухчастном» формате (команда + короткий комментарий в логах скрипта).
* Перед коммитом всегда запускайте `python -m compileall main.py` и `scripts/detect_main_drift.py`.

## Тонкий контур TVOI

В тонком контуре HTTP-сервисы и очереди управляются пользователем `deploy`. Все unit-файлы лежат в репозитории и устанавливаются
скриптом `scripts/deploy.sh`. Скрипт:

* создаёт каталоги `inbox/`, `processed/`, `failed/` (и `inbox/.tmp`) с владельцем `deploy:deploy`;
* обновляет и перезапускает `innertrade-api`, `tvoi_gateway`, `tvoi_consumer`, `pre_forwarder`;
* глушит исторический `screener.service` и освобождает порт `8088` перед запуском API;
* выполняет смоук-проверку `/health` и прогоняет тестовый сигнал по цепочке `gateway → consumer`.

Все входящие запросы направляются на `http://127.0.0.1:8787/tvoi`. Nginx больше не используется в качестве прокси для `/tvoi`.
Если требуется изменить пользователя/группу или каталоги, достаточно переопределить переменные окружения `APP`, `APP_USER`,
`APP_GROUP` при запуске `scripts/deploy.sh`.

### Смоук-проверка

1. Запустите `scripts/deploy.sh` от имени пользователя с правами `root`. Скрипт завершится с ошибкой, если какой-либо сервис не
   поднялся или тестовый файл не прошёл всю цепочку.
2. Убедитесь, что POST-запрос

   ```bash
   curl -sS -X POST http://127.0.0.1:8787/tvoi \
     -H 'Content-Type: application/json' \
     -d '{"type":"PRE","symbol":"TESTUSDT","tf":"5m","price":123,"link":"http://example.com"}'
   ```

   возвращает JSON с `"ok": true` и именем файла.
3. После запроса файл с таким именем быстро исчезает из `inbox/` и появляется в `processed/`.
4. `journalctl -u tvoi_consumer -n 20` должен содержать строку вида `TVOI PRE sent=True | file=<имя файла>`. Ошибки
   появляются как `sent=False | reason=...` в stderr unit-а.

При необходимости можно повторно запускать `scripts/deploy.sh` — он идемпотентен и не оставляет лишних процессов на порту
8088.
