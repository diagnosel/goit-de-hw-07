# ДЗ7 — пояснення й повторення роботи

## Результат і спосіб виконання

25 жовтня 2026 року DAG `aurika_hw07_medals` виконав 6 реальних локальних runs: 5 success із паузою 5 секунд та один очікуваний failed із 35 секундами. Перевірено всі три випадково обрані гілки. Для кожного run MySQL містить рівно один результат із власним id.

Використано `dag.test()` Airflow 2.11.2: оператори, SQL, XCom, branching, sleep і SqlSensor виконалися насправді, а стани записалися в metadata DB. Окремі 8 unit-тестів використовують підставлені відповіді БД і не є доказом цих runs.

`dag.test()` виконує tasks послідовно в одному процесі. Ця перевірка не доводить роботу scheduler/executor, їхню паралельність чи примусове застосування timeout. Ліміти задані в DAG; негативний run фактично завершився за близько 35 секунд завдяки failure-умові sensor. У виконаних через `dag.test` task instances поля start_date/duration можуть бути NULL; тривалість видно за часом run і логами.

Airflow UI справжній, читає ту саму metadata DB. Scheduler не запускався, тому його попередження на скриншотах очікуване. Стан DAG paused не заважає ручному `dag.test`.

## Що було підготовлено

Навчальні ДЗ3–6 і правила AGENTS.md перевірено в навчальній папці. ПІБ підтверджено попередньою роботою: Григорова Ауріка Вадимівна. Робочі підключення B4/ILM не використовувалися. Наявні чужі зміни не змінювалися.

Спочатку перевірено шлях через навчальний sandbox GoIT. Його доступу не було. Після наданого прикладу локальної роботи підготовлено окремі Airflow та MySQL у папці цієї задачі. Віддалений GoIT для виконаних перевірок не потрібний.

Airflow встановлено в окремий Python venv за офіційним constraints-файлом. MySQL взято з офіційного portable archive для Apple Silicon, без системного інсталятора й launchd. Docker Desktop не встановлювався. Постійні облікові дані й зовнішній доступ не створювалися.

## Дані

Використано справжній публічний Olympic_Athlete_Event_Results.csv із зафіксованого commit. URL і SHA-256 збережено в `evidence/source.json`. Контрольна сума: `4a76c66fc6f69a2f69b4a953298d475fd61fb46a40ac5995ea738bc94695905b`.

Із 316834 рядків у власну локальну `olympic_dataset.athlete_event_results` перенесено номер вихідного рядка й `medal`. Це потрібна цій задачі проєкція CSV. Тотожність віддаленому dataset GoIT не підтверджувалася. Контрольні кількості: Bronze 14939, Silver 14676, Gold 15072, без медалі 272147. Рахуються записи спортсменів, а не унікальні змагання. CSV Counter, SQL GROUP BY і результати DAG збігаються.

Лише окремий скрипт підготовки `import_source.py` створює локальну source table та імпортує CSV. Сам DAG тільки читає source table і записує власні результати в `aurika_hryhorova.aurika_hw07_medal_counts`.

## Порядок кроків DAG

1. `create_table` виконує CREATE TABLE IF NOT EXISTS. Поля: id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, medal_type VARCHAR(6), count BIGINT UNSIGNED, created_at DATETIME(6).
2. `choose_medal` окремо викликає random.choice для Bronze, Silver, Gold. XCom зберігає обрану назву.
3. `branch` — окремий BranchPythonOperator. Він повертає id лише однієї count task. Дві інші стають skipped.
4. `count_bronze`, `count_silver`, `count_gold` — три окремі PythonOperator з MySqlHook. Кожна рахує відповідну медаль, вставляє count, medal_type, UTC_TIMESTAMP(6), робить commit і повертає lastrowid. При помилці виконується rollback. Параметри медалі передаються окремо від SQL.
5. `delay` — PythonOperator з time.sleep. Параметр delay_seconds дозволяє цілі 0..60, типово 5. Trigger rule none_failed_min_one_success пропускає skipped, але не приховує failed обраної гілки.
6. `check_freshness` — стандартний SqlSensor. Він перевіряє id, повернений count task цього run, та обрану медаль. Інший свіжий рядок не дасть випадкового success.

В одному run створюється один рядок, тому за його id перевіряється найновіший і єдиний результат саме цього run. max_active_runs=1 і max_active_tasks=1 задані в DAG. Retries вимкнено, щоб автоматично не дублювати INSERT після часткової помилки.

## Час і завершення sensor

MySQL вставляє UTC_TIMESTAMP(6) у DATETIME(6), sensor порівнює з UTC_TIMESTAMP(6) цього самого сервера. Системна зона MySQL під час перевірки була EET; UI показує UTC, логи мають offset +0200. Вручну зсув часу не віднімається.

SQL повертає TIMESTAMPDIFF(MICROSECOND, created_at, UTC_TIMESTAMP(6)). Діапазон 0..30000000 включно приймається. 30000001 вже відхиляється. Старий, майбутній, NULL або відсутній рядок викликає failure. Є timeout=10, execution_timeout=20 і soft_fail=False. Старий рядок не стане молодшим, тому failure-предикат одразу завершує sensor, а не чекає його оновлення. Sensor — єдина кінцева task.

## Як повторити з чистого середовища

Команди розраховані на macOS ARM64 та власну нову папку. Потрібні Python 3.12, curl і tar. У цій сесії runtime був у сусідній папці задачі; нижче всі шляхи відносні до кореня проєкту, щоб повторення не залежало від персонального шляху на Mac.

Для нового експерименту візьміть окрему копію коду без попередніх `evidence/runs.json` та runtime. Не очищайте наявні результати й таблиці. Скрипти імпорту та перевірки розраховані тільки на власний ізольований MySQL на 13307; сам по собі loopback не доводить належність іншого локального сервера.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
python -m pip check
mkdir -p .runtime
curl -fL https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.11-macos15-arm64.tar.gz \
  -o .runtime/mysql.tar.gz
curl -fL https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.11-macos15-arm64.tar.gz.md5 \
  -o .runtime/mysql.expected.md5
md5 -q .runtime/mysql.tar.gz
cat .runtime/mysql.expected.md5
```

Обидві MD5 мають збігатися: `6e89113f04f2af85d0a164573493db3a`. Додатково SHA-256 отриманого архіву: `b96e00493bc3499b9ffd7f08d65c5d64933af0383a8287d9873b64f94c2d6009`. Після звірки розпакуйте й запустіть тільки цей новий datadir:

```bash
tar -xzf .runtime/mysql.tar.gz -C .runtime
mkdir -m 700 .runtime/mysql-data
MYSQL_DIR="$PWD/.runtime/mysql-8.4.11-macos15-arm64"
"$MYSQL_DIR/bin/mysqld" --no-defaults --basedir="$MYSQL_DIR" \
  --datadir="$PWD/.runtime/mysql-data" --initialize-insecure
"$MYSQL_DIR/bin/mysqld" --no-defaults --basedir="$MYSQL_DIR" \
  --datadir="$PWD/.runtime/mysql-data" --bind-address=127.0.0.1 --port=13307 \
  --mysqlx=0 --socket="$PWD/.runtime/mysql.sock" \
  --pid-file="$PWD/.runtime/mysql.pid" --log-error="$PWD/.runtime/mysql-server.log" \
  --skip-log-bin --innodb-buffer-pool-size=128M --daemonize
```

Це одноразовий локальний root без пароля, приватний datadir, тільки loopback. Не використовуйте цей режим для постійного чи віддаленого сервера. Якщо порт зайнятий, спочатку встановіть, кому він належить; не підключайте скрипти до довільного наявного сервера. Після роботи сервер потрібно зупинити.

```bash
curl -fL 'https://media.githubusercontent.com/media/sfc-gh-mconsoli/olympic_games_data_hub/21b851f8fbe775332b1bc43b9dac213b4087e80b/dataset/Olympic_Athlete_Event_Results.csv' \
  -o .runtime/Olympic_Athlete_Event_Results.csv
python scripts/import_source.py .runtime/Olympic_Athlete_Event_Results.csv
export AIRFLOW_HOME="$PWD/.runtime/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=utc
export HW07_SCHEMA=aurika_hryhorova
export AIRFLOW_CONN_HW07_MYSQL='{"conn_type":"mysql","host":"127.0.0.1","port":13307,"login":"root","schema":"aurika_hryhorova","extra":{"client":"mysql-connector-python"}}'
airflow db migrate
python scripts/run_checks.py > .runtime/airflow-runs.log 2>&1
python scripts/verify_results.py
```

Connection надходить лише через поточне оточення процесу; не записується в Git або постійне сховище. Імпортер перевіряє CSV hash і відмовляється наповнювати непорожню source table. Runner відмовляється перезаписувати runs.json. Він спочатку виконує 35 секунд, потім 5-секундні runs до випадкового покриття всіх трьох медалей, максимум 20 запусків. Випадковий вибір не підміняється.

Окремі mock-тести:

```bash
python -m unittest discover -s tests -v
```

## Перегляд справжньої історії та таблиці

У цьому одноразовому локальному UI використано read-only Viewer без створення користувача. Нижче конфігурація тільки для нової metadata DB і loopback:

```bash
cat > "$AIRFLOW_HOME/webserver_config.py" <<'PY'
from flask_appbuilder.security.manager import AUTH_DB
AUTH_TYPE = AUTH_DB
AUTH_ROLE_PUBLIC = "Viewer"
WTF_CSRF_ENABLED = True
PY
airflow dags reserialize
airflow webserver --hostname 127.0.0.1 --port 18087
```

Відкрийте http://127.0.0.1:18087/home, знайдіть aurika_hw07_medals, виберіть run та Graph або Details. Для негативного run у Details видно delay_seconds: 35 та failed. У Graph count_gold і delay зелені, SqlSensor червоний, дві інші гілки skipped. Для успішного run Bronze count_bronze і sensor зелені.

В іншому терміналі з активованим тим самим venv:

```bash
python scripts/view_mysql.py
```

http://127.0.0.1:18088/ показує live SELECT результатів і контрольний GROUP BY. Це власний read-only переглядач MySQL. Скриншот таблиці отримано з нього; Airflow UI ним не підмінено.

Після перегляду зупиніть webserver і переглядач Ctrl+C у відповідних терміналах. Зупинка лише власного MySQL:

```bash
"$MYSQL_DIR/bin/mysqladmin" --host=127.0.0.1 --port=13307 --user=root shutdown
unset AIRFLOW_CONN_HW07_MYSQL
```

## Фактичні перевірки

Файли evidence/runs.json та real-verification.json містять усі run ids, стани 8 tasks і відповідні id MySQL. Sensor побачив 35.063458 секунди в негативному run та 5.041189–5.062583 у позитивних. Лог збережено як evidence/airflow-runs.log; у ньому тільки персональний абсолютний корінь робочої папки замінено на `<workspace>`, SQL і результати не змінені.

Перевірено parsing, сумісність пакетів, 8 mock-тестів, SQL counts, XCom id, структуру таблиці, реальну межу 30.000001 секунди, 6 real runs, пропуск двох гілок, документи й справжні скриншоти. Незалежний reviewer додатково прочитав MySQL і metadata SQLite без записів. Оцінка за критеріями не виставляється автоматично.

## Помилки й виправлення

У першому mock-тесті Jinja відхилив MagicMock; тестовий об'єкт замінено простим SimpleNamespace. Reviewer виявив ризик округлення SQL після ділення мікросекунд; sensor тепер порівнює цілі числа. 30000001 відхиляється і в mock, і в реальній MySQL.

Спроба підготувати старий Airflow 2.8 зі sandbox натрапила на відсутній SDK для mysqlclient. Для локального виконання використано вже встановлений і перевірений Airflow 2.11.2 та mysql-connector-python. Перший запуск MySQL у sandbox процесу не завершив ініціалізацію; той самий власний datadir успішно ініціалізовано дозволеним локальним процесом. Системні налаштування не змінювалися.

Під час збирання скриншотів тимчасово від'єдналося середовище керування Mac. Файли, metadata і MySQL перевірено після відновлення; UI перезапущено, скриншоти знято з реального UI через Playwright. Runs повторно не створювалися й стани вручну не змінювалися.

## Здача

Текстовий документ зі скриншотами — `docs/report.html`; усі зображення вбудовані, тому документ відкривається без додаткових файлів. Поруч є Markdown-копія. Фінальний ZIP містить код, документ і докази, без venv, runtime, CSV, credentials або metadata DB. Власний публічний repo: https://github.com/diagnosel/goit-de-hw-07. Документація: https://app.notion.com/p/401a96dc80e58106af14c76711d45928?pvs=204.

Завантаження архіву й посилання до LMS та вибір політики оцінювання залишаються за користувачкою.
