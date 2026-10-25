# ДЗ7 — Apache Airflow

Навчальна робота Григорової Ауріки Вадимівни. DAG випадково обирає Bronze, Silver або Gold, рахує записи цієї медалі, зберігає результат у власну таблицю MySQL і перевіряє свіжість після паузи.

## Результат

25 жовтня 2026 року виконано **6 локальних запусків Airflow**: 5 success з паузою 5 секунд і 1 очікуваний failed з паузою 35 секунд. Успішно перевірено всі три гілки. У негативному запуску sensor прочитав вік **35.063458 секунди** й завершився failed. Дві невибрані гілки в кожному run мають skipped.

Використано `dag.test()` із реальною MySQL, SQL-запитами та `time.sleep`. Окремо виконано 8 mock-тестів із підставленими відповідями БД. Scheduler для такого способу виконання не потрібен. Робота scheduler/executor, паралельність і примусове застосування timeout цими runs не перевірялися. Історія збережена в metadata DB та доступна у локальному Airflow UI. Попередження UI про зупинений scheduler очікуване для цього способу перевірки.

Середовище: macOS на Apple Silicon, Python 3.12.4, Airflow 2.11.2, MySQL Community 8.4.11, MySQL provider 6.5.0, common-sql 1.32.0, mysql-connector-python 9.6.0. Docker і сервери GoIT не використовувалися.

## Дані

Публічний [Olympic_Athlete_Event_Results.csv](https://github.com/sfc-gh-mconsoli/olympic_games_data_hub/blob/21b851f8fbe775332b1bc43b9dac213b4087e80b/dataset/Olympic_Athlete_Event_Results.csv) із зафіксованого commit містить 316 834 записи. У власну локальну `olympic_dataset.athlete_event_results` імпортовано номер кожного вихідного рядка та поле `medal`; інші поля цій задачі не потрібні. Це локальна проєкція публічного CSV, а не копія віддаленої навчальної БД GoIT.

SHA-256 CSV: `4a76c66fc6f69a2f69b4a953298d475fd61fb46a40ac5995ea738bc94695905b`.

Контрольні кількості: Bronze — 14 939, Silver — 14 676, Gold — 15 072. Пораховано записи спортсменів, включно з учасниками команд; це не кількість унікальних подій. Python Counter із CSV, GROUP BY у MySQL і результати DAG збігаються.

## Як працює DAG

```text
create_table → choose_medal → branch
                             ├─ count_bronze ─┐
                             ├─ count_silver ─┼─ delay → check_freshness
                             └─ count_gold ───┘
```

Власна таблиця `aurika_hryhorova.aurika_hw07_medal_counts` має чотири поля: `id` з auto increment і primary key, `medal_type`, `count`, `created_at`.

`choose_medal` викликає `random.choice`; окремий BranchPythonOperator читає результат через XCom. Кожна з трьох count tasks має свій тип медалі. SQL-параметри передаються окремо від тексту запиту.

`delay` використовує `none_failed_min_one_success`: skipped-гілки не блокують продовження, failed обраної гілки не приховується. За ним стоїть стандартний `SqlSensor` зі звичайним `all_success`; sensor є єдиною кінцевою task.

Count task повертає id вставленого рядка. Sensor читає лише цей id та обрану медаль. Один run вставляє один рядок, отже перевіряється його найновіший і єдиний результат. Новий запис іншого run не може дати випадковий успіх. `max_active_runs=1`, `max_active_tasks=1`, retries вимкнено.

Час запису та перевірки обчислюється MySQL через `UTC_TIMESTAMP(6)`. DATETIME зберігається в UTC незалежно від системної часової зони. SQL повертає цілі мікросекунди; приймається діапазон `0..30000000`. Майбутній, старий, NULL або відсутній рядок викликає failure. `timeout=10`, `execution_timeout=20`, `soft_fail=False`; старий рядок завершує sensor одразу за failure-умовою.

## Як повторити

Усі команди нижче виконуються з папки цього проєкту. Для нового експерименту потрібні окрема metadata DB та порожній власний MySQL datadir. Наявні результати й навчальні таблиці не очищайте.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
python -m pip check
mkdir -p .runtime
```

Підготуйте власний ізольований MySQL на `127.0.0.1:13307`. У перевірці використано офіційний portable archive [MySQL Community 8.4.11 для macOS ARM64](https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.11-macos15-arm64.tar.gz), перевірений за [MD5 видавця](https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.11-macos15-arm64.tar.gz.md5). Системний інсталятор і launchd не використовувалися. Детальні команди запуску й зупинки наведено в `docs/guide.uk.md`.

Для одноразового локального сервера в цій перевірці використано `--initialize-insecure`: root без пароля, доступ лише через loopback, datadir з правами 700. Після роботи сервер зупиняється. Для постійного середовища потрібні власні належні облікові дані; жодних секретів у repo немає.

Завантажте вихідний CSV, перевірте checksum та імпортуйте його:

```bash
curl -fL 'https://media.githubusercontent.com/media/sfc-gh-mconsoli/olympic_games_data_hub/21b851f8fbe775332b1bc43b9dac213b4087e80b/dataset/Olympic_Athlete_Event_Results.csv' \
  -o .runtime/Olympic_Athlete_Event_Results.csv
python scripts/import_source.py .runtime/Olympic_Athlete_Event_Results.csv
```

Імпортер перевіряє SHA-256 і відмовляється замінювати або повторно наповнювати непорожню source table. Він призначений лише для власного локального сервера. Підготовка схеми й CSV відокремлена від DAG; звичайні запуски DAG тільки читають source table.

Налаштуйте окреме середовище Airflow. Приклад нижче стосується лише зазначеного тимчасового локального сервера:

```bash
export AIRFLOW_HOME="$PWD/.runtime/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export HW07_SCHEMA=aurika_hryhorova
export AIRFLOW_CONN_HW07_MYSQL='{"conn_type":"mysql","host":"127.0.0.1","port":13307,"login":"root","schema":"aurika_hryhorova","extra":{"client":"mysql-connector-python"}}'
airflow db migrate
python scripts/run_checks.py
python scripts/verify_results.py
```

Runner не запускається на віддаленому host і не перезаписує наявний `evidence/runs.json`. Для нового набору перевірок використовуйте окрему копію робочих файлів без попереднього `runs.json`, нову metadata DB й новий ізольований MySQL. Скрипт використовує `random.choice` і повторює 5-секундні запуски до появи всіх трьох успішних гілок, максимум 20 runs.

Для перегляду історії у UI:

```bash
airflow dags reserialize
airflow webserver --hostname 127.0.0.1 --port 18087
```

Потрібна локальна конфігурація доступу до UI. Для скриншотів цієї перевірки новий UI мав read-only роль Viewer без облікового запису й слухав лише loopback. Паролі для постійного доступу не створювалися. Для live SELECT таблиці: `python scripts/view_mysql.py`, далі `http://127.0.0.1:18088/`. Це окрема сторінка для перегляду даних MySQL; вона виконує SELECT при відкритті.

## Докази й документ

- `evidence/runs.json` — фактичні run ids, параметри й task states.
- `evidence/real-verification.json` — незалежна звірка MySQL, XCom id, CSV totals і межі 30 секунд.
- `evidence/source.json` — URL, SHA-256 та контрольні кількості CSV.
- `evidence/screenshots/` — Airflow UI та результати SELECT таблиці.
- `docs/report.pdf` — текстовий документ зі скриншотами для здачі.
- `docs/report.html` — той самий документ для відкриття у браузері; зображення вбудовані.
- `evidence/unit-tests.log` — окремі 8 mock-тестів коду, які не замінюють реальні runs.

Пояснення українською та журнал кроків: [Notion](https://app.notion.com/p/401a96dc80e58106af14c76711d45928?pvs=204). Для здачі потрібно завантажити ZIP і посилання на репозиторій у LMS.

Джерела: [Airflow 2.11.2](https://airflow.apache.org/docs/apache-airflow/2.11.2/core-concepts/dags.html), [SqlSensor 1.32.0](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/1.32.0/_api/airflow/providers/common/sql/sensors/sql/index.html). Приклад локального способу перевірки: [slipkoliudmyla/goit-de-hw-07](https://github.com/slipkoliudmyla/goit-de-hw-07). Код і результати цієї роботи збережено в цьому репозиторії.
