# Перевірка роботи перед публікацією

25 жовтня 2026 року виконано повторне незалежне read-only review коду та реальних доказів.

## Шість критеріїв

- Створення таблиці: власна схема, CREATE TABLE IF NOT EXISTS, чотири потрібні поля, PK і auto increment. Структуру підтверджено реальною MySQL.
- Random: окремий PythonOperator із random.choice, вибір не підміняється в real runs.
- Branch: окремий BranchPythonOperator; у всіх 6 runs одна count task success і дві skipped.
- Count: три окремі tasks, параметризований SELECT із source table й INSERT у власну; числа незалежно збігаються з CSV і SQL GROUP BY.
- Delay: PythonOperator з time.sleep; фактичні паузи 5 та 35 секунд, join none_failed_min_one_success. Failure не маскується.
- Sensor: стандартний SqlSensor, UTC, точний XCom id поточного run, цілі мікросекунди, failure для старого/майбутнього/NULL/відсутнього рядка, обмеження часу. Реально отримано success близько 5 секунд і failed при 35.063458.

## Код і реальні докази

Airflow 2.11.2, mysql provider 6.5.0, common-sql 1.32.0 і mysql-connector-python 9.6.0 сумісні; pip check успішний, DagBag без import errors, 8 mock-тестів пройшли повторно. Нові Python scripts парсяться.

Під час першого review знайдено ризик SQL-округлення на 30.000001 секунди. Ділення замінено на цілі мікросекунди. Reviewer повторно підтвердив виправлення; справжня MySQL відхиляє 30000001.

Незалежно read-only звірено SHA-256 CSV, 316834 рядки, counts, MySQL 8.4.11 і loopback bind, усі 6 рядків результатів і чотири колонки таблиці, metadata SQLite, XCom ids 1..6, rendered sensor parameters і raw log. Негативний sensor завершився одразу після SQL-перевірки. Підтверджених дефектів DAG після виправлення немає.

## Межі перевірки

Runs виконано через dag.test: реальні SQL, sleep та Airflow tasks у одному процесі. Scheduler/executor, їхня concurrency та примусове застосування timeout не тестувалися. Для task instances можуть бути NULL start_date/duration; тривалість підтверджено run timestamps і логами. Warning scheduler у UI не приховувався.

Дані — власна локальна проєкція medal публічного CSV, не remote GoIT dataset. Його тотожність CSV не доведена. Скрипти призначені лише для виділеної локальної БД; loopback довільного сервера сам по собі не доводить його належності.

## Комплект перед push

Справжні скриншоти Airflow success/failed/35s config, історія runs і live SELECT MySQL переглянуті. HTML документ містить вбудовані оригінальні зображення; PDF — друк того самого документа. Інструкція розрізняє mock-тести, реальне виконання і його межі.

Фінальний staged diff та ZIP перевіряються окремо перед push. Результат остаточної перевірки записується в evidence/package-check.json; підтвердження публікації та remote SHA зберігаються локально окремо, щоб не створювати самопосилання commit hash.

У Git і ZIP не повинні входити .env, credentials, venv, runtime, MySQL data/binaries, CSV, metadata DB, кеші, .git або зашифровані sandbox delivery artifacts. Документацію й коментарі переглянуто на природність, без вигаданих claims про авторство чи досвід. Здача в LMS не виконується автоматично.
