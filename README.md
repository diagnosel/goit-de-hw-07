# ДЗ7 — Apache Airflow

Навчальна робота Григорової Ауріки Вадимівни. DAG випадково обирає Bronze, Silver або Gold, рахує відповідні записи в `olympic_dataset.athlete_event_results` та зберігає результат у власну таблицю. Після паузи SQL sensor перевіряє вік запису.

**Статус: локальна реалізація, ще не готово до здачі.** DAG парситься, 8 локальних тестів пройшли. Навчальне MySQL-підключення та дозволену власну схему не підтверджено. Справжні success/failed запуски, результати SQL і скриншоти ще відсутні. Репозиторій на GitHub ще не створено, push не виконувався.

## Файли

- `dags/aurika_hw07.py` — DAG із вісьмома tasks.
- `tests/test_dag.py` — локальні перевірки з підставленими відповідями MySQL.
- `docs/guide.uk.md` — пояснення кроків, запуску та перевірки.
- `docs/report.uk.md` — короткий документ про поточний результат і відсутні докази.
- `evidence/verification.json` — фактичні версії, граф та статус виконання.
- `evidence/unit-tests.log` — журнал локальних тестів. Числа в цьому журналі тестові.

## Середовище

Перевірено: Python 3.12.4, Airflow 2.11.2, MySQL provider 6.5.0, common-sql provider 1.32.0, mysql-connector-python 9.6.0. Імпорти відповідають Airflow 2; сумісність з Airflow 3 не заявляється. Docker не потрібен.

Команди з папки проєкту:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
python -m pip check
export AIRFLOW_HOME="$PWD/.runtime/airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
python -m unittest discover -s tests -v
```

Це окреме середовище для перевірки коду. Воно не містить навчальної MySQL-бази чи доступу до неї. SQLite metadata Airflow і MySQL з олімпійськими даними — різні бази.

## Підключення та запуск

Потрібен наявний дозволений Airflow connection `hw07_mysql`. У ньому слід вибрати `client: mysql-connector-python` у JSON Extra, бо `mysqlclient` тут не встановлювався. Інші значення підключення беруться лише з навчального середовища. Секрети не записуються у цей репозиторій.

`HW07_SCHEMA` має містити **підтверджену власну навчальну схему**. Значення за замовчуванням `aurika_hryhorova` — пропозиція назви, існування та дозвіл на неї не підтверджено. DAG не створює схему. Він створює лише власну таблицю `aurika_hw07_medal_counts`. Для роботи потрібні SELECT на source table і CREATE, SELECT, INSERT на власну таблицю.

Після підтвердження доступу встанови `HW07_SCHEMA` у процесах Airflow, додай файл у налаштовану папку DAGs, перевір import errors та запусти два різні runs послідовно:

```bash
airflow dags trigger aurika_hw07_medals \
  --run-id hw07_success_01 --conf '{"delay_seconds": 5}'
```

Після завершення першого:

```bash
airflow dags trigger aurika_hw07_medals \
  --run-id hw07_stale_01 --conf '{"delay_seconds": 35}'
```

CLI має використовувати те саме середовище Airflow, що й scheduler та UI. Саме `dags trigger` без запущеного scheduler не виконує tasks. Не запускай локальний scheduler на чужій metadata DB. Детальні очікувані результати, SQL і список потрібних скриншотів наведено в [інструкції](docs/guide.uk.md).

## Принцип перевірки

Пауза має trigger rule `none_failed_min_one_success`: одна успішна гілка й дві skipped дозволяють продовження; failed гілка його блокує. Sensor має звичайне `all_success` та є єдиною кінцевою task, тому його failure не приховується.

Власна таблиця має рівно чотири поля: `id`, `medal_type`, `count`, `created_at`. Обрана task повертає id вставленого рядка через XCom. Sensor читає лише цей id та обрану медаль. Для цього запуску вставляється один рядок, тож перевіряється найновіший і єдиний результат цього запуску. Новіший рядок іншого запуску на результат не впливає. `max_active_runs=1`, автоматичних retries немає.

Запис і порівняння часу виконуються сервером MySQL через `UTC_TIMESTAMP(6)`. Допустимий вік — від 0 до 30 секунд включно. Старий, майбутній або відсутній запис викликає помилку. Стандартний `SqlSensor` має `timeout=10`, `execution_timeout=20`, `soft_fail=False`; при віці понад 30 секунд він завершується одразу за failure-умовою. Пауза 35 секунд не збільшує допустимий вік запису.

## Перед здачею

Потрібно отримати реальні два runs, скриншоти Airflow UI й таблиці, доповнити документ, повторити review та перевірку секретів. Лише потім можна створювати публічний `diagnosel/goit-de-hw-07`, робити push і перевіряти remote SHA. Поточний архів — проміжна копія, не повна здача. LMS не надсилали.

Документація в [Notion](https://app.notion.com/p/401a96dc80e58106af14c76711d45928?pvs=204).

Джерела: [Airflow 2.11.2 та branching/trigger rules](https://airflow.apache.org/docs/apache-airflow/2.11.2/core-concepts/dags.html), [SqlSensor 1.32.0](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/1.32.0/_api/airflow/providers/common/sql/sensors/sql/index.html), [MySQL provider 6.5.0](https://airflow.apache.org/docs/apache-airflow-providers-mysql/6.5.0/index.html).
