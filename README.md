# Я.Практикум, DE, Итоговый проект

# I. Схема пайплайна

## 0. Сервис Практикума будет эмулировать реальную жизнь, отправляя поток мне в Кафку в Я.Облаке

## 1. Спаркоджоб будет приземлять стрим в две таблицы постгреса as is
(`payload` просто строкой), с некоторыми зачатками дедупликации с ватермарком 5 минут.

*Он просто висит резидентом в контейнере. На прод - наверное в под-е кубера или т.п., и то на прод, я так понимаю, приземление потока работало бы без посредничества постгреса, сразу в Вертику.*

`src/py/stage_stream.py`

Доступы к Fafka, Постгресу, имя топика в Кафке, структура таблицы в Постгресе - всё зашито в код.

Ниже подробно описано, как запускал стриминговый сервис Практикума, как запускал этого воркера в контейнере.

## 2. Один DAG под Airflow: запускается на даты октября 2022-го года backfill-ом

`src/dags/project_final.py`

Параметры бэкфилла:

```python
from airflow.decorators import dag, task
# ...
default_args = {
    'owner': 'student',
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': True,  # допустим, это надо для корректности витрин
    'wait_for_downstream': True
}
# ...
@dag(
    default_args=default_args,
    start_date=pendulum.parse('2022-10-02'),
    end_date=pendulum.parse('2022-11-01'),
    catchup=True,
    schedule="@daily",
)

```

Граф:

```python
    chain(
        init(),
        [dump_currencies(), dump_transactions()],
        [load_currencies(), load_transactions()],
        make_dwh(),  # pass
        make_cdm()
    )
```

Доступы к Vertica DAG сам заполнит,
а доступы к Postgresql будет искать в Connections Airflow по имени `pg_connection`.

Во все task-и передаётся `execution_date`, работа идёт по дате `-1 день` от этой даты (сегодня отрабатываем стрим за вчера, так как сегодня он уже (допущение) заполнен полностью).

Ниже подробнее расписано, как что запускал в контейнере.

### 2.1. init()

Забивает в Variables Airflow переменне для доступа к моей Вертике в облаке Практикума, определяет пути к директориям с сиквелом, создаёт таблицы в Вертике.

### 2.2. dump_...(), load_...(): Перекидываем поток, "приземлённый" в Постгрес as is, в стейдж Вертики с раскладкой payload по полям.

Отрабатывает наш DAG по датам, и в Вертике партиционирование по датам же мы делаем.

Соответственно вся работа с данными реализована цепочкам по схеме удаления партиции, заливки партиции.

Стейджинг таблиц Постгреса в таблицы Вертики делаем через файл: удалили партицию в Вертике, выгрузили данные из Постгреса в файл, залили файл в Вертику.

### 2.3. make_dwh(),  `# pass`: Ядро DWH - не делаем

Начал было делать, придумал снежинку, но увидел, что по ТЗ проекта - не требуется.

Ниже есть намётки.

### 2.4. make_cdm()

Записи в витрину добавляем в том же DAG-e, по той же схеме: удаляем партицию по дате, добавляем записи за дату.

На всякий случай в сиквеле предусмотрено так же WHERE PK NOT IN target, как делали в модуле про Вертику.


## 3. Скриншоты дашбоардов в BI

`src/img`

# II. Как я всё это делал

## 0. Организуем наполнение источника (Кафки)

Для проекта использую ту же Кафку,что осталась ещё от модуля про Я.Облако

Завожу в ней один топик: `transaction-service-input`.
В него будут литься и транзакции, и курсы валют (обработчик будет отличать их по `object_type` в сообщениях: `TRANSACTION` vs. `CURRENCY`).

Далее прошу сервис Я.Практикума начинать и прекращать лить в него данные через рест-апи сервиса.

Прекратить:
```bash
curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/project/delete_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "student": "sergei_baranov"
}
EOF

{
    "response": {
        "message": "Kafka settings for final project for for student sergei_baranov has been deleted.",
        "status": "OK"
    }
}
```

Начать:
```bash
curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/project/register_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "student": "sergei_baranov",
    "kafka_connect":{
        "host": "rc1a-sd5jrikpd9jcve1c.mdb.yandexcloud.net",
        "port": 9091,
        "topic": "transaction-service-input",
        "producer_name": "producer_consumer",
        "producer_password": "sprint_11"
    }
}
EOF

{
    "response": {
        "message": "Saved.",
        "status": "OK"
    }
}
```

**NB:**

Топик нельзя удалять и создавать новый с тем же именем: сервис наполнения сломается на этом.

**Как очистить топик:**

[How to delete records from a Kafka topic](https://dev.to/de_maric/how-to-delete-records-from-a-kafka-topic-464g)

### как читаем этот топик из моей Кафки (убедиться, что сервис я.практикума наполняет мне топик)

- нам дали докер-образ, в нём есть всякое, например пайспарк;
загружаем образ, далее запускаем контейнер из образа, смотрим ид контейнера

```bash
vonbraun@VonBraun:~$ docker image pull sindb/de-final-prj:latest 
latest: Pulling from sindb/de-final-prj
...
Status: Downloaded newer image for sindb/de-final-prj:latest
docker.io/sindb/de-final-prj:latest

vonbraun@VonBraun:~$ docker images
REPOSITORY            TAG       IMAGE ID        CREATED         SIZE
...
sindb/de-final-prj    latest    28db6575225e    4 months ago    3.27GB
...

vonbraun@VonBraun:~$ docker image inspect 28db6575225e
[
    {
        "Id": "sha256:28db6575225edb23a4abf6f1a1a1084f095629db32a224ae50d6430992318371",
        "RepoTags": [
            "sindb/de-final-prj:latest"
        ],
...

vonbraun@VonBraun:~$ docker run -d -p 8998:8998 -p 8280:8280 -p 15432:5432 --name=de-final-prj-local sindb/de-final-prj:latest
d5911a381ff812de624aae5231dcaad7930fcbda486996e68db76bf2c1a05005

vonbraun@VonBraun:~$ docker ps -a
CONTAINER ID   IMAGE                       COMMAND                  CREATED         STATUS         PORTS                                                                                                                                                   NAMES
d5911a381ff8   sindb/de-final-prj:latest   "/opt/entrypoint.sh …"   7 seconds ago   Up 6 seconds   3000/tcp, 0.0.0.0:8280->8280/tcp, :::8280->8280/tcp, 3002/tcp, 0.0.0.0:8998->8998/tcp, :::8998->8998/tcp, 0.0.0.0:15432->5432/tcp, :::15432->5432/tcp   de-final-prj-local

# d5911a381ff8

vonbraun@VonBraun:~$ docker exec -it d5911a381ff8 bash

root@1edcb5e8b32f:/.utils# pyspark --version
Welcome to
...

```

- закидываем в контейнер сертификат для доступа к моей кафке в я.облаке

```bash
vonbraun@VonBraun:~$ docker cp /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt d5911a381ff8:/YandexInternalRootCA.crt
...
Successfully copied 5.12kB to d5911a381ff8:/YandexInternalRootCA.crt

```

- ставим confluent_kafka

```bash
root@1edcb5e8b32f:/.utils# pip install confluent_kafka
Collecting confluent_kafka
...
```

- закидываем в контейнер скрипт для чтения 7К сообщений:

```bash
vonbraun@VonBraun:~$ docker cp /home/vonbraun/YA_DE/de-project-final/src/py/consume_stream.py d5911a381ff8:/consume_stream.py
...
Successfully copied 4.096kB to d5911a381ff8:/consume_stream.py

```

- запускаем

```bash
vonbraun@VonBraun:~$ docker exec -it d5911a381ff8 bash

root@1edcb5e8b32f:/.utils# python3 /consume_stream.py

```

Вот оно висит, ждёт сообщений.

Идём в рест-сервис практикума и просим наполнять очередь для меня.
(см. выше), и видим, что сообщения пришли и считаны на консоль

```bash
root@1edcb5e8b32f:/.utils# python3 /consume_stream.py
{'object_id': '0a642f30-de5f-5fdf-a2e9-a3f87c025848', 'object_type': 'CURRENCY', 'sent_dttm': '2022-10-01T00:00:00', 'payload': {'date_update': '2022-10-01 00:00:00', 'currency_code': 410, 'currency_code_with': 430, 'currency_with_div': 0.93}}
{'object_id': '31bf306d-0102-5a1f-8325-82ad16fefe15', 'object_type': 'CURRENCY', 'sent_dttm': '2022-10-01T00:00:00', 'payload': {'date_update': '2022-10-01 00:00:00', 'currency_code': 410, 'currency_code_with': 470, 'currency_with_div': 0.93}}
...
{'object_id': '2b4f85d6-6cac-4c71-8b2a-df13a7c2c8ad', 'object_type': 'TRANSACTION', 'sent_dttm': '2022-10-01T00:08:08', 'payload': {'operation_id': '2b4f85d6-6cac-4c71-8b2a-df13a7c2c8ad', 'account_number_from': 914810, 'account_number_to': 5871591, 'currency_code': 410, 'country': 'canada', 'status': 'done', 'transaction_type': 'sbp_outgoing', 'amount': -492625, 'transaction_dt': '2022-10-01 00:08:08'}}
{'object_id': '57ffda56-2e0c-43fe-a2f0-8fcb89705232', 'object_type': 'TRANSACTION', 'sent_dttm': '2022-10-01T00:07:44', 'payload': {'operation_id': '57ffda56-2e0c-43fe-a2f0-8fcb89705232', 'account_number_from': 903810, 'account_number_to': 2244989, 'currency_code': 420, 'country': 'usa', 'status': 'queued', 'transaction_type': 'c2a_incoming', 'amount': 400000, 'transaction_dt': '2022-10-01 00:07:44'}}
...
{'object_id': '8e27055d-de44-4dea-812b-98100d6537e2', 'object_type': 'TRANSACTION', 'sent_dttm': '2022-10-01T00:33:12', 'payload': {'operation_id': '8e27055d-de44-4dea-812b-98100d6537e2', 'account_number_from': 903810, 'account_number_to': 1192142, 'currency_code': 430, 'country': 'russia', 'status': 'done', 'transaction_type': 'c2a_incoming', 'amount': 530000, 'transaction_dt': '2022-10-01 00:33:12'}}
{'object_id': 'f4c4ebdb-81f2-4431-9459-8a106dec463c', 'object_type': 'TRANSACTION', 'sent_dttm': '2022-10-01T00:32:51', 'payload': {'operation_id': 'f4c4ebdb-81f2-4431-9459-8a106dec463c', 'account_number_from': 903810, 'account_number_to': 685446, 'currency_code': 430, 'country': 'russia', 'status': 'queued', 'transaction_type': 'c2a_incoming', 'amount': 150000, 'transaction_dt': '2022-10-01 00:32:51'}}
{'object_id': 'f4c4ebdb-81f2-4431-9459-8a106dec463c', 'object_type': 'TRANSACTION', 'sent_dttm': '2022-10-01T00:33:03', 'payload': {'operation_id': 'f4c4ebdb-81f2-4431-9459-8a106dec463c', 'account_number_from': 903810, 'account_number_to': 685446, 'currency_code': 430, 'country': 'russia', 'status': 'in_progress', 'transaction_type': 'c2a_incoming', 'amount': 150000, 'transaction_dt': '2022-10-01 00:33:03'}}
...

```

Отлично.
Мы получаем данные в поток, из потока, эта фаза настроена.

## 1. Приземляем стрим из Кафки в Постгрес в потоке, резидентным процессом со спарк-джобом

У нас два спринта были про Кафку: "Потоковая обработка данных" и "Облачные технологии".

В спринте "Потоковая обработка данных" мы делали спаркоджобы, которые крутились в контейнере и пробрасывали стрим в Постгрес.

Портируемся оттуда.

### Создаём схему и таблицы

Коннектимся к докерному постгресу через порт `15432`.
Ну и в DBeaver уже настроен на этот порт, его в нескольких спринтах использовали.

БД используем `de`.
Схему `stg` в ней создадим, и две таблицы.

```sql
vonbraun@VonBraun:~$ psql postgresql://jovyan:jovyan@localhost:15432/de
psql (14.6 (Ubuntu 14.6-0ubuntu0.22.04.1), server 13.8 (Ubuntu 13.8-1.pgdg20.04+1))
...

de=# \ds
Did not find any relations.
de=# \l
                                List of databases
    Name    |  Owner   | Encoding | Collate |  Ctype  |     Access privileges     
------------+----------+----------+---------+---------+---------------------------
 airflow_db | postgres | UTF8     | C.UTF-8 | C.UTF-8 | =Tc/postgres ...
 de         | jovyan   | UTF8     | C.UTF-8 | C.UTF-8 | 
 jovyan     | jovyan   | UTF8     | C.UTF-8 | C.UTF-8 | 
 metabase   | jovyan   | UTF8     | C.UTF-8 | C.UTF-8 | 
...
(7 rows)

de=# CREATE SCHEMA stg;
CREATE SCHEMA
de=# \dn
  List of schemas
  Name  |  Owner   
--------+----------
 public | postgres
 stg    | jovyan
(2 rows)

```

Структура таблиц в постгресе будет такая:

```sql
CREATE SCHEMA IF NOT EXISTS stg;

DROP TABLE IF EXISTS stg.currencies;
CREATE TABLE IF NOT EXISTS stg.currencies(
    "id" SERIAL PRIMARY KEY,
    "object_id" VARCHAR NOT NULL,
    "object_type" VARCHAR NOT NULL,
    "sent_dttm" TIMESTAMP NOT NULL,
    "payload" VARCHAR NOT NULL
)
;
CREATE INDEX IF NOT EXISTS stg_currencies_object_id_index
    ON ONLY stg.currencies (object_id)
;

DROP TABLE IF EXISTS stg.transactions;
CREATE TABLE IF NOT EXISTS stg.transactions(
    "id" SERIAL PRIMARY KEY,
    "object_id" VARCHAR NOT NULL,
    "object_type" VARCHAR NOT NULL,
    "sent_dttm" TIMESTAMP NOT NULL,
    "payload" VARCHAR NOT NULL
)
;
CREATE INDEX IF NOT EXISTS stg_transactions_object_id_index
    ON ONLY stg.transactions (object_id)
;

```

Уникальный ключ на `object_id` не вешаем, так как PySpark из коробки не умеет делать upsert.
Льём всё подряд, а дедупликацию будем делать на фазе переброса в стейдж Вертики.
По этому поводу индексируем `object_id`.

```sql
...
de=# \dt stg.*
           List of relations
 Schema |     Name     | Type  | Owner  
--------+--------------+-------+--------
 stg    | currencies   | table | jovyan
 stg    | transactions | table | jovyan
(2 rows)

```

### Копируем в контейнер и запускаем воркер `stage_stream.py`

На уровне спарка делаем стриму дедупликацию на ватермарк 5 минут:
```python
.dropDuplicates(['object_type', 'object_id']) \
.withWatermark('sent_dttm', '5 minutes')
```
Это снимет нагрузку с постгреса и облегчит дедупликацию при переброске в вертику.

Копируем воркера в контейнер

```bash
vonbraun@VonBraun:~$ docker cp /home/vonbraun/YA_DE/de-project-final/src/py/stage_stream.py 1edcb5e8b32f:/stage_stream.py
...
Successfully copied 5.12kB to d5911a381ff8:/stage_stream.py

```

Запускаем:

```bash
root@1edcb5e8b32f:/.utils# python3 /stage_stream.py

:: loading settings :: url = jar:file:/opt/spark/jars/ivy-2.5.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
...

====>
```
работает

Проверяем в постгресе:

```bash

de=# select count(*) from stg.currencies;
 count 
-------
  1100
(1 row)

de=# select count(*) from stg.transactions;
 count 
-------
   237
(1 row)

de=# select * from stg.currencies limit 1;

 id | object_id                            | object_type | sent_dttm           | payload
  1 | 0a642f30-de5f-5fdf-a2e9-a3f87c025848 | CURRENCY    | 2022-10-01 00:00:00 | {"date_update":"2022-10-01 00:00:00","currency_code":410,"currency_code_with":430,"currency_with_div":0.93}
(1 row)

de=# select * from stg.transactions limit 1;

 id | object_id                            | object_type | sent_dttm           | payload
  1 | fe2eccf0-b29e-4068-aefa-bc8726e9d85f | TRANSACTION | 2022-10-01 00:48:04 | {"operation_id":"fe2eccf0-b29e-4068-aefa-bc8726e9d85f","account_number_from":914810,"account_number_to":3213461,"currency_code":420,"country":"usa","status":"in_progress","transaction_type":"sbp_outgoing","amount":-100000,"transaction_dt":"2022-10-01 00:48:04"}
(1 row)

```

## Airflow

### dags_folder

```bash
root@1edcb5e8b32f:/opt/airflow# airflow config get-value core dags_folder
/lessons/dags
```

### DAG соотв. будем копировать сюда:

```bash
/lessons/dags/project_final.py
```

### Sql разместим в:

```bash
/lessons/sql
```

### либа для вертики

Идём в контейнер и исполняем

```bash
pip install apache-airflow-providers-vertica
```

## Стейджим суточную часть в Вертику айрфлоу-джобой раз в сутки за вчера

### Структура таблиц в STAGING

```sql
DROP TABLE IF EXISTS "SERGEI_BARANOVTUTBY__STAGING"."currencies";
DROP TABLE IF EXISTS "SERGEI_BARANOVTUTBY__STAGING"."transactions";

CREATE TABLE "SERGEI_BARANOVTUTBY__STAGING"."currencies" (
    object_id UUID PRIMARY KEY,
    sent_dttm TIMESTAMP(3),
    date_update TIMESTAMP(0),
    currency_code INTEGER,
    currency_code_with INTEGER,
    currency_with_div NUMERIC(8,2),
    CONSTRAINT currencies_pkey PRIMARY KEY (object_id)
)
ORDER BY date_update, object_id
SEGMENTED BY HASH(date_update, object_id) ALL NODES
PARTITION BY "date_update"::date
GROUP BY calendar_hierarchy_day("date_update"::date, 3, 2)
;

CREATE TABLE "SERGEI_BARANOVTUTBY__STAGING"."transactions" (
    object_id UUID PRIMARY KEY,
    sent_dttm TIMESTAMP(3),
    operation_id UUID,
    account_number_from INTEGER,
    account_number_to INTEGER,
    currency_code INTEGER,
    country VARCHAR(64),
    status VARCHAR(32),
    transaction_type VARCHAR(32),
    amount: INTEGER,
    transaction_dt: TIMESTAMP(0)
)
ORDER BY transaction_dt, object_id
SEGMENTED BY HASH(transaction_dt, object_id) ALL NODES
PARTITION BY "transaction_dt"::date
GROUP BY calendar_hierarchy_day("transaction_dt"::date, 3, 2)
;
```

это всё DAG сам сделает - см. в дире `sql` сиквельные скрипты

### Коннект к Вертике

В DAG-e в инит-таске заполняются переменные в Variables айрфлоу, потом они же и юзаются

### Коннект к Постгресу

DAG предполагает, что в Айрфлоу в Соединениях заведён коннекшн к постгресу, с именем `pg_connection` (там jovyan)

### Как будем загружать данные в Vertica

В спринте про Вертику мы брали данные в s3, и потом из файла лили в Верику.
Сейчас данные будут в постгресе, а не в s3, но поступим так же - последовательно для `currencies` и `transactions`:

- удаляем файл
- сливаем питоном постгрес в файл (в SELECT-е дедуплицируем записи по object_id, в запросе или питонячем обработчике раскладываем json на поля)
```
NB: если бы мы на предыдущем шаге датафрейм из Кафки раскладывали по полям в постгрес, то сейчас мы бы делали "psql ... -c 'copy (select ...) ...'" или что-то в этом роде.
Но мы на том шаге исходили из того, что важнее скорость "приземления" потока, а не удобство последующих шагов.

Но попробуем json-функции постгреса заюзать и слиться в файл через psycopg2 copy_expert()

как-то так запрос сформулируем:

WITH "cte_json" AS (
    SELECT "object_id", cast("payload" AS json) AS "payload_json" FROM "stg"."currencies"
)
SELECT DISTINCT ON ("object_id")
    "object_id",
    "payload_json"->>'date_update' AS "date_update",
    "payload_json"->>'currency_code' AS "currency_code"
FROM
    "cte_json"
WHERE
    ("payload_json"->>'date_update')::date = ('2022-10-03'::date - INTERVAL '1 day')::date
;
```
- транкейтим таблицу в Вертика (DROP_PARTITIONS(), вернее)
- заливаем файл через COPY ... FROM LOCAL ... REJECTED DATA AS TABLE ...

---

- Вопрос: Делаем это для всей таблицы?
ДАГ же будет запускаться на дату...
А постгрес у нас эмулирует поток...
  - Решение: DROP_PARTITIONS().
  выбираем из постгреса только данные за одну дату по `("payload"->>'date_update')::date` для `currencies` и `("payload"->>'transaction_dt')::date` для `transactions`,
  DISTINCT-им в SELECT-е по `ON (object_id)`, DROP_PARTITIONS() в Вертике, далее COPY FROM LOCAL.

Таким образом STG в Вертике переписывается данными за одну дату полностью, так как партиции у нас по дате, далее по SТG в DDS уже идёт `upsert` по `object_id`, и витрина - ещё далее - строится по DDS.

---

- Вопрос: зачищать ли постгрес?
  - Решение: в учебном проекте не будем, а в реальной жизни... Там например не будет постгреса, там из Кафки мы будем литься в Вертику, и будет своя логика...
  Но помечаем тут, что это не забывчивость, а сознательное упрощение учебного проекта :).

### Таск в даг-е

Делаем init(), далее параллелим стейджинг двух таблиц, далее пути сводим опять к общей таске

```python
    chain(
        init(),
        [dump_currencies(), dump_transactions()],
        [load_currencies(), load_transactions()],
        make_dwh(),
        make_cdm()
    )
```

данная фаза - это

```
...
        [dump_currencies(), dump_transactions()],
        [load_currencies(), load_transactions()],
...
```

### DAG-файл, сиквельные скрипты:

```bash
vonbraun@VonBraun:~$ \
docker cp \
/home/vonbraun/YA_DE/de-project-final/src/dags \
1edcb5e8b32f:/lessons
Successfully copied 7.68kB to 1edcb5e8b32f:/lessons

vonbraun@VonBraun:~$ \
docker cp \
/home/vonbraun/YA_DE/de-project-final/src/sql \
1edcb5e8b32f:/lessons
Successfully copied 3.584kB to 1edcb5e8b32f:/lessons

```

### Параметры DAG-а:

```python
from airflow.decorators import dag, task
# ...
@dag(
    default_args=default_args,
    start_date=pendulum.parse('2022-10-02'),
    end_date=pendulum.parse('2022-11-01'),
    catchup=True,
    schedule="@daily",
)

```

### ИТОГО

Поток из Кафки в Вертику стейджим через Постгрес (в pyspark jdbc write статичного датафрейма, котоырй образуется в foreachBatch), оттуда файлом, перед заливкой файла удаляем партицию.

Возможно как-то так мы могли сработать и из PySpark - сливать датафрейм статичный (батч) в файл, файл отправлять в Вертику.
Вопрос только в том, как ту таблицу пришлось бы партиционировать, чтобы заливать те батчи. Возможно никак, просто ДОливать. Но с этим всем посоветовано в ТЗ к финальному проекту прямо сейчас не заморачиваться.

## ~~Апдейтим DDS в Вертике раз в сутки, таской после стейджа~~

**ЭТО НЕ ТРЕБУЕТСЯ СОГЛАСНО ТЗ,**
НО Я **БЫ** ДЕЛАЛ ТАК
(полезно хотя бы для очистки стейджа констрайнтами):

### Core в __DWH Вертики - Снежинка:

#### факты - Транзакции

- transactions_done (transaction_operations) (только `done`, без аккаунтов `< 0`)
  - hk (из operation_id стейджа, он же object_id)
  - account_number_from INTEGER (FK accounts.account_number)
  - account_number_to INTEGER (FK accounts.account_number)
  - transaction_dt timestamp(0)
  - date (FK: dates.date)
  - currency_code (FK: currencies.currency_code)
  - country (FK: countries.country)
  - ~~status~~ - не требуется, берём только `done` (есть непонятки с `chargeback`, но тут то ли считать их в минус, то ли тоже учитывать как и `done`, я сделаю **допущение**, что их просто не надо учитывать)
  - type (FK: transaction_types.type)
  - amount INTEGER (в "копейках")
  - НЕ УВЕРЕН, так как у transaction_dt точночть до секунд: UNIQ (account_number_from, account_number_to, datetime, currency_code)

#### измерения

- accounts
  - hk_account_number INTEGER PRIMARY KEY, --  hash(account_number)
  - account_number INTEGER
- dates
  - hk_date INTEGER PRIMARY KEY, --  hash(date)
  - date DATE
- countries
  - hk_country INTEGER PRIMARY KEY, --  hash(country)
  - country VARCHAR(64)
- currencies
  - hk_currency INTEGER PRIMARY KEY, --  hash(currency_code)
  - currency_code INTEGER PRIMARY KEY
- transaction_types
  - hk_type INTEGER PRIMARY KEY, --  hash(type)
  - type VARCHAR(64)
- ~~transaction_statuses~~ - не требуется
  - ~~hk_status~~, --  hash(status)
  - ~~status~~ VARCHAR(64)
- transaction_accounts
  - hk_id INTEGER PRIMARY KEY, --  hash(id)
  - id INTEGER
- currecies_rates (currencies_updates) - SCD 1 (Перезапись по hk_rate)
  - hk_rate INTEGER PRIMARY KEY, --  hash(date, currency_code, currency_code_with)
  - date DATE
  - currency_code INTEGER
  - currency_code_with INTEGER
  - currency_with_div NUMERIC(8, 2)
  - CONSTRAINT currecies_rates_date_fkey FOREIGN KEY (date) REFERENCES dates(date),
  - CONSTRAINT currecies_rates_currency_code_fkey FOREIGN KEY (currency_code) REFERENCES currencies(currency_code),
  - CONSTRAINT currecies_rates_currency_code_with_fkey FOREIGN KEY (currency_code_with) REFERENCES currencies(currency_code),

### Таск в даг-е

данная фаза - это **была бы** make_dwh(),

```
...
        make_dwh(),  # pass
...
```

## Апдейтим витрину в Вертике раз в сутки, таской после Core

**по факту, как оказалось по ТЗ, сразу после стейджа, без DDS**

### Таск в даг-е

данная фаза - это make_cdm(),

```
...
        make_cdm(),
...
```

### DDL на витрину

**NB: Витрину global_metrics я сделал не в SERGEI_BARANOVTUTBY__DWH, а в SERGEI_BARANOVTUTBY**

- date_update — дата расчёта,
- currency_from — код валюты транзакции;
- amount_total — общая сумма транзакций по валюте в долларах;
- cnt_transactions — общий объём транзакций по валюте;
- avg_transactions_per_account — средний объём транзакций с аккаунта;
- cnt_accounts_make_transactions — количество уникальных аккаунтов с совершёнными транзакциями по валюте.

```sql
-- прописано в init.sql и исполняется на фазе init()
CREATE TABLE "SERGEI_BARANOVTUTBY"."global_metrics" (
    hk_global_metrics INTEGER PRIMARY KEY, -- hash(date_update, currency_from)
    date_update DATE NOT NULL,
    currency_from INTEGER NOT NULL,
    amount_total NUMERIC(18,2) NOT NULL, -- какие угодно мелкие валюты могут быть, иранские реалы, и вуаля
    cnt_transactions INTEGER NOT NULL,
    avg_transactions_per_account NUMERIC(12,4) NOT NULL,
    cnt_accounts_make_transactions INTEGER NOT NULL,
    load_dt DATETIME NOT NULL,
    load_src VARCHAR(32) NOT NULL
)
ORDER BY date_update, currency_from
SEGMENTED BY hk_global_metrics ALL NODES
PARTITION BY "date_update"::date
GROUP BY calendar_hierarchy_day("date_update"::date, 3, 2)
;
```

### Сиквел для инсерта

`src/sql/cdm.sql`

```sql
INSERT INTO SERGEI_BARANOVTUTBY.global_metrics
    (
        hk_global_metrics,
        date_update,
        currency_from,
        amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions
        load_dt,
        load_src
    )
WITH cte_currencies_420 AS (
    -- на дату расчёта ближайшие в прошлое курсы к доллару
    -- {anchor_date_s} например 2022-10-05
    (
        SELECT DISTINCT
            (FIRST_VALUE(currency_code) OVER w)      as currency_code,
            (FIRST_VALUE(currency_code_with) OVER w) as currency_code_with,
            (FIRST_VALUE(currency_with_div) OVER w)  as currency_with_div,
            (FIRST_VALUE(date_update) OVER w)::date  as date_update
        FROM
            "SERGEI_BARANOVTUTBY__STAGING"."currencies"
        WHERE
            currency_code_with = 420 -- usa dollar
            AND date_update::date <= '{anchor_date_s}'::date
        WINDOW w AS (
            PARTITION BY currency_code
            ORDER BY date_update DESC
        )
    )
    UNION ALL
    (
        SELECT
            420 as currency_code,
            420 as currency_code_with,
            1.0 as currency_with_div,
            '{anchor_date_s}'::date as date_update
    )
)
SELECT
    hash(t.transaction_dt::date, t.currency_code)      as hk_global_metrics
    t.transaction_dt::date                             as date_update,
    t.currency_code                                    as currency_from,
    SUM((t.amount * c.currency_with_div))              as amount_total,
    COUNT(*)                                           as cnt_transactions,
    (COUNT(*) / COUNT(DISTINCT t.account_number_from)) as avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from)              as cnt_accounts_make_transactions,
    now()                                              as load_dt,
    'project_final.py'                                 as load_src
FROM
    "SERGEI_BARANOVTUTBY__STAGING"."transactions" AS "t"
    INNER JOIN cte_currencies_420 AS "c" ON (
        "c"."currency_code" = "t"."currency_code"
        -- AND c.date_update::date = transaction_dt::date
    )
WHERE
    transaction_dt::date = '{anchor_date_s}'::date
    -- AND status = 'done'
    AND t.account_number_from > 0
    AND t.account_number_to > 0
    AND hash(t.transaction_dt::date, t.currency_code) NOT IN (
        SELECT hk_global_metrics FROM SERGEI_BARANOVTUTBY.global_metrics
    )
GROUP BY
    t.transaction_dt::date,
    t.currency_code
;
```

## Metabase

метабейз в докере от Практикума не видит Вертику.

из обсуждения однокашниками в Пачке делаем так: запускаем другой контейнер Метабйз, скармливаем ему драйвер Вертики

```bash
vonbraun@VonBraun:~/YA_DE$ mkdir /home/vonbraun/YA_DE/metabase_plugins
vonbraun@VonBraun:~/YA_DE$ 
```

в /home/vonbraun/YA_DE/metabase_plugins сливаем отсюда `https://my.vertica.com/download/vertica/client-drivers/` вот этот файл: `vertica-client-11.1.1-0.x86_64.tar.gz`

далее chmod-им на 777 на всякий случай, так как "Note that Metabase will use this directory to extract plugins bundled with the default Metabase distribution (such as drivers for various databases such as SQLite), thus it must be readable and writable by Docker." (`https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker#adding-external-dependencies-or-plugins`)

```bash
vonbraun@VonBraun:~/YA_DE$ chmod -R 0777 /home/vonbraun/YA_DE/metabase_plugins
vonbraun@VonBraun:~/YA_DE$ ls -lah /home/vonbraun/YA_DE/metabase_plugins
total 41M
drwxrwxrwx  2 vonbraun vonbraun 4,0K мар  8 21:03 .
drwxrwxr-x 27 vonbraun vonbraun 4,0K мар  8 20:41 ..
-rwxrwxrwx  1 vonbraun vonbraun  41M мар  8 20:44 vertica-client-11.1.1-0.x86_64.tar.gz
```

Далее запускаем докера с Метабейзом

```bash
docker run -d -p 3000:3000 --mount type=bind,source=/home/vonbraun/YA_DE/metabase_plugins,destination=/plugins --name metabase metabase/metabase
```

Далее опять http://localhost:3000/setup и т.п. - НЕТ ТАМА ВЕРТИКИ

смотрим логи контейнера

```bash
vonbraun@VonBraun:~/YA_DE$ docker logs 82eabc1240a3
...
2023-03-08 18:20:43,564 INFO metabase.plugins :: Loading plugins in /plugins...
2023-03-08 18:20:43,665 INFO util.files :: Extract file /modules/sparksql.metabase-driver.jar -> /plugins/sparksql.metabase-driver.jar
2023-03-08 18:20:43,710 INFO util.files :: Extract file /modules/sqlserver.metabase-driver.jar -> /plugins/sqlserver.metabase-driver.jar
2023-03-08 18:20:43,717 INFO util.files :: Extract file /modules/presto-jdbc.metabase-driver.jar -> /plugins/presto-jdbc.metabase-driver.jar
2023-03-08 18:20:43,760 INFO util.files :: Extract file /modules/sqlite.metabase-driver.jar -> /plugins/sqlite.metabase-driver.jar
2023-03-08 18:20:43,781 INFO util.files :: Extract file /modules/vertica.metabase-driver.jar -> /plugins/vertica.metabase-driver.jar
...

2023-03-08 18:20:44,104 INFO plugins.dependencies :: Metabase cannot initialize plugin Metabase Vertica Driver due to required dependencies. Metabase requires the Vertica JDBC driver in order to connect to Vertica databases, but we can't ship it as part of Metabase due to licensing restrictions. See https://metabase.com/docs/latest/administration-guide/databases/vertica.html for more details.
...
```

идём в https://www.metabase.com/docs/latest/databases/connections/vertica

видим, что
- во-первых, с некотрой долей вероятности маунтить надо диру `/app/plugins` контейнера, а не `/plugins`
- jar-ник надо назвать не vertica-client-11.1.1-0.x86_64.tar.gz, а vertica-jdbc-8.0.0-0.jar - ну за исключением версии

Смотрим, что у меня в /home/vonbraun/YA_DE/metabase_plugins - с маунто всё норм, Метабейз накидала туда кучу jar-ников.

Останавливаем еонтейнер, и кидаем тула не .tar.gz, а выгребаем из него .jar
(`vertica-jdbc-11.1.1-0.jar`)

Стартуем контейнер.

**УРА! Vertica теперь есть в списке на `http://localhost:3000/admin/databases/create`**


### Дашборд

Что там не так:

Сервис Практикума льёт мне в Кафку транзакции как-то... не за все даты :).

Соотв. графики "такие себе".

`http://localhost:3000/dashboard/1-de-final`

1. Без применённых фильтров (дефалтные - весь месяц, все валюты)

```
src/img/dashboard1.png
```

![без фильтров](src/img/dashboard1.png)

2. С фильтрами

```
src/img/dashboard2.png
```

![с фильтрами](src/img/dashboard2.png)


### Графики

1. `http://localhost:3000/question/1-summy-perevodov-v-raznyh-valyuta`

суммы переводов в разных валютах - общий объём транзакций по валюте - `cnt_transactions`

```
src/img/cnt_transactions.png
```

![общий объём транзакций по валюте](src/img/cnt_transactions.png)

2. `http://localhost:3000/question/2-srednee-kolichestva-tranzakcij-na-pol-zovatel`

среднее количества транзакций на пользователя - средний объём транзакций с аккаунта - `avg_transactions_per_account`

```
src/img/avg_transactions_per_account.png
```

![среднее количества транзакций на пользователя](src/img/avg_transactions_per_account.png)

3. `http://localhost:3000/question/3-kolichestvo-unikal-nyh-pol-zovatelej-kotorye-sovershayut-tranzakcii-v-valyu`

количество уникальных пользователей, которые совершают транзакции в валютах - количество уникальных аккаунтов с совершёнными транзакциями по валюте - `cnt_accounts_make_transactions`

```
src/img/cnt_accounts_make_transactions.png
```

![количество уникальных пользователей, которые совершают транзакции в валютах](src/img/cnt_accounts_make_transactions.png)

4. `http://localhost:3000/question/4-obshij-oborot-kompanii-v-edinoj-valyu`

общий оборот компании в единой валюте - общая сумма транзакций по валюте в долларах - `amount_total`

```
src/img/amount_total.png
```

![общий оборот компании в единой валюте](src/img/amount_total.png)


---

Как-то так...

---
---
---


# III. Исходный readme

Я делал проект ещё до того как получил доступ к репе, и я по итогу всё же не стал бить DAG на две штуки:
**мне кажется, я нормально реализовал одним DAG-ом отработку по дням**.

### Структура репозитория
Файлы в репозитории будут использоваться для проверки и обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре: так будет проще соотнести задания с решениями.

Внутри `src` расположены папки:
- `/src/dags` - вложите в эту папку код DAG, который поставляет данные из источника в хранилище. Назовите DAG `1_data_import.py`. Также разместите здесь DAG, который обновляет витрины данных. Назовите DAG `2_datamart_update.py`.
- `/src/sql` - сюда вложите SQL-запрос формирования таблиц в `STAGING`- и `DWH`-слоях, а также скрипт подготовки данных для итоговой витрины.
- `/src/py` - если источником вы выберете Kafka, то в этой папке разместите код запуска генерации и чтения данных в топик.
- `/src/img` - здесь разместите скриншот реализованного над витриной дашборда.
