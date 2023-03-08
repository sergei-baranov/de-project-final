import string
import logging
import os
import string
from contextlib import closing
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
# get_current_context не нужен, декораторы транслируют контекст в именованные аргументы тасков,
# но оставим закомментированным, чтобы когда надо вспомнить
# from airflow.operators.python import get_current_context
from airflow.models.baseoperator import chain  # https://docs.astronomer.io/learn/managing-dependencies

import psycopg2
# from psycopg2.extensions import register_adapter, AsIs

import vertica_python

log = logging.getLogger(__name__)

# copy_expert():
# хорошая ссылка, и прям от айрфлоу:
# https://airflow.apache.org/docs/apache-airflow-providers-postgres/2.2.0/_modules/airflow/providers/postgres/hooks/postgres.html


conn_info_vertica = {
    'host': '51.250.75.20',
    'port': '5433',
    'user': '***',
    'password': '***',
    'database': 'dwh',
    'autocommit': False
}

pg_conn = BaseHook.get_connection('pg_connection')
pg_conn_args = {
    'dbname': pg_conn.schema,
    'user': pg_conn.login,
    'password': pg_conn.password,
    'host': pg_conn.host,
    'port': pg_conn.port
}


def exec_sql_vertica(sql: string):
    global conn_info_vertica
    conn_info_vertica['user'] = Variable.get('baranov_vertica_uname')
    conn_info_vertica['password'] = Variable.get('baranov_vertica_password')
    # log.info(pprint.pformat(conn_info_vertica))
    with vertica_python.connect(**conn_info_vertica) as conn, conn.cursor() as curs:
        curs.execute(sql)
        conn.commit()
        curs.close()
        conn.close()


def fill_init_vars():
    """
    заполняет Variable Airflow:
    credentials для conn_info_vertica в Variables,
    sql_path (путь к дире с файлами sql-запросов)
    """

    # Fill creds into conn_info_vertica:
    # как бы мы знаем, что credintials в коде хранить нельзя,
    # но как бы мы понимаем, что у ревьювера нет моих credintials,
    # и мне их таки надо подсунуть в Variable программно.

    # ответвление по поводу самим же и заполнить переменные,
    # которые читаем
    uname = 'SERGEI_BARANOVTUTBY'
    pwd = '5YlE0nqGcpEVJVq'
    vertica_uname = None
    try:
        vertica_uname = Variable.get('baranov_vertica_uname')
    except Exception as e:
        Variable.set('baranov_vertica_uname', uname)
    vertica_password = None
    try:
        vertica_password = Variable.get('baranov_vertica_password')
    except Exception as e:
        Variable.set('baranov_vertica_password', pwd)

    vertica_uname = Variable.get('baranov_vertica_uname')
    if vertica_uname is None:
         Variable.set('baranov_vertica_uname', uname)
    vertica_password = Variable.get('baranov_vertica_password')
    if vertica_password is None:
         Variable.set('baranov_vertica_password', pwd)

    # нормальная логика
    global conn_info_vertica
    conn_info_vertica['user'] = Variable.get('baranov_vertica_uname')
    conn_info_vertica['password'] = Variable.get('baranov_vertica_password')

    # почему-то не хочет log.info форматировать сама, через *args, форматирую тут и далее явно
    log.info("filled conn_info_vertica creds: 'user' len is '{0}', 'password' len is '{1}'".format(
        str(len(conn_info_vertica['user'])), str(len(conn_info_vertica['password']))
    ))

    # Fill sql_path variable:
    curr_dir_path = os.path.dirname(os.path.abspath(__file__))
    log.info("curr_dir_path is: '" + str(curr_dir_path) + "'")
    sql_path = os.path.abspath(curr_dir_path + '/../sql')
    log.info("filled sql_path var: '" + str(sql_path) + "'")
    Variable.set('sql_path', sql_path)


# сам себе: docker cp \
# /home/vonbraun/YA_DE/Выпускной_проект/ya-de-project-final/lessons/sql/init.sql \
# 1edcb5e8b32f:/lessons/sql/init.sql
# docker cp \
# /home/vonbraun/YA_DE/Выпускной_проект/ya-de-project-final/lessons/sql \
# 1edcb5e8b32f:/lessons/sql
def make_init_sql():
    """
    исполняет init.sql, там DDL на создание таблиц в Vertica IF NOT EXISTS
    """
    sql_path = Variable.get('sql_path')
    with open(sql_path + '/init.sql') as file:
        init_query = file.read()
        exec_sql_vertica(init_query)

def make_cdm_sql(execution_date: datetime):
    """
    удаляет из витрины партицию на дату,
    затем исполняет cdm.sql, там DDL на добавление новых записей в витрину
    """
    anchor_date = execution_date - timedelta(days=1)
    anchor_date_s = anchor_date.strftime("%Y-%m-%d")

    sql_truncate = """
SELECT DROP_PARTITIONS(
    'SERGEI_BARANOVTUTBY.global_metrics',
    '{anchor_date_s}',
    '{anchor_date_s}'
)
;
    """.format(anchor_date_s=anchor_date_s)
    exec_sql_vertica(sql_truncate)

    sql_path = Variable.get('sql_path')
    with open(sql_path + '/cdm.sql') as file:
        insert_query = file.read()
        insert_query = insert_query.format(anchor_date_s=anchor_date_s)
        exec_sql_vertica(insert_query)


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


# schedule_interval='0 0 * * *',  # @daily	Run once a day at midnight	0 0 * * *
@dag(
    default_args=default_args,
    start_date=pendulum.parse('2022-10-02'),
    end_date=pendulum.parse('2022-11-01'),
    catchup=True,
    schedule="@daily",
)
def project_final():
    # the so called context objects, are directly accesible in task-decorated functions.
    # This means that there is no need to import get_current_context anymore.
    # The context objects are accesible just by declaring the parameterss in the task signature
    @task(task_id='init')
    def init(execution_date: datetime = None):
        fill_init_vars()
        make_init_sql()

    @task(task_id='dump_currencies')
    def dump_currencies(execution_date: datetime = None):
        anchor_date = execution_date - timedelta(days=1)
        anchor_date_s = anchor_date.strftime("%Y-%m-%d")

        sql_path = Variable.get('sql_path')
        dump_dir = sql_path + '/data'  # has leading slash, has no trailing slash

        is_exist = os.path.exists(dump_dir)
        if not is_exist:
            os.makedirs(dump_dir)

        dump_file = 'dump_currencies.{0}.csv'.format(anchor_date_s)
        save_file_path = dump_dir + '/' + dump_file
        if not os.path.isfile(save_file_path):
            with open(save_file_path, 'w'):
                pass

        with open(sql_path + '/dump_currencies.sql') as file:
            sql_dump = file.read()
        sql_dump = sql_dump.format(anchor_date_s)
        sql_copy = "COPY ({0}) TO STDOUT WITH CSV DELIMITER ','".format(sql_dump)

        with open(save_file_path, 'w') as file:
            with closing(psycopg2.connect(**pg_conn_args)) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql_copy, file)
                    file.truncate(file.tell())
                    conn.commit()

        file_stats = os.stat(save_file_path)
        bytes_size = file_stats.st_size
        log.info("dump_currencies into file '{0}' is done ({1} Bytes)".format(
            save_file_path, str(bytes_size)
        ))

    @task(task_id='load_currencies')
    def load_currencies(execution_date: datetime = None):
        anchor_date = execution_date - timedelta(days=1)
        anchor_date_s = anchor_date.strftime("%Y-%m-%d")

        sql_path = Variable.get('sql_path')
        dump_dir = sql_path + '/data'  # has leading slash, has no trailing slash
        dump_file = 'dump_currencies.{0}.csv'.format(anchor_date_s)
        dump_path = dump_dir + '/' + dump_file

        file_stats = os.stat(dump_path)
        bytes_size = file_stats.st_size
        if bytes_size < 1:
            return

        schema = 'SERGEI_BARANOVTUTBY__STAGING'
        table = 'currencies'
        table_path = '"{0}"."{1}"'.format(schema, table)
        table_rejected = 'currencies_rej'
        table_rejected_path = '"{0}"."{1}"'.format(schema, table_rejected)
        # транкейтить бужем только партицию по дате
        # sql_truncate = 'TRUNCATE TABLE "' + schema + '"."' + table + '";'
        sql_truncate = """
SELECT DROP_PARTITIONS(
    '{0}.{1}',
    '{2}',
    '{2}'
)
;
        """.format(schema, table, anchor_date_s)
        exec_sql_vertica(sql_truncate)
        sql_statement = """
COPY {0}
FROM LOCAL '{1}' DELIMITER ','
REJECTED DATA AS TABLE {2}
;
        """.format(table_path, dump_path, table_rejected_path)
        exec_sql_vertica(sql_statement)
        log.info("load staged file {0} into __STAGING table {1} is done".format(
            dump_path, table_path
        ))
        # не совсем понятно, надо ли файлы сохранять или чистить,
        # и на какую глубину хранить...
        # для учебного проекта чистить не будем вовсе, просто фиксанём, что надо

    @task(task_id='dump_transactions')
    def dump_transactions(execution_date: datetime = None):
        anchor_date = execution_date - timedelta(days=1)
        anchor_date_s = anchor_date.strftime("%Y-%m-%d")

        sql_path = Variable.get('sql_path')
        dump_dir = sql_path + '/data'  # has leading slash, has no trailing slash

        is_exist = os.path.exists(dump_dir)
        if not is_exist:
            os.makedirs(dump_dir)

        dump_file = 'dump_transactions.{0}.csv'.format(anchor_date_s)
        save_file_path = dump_dir + '/' + dump_file
        if not os.path.isfile(save_file_path):
            with open(save_file_path, 'w'):
                pass

        with open(sql_path + '/dump_transactions.sql') as file:
            sql_dump = file.read()
        sql_dump = sql_dump.format(anchor_date_s)
        sql_copy = "COPY ({0}) TO STDOUT WITH CSV DELIMITER ','".format(sql_dump)

        with open(save_file_path, 'w') as file:
            with closing(psycopg2.connect(**pg_conn_args)) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql_copy, file)
                    file.truncate(file.tell())
                    conn.commit()

        file_stats = os.stat(save_file_path)
        bytes_size = file_stats.st_size
        log.info("dump_transactions into file '{0}' is done ({1} Bytes)".format(
            save_file_path, str(bytes_size)
        ))

    @task(task_id='load_transactions')
    def load_transactions(execution_date: datetime = None):
        anchor_date = execution_date - timedelta(days=1)
        anchor_date_s = anchor_date.strftime("%Y-%m-%d")

        sql_path = Variable.get('sql_path')
        dump_dir = sql_path + '/data'  # has leading slash, has no trailing slash
        dump_file = 'dump_transactions.{0}.csv'.format(anchor_date_s)
        dump_path = dump_dir + '/' + dump_file

        file_stats = os.stat(dump_path)
        bytes_size = file_stats.st_size
        if bytes_size < 1:
            return

        schema = 'SERGEI_BARANOVTUTBY__STAGING'
        table = 'transactions'
        table_path = '"{0}"."{1}"'.format(schema, table)
        table_rejected = 'transactions_rej'
        table_rejected_path = '"{0}"."{1}"'.format(schema, table_rejected)
        # транкейтить бужем только партицию по дате
        # sql_truncate = 'TRUNCATE TABLE "' + schema + '"."' + table + '";'
        sql_truncate = """
SELECT DROP_PARTITIONS(
           '{0}.{1}',
           '{2}',
           '{2}'
)
;
        """.format(schema, table, anchor_date_s)
        exec_sql_vertica(sql_truncate)
        sql_statement = """
COPY {0}
FROM LOCAL '{1}' DELIMITER ','
REJECTED DATA AS TABLE {2}
;
        """.format(table_path, dump_path, table_rejected_path)
        exec_sql_vertica(sql_statement)
        log.info("load staged file {0} into __STAGING table {1} is done".format(
            dump_path, table_path
        ))
        # не совсем понятно, надо ли файлы сохранять или чистить,
        # и на какую глубину хранить...
        # для учебного проекта чистить не будем вовсе, просто фиксанём, что надо

    @task(task_id='make_dwh')
    def make_dwh(execution_date: datetime = None):
        pass  # по тз оказалось, что ядро dwh делать не надо

    @task(task_id='make_cdm')
    def make_cdm(execution_date: datetime = None):
        make_cdm_sql(execution_date)

    chain(
        init(),
        [dump_currencies(), dump_transactions()],
        [load_currencies(), load_transactions()],
        make_dwh(),  # pass
        make_cdm()
    )


_ = project_final()
