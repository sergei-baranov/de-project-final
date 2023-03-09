import logging
import os
from datetime import datetime, timedelta
import string
from contextlib import closing
import psycopg2
# from psycopg2.extensions import register_adapter, AsIs
from airflow.models.connection import Connection
import vertica_python
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
# get_current_context не нужен, декораторы транслируют контекст в именованные аргументы тасков,
# но оставим закомментированным, чтобы когда надо вспомнить
# from airflow.operators.python import get_current_context
from airflow.models.baseoperator import chain  # https://docs.astronomer.io/learn/managing-dependencies

# я не смог заставить Airflow увидеть модуль ни в /lessons/dags,
# ни в /lessons/plugins, хотя root@1edcb5e8b32f:/.utils# airflow info показал
# python_path | /usr/local/bin:/usr/lib/python39.zip:/usr/lib/python3.9:/usr/lib/python3.9/lib-dynload:/usr/local/lib/python3.9/
#             | dist-packages:/usr/lib/python3/dist-packages:/lessons/dags:/opt/airflow/config:/lessons/plugins
# from project_final.PgRepository import PgRepository
# from project_final.VerticaRepository import VerticaRepository

# copy_expert():
# хорошая ссылка, и прям от айрфлоу:
# https://airflow.apache.org/docs/apache-airflow-providers-postgres/2.2.0/_modules/airflow/providers/postgres/hooks/postgres.html
class PgRepository:
    pg_conn: Connection = None
    pg_conn_args: dict = {}

    def __init__(self, pg_conn: Connection) -> None:
        self.pg_conn = pg_conn
        self.pg_conn_args = {
            'dbname': pg_conn.schema,
            'user': pg_conn.login,
            'password': pg_conn.password,
            'host': pg_conn.host,
            'port': pg_conn.port
        }

    def dump_transactions(self, sql_path: string, anchor_date_s: string, log: logging.Logger):
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
            with closing(psycopg2.connect(**self.pg_conn_args)) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql_copy, file)
                    file.truncate(file.tell())
                    conn.commit()

        file_stats = os.stat(save_file_path)
        bytes_size = file_stats.st_size
        log.info("dump_transactions into file '{0}' is done ({1} Bytes)".format(
            save_file_path, str(bytes_size)
        ))

    def dump_currencies(self, sql_path: string, anchor_date_s: string, log: logging.Logger):
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
            with closing(psycopg2.connect(**self.pg_conn_args)) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql_copy, file)
                    file.truncate(file.tell())
                    conn.commit()

        file_stats = os.stat(save_file_path)
        bytes_size = file_stats.st_size
        log.info("dump_currencies into file '{0}' is done ({1} Bytes)".format(
            save_file_path, str(bytes_size)
        ))


class VerticaRepository:
    vertica_conn: Connection = None
    conn_info_vertica: dict = {}

    def __init__(self, vertica_conn: Connection) -> None:
        self.vertica_conn = vertica_conn
        self.conn_info_vertica = {
            'host': vertica_conn.host,
            'port': vertica_conn.port,
            'user': vertica_conn.login,
            'password': vertica_conn.password,
            'database': vertica_conn.schema,
            'autocommit': False
        }

    def exec_sql(self, sql: string):
        with vertica_python.connect(**self.conn_info_vertica) as conn, conn.cursor() as curs:
            curs.execute(sql)
            conn.commit()
            curs.close()
            conn.close()

    def make_init_sql(self, sql_path: string):
        """
        исполняет init.sql, там DDL на создание таблиц в Vertica IF NOT EXISTS
        """
        with open(sql_path + '/init.sql') as file:
            init_query = file.read()
        self.exec_sql(init_query)

    def load_currencies(self, sql_path: string, anchor_date_s: string, log: logging.Logger):
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
        self.exec_sql(sql_truncate)

        sql_statement = """
COPY {0}
FROM LOCAL '{1}' DELIMITER ','
REJECTED DATA AS TABLE {2}
;
        """.format(table_path, dump_path, table_rejected_path)
        self.exec_sql(sql_statement)
        log.info("load staged file {0} into __STAGING table {1} is done".format(
            dump_path, table_path
        ))
        # не совсем понятно, надо ли файлы сохранять или чистить,
        # и на какую глубину хранить...
        # для учебного проекта чистить не будем вовсе, просто фиксанём, что надо

    def load_transactions(self, sql_path: string, anchor_date_s: string, log: logging.Logger):
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

        self.exec_sql(sql_truncate)
        sql_statement = """
COPY {0}
FROM LOCAL '{1}' DELIMITER ','
REJECTED DATA AS TABLE {2}
;
        """.format(table_path, dump_path, table_rejected_path)

        self.exec_sql(sql_statement)
        log.info("load staged file {0} into __STAGING table {1} is done".format(
            dump_path, table_path
        ))
        # не совсем понятно, надо ли файлы сохранять или чистить,
        # и на какую глубину хранить...
        # для учебного проекта чистить не будем вовсе, просто фиксанём, что надо

    def make_cdm_sql(self, sql_path: string, anchor_date_s: string):
        """
        удаляет из витрины партицию на дату,
        затем исполняет cdm.sql, там DDL на добавление новых записей в витрину
        """

        sql_truncate = """
SELECT DROP_PARTITIONS(
    'SERGEI_BARANOVTUTBY.global_metrics',
    '{anchor_date_s}',
    '{anchor_date_s}'
)
;
        """.format(anchor_date_s=anchor_date_s)
        self.exec_sql(sql_truncate)

        with open(sql_path + '/cdm.sql') as file:
            insert_query = file.read()
            insert_query = insert_query.format(anchor_date_s=anchor_date_s)
            self.exec_sql(insert_query)


log = logging.getLogger(__name__)

pg_repository = PgRepository(BaseHook.get_connection('pg_connection'))
vertica_repository = VerticaRepository(BaseHook.get_connection('vertica_connection'))


def fill_init_vars():
    """
    заполняет Variable Airflow:
    sql_path (путь к дире с файлами sql-запросов)
    """

    # Fill sql_path variable:
    curr_dir_path = os.path.dirname(os.path.abspath(__file__))
    log.info("curr_dir_path is: '" + str(curr_dir_path) + "'")
    sql_path = os.path.abspath(curr_dir_path + '/../sql')
    log.info("filled sql_path var: '" + str(sql_path) + "'")
    Variable.set('sql_path', sql_path)


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
        vertica_repository.make_init_sql(Variable.get('sql_path'))

    @task(task_id='dump_currencies')
    def dump_currencies(execution_date: datetime = None):
        pg_repository.dump_currencies(
            Variable.get('sql_path'),
            (execution_date - timedelta(days=1)).strftime("%Y-%m-%d"),
            log
        )

    @task(task_id='load_currencies')
    def load_currencies(execution_date: datetime = None):
        vertica_repository.load_currencies(
            Variable.get('sql_path'),
            (execution_date - timedelta(days=1)).strftime("%Y-%m-%d"),
            log
        )

    @task(task_id='dump_transactions')
    def dump_transactions(execution_date: datetime = None):
        pg_repository.dump_transactions(
            Variable.get('sql_path'),
            (execution_date - timedelta(days=1)).strftime("%Y-%m-%d"),
            log
        )

    @task(task_id='load_transactions')
    def load_transactions(execution_date: datetime = None):
        vertica_repository.load_transactions(
            Variable.get('sql_path'),
            (execution_date - timedelta(days=1)).strftime("%Y-%m-%d"),
            log
        )

    @task(task_id='make_dwh')
    def make_dwh(execution_date: datetime = None):
        pass  # по тз оказалось, что ядро dwh делать не надо

    @task(task_id='make_cdm')
    def make_cdm(execution_date: datetime = None):
        vertica_repository.make_cdm_sql(
            Variable.get('sql_path'),
            (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
        )

    chain(
        init(),
        [dump_currencies(), dump_transactions()],
        [load_currencies(), load_transactions()],
        make_dwh(),  # pass
        make_cdm()
    )


_ = project_final()
