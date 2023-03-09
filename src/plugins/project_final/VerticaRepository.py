import string
import os
from contextlib import closing
import logging
import vertica_python
from airflow.models.connection import Connection


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
