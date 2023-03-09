import string
import os
from contextlib import closing
import logging
import psycopg2
# from psycopg2.extensions import register_adapter, AsIs
from airflow.models.connection import Connection

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
