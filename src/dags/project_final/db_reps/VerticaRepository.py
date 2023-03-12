import string
import os
from contextlib import closing
import logging
import vertica_python
from airflow.models.connection import Connection


class VerticaRepository:
    TABLE_STAGE_CURRENCIES = 'SERGEI_BARANOVTUTBY__STAGING.currencies'
    TABLE_STAGE_CURRENCIES_REJECTED = 'SERGEI_BARANOVTUTBY__STAGING.currencies_rej'
    TABLE_STAGE_TRANSACTIONS = 'SERGEI_BARANOVTUTBY__STAGING.transactions'
    TABLE_STAGE_TRANSACTIONS_REJECTED = 'SERGEI_BARANOVTUTBY__STAGING.transactions_rej'
    TABLE_CDM_GLOBAL_METRICS = 'SERGEI_BARANOVTUTBY.global_metrics'

    vertica_conn: Connection = None
    conn_info_vertica: dict = {}
    sql_path: string
    anchor_date_s: string

    def __init__(self,
                 vertica_conn: Connection,
                 sql_path: string) -> None:

        log = logging.getLogger(__name__)

        is_dir = os.path.isdir(sql_path)
        if is_dir is False:
            err_str = f"Existing dir path expecting in the sql_path arg, but {sql_path} given"
            log.error(err_str)
            raise ValueError(err_str)
        self.sql_path = sql_path

        dict_vars = vars(vertica_conn)
        check_list = ['host', 'port', 'login', 'schema']
        for check_value in check_list:
            if (
                   check_value not in dict_vars
                   or bool(dict_vars[check_value]) is False
               ):
                err_str = f"Nonempty Connection.{check_value} member expecting"
                log.error(err_str)
                raise ValueError(err_str)
        if bool(vertica_conn.password) is False:
            err_str = f"Connection.password member expecting"
            log.error(err_str)
            raise ValueError(err_str)

        self.vertica_conn = vertica_conn
        self.conn_info_vertica = {
            'host': vertica_conn.host,
            'port': vertica_conn.port,
            'user': vertica_conn.login,
            'password': vertica_conn.password,
            'database': vertica_conn.schema,
            'autocommit': False
        }

        try:
            with closing(vertica_python.connect(**self.conn_info_vertica)) as conn:
                pass
        except vertica_python.errors.ConnectionError as e:
            # просто переподнимаю... по сути мог и не трай-кетчить, НО:
            # возможно в таких случаях принято что-то дописывать от себя в текст ексепшна;
            # и до кучи в лог напишем
            # log.error("Connection error: %s" % e)
            log.error(f"Test connection error: {e}")
            raise e

    def exec_sql(self, sql: string):
        with closing(vertica_python.connect(**self.conn_info_vertica)) as conn:
            with closing(conn.cursor()) as curs:
                curs.execute(sql)
                conn.commit()

    def make_init_sql(self):
        """
        исполняет init.sql, там DDL на создание таблиц в Vertica IF NOT EXISTS
        """
        with open(self.sql_path + '/init.sql') as file:
            init_query = file.read()
        self.exec_sql(init_query)

    def __load_table(self,
                     anchor_date_s: string,
                     dump_file_path: string,
                     table_name: string,
                     rejected_table_name: string):
        # создаём файл
        bytes_size = os.path.getsize(dump_file_path)
        if bytes_size < 1:
            return

        # удаляем партицию по дате
        sql_truncate = f"""
SELECT DROP_PARTITIONS(
    '{table_name}',
    '{anchor_date_s}',
    '{anchor_date_s}'
)
;
        """
        self.exec_sql(sql_truncate)

        # заливаем данные из файла (на дату)
        sql_statement = f"""
COPY {table_name}
FROM LOCAL '{dump_file_path}' DELIMITER ','
REJECTED DATA AS TABLE {rejected_table_name}
;
        """
        self.exec_sql(sql_statement)

        log = logging.getLogger(__name__)
        log.info(f"load staged file {dump_file_path} into table {table_name} is done")
        #  если будет надо удалять файлы - то вот в этом месте кода

    def load_currencies(self, anchor_date_s: string):
        self.__load_table(anchor_date_s,
                          f"{self.sql_path}/data/dump_currencies.{anchor_date_s}.csv",
                          self.TABLE_STAGE_CURRENCIES,
                          self.TABLE_STAGE_CURRENCIES_REJECTED)

    def load_transactions(self, anchor_date_s: string):
        self.__load_table(anchor_date_s,
                          f"{self.sql_path}/data/dump_transactions.{anchor_date_s}.csv",
                          self.TABLE_STAGE_TRANSACTIONS,
                          self.TABLE_STAGE_TRANSACTIONS_REJECTED)

    def make_cdm_sql(self, anchor_date_s: string):
        """
        удаляет из витрины партицию на дату,
        затем исполняет cdm.sql, там DDL на добавление новых записей в витрину
        """

        # удаляем партицию по дате
        sql_truncate = f"""
SELECT DROP_PARTITIONS(
    '{self.TABLE_CDM_GLOBAL_METRICS}',
    '{anchor_date_s}',
    '{anchor_date_s}'
)
;
        """
        self.exec_sql(sql_truncate)

        # в скрипте сиквел, который делает insert select
        with open(self.sql_path + '/cdm.sql') as file:
            insert_query = file.read()
            insert_query = insert_query.format(anchor_date_s=anchor_date_s)
            self.exec_sql(insert_query)
