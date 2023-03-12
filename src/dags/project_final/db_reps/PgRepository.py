import string
import os
from contextlib import closing
import logging
import psycopg2
from airflow.models.connection import Connection


class PgRepository:
    pg_conn: Connection = None
    pg_conn_args: dict = {}
    sql_path: string

    def __init__(self,
                 pg_conn: Connection,
                 sql_path: string) -> None:

        log = logging.getLogger(__name__)

        is_dir = os.path.isdir(sql_path)
        if is_dir is False:
            err_str = f"Existing dir path expecting in the sql_path arg, but {sql_path} given"
            log.error(err_str)
            raise ValueError(err_str)
        self.sql_path = sql_path

        dict_vars = vars(pg_conn)
        check_list = ['host', 'port', 'login', 'schema']
        for check_value in check_list:
            if (
                   check_value not in dict_vars
                   or bool(dict_vars[check_value]) is False
               ):
                err_str = f"Nonempty Connection.{check_value} member expecting"
                log.error(err_str)
                raise ValueError(err_str)
        if bool(pg_conn.password) is False:
            err_str = f"Connection.password member expecting"
            log.error(err_str)
            raise ValueError(err_str)

        self.pg_conn = pg_conn
        self.pg_conn_args = {
            'dbname': pg_conn.schema,
            'user': pg_conn.login,
            'password': pg_conn.password,
            'host': pg_conn.host,
            'port': pg_conn.port
        }

        try:
            with closing(psycopg2.connect(**self.pg_conn_args)) as conn:
                pass
        except psycopg2.OperationalError as e:
            # просто переподнимаю... по сути мог и не трай-кетчить, НО:
            # возможно в таких случаях принято что-то дописывать от себя в текст ексепшна;
            # и до кучи в лог напишем
            log.error(f"Test connection error: {e}")
            raise e

    def __make_sql_for_dump(self, table_name: string, anchor_date_s: string) -> string:
        with open(f'{self.sql_path}/dump_{table_name}.sql') as file:
            sql_dump = file.read()
        sql_dump = sql_dump.format(anchor_date_s)
        sql_copy = f"COPY ({sql_dump}) TO STDOUT WITH CSV DELIMITER ','"

        return sql_copy

    def __make_file_for_dump(self, table_name: string, anchor_date_s: string) -> string:
        dump_dir = self.sql_path + '/data'
        is_exist = os.path.exists(dump_dir)
        if not is_exist:
            os.makedirs(dump_dir)
        dump_file = f"dump_{table_name}.{anchor_date_s}.csv"
        save_file_path = dump_dir + '/' + dump_file
        if not os.path.isfile(save_file_path):
            with open(save_file_path, 'w') as file:
                pass

        return save_file_path

    def dump_table(self, table_name: string, anchor_date_s: string):
        # создаём директорию и файл для дампа
        save_file_path = self.__make_file_for_dump(table_name, anchor_date_s)
        # подготавливаем запрос для copy_expert()
        sql_copy = self.__make_sql_for_dump(table_name, anchor_date_s)
        # теперь psycopg2 всё сделает за нас
        with open(save_file_path, 'w') as file:
            with closing(psycopg2.connect(**self.pg_conn_args)) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql_copy, file)
                    file.truncate(file.tell())
                    conn.commit()
        # логируем результат по размеру файла
        bytes_size = os.path.getsize(save_file_path)
        log = logging.getLogger(__name__)
        log.info(
            f"dump of the table {table_name} into file '{save_file_path}' is done ({bytes_size} Bytes)")
