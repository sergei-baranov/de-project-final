import logging
import os
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
# get_current_context не нужен, декораторы транслируют контекст в именованные аргументы тасков,
# но оставим закомментированным, чтобы когда надо вспомнить
# from airflow.operators.python import get_current_context
from airflow.models.baseoperator import chain  # https://docs.astronomer.io/learn/managing-dependencies

from project_final.PgRepository import PgRepository
from project_final.VerticaRepository import VerticaRepository

log = logging.getLogger(__name__)

conn_info_vertica = {
    'host': '51.250.75.20',
    'port': '5433',
    'user': '***',
    'password': '***',
    'database': 'dwh',
    'autocommit': False
}
pg_repository = PgRepository(BaseHook.get_connection('pg_connection'))
vertica_repository = VerticaRepository(conn_info_vertica)


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

    global vertica_repository
    vertica_repository = VerticaRepository(conn_info_vertica)

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
