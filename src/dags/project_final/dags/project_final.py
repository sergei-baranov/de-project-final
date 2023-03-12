import logging
import os
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import chain
# Airflow имеет уже dags_folder в PYTHONPATH (но с пайшармом беда)
from project_final.db_reps.PgRepository import PgRepository
from project_final.db_reps.VerticaRepository import VerticaRepository

log = logging.getLogger(__name__)

curr_dir_path = os.path.dirname(__file__)
sql_path = os.path.abspath(os.path.join(curr_dir_path, '..', '..', '..', 'sql'))
log.info(f"sql_path is: '{sql_path}'")

pg_repository = PgRepository(BaseHook.get_connection('pg_connection'), sql_path)
vertica_repository = VerticaRepository(BaseHook.get_connection('vertica_connection'), sql_path)

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


@dag(
    default_args=default_args,
    start_date=pendulum.parse('2022-10-02'),
    end_date=pendulum.parse('2022-11-01'),
    catchup=True,
    schedule="@daily",
)
def project_final():
    @task(task_id='init')
    def init(execution_date: datetime = None):
        yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
        vertica_repository.make_init_sql()

    @task(task_id='dump_currencies')
    def dump_currencies(execution_date: datetime = None):
        yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
        pg_repository.dump_table('currencies', yesterday)

    @task(task_id='load_currencies')
    def load_currencies(execution_date: datetime = None):
        yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
        vertica_repository.load_currencies(yesterday)

    @task(task_id='dump_transactions')
    def dump_transactions(execution_date: datetime = None):
        yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
        pg_repository.dump_table('transactions', yesterday)

    @task(task_id='load_transactions')
    def load_transactions(execution_date: datetime = None):
        yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
        vertica_repository.load_transactions(yesterday)

    @task(task_id='make_dwh')
    def make_dwh(execution_date: datetime = None):
        pass

    @task(task_id='make_cdm')
    def make_cdm(execution_date: datetime = None):
        yesterday = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
        vertica_repository.make_cdm_sql(yesterday)

    chain(
        init(),
        [dump_currencies(), dump_transactions()],
        [load_currencies(), load_transactions()],
        make_dwh(),
        make_cdm()
    )


_ = project_final()
