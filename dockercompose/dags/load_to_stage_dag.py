from airflow.decorators import dag, task
from datetime import datetime
from scripts.load_to_stage import *
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook

# получение словаря с таблицами и их данными из файла load_to_stage
tables_data = tables_with_config

staging_schema = 'public'
source_schema = 'source'


# даг запускается по триггеру загрузки и обновлений в источнике
@dag(dag_id="load_to_stage_dag", schedule_interval=None,
     start_date=datetime(2025, 4, 7), catchup=False, max_active_runs=1)
def load_to_stage_dag():
    # получение подключений к источнику и хранилищу из airflow
    hook_source = PostgresHook(postgres_conn_id="source_conn_pg")
    conn_source = hook_source.get_conn()
    hook_stage = PostgresHook(postgres_conn_id="dwh_conn_pg")
    conn_stage = hook_stage.get_conn()

    tasks = []
    for table_name, table_data in tables_with_config.items():
        # создание таска для загрузки данных для каждой таблицы
        @task(task_id=f"load_{table_name}_to_stage")
        def load_table_to_stage_task(table_name=table_name, table_data=table_data):
            integrate_table(stg_schema=staging_schema,
                            src_schema=source_schema,
                            table_name=table_name,
                            columns=table_data['columns'],
                            timestamp_column=table_data['timestamp_column'],
                            src_conn=conn_source,
                            stg_conn=conn_stage)

        tasks.append(load_table_to_stage_task())

        # установка зависимостей
        chain(*tasks)


load_to_stage_dag()
