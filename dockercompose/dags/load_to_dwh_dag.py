from airflow.decorators import dag, task
from datetime import datetime, timedelta

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scripts.load_to_dwh import *
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook

staging_schema = 'public'
dwh_schema = 'dwh'


@dag(dag_id="load_to_dwh_dag", schedule_interval=timedelta(hours=3),
     start_date=datetime(2025, 4, 4, 0, 37), catchup=False, max_active_runs=1)
def load_to_dwh_dag():
    # получение подключения к хранилищу
    hook_dwh = PostgresHook(postgres_conn_id="dwh_conn_pg")
    conn_dwh = hook_dwh.get_conn()

    load_with_scd1_tasks = []
    load_with_scd2_tasks = []
    load_fact_tasks = []

    # создание тасок для загрузки данных в таблицы с scd1
    # данные для таблиц находятся в словарях в файле load_to_dwh
    for table_name, table_data in tables_with_scd1_data.items():
        @task(task_id=f'load_{table_name}_table')
        def load_scd1table_data(table_name=table_name, table_data=table_data):
            load_dim_with_scd1(stg_schema=staging_schema,
                               dwh_schema=dwh_schema,
                               table_name=table_name,
                               non_key_columns=table_data['non_key_columns'],
                               key_column=table_data['key_column'],
                               conn=conn_dwh)

        load_with_scd1_tasks.append(load_scd1table_data())

    # создание тасок для загрузки данных в таблицы с scd2
    for table_name, table_data in tables_with_scd2_data.items():
        @task(task_id=f'load_{table_name}_table')
        def load_scd2table_data(table_name=table_name, table_data=table_data):
            load_dim_with_scd2(stg_schema=staging_schema,
                               dwh_schema=dwh_schema,
                               table_name=table_name,
                               columns=table_data['columns'],
                               key_column=table_data['key_column'],
                               conn=conn_dwh)

        load_with_scd2_tasks.append(load_scd2table_data())

    # создание тасок для загрузки данных в таблицы фактов
    for table_name, table_data in fact_tables_data.items():
        @task(task_id=f'load_{table_name}_table')
        def load_fact_table_data(table_name=table_name, table_data=table_data):
            load_fact(stg_schema=staging_schema,
                      dwh_schema=dwh_schema,
                      table_name=table_name,
                      date_columns=table_data['date_columns'],
                      sk_mapping=table_data['mapping'],
                      matching_columns=table_data['matching_columns'],
                      conn=conn_dwh)

        load_fact_tasks.append(load_fact_table_data())

    #витрины обновляются после каждой загрузки в двх
    trigger_dms_update_task = TriggerDagRunOperator(
        task_id='trigger_dms_update_dag',
        trigger_dag_id='update_data_marts_dag'
        # reset_dag_run=True
    )
    # установка порядка выполнения scd1 -> scd2 -> facts -> dms_update
    chain(*load_with_scd1_tasks, *load_with_scd2_tasks, *load_fact_tasks,trigger_dms_update_task)


load_to_dwh_dag()
