from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scripts.update_data_marts import update_data_marts

dm_schema = 'dm'
dwh_schema = 'dwh'

# даг запускается по триггеру после обновлений в хранилище
@dag(dag_id="update_data_marts_dag", schedule_interval=None,
     start_date=datetime(2025, 4, 4), catchup=False, max_active_runs=1)
def update_data_marts_dag():
    # получение подключения к dwh
    hook_dwh = PostgresHook(postgres_conn_id="dwh_conn_pg")
    conn = hook_dwh.get_conn()

    # витрины обновляются все вместе
    @task
    def update_data_marts_task():
        update_data_marts(conn=conn,
                          dwh_schema=dwh_schema,
                          dm_schema=dm_schema)

    update_data_marts_task()


update_data_marts_dag()
