from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from scripts.generate_data_in_source import *
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(dag_id="generate_data_in_source_dag", schedule_interval="@hourly",
     start_date=datetime(2025, 4, 7), catchup=False, max_active_runs=1)
def generate_data_in_source_dag():
    # получение подключения к источнику
    hook = PostgresHook(postgres_conn_id="source_conn_pg")
    conn = hook.get_conn()

    @task  # генерация обновлений для измерений с scd2
    def generate_changes_task():
        generate_guests_changes(conn)
        generate_rooms_changes(conn)
        generate_maintenance_types_changes(conn)

    @task  # генерация новых данных
    def generate_new_data_task():
        generate_guest_data(conn)
        generate_bookings(conn)
        generate_stays_and_revenue(conn)
        generate_maintenance_facts(conn)

    # все изменения в источнике сразу загружаются в стейдж и накапливаются там до следующей загрузки в хранилище
    trigger_load_to_stage_task = TriggerDagRunOperator(
        task_id='trigger_load_to_stage',
        trigger_dag_id='load_to_stage_dag'
    )

    generate_changes_task() >> generate_new_data_task() >> trigger_load_to_stage_task


generate_data_in_source_dag()
