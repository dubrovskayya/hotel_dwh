from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


# начало логирования процесса
def start_logging(schema, table_name, start_time, conn, log_table_name):
    with conn.cursor() as cursor:
        log_start_query = f"""
                    INSERT INTO {schema}.{log_table_name} (table_name,start_time,status)
                    VALUES ('{table_name}','{start_time}','started');
                    """
        cursor.execute(log_start_query)
        conn.commit()


# завершение логирования
def end_logging(schema, table_name, start_time, conn, row_count, log_table_name):
    with conn.cursor() as cursor:
        end_time = datetime.now()
        update_log_query = f"""
                    UPDATE {schema}.{log_table_name}
                    SET end_time = '{end_time}'::timestamp, 
                    records_processed = {row_count}
                    ,status = 'success'
                    ,load_time = '{end_time}'::timestamp - start_time
                    WHERE table_name = '{table_name}' AND start_time = '{start_time}';
                    """
        cursor.execute(update_log_query)
        conn.commit()


# завершение логирования с ошибкой
def end_logging_with_error(schema, table_name, start_time, conn, row_count, log_table_name, error):
    with conn.cursor() as cursor:
        update_log_query = f"""
                    UPDATE {schema}.{log_table_name}
                    SET end_time = '{datetime.now()}'::timestamp, 
                    records_processed = {row_count}
                    ,status = 'failed'
                    ,error_message = %s
                    WHERE table_name = '{table_name}' AND start_time = '{start_time}';
                    """
        cursor.execute(update_log_query, (error,))
        conn.commit()


# получение времени последнего процесса загрузки из таблицы логов
def get_last_load_time(schema, table_name, cursor, log_table_name):
    find_last_process_time_query = f"""
                    SELECT start_time FROM {schema}.{log_table_name}
                    WHERE table_name = '{table_name}'
                    AND status = 'success'
                    ORDER BY start_time DESC
                    LIMIT 1 
                    """
    cursor.execute(find_last_process_time_query)
    last_process_time = cursor.fetchone()
    # если нет последнего времени последнего процесса, то текущий процесс считается первым
    # и используется искусственное время
    if not last_process_time:
        last_process_time = datetime(1111, 11, 11, 11, 11, 11)
    else:
        last_process_time = last_process_time[0]
    return last_process_time


# загрузка записей для таблиц с scd2
def load_dim_with_scd2(stg_schema, dwh_schema, table_name, columns, key_column, conn):
    with conn.cursor() as cursor:
        start_time = datetime.now()
        # начало логирования
        start_logging(dwh_schema, table_name, start_time, conn, 'dwh_load_logs')
        try:
            # получение времени последней успешной загрузки
            last_process_time = get_last_load_time(dwh_schema, table_name, cursor, 'dwh_load_logs')

            # формирование условий для обновления: если хотя бы одно значение в stg не совпадает с dw, запись считается обновленной
            update_conditions_placeholder = ' OR '.join([f'stg.{i}<>dw.{i}' for i in columns])  #

            columns_placeholder = ', '.join(columns)

            # cte для получения записей, которые были добавлены позже последнего
            # процесса загрузки в dwh и номера их версии в стейдже
            actual_records_in_stage_placeholder = f"""
             WITH cte AS (
                        SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY {key_column} ORDER BY last_update) AS version_number
                        FROM {stg_schema}.{table_name}
                        WHERE load_dttm > '{last_process_time}' -- только записи, которые были добавлены позже последнего процесса
                            )
            """
            logger.info(f'{actual_records_in_stage_placeholder}')

            # получение максимального количества версий одной записи,
            # которые накопились в стейдже с момента последней загрузки в dwh
            get_max_number_of_versions = f"""
                                   {actual_records_in_stage_placeholder}
                                    SELECT MAX(version_number) AS n
                                    FROM cte;
                                         """
            cursor.execute(get_max_number_of_versions)
            max_number_of_versions = cursor.fetchone()
            logger.info(f'{max_number_of_versions}')
            if max_number_of_versions[0] is not None:
                max_number_of_versions = max_number_of_versions[0]
                logger.info(f'Max level of versions for {table_name} at {datetime.now()} is {max_number_of_versions}')
            else:  # если значение None, значит новых записей в стейдже нет и процесс завершается
                logger.info(f'No new records in stage for {table_name}. Process ended successfully.')
                end_logging(dwh_schema, table_name, start_time, conn, 0, 'dwh_load_logs')
                return

            total_updated = 0
            total_inserted = 0
            total_closed = 0

            """
            для каждого уровня версий в стейдже выполняются следующие шаги:
                - закрытие версий записей, для которых есть изменения
                - вставка новых записей и новых вресий записей, закрытых на предыдущем шаге
                - закрытие записей, которые больше не активны в источнике
            
            каждый уровень обрабатывается отдельно для того, чтобы гарантировать 
            корректную историю изменений в порядке их появления в источнике
            """
            for i in range(1, max_number_of_versions + 1):
                # закрытие записей, для которых есть обновления
                update_records_query = f"""
                                {actual_records_in_stage_placeholder}
                                UPDATE {dwh_schema}.{table_name} dw 
                                SET 
                                    valid_to = stg.last_update, -- для закрытия используется время последнего обновления в источнике
                                    is_active = FALSE         
                                FROM 
                                    cte stg
                                WHERE 
                                    stg.version_number = {i}
                                    AND stg.is_active_in_source = TRUE
                                    AND dw.is_active = TRUE 
                                    AND dw.{key_column} = stg.{key_column} 
                                    AND ({update_conditions_placeholder}); 
                            """

                # добавление новых записей и новых версий
                insert_new_records_query = f"""
                    {actual_records_in_stage_placeholder}
                    INSERT INTO {dwh_schema}.{table_name} 
                    (
                        {columns_placeholder}, 
                        valid_from, 
                        valid_to, 
                        is_active
                    )
                    SELECT -- добавление записей, которых еще нет в хранилище
                        {columns_placeholder},
                        pgd.last_update,
                        '9999-12-31 23:59:59'::timestamp AS valid_to,
                        TRUE AS is_active
                    FROM 
                        cte pgd
                    WHERE 
                        pgd.version_number = {i} 
                        AND pgd.is_active_in_source = TRUE
                        AND NOT EXISTS (
                            SELECT 1
                            FROM {dwh_schema}.{table_name} dgd
                            WHERE dgd.{key_column} = pgd.{key_column}
                        )
                    UNION
                    SELECT -- добавление новых версий для записей, которые были закрыты в текущем процессе
                        {columns_placeholder},
                        pgd.last_update,
                        '9999-12-31 23:59:59'::timestamp AS valid_to,
                        TRUE AS is_active
                    FROM 
                        cte pgd
                    WHERE 
                        pgd.version_number = {i},
                        AND pgd.is_active_in_source = TRUE
                        AND EXISTS (
                            SELECT 1
                            FROM {dwh_schema}.{table_name} dgd
                            WHERE 
                                dgd.{key_column} = pgd.{key_column}
                                AND dgd.is_active = FALSE
                                AND dgd.valid_to = pgd.last_update 
                        )
                """

                # закрытие неактивных записей
                close_records_query = f"""
                                 {actual_records_in_stage_placeholder}
                                UPDATE {dwh_schema}.{table_name} dw
                                SET 
                                    is_active = FALSE,          
                                    valid_to = stg.last_update    
                                FROM 
                                    cte stg
                                WHERE 
                                    stg.version_number={i}
                                    AND dw.{key_column} = stg.{key_column}       
                                    AND dw.is_active = TRUE                
                                    AND stg.is_active_in_source = FALSE;  --неактивные записи в источнике
                            """
                cursor.execute(update_records_query)
                total_updated += cursor.rowcount
                cursor.execute(insert_new_records_query)
                total_inserted += cursor.rowcount
                cursor.execute(close_records_query)
                total_closed += cursor.rowcount

            conn.commit()

            # обновление записи в таблице логов
            end_logging(dwh_schema, table_name, start_time, conn, total_closed + total_updated + total_inserted,
                        'dwh_load_logs')
            logger.info(
                f'Records from {stg_schema} to {dwh_schema} for table {table_name} were loaded successfully.'
                f'{total_closed + total_updated + total_inserted} records were processed.'
                f'{total_updated} were updated.'
                f'{total_closed} were closed.'
                f'{total_inserted} were added.')
        # логирование в случае ошибки и остановка процесса
        except Exception as e:
            logger.error(f'Error during loading from stage to dwh for table {table_name}. Error : {str(e)}')
            end_logging_with_error(dwh_schema, table_name, start_time, conn, 0, 'dwh_load_logs', str(e))
            raise


# загрузка записей для таблиц с scd1
def load_dim_with_scd1(stg_schema, dwh_schema, table_name, non_key_columns, key_column, conn):
    with conn.cursor() as cursor:
        start_time = datetime.now()
        # начало логирования
        start_logging(dwh_schema, table_name, start_time, conn, 'dwh_load_logs')
        try:
            # получение времени последней успешной загрузки
            last_process_time = get_last_load_time(dwh_schema, table_name, cursor, 'dwh_load_logs')

            # формирование условйи для обновления
            update_conditions_placeholder = ' OR '.join([f'stg.{i}<>dw.{i}' for i in non_key_columns])
            # формирование строки с установкой новых значений
            update_values_placeholder = ', '.join([f'{i}=stg.{i}' for i in non_key_columns])
            # обновление записей с изменениями
            update_records_query = f"""
            UPDATE {dwh_schema}.{table_name} dw
                SET 
                    {update_values_placeholder}         
                FROM 
                    {stg_schema}.{table_name} stg
                WHERE 
                stg.{key_column}=dw.{key_column}
                AND stg.load_dttm > '{last_process_time}' -- только записи, которые были добавлены после последнего процесса
                AND stg.load_dttm = 
                    (SELECT MAX(load_dttm) FROM {stg_schema}.{table_name}  stg1
                    WHERE stg1.{key_column}=stg.{key_column}) -- обновление на самую последнюю версию из стейджа
                AND ({update_conditions_placeholder});           
            """

            columns_placeholder = f'{key_column},' + ', '.join(non_key_columns)
            # добавление новых записей
            insert_new_records_query = f"""
                INSERT INTO {dwh_schema}.{table_name} 
                (
                    {columns_placeholder}
                )
                SELECT 
                    {columns_placeholder}
                FROM 
                    {stg_schema}.{table_name} stg
                WHERE 
                    stg.load_dttm > '{last_process_time}'  -- только записи, которые были добавлены после последнего процесса
                    AND stg.load_dttm = 
                        (SELECT MAX(load_dttm) FROM {stg_schema}.{table_name}  stg1
                        WHERE stg1.{key_column}=stg.{key_column}) -- добавление самой последней версии из стейджа
                    AND NOT EXISTS (
                        SELECT 1
                        FROM {dwh_schema}.{table_name} dw
                        WHERE dw.{key_column} = stg.{key_column} 
                    )
            """
            cursor.execute(update_records_query)
            rows_updated_count = cursor.rowcount
            cursor.execute(insert_new_records_query)
            rows_inserted_count = cursor.rowcount
            conn.commit()

            # обновление записи в таблице логов
            end_logging(dwh_schema, table_name, start_time, conn, rows_updated_count + rows_inserted_count,
                        'dwh_load_logs')
            logger.info(
                f'Records from {stg_schema} to {dwh_schema} for table {table_name} were loaded successfully.'
                f'{rows_updated_count + rows_inserted_count} records were processed.')
        # логирование в случае ошибки и остановка процесса
        except Exception as e:
            logger.error(f'Error during loading from stage to dwh for table {table_name}. Error : {str(e)}')
            end_logging_with_error(dwh_schema, table_name, start_time, conn, 0, 'dwh_load_logs', str(e))
            raise


# загрузка записей для таблиц фактов
def load_fact(stg_schema, dwh_schema, table_name, date_columns, sk_mapping, matching_columns, conn):
    with conn.cursor() as cursor:
        start_time = datetime.now()
        # начало логирования
        start_logging(dwh_schema, table_name, start_time, conn, 'dwh_load_logs')

        try:
            # получение времени последней успешной загрузки
            last_process_time = get_last_load_time(dwh_schema, table_name, cursor, 'dwh_load_logs')

            # формирование имен столбцов с датами для таблицы в dwh
            dwh_date_columns_placeholder = ',' + ', '.join([f'{i}_id' for i in date_columns]) if len(
                date_columns) > 0 else ''

            # формирование имен столбцов с суррогатными ключами для таблицы в dwh
            dwh_sk_columns_placeholder = ',' + ', '.join(
                [f'{clmns["sk_column"]}' for tbl, clmns in sk_mapping.items()]) if len(sk_mapping) > 0 else ''

            # формирование имен общих столбцов для таблиц в dwh и stage
            matching_columns_placeholder = ', '.join(matching_columns)

            # преобразование дат для совпадения с id в таблице dim_date
            transformed_date_columns_placeholder = ',' + ', '.join(
                [f"to_char({i}, 'YYYYMMDD')::int as {i}_id" for i in date_columns]) if len(date_columns) > 0 else ''

            # формирование join-команд для присоединения таблиц измерений и получения из них суррогатных ключей для соответствующих записей
            # используется словарь sk_mapping в формате {'имя таблицы':{'key_column':'название столбца соединения',
            # 'sk_column':'название столбца содержащего нужный sk'}}
            joins_placeholder = ' '.join(
                [f"""JOIN {dwh_schema}.{tbl} ON {dwh_schema}.{tbl}.{clmns['key_column']}=stg.{clmns['key_column']} 
            AND stg.created_at BETWEEN {dwh_schema}.{tbl}.valid_from AND {dwh_schema}.{tbl}.valid_to
            """ for tbl, clmns in sk_mapping.items()])

            # формирование списка нужных sk-столбцов для выбора их из результирующей таблицы
            stg_sk_columns_placeholder = ' ,' + ', '.join(
                [f"""{dwh_schema}.{tbl}.{clmns['sk_column']} as  {clmns['sk_column']}""" for tbl, clmns in
                 sk_mapping.items()]) if len(sk_mapping) > 0 else ''

            """
            запрос выполняет выборку данных из таблицы stage-схемы и присоединяет к ней таблицы измерений 
            из dwh-схемы для получения актуальных суррогатных ключей (SK)

            для выбора SK, актуальных на момент создания факт-записи, в join используется условие: 
            дата создания факта должна находиться между valid_from и valid_to в записях таблиц измерений
            
            даты преобразуются в формат, соответствующий таблице измерений date_dim (YYYYMMDD), 
            чтобы обеспечить корректное связывание
            
            запятые между типами столбцов добавляются в плейсхолдеры, для корректной работы при отсутствии
            какого-то типа столбцов
            
            в таблицу в dwh-схеме данные вставляются так:
              совпадающие столбцы остаются без изменений
              столбцы с датами заменяются на соответствующие ключи из таблицы date_dim
              бизнес-ключи измерений заменяются на актуальные суррогатные ключи (SK), 
              выбранные на основе условия valid_from и valid_to
            """
            insert_records_query = f"""
                INSERT INTO {dwh_schema}.{table_name} 
                (
                    {matching_columns_placeholder}       -- колонки для совпадающих данных
                    {dwh_date_columns_placeholder}       -- дата-колонки из dwh-таблицы
                    {dwh_sk_columns_placeholder}          -- суррогатные ключи из dwh-таблицы
                )
                SELECT 
                    {matching_columns_placeholder}       -- колонки для совпадающих данных
                    {transformed_date_columns_placeholder} -- преобразованные дата-колонки
                    {stg_sk_columns_placeholder}          -- суррогатные ключи из результирующей таблицы
                FROM 
                    {stg_schema}.{table_name} stg         -- исходная таблица на стейдже
                    {joins_placeholder}                   -- JOIN'ы для связывания с таблицами измерений из dwh
                WHERE 
                    stg.load_dttm > '{last_process_time}';  --только записи, которые были добавлены после последнего процесса
            """
            # print(insert_records_query)
            cursor.execute(insert_records_query)
            rows_inserted = cursor.rowcount
            conn.commit()

            # обновление записи в таблице логов
            end_logging(dwh_schema, table_name, start_time, conn, rows_inserted, 'dwh_load_logs')
            logger.info(f'Records from {stg_schema} to {dwh_schema} for table {table_name} were loaded successfully.'
                        f'{rows_inserted} new records were inserted.')
        # логирование в случае ошибки и остановка процесса
        except Exception as e:
            logger.error(f'Error during loading from stage to dwh for table {table_name}. Error : {str(e)}')
            end_logging_with_error(dwh_schema, table_name, start_time, conn, 0, 'dwh_load_logs', str(e))
            raise


# дайменшаны должнв загужаться перд фактами
# if __name__ == "__main__":
#     ### booking dim
#     booking_dim_columns_nonkey = ['status', 'expected_length_of_stay']
#     load_dim_with_scd1('public', 'dwh', 'booking_dim', booking_dim_columns_nonkey, 'booking_id')
#
#     ### guests dim
#     guests_dim_columns = ['guest_id',
#                           'first_name',
#                           'last_name',
#                           'gender',
#                           'date_of_birth',
#                           'nationality',
#                           'country_of_residence']
#
#     load_dim_with_scd2('public', 'dwh', 'guests_dim', guests_dim_columns, 'guest_id')
#
#     ### дим расходов
#     maintenance_dim_columns = ['maintenance_type_id',
#                                'type',
#                                'current_price',
#                                'additional_info']
#     load_dim_with_scd2('public', 'dwh', 'room_maintenance_type_dim', maintenance_dim_columns, 'maintenance_type_id')
#
#     ### rooms dim
#     rooms_dim_columns = ['room_category_id', 'category_name', 'current_price']
#
#     load_dim_with_scd2('public', 'dwh', 'room_category_dim', rooms_dim_columns, 'room_category_id')
#
#     ### booking fact
#     booking_fact_mapping = {
#         'guests_dim': {'key_column': 'guest_id', 'sk_column': 'guest_sk'},
#         'room_category_dim': {'key_column': 'room_category_id', 'sk_column': 'room_category_sk'},
#
#     }
#     booking_date_columns = ['checkin_date', 'checkout_date']
#     booking_matching_columns = ['visit_purpose', 'booking_id']
#
#     load_fact('public', 'dwh', 'booking_fact', booking_date_columns, booking_fact_mapping, booking_matching_columns)
#
#     ### stay fact
#     stay_fact_mapping = {}
#     stay_fact_date_columns = []
#     stay_fact_matching_columns = ['booking_id', 'days_late', 'days_extend', 'rating', 'total_paid', 'total_nights']
#     load_fact('public', 'dwh', 'stay_fact', stay_fact_date_columns, stay_fact_mapping, stay_fact_matching_columns)
#
#     ###факт расходов
#     maintenance_fact_mapping = {
#         'room_category_dim': {'key_column': 'room_category_id', 'sk_column': 'room_category_sk'},
#         'room_maintenance_type_dim': {'key_column': 'maintenance_type_id', 'sk_column': 'maintenance_type_sk'}
#     }
#
#     maintenance_fact_date_columns = ['date']
#     maintenance_fact_matching_columns = ['amount', 'is_unscheduled']
#     load_fact('public', 'dwh', 'maintenance_expense_fact', maintenance_fact_date_columns, maintenance_fact_mapping,
#               maintenance_fact_matching_columns)
#
#     ### revenue fact
#
#     revenue_fact_mapping = {
#         'room_category_dim': {'key_column': 'room_category_id', 'sk_column': 'room_category_sk'}
#     }
#     revenue_fact_date_columns = ['date']
#     revenue_fact_matching_columns = ['amount']
#
#     load_fact('public', 'dwh', 'revenue_fact', revenue_fact_date_columns, revenue_fact_mapping,
#               revenue_fact_matching_columns)


tables_with_scd1_data = {
    'booking_dim': {
        'non_key_columns': ['status', 'expected_length_of_stay'],
        'key_column': 'booking_id'
    }
}

tables_with_scd2_data = {
    'room_maintenance_type_dim': {
        'columns': [
            'maintenance_type_id',
            'type',
            'current_price',
            'additional_info'
        ],
        'key_column': 'maintenance_type_id'
    },
    'guests_dim': {
        'columns': [
            'guest_id',
            'first_name',
            'last_name',
            'gender',
            'date_of_birth',
            'nationality',
            'country_of_residence'
        ],
        'key_column': 'guest_id'
    },
    'room_category_dim': {
        'columns': [
            'room_category_id',
            'category_name',
            'current_price'
        ],
        'key_column': 'room_category_id'
    }
}

fact_tables_data = {
    'revenue_fact': {
        'date_columns': ['date'],
        'mapping': {
            'room_category_dim': {'key_column': 'room_category_id', 'sk_column': 'room_category_sk'}
        },
        'matching_columns': ['amount']
    },
    'booking_fact': {
        'date_columns': ['checkin_date', 'checkout_date'],
        'mapping': {
            'guests_dim': {'key_column': 'guest_id', 'sk_column': 'guest_sk'},
            'room_category_dim': {'key_column': 'room_category_id', 'sk_column': 'room_category_sk'}
        },
        'matching_columns': ['visit_purpose', 'booking_id']
    },
    'stay_fact': {
        'date_columns': [],
        'mapping': {},
        'matching_columns': ['booking_id', 'days_late', 'days_extend', 'rating', 'total_paid', 'total_nights']
    },
    'maintenance_expense_fact': {
        'date_columns': ['date'],
        'mapping': {
            'room_category_dim': {'key_column': 'room_category_id', 'sk_column': 'room_category_sk'},
            'room_maintenance_type_dim': {'key_column': 'maintenance_type_id', 'sk_column': 'maintenance_type_sk'}
        },
        'matching_columns': ['amount', 'is_unscheduled']
    }
}
