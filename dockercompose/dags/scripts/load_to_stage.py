from datetime import datetime
import logging
from scripts.load_to_dwh import start_logging, end_logging, end_logging_with_error, get_last_load_time
from scripts.validate_data import validate_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


def integrate_table(stg_schema, src_schema, table_name, columns, relationships, timestamp_column, src_conn, stg_conn):
    with (src_conn.cursor() as src_cursor, stg_conn.cursor() as stg_cursor):
        # время загрузки текущей в стейдж
        load_dttm = datetime.now()
        # начало логирования
        start_logging(stg_schema, table_name, load_dttm, stg_conn, 'stage_load_logs')
        try:
            # получение времени последней успешной загрузки
            last_load_to_stg = get_last_load_time(stg_schema, table_name, stg_cursor, 'stage_load_logs')

            # присоединение связанных таблиц для проверки целостности ссылок в источнике
            joins_placeholder = ' '.join(
                [f"""LEFT JOIN {src_schema}.{table} on {src_schema}.{table}.{column} = t.{column}""" for table, column
                 in
                 relationships.items()])
            rel_existence_conditions_placeholder = ' AND ' + ' AND '.join(
                [f"""{src_schema}.{table}.{column} IS NOT NULL""" for table, column in relationships.items()]) if len(relationships) > 0 else ''

            # получение данных с корректными ссылками из источника
            extract_data_query = f"""
            SELECT {', '.join([f't.{clmn}' for clmn in columns])} FROM {src_schema}.{table_name} t
            {joins_placeholder}
            WHERE {timestamp_column} > '{last_load_to_stg}' -- только новые записи
            {rel_existence_conditions_placeholder};
            """
            logger.info(extract_data_query)
            src_cursor.execute(extract_data_query)
            table_data = src_cursor.fetchall()
            # валидация данных
            validated_data = validate_data(table_data, columns, table_name, logger)
            # загрузка в таблицу
            load_data_to_stage_query = f"""
            INSERT INTO {stg_schema}.{table_name} ({', '.join(columns)},load_dttm) 
            VALUES ({','.join(['%s'] * len(columns))},'{load_dttm}');
            """
            stg_cursor.executemany(load_data_to_stage_query, validated_data)
            stg_conn.commit()

            # обновление записи в таблице логов
            end_logging(stg_schema, table_name, load_dttm, stg_conn, len(table_data), 'stage_load_logs')
            logger.info(f'{len(table_data)} records were loaded to staging schema for table: {table_name}')
        # логирование в случае ошибки и остановка процесса
        except Exception as e:
            logger.error(f'Error during loading from source to stage for table {table_name}. Error : {str(e)}')
            stg_conn.rollback()
            end_logging_with_error(stg_schema, table_name, load_dttm, stg_conn, 0, 'stage_load_logs', str(e))
            raise


tables_with_config = {
    'guests_dim': {
        'columns': [
            'guest_id',
            'first_name',
            'last_name',
            'gender',
            'date_of_birth',
            'nationality',
            'country_of_residence',
            'is_active_in_source',
            'last_update'
        ],
        'timestamp_column': 'last_update',
        'relationships': {}
    },
    'room_category_dim': {
        'columns': [
            'room_category_id',
            'category_name',
            'current_price',
            'is_active_in_source',
            'last_update'
        ],
        'timestamp_column': 'last_update',
        'relationships': {}
    },
    'booking_dim': {
        'columns': [
            'booking_id',
            'status',
            'expected_length_of_stay',
            'last_update'
        ],
        'timestamp_column': 'last_update',
        'relationships': {}
    },
    'room_maintenance_type_dim': {
        'columns': [
            'maintenance_type_id',
            'type',
            'current_price',
            'additional_info',
            'last_update',
            'is_active_in_source'
        ],
        'timestamp_column': 'last_update',
        'relationships': {}
    },
    'booking_fact': {
        'columns': [
            'guest_id',
            'room_category_id',
            'checkin_date',
            'checkout_date',
            'visit_purpose',
            'booking_id',
            'created_at'
        ],
        'timestamp_column': 'created_at',
        'relationships': {'guests_dim': 'guest_id',
                          'room_category_dim': 'room_category_id'}
    },

    'stay_fact': {
        'columns': [
            'booking_id',
            'days_late',
            'days_extend',
            'rating',
            'total_paid',
            'total_nights',
            'created_at'
        ],
        'timestamp_column': 'created_at',
        'relationships': {}
    },
    'revenue_fact': {
        'columns': [
            'room_category_id',
            'date',
            'amount',
            'created_at'
        ],
        'timestamp_column': 'created_at',
        'relationships': {'room_category_dim': 'room_category_id'}
    },

    'maintenance_expense_fact': {
        'columns': [
            'room_category_id',
            'date',
            'amount',
            'maintenance_type_id',
            'created_at',
            'is_unscheduled'
        ],
        'timestamp_column': 'created_at',
        'relationships': {'room_category_dim': 'room_category_id',
                          'room_maintenance_type_dim': 'maintenance_type_id'}
    }
}
