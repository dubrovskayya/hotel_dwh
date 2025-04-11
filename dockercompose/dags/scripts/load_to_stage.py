from datetime import datetime
import logging
import psycopg2
from scripts.load_to_dwh import start_logging,end_logging,end_logging_with_error,get_last_load_time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


def integrate_table(stg_schema, src_schema, table_name, columns, timestamp_column,src_conn,stg_conn):
    with src_conn.cursor() as src_cursor, stg_conn.cursor() as stg_cursor:
        # время загрузки текущей в стейдж
        load_dttm = datetime.now()
        # начало логирования
        start_logging(stg_schema,table_name,load_dttm,stg_conn,'stage_load_logs')
        try:
            # получение времени последней успешной загрузки
            last_load_to_stg=get_last_load_time(stg_schema,table_name,stg_cursor,'stage_load_logs')

            # записи переносятся из источника в стейдж только если время их обновления
            # или создания в источнике больше, чем время последней загрузки в стейдж
            extract_data_query = f"""
            SELECT {', '.join(columns)} FROM {src_schema}.{table_name}
            WHERE {timestamp_column} > '{last_load_to_stg}' 
            """
            src_cursor.execute(extract_data_query)
            table_data = src_cursor.fetchall()

            #загрузка в таблицу
            load_data_to_stage_query = f"""
            INSERT INTO {stg_schema}.{table_name} ({', '.join(columns)},load_dttm)
            VALUES ({','.join(['%s'] * len(columns))},'{load_dttm}')
            """
            stg_cursor.executemany(load_data_to_stage_query, table_data)
            stg_conn.commit()

            # обновление записи в таблице логов
            end_logging(stg_schema,table_name,load_dttm,stg_conn,len(table_data),'stage_load_logs')
            logger.info(f'{len(table_data)} records were loaded to staging schema for table: {table_name}')
        # логирование в случае ошибки и остановка процесса
        except Exception as e:
            logger.error(f'Error during loading from source to stage for table {table_name}. Error : {str(e)}')
            end_logging_with_error(stg_schema, table_name, load_dttm, stg_conn, 0, 'stage_load_logs', str(e))
            raise



# Списки колонок для каждой таблицы
# if __name__ == "__main__":
#     guests_dim_data = [
#         'guest_id',  # Уникальный бизнес-идентификатор (UUID)
#         'first_name',  # Имя
#         'last_name',  # Фамилия
#         'gender',  # Пол
#         'date_of_birth',  # Дата рождения
#         'nationality',  # Национальность
#         'country_of_residence',
#         'is_active_in_source',
#         'last_update'# Страна проживания
#     ]
#     integrate_table('public', 'source', 'guests_dim', guests_dim_data, 'last_update')
#
#     room_category_dim_data = [
#         'room_category_id',  # Бизнес-ключ (например, уникальный ID)
#         'category_name',  # Название категории номера
#         'current_price',
#         'is_active_in_source',
#         'last_update'
#     ]
#     integrate_table('public', 'source',  'room_category_dim', room_category_dim_data, 'last_update')
#
#     booking_fact_data = [
#         'guest_id',  # Логическая связь с измерением гостей
#         'room_category_id',  # Логическая связь с измерением категорий номеров
#         'checkin_date',  # Логическая связь с измерением дат (check-in)
#         'checkout_date',  # Логическая связь с измерением дат (check-out)
#         'visit_purpose',  # Цель визита
#         'booking_id',
#         'created_at'# Бизнес-ключ бронирования
#     ]
#     integrate_table('public', 'source',  'booking_fact', booking_fact_data, 'created_at')
#
#     booking_dim_data = [
#         'booking_id',  # Бизнес-ключ бронирования
#         'status',  # Статус бронирования
#         'expected_length_of_stay',
#         # Ожидаемая длительность пребывания
#     ]
#     integrate_table('public', 'source',  'booking_dim', booking_dim_data, 'last_update')
#
#     stay_fact_data = [
#         'booking_id',  # Логическая связь с измерением бронирований
#         'days_late',  # Количество дней опоздания
#         'days_extend',  # Количество дней продления
#         'rating',  # Рейтинг
#         'total_paid',  # Общая сумма оплаты
#         'total_nights',
#         'created_at'# Общее количество ночей
#     ]
#
#     revenue_fact_data = [
#         'room_category_id',  # Логическая связь с измерением категорий номеров
#         'date',  # Дата
#         'amount',
#         'created_at'# Сумма дохода
#     ]
#     integrate_table('public', 'source',  'revenue_fact', revenue_fact_data, 'created_at')
#
#     integrate_table('public', 'source', 'stay_fact', stay_fact_data, 'created_at')
#
#     maintenance_dim_data=[
#     'maintenance_type_id',
#         'type',
#         'current_price',
#         'additional_info',
#         'last_update',
#         'is_active_in_source'
#     ]
#     integrate_table('public', 'source', 'room_maintenance_type_dim',maintenance_dim_data,'last_update')
#
#     maintenance_fact_data = [
#     'room_category_id',
#         'date',
#         'amount',
#         'maintenance_type_id',
#         'created_at',
#         'is_unscheduled'
#     ]
#     integrate_table('public', 'source', 'maintenance_expense_fact',maintenance_fact_data,'created_at')

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
        'timestamp_column': 'last_update'
    },
    'room_category_dim': {
        'columns': [
            'room_category_id',
            'category_name',
            'current_price',
            'is_active_in_source',
            'last_update'
        ],
        'timestamp_column': 'last_update'
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
        'timestamp_column': 'created_at'
    },
    'booking_dim': {
        'columns': [
            'booking_id',
            'status',
            'expected_length_of_stay',
            'last_update'
        ],
        'timestamp_column': 'last_update'
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
        'timestamp_column': 'created_at'
    },
    'revenue_fact': {
        'columns': [
            'room_category_id',
            'date',
            'amount',
            'created_at'
        ],
        'timestamp_column': 'created_at'
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
        'timestamp_column': 'last_update'
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
        'timestamp_column': 'created_at'
    }
}