import random
from faker import Faker
from datetime import datetime, timedelta
import logging

fake = Faker()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


# генерация новых гостей
def generate_guest_data(conn):
    new_guests_number = random.randint(500, 3000)  # случайное количество новых гостей
    nationalities = ['American', 'Canadian', 'Chinese', 'Indian', 'Brazilian', 'German',
                     'French', 'Italian', 'Japanese', 'Russian']
    countries = ['USA', 'Canada', 'China', 'India', 'Brazil',
                 'Germany', 'France', 'Italy', 'Japan', 'Russia']
    # распределение возрастных групп
    age_groups = [[18, 30], [30, 60], [60, 90]]
    age_weights = [3, 5, 2]
    load_time = datetime.now()  # время загрузки
    is_active = True  # все новые записи считаются активными в источнике

    guests = []
    for _ in range(new_guests_number):
        business_id = fake.uuid4()  # уникальный бизнес-айди
        gender = random.choice(['Male', 'Female'])  # пол
        first_name = fake.first_name_female() if gender == 'Female' else fake.first_name_male()  # имя
        last_name = fake.last_name()  # фамилия

        # генерация возраста с распределением
        age_group = random.choices(age_groups, age_weights)[0]
        date_of_birth = fake.date_of_birth(minimum_age=age_group[0],
                                           maximum_age=age_group[1])  # дата рождения (18–90 лет)
        formatted_date_of_birth = date_of_birth.strftime('%Y-%m-%d')

        nationality = random.choice(nationalities)  # национальность из списка
        country_of_residence = random.choice(countries)  # страна проживания из списка

        guest = (
            business_id, first_name, last_name, gender,
            formatted_date_of_birth, nationality, country_of_residence,
            load_time, is_active
        )
        guests.append(guest)  # добавление гостя в список
    logger.info(f'Generated {len(guests)} record for guests table.')
    with conn.cursor() as cursor:
        insert_guests_query = f"""
            INSERT INTO source.guests_dim (
                guest_id,
                first_name,
                last_name,
                gender,
                date_of_birth,
                nationality,
                country_of_residence,
                last_update,
                is_active_in_source
            )
            VALUES ({','.join(['%s'] * 9)});
        """
        cursor.executemany(insert_guests_query, guests)
        conn.commit()
        logger.info('Records were inserted successfully.')


# заполнение начальных значений в таблице измерений room_category_dim(1 раз)
def fill_initial_room_types(conn):
    # типы номеров с начальными ценами
    room_types = {
        'standard': 100.00,
        'deluxe': 150.00,
        'suite': 250.00,
        'executive suite': 400.00,
        'family room': 200.00,
        'studio': 180.00,
        'city view': 300.00,
        'mountain view': 280.00
    }
    initial_records = []

    # искусственная дата первого заполнения
    start_date = datetime(2024, 1, 1, 0, 0, 0)

    # генерация записей 
    for key, value in room_types.items():
        initial_records.append((fake.uuid4(), key, value, start_date, True))

    # вставка записей
    insert_types_query = """
        INSERT INTO source.room_category_dim 
        (room_category_id, category_name, current_price, last_update, is_active_in_source)
        VALUES (%s,%s,%s,%s,%s);
        """

    with conn.cursor() as cursor:
        cursor.executemany(insert_types_query, initial_records)
        conn.commit()
        logger.info('Initial room categories were inserted successfully.')


# генерация данных о бронированиях для таблиц booking_fact и booking_dim
def generate_bookings(conn):
    visit_purposes = ['business', 'tourism', 'family_visit', 'other']
    num = random.randint(1000, 5000)  # количество новых бронирований
    # генерация бизнес-id бронирований
    booking_ids = [fake.uuid4() for _ in range(num)]
    status = 'waiting'  # начальный статус всех бронирований

    with conn.cursor() as cursor:
        # получения списка id для активных гостей
        cursor.execute("""SELECT guest_id 
                       FROM source.guests_dim 
                       WHERE is_active_in_source = TRUE""")
        available_guests = cursor.fetchall()
        available_guests = [i[0] for i in available_guests]
        # выбор num уникальных гостей
        guest_ids = random.sample(available_guests, num)

        # получения списка id для активных номеров
        cursor.execute("""SELECT room_category_id
                       FROM source.room_category_dim 
                       WHERE is_active_in_source = TRUE
                       ORDER BY category_name""")
        available_rooms = cursor.fetchall()
        available_rooms = [i[0] for i in available_rooms]
        # выбор num типов номеров
        room_weights = [3, 3, 1.5, 4, 4, 4, 4, 2]
        room_ids = random.choices(available_rooms, weights=room_weights, k=num)

        dim_data = []
        fact_data = []
        # время создания записей
        creation_time = datetime.now()

        # генерация даты со смещением сторону лета/зимы
        # 35% - летняя дата, 25% - зимняя, 40% - случайная дата в течение года
        def generate_date():
            n = random.randint(1, 21)
            if n <= 7:
                return fake.date_between_dates(datetime(2024, 6, 1), datetime(2024, 8, 31))
            elif n <= 12:
                return fake.date_between_dates(datetime(2024, 12, 1), datetime(2025, 2, 28))
            else:
                return fake.date_between(start_date='-1y', end_date='today')

        for i in range(num):
            guest_id = guest_ids[i]  # id гостя из списка
            room_id = room_ids[i]  # id типа номера из списка
            expected_length_of_stay = random.randint(1, 14)  # случайное количество планируемых дней пребывания
            checkin_date = generate_date()  # дата заезда (в течение года)
            checkout_date = checkin_date + timedelta(days=expected_length_of_stay)  # дата выезда
            visit_purpose = random.choice(visit_purposes)  # случайная цель из списка
            booking_id = booking_ids[i]  # id бронирования из списка

            dim_data.append((booking_id, status, expected_length_of_stay, creation_time))
            fact_data.append(
                (guest_id, room_id, checkin_date, checkout_date, visit_purpose, booking_id, creation_time))

        # добавление записей в таблицу измерений
        insert_bookings_into_dim_table = """
                    INSERT INTO source.booking_dim
                    (booking_id,status,expected_length_of_stay,last_update)
                    VALUES (%s, %s, %s, %s)
                    """

        # добавление записей в таблицу фактов
        insert_bookings_into_fact_table = """
                    INSERT INTO source.booking_fact
                    (guest_id, room_category_id, checkin_date,
                    checkout_date, visit_purpose, booking_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """

        cursor.executemany(insert_bookings_into_dim_table, dim_data)
        cursor.executemany(insert_bookings_into_fact_table, fact_data)
        conn.commit()

        logger.info(f'{num} records were inserted into booking_fact table successfully.')
        logger.info(f'{num} records were inserted into booking_dim table successfully.')


# генерация данных о проживаниях и доходах для таблиц stay_fact и revenue_fact
def generate_stays_and_revenue(conn):
    # генерация случайного процента заселения
    stay_rate = random.randint(60, 100) / 100

    with conn.cursor() as cursor:
        # получение данных об открытых бронированиях
        available_bookings_query = """SELECT booking_id,expected_length_of_stay 
                                    FROM source.booking_dim 
                                    WHERE status='waiting'"""
        cursor.execute(available_bookings_query)
        booking_data = cursor.fetchall()
        length_mapping = {i[0]: i[1] for i in booking_data}
        waiting_bookings = [i[0] for i in booking_data]

        # создание списка случайных бронирований, для которых будут добавлены заселения
        bookings_with_stay = random.sample(waiting_bookings, round(
            len(waiting_bookings) * stay_rate))  # составляют stay_rate% от всех открытых бронирований
        # список оставшихся бронирований, которые будут отменены
        bookings_without_stay = list(set(waiting_bookings) - set(bookings_with_stay))

        # получение информации о бронированиях, для которых будут добавлены заселения
        booking_info_query = """SELECT booking_id,rc.room_category_id, checkout_date, rc.current_price from source.booking_fact fb
                            JOIN source.room_category_dim rc 
                            ON fb.room_category_id = rc.room_category_id 
                            WHERE booking_id = any(%s::uuid[]) -- id должен быть в списке bookings_with_stay 
                            """
        cursor.execute(booking_info_query, (bookings_with_stay,))
        book_info_mapping = {i[0]: {'category': i[1], 'checkout_date': i[2], 'price': i[3]} for i in
                             cursor.fetchall()}

        stay_data = []
        revenue_data = []

        # возможные оценки и их распределение
        ratings = [1, 2, 3, 4, 5]
        r_weights = [1, 1, 5, 10, 8]

        # возможные дни сокращения/продления и их распределение
        days_diff = [0, 1, 2, 3, 4, 5]
        d_weights = [19, 2, 2, 1, 1, 1]

        # время создания записей
        creation_time = datetime.now()

        for i in bookings_with_stay:
            stay_length = length_mapping[i]  # предполагаемая длина проживания
            days_late = random.choices(days_diff, d_weights)[0]  # случайное количество дней сокращения
            days_extend = random.choices(days_diff, d_weights)[0]  # случайное количество дней продления
            if days_late < stay_length:
                stay_length -= days_extend
            stay_length += days_extend

            rating = random.choices(ratings, r_weights)[0]  # случайная оценка
            total_paid = book_info_mapping[i]['price'] * stay_length  # цена за фактическое количество дней

            stay_data.append((i, days_late, days_extend, rating, total_paid, stay_length, creation_time))
            revenue_data.append((
                book_info_mapping[i]['category'], book_info_mapping[i]['checkout_date'], total_paid,
                creation_time))

        # добавление записей в таблицу фактов проживания
        add_stays_query = """INSERT INTO source.stay_fact 
                            (booking_id,days_late,days_extend,rating,total_paid,total_nights,created_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            """
        # добавление записей в таблицу фактов дохода
        add_revenue_query = """INSERT INTO source.revenue_fact
                            (room_category_id,date,amount,created_at)
                            VALUES (%s,%s,%s,%s)
                            """
        # обновление статуса в таблице измерений для отмененных бронирований
        close_bookings_without_stay_query = f"""UPDATE source.booking_dim 
                                            SET status='cancelled',last_update='{datetime.now()}'
                                            WHERE booking_id = any(%s::uuid[])
                                            """
        # обновление статуса в таблице измерений для бронирований с заселением
        close_bookings_with_stay_query = f"""UPDATE source.booking_dim 
                                            SET status='checked_in',last_update='{datetime.now()}'
                                            WHERE booking_id = any(%s::uuid[])
                                            """

        cursor.execute(close_bookings_without_stay_query, (bookings_without_stay,))
        cursor.execute(close_bookings_with_stay_query, (bookings_with_stay,))
        cursor.executemany(add_stays_query, stay_data)
        cursor.executemany(add_revenue_query, revenue_data)
        conn.commit()

        logger.info(f'{len(bookings_with_stay)} bookings were confirmed.')
        logger.info(f'{len(stay_data)} records were inserted into stay_fact table successfully.')
        logger.info(f'{len(revenue_data)} records were inserted into revenue_fact table successfully.')


# заполнение начальных значений в таблице измерений room_maintenance_type_dim(1 раз)
def fill_initial_maintenance_types(conn):
    # типы обслуживаний номеров
    maintenance_types = {
        "cosmetic repairs": 200.00,
        "floor repairs": 70.00,
        "plumbing maintenance": 60.00,
        "textile replacement": 50.00,
        "cleaning and disinfection": 50.00,
        "lock replacement": 25.00,
        "appliance replacement": 150.00,
        "furniture renovation": 250.00,
        "interior design": 70.00
    }

    initial_records = []
    # искусственная дата первого заполнения
    start_date = datetime(2024, 1, 1, 0, 0, 0)
    # генерация записей
    for key, value in maintenance_types.items():
        initial_records.append((fake.uuid4(), key, value, 'no info', start_date, True))
    # вставка записей
    insert_types_query = """
                        INSERT INTO source.room_maintenance_type_dim 
                        (maintenance_type_id,type,current_price,additional_info,last_update,is_active_in_source)
                        VALUES (%s,%s,%s,%s,%s,%s);
                        """
    with conn.cursor() as cursor:
        cursor.executemany(insert_types_query, initial_records)
        conn.commit()
        logger.info('Initial maintenance types were inserted successfully.')


# генерация данных об обслуживании номеров для таблицы maintenance_expense_fact
def generate_maintenance_facts(conn):
    # случайное количество обслуживаний
    maintenance_count = random.randint(80, 150)
    with conn.cursor() as cursor:
        # получение id активных номеров
        get_active_room_types_query = """
                                    SELECT room_category_id FROM source.room_category_dim
                                    WHERE is_active_in_source = TRUE
                                    """
        cursor.execute(get_active_room_types_query)
        available_rooms = cursor.fetchall()
        available_rooms = [i[0] for i in available_rooms]

        # получение id и цен активных типов обслуживаний
        get_active_maintenance_types_query = """
                                            SELECT maintenance_type_id,current_price 
                                            FROM source.room_maintenance_type_dim
                                            WHERE is_active_in_source = TRUE
                                            """
        cursor.execute(get_active_maintenance_types_query)
        available_types_with_prices = cursor.fetchall()
        available_types = [i[0] for i in available_types_with_prices]
        type_price_mapping = {i[0]: i[1] for i in available_types_with_prices}

        maintenance_facts = []
        # время создания записей
        creation_time = datetime.now()

        is_unscheduled_options = [True, False]
        is_unscheduled_weights = [1, 3]

        for i in range(maintenance_count):
            room_category_id = random.choice(available_rooms)  # случайный тип номера
            maintenance_type_id = random.choice(available_types)  # случайный тип обслуживания
            m_price = type_price_mapping[maintenance_type_id]  # цена выбранного типа обслуживания
            m_date = fake.date_between(start_date='-1y', end_date='today')  # случайная дата
            is_unscheduled = random.choices(is_unscheduled_options, is_unscheduled_weights)[
                0]  # запланированное/незапланированное
            maintenance_facts.append(
                (room_category_id, m_date, m_price, maintenance_type_id, creation_time, is_unscheduled))

        # вставка записей
        insert_facts_query = """
                            INSERT INTO source.maintenance_expense_fact
                            (room_category_id,date,amount,maintenance_type_id,created_at,is_unscheduled)
                            VALUES (%s,%s,%s,%s,%s,%s);
                            """
        cursor.executemany(insert_facts_query, maintenance_facts)
        conn.commit()

        logger.info(
            f'{len(maintenance_facts)} records were inserted into maintenance_expense_fact table successfully.')


# генерация обновлений данных для таблицы guests_dim
def generate_guests_changes(conn):
    countries = ["USA", "Canada", "China", "India", "Brazil", "Germany", "France", "Italy", "Japan", "Russia"]
    with conn.cursor() as cursor:
        # время обновления записей
        update_time = datetime.now()

        # получение списка id активных гостей
        get_active_guests_query = """
        SELECT guest_id FROM source.guests_dim
        WHERE is_active_in_source = TRUE
        """
        cursor.execute(get_active_guests_query)
        active_guests_ids = cursor.fetchall()
        # если активных гостей нет, процесс завершается
        if len(active_guests_ids) < 1:
            return
        active_guests_ids = [i[0] for i in active_guests_ids]

        # случайное количество гостей для которых будут изменения
        change_last_name_count = random.randint(0, 25)
        change_country_count = random.randint(0, 25)

        for i in range(change_last_name_count):
            guest_id = random.choice(active_guests_ids)  # случайный id из списка
            new_last_name = fake.last_name()

            # обновление фамилии
            update_query = f"""
            UPDATE source.guests_dim
            SET last_name = '{new_last_name}',
            last_update = '{update_time}'::timestamp
            WHERE guest_id = '{guest_id}';
            """
            cursor.execute(update_query)
        logger.info(f'Last name was successfully updated for {change_last_name_count} guests.')

        for i in range(change_country_count):
            guest_id = random.choice(active_guests_ids)  # случайный id из списка
            new_country = random.choice(countries)

            # обновление страны проживания
            update_query = f"""
                            UPDATE source.guests_dim
                            SET country_of_residence = '{new_country}',
                            last_update = '{update_time}'::timestamp
                            WHERE guest_id = '{guest_id}';
                            """
            cursor.execute(update_query)
        logger.info(f'Country was successfully updated for {change_country_count} guests.')
        conn.commit()


# генерация обновлений цен номеров для таблицы room_category_dim
def generate_rooms_changes(conn):
    with conn.cursor() as cursor:
        # время обновления записей
        update_time = datetime.now()

        # получение списка id и цен активных номеров
        get_active_rooms_query = """
                                SELECT room_category_id, current_price FROM source.room_category_dim
                                WHERE is_active_in_source = TRUE
                                """
        cursor.execute(get_active_rooms_query)
        active_rooms_info = cursor.fetchall()
        # если активных номеров нет, процесс завершается
        if len(active_rooms_info) < 1:
            return

        active_rooms_ids = [i[0] for i in active_rooms_info]
        active_rooms_id_price = {i[0]: i[1] for i in active_rooms_info}

        change_rooms_number = random.randint(0, 4)
        rooms_to_change = random.sample(active_rooms_ids,
                                        change_rooms_number)  # выбор случайных номеров для изменения

        for room_id in rooms_to_change:
            # случайное изменение цены
            price_change = random.randint(-10, 20)
            # новая цена применяется только если она больше 80
            new_price = active_rooms_id_price[room_id] + price_change if active_rooms_id_price[
                                                                             room_id] + price_change >= 80 else 80
            update_query = f"""
                            UPDATE source.room_category_dim
                            SET current_price = {new_price},
                            last_update = '{update_time}'::timestamp
                            WHERE room_category_id = '{room_id}';
                            """
            cursor.execute(update_query)
        conn.commit()
        logger.info(f'Price was successfully updated for {change_rooms_number} room types.')


# генерация обновлений цен обслуживания номеров для таблицы room_maintenance_type_dim
def generate_maintenance_types_changes(conn):
    with conn.cursor() as cursor:
        # время обновления записей
        update_time = datetime.now()

        # получение списка id активных типов обслуживаний
        get_active_maintenance_types_query = """
                                SELECT maintenance_type_id FROM source.room_maintenance_type_dim
                                WHERE is_active_in_source = TRUE
                                """
        cursor.execute(get_active_maintenance_types_query)

        active_types_info = cursor.fetchall()
        # если активных типов нет, процесс завершается
        if len(active_types_info) < 1:
            return

        active_types_ids = [i[0] for i in active_types_info]

        change_types_number = random.randint(0, 2)
        types_to_change = random.sample(active_types_ids,
                                        change_types_number)  # выбор случайных типов для изменения

        for m_type in types_to_change:
            # случайное изменение цены
            price_change = random.randint(1, 6)

            # обновление цены
            update_query = f"""
                                            UPDATE source.room_maintenance_type_dim
                                            SET current_price = current_price + {price_change},
                                            last_update = '{update_time}'::timestamp
                                            WHERE maintenance_type_id = '{m_type}';
                                            """
            cursor.execute(update_query)
        conn.commit()
        logger.info(f'Price was successfully updated for {change_types_number} room maintenance types.')


# заполнение таблицы измерений с датами в dwh(1 раз)
def fill_dates_dim_in_dwh(dwh_conn):
    generate_dates_query = """
        INSERT INTO dwh.date_dim (
            date_id,               
            full_date,              
            year,                   
            quarter,               
            month,                
            month_name,           
            day_of_month,          
            day_of_week,            
            day_name,               
            week_of_year,           
            is_weekend,             
            season                  
        )
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INT AS date_id,
            d AS full_date,                        
            EXTRACT(YEAR FROM d) AS year,         
            'Q' || EXTRACT(QUARTER FROM d) AS quarter,
            EXTRACT(MONTH FROM d) AS month,        
            TO_CHAR(d, 'Month') AS month_name,    
            EXTRACT(DAY FROM d) AS day_of_month,  
            EXTRACT(ISODOW FROM d) AS day_of_week,  
            TO_CHAR(d, 'Day') AS day_name,         
            EXTRACT(WEEK FROM d) AS week_of_year,  
            CASE 
                WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE 
                ELSE FALSE 
            END AS is_weekend,                    
            CASE
                WHEN EXTRACT(MONTH FROM d) IN (12, 1, 2) THEN 'Winter' 
                WHEN EXTRACT(MONTH FROM d) IN (3, 4, 5) THEN 'Spring'  
                WHEN EXTRACT(MONTH FROM d) IN (6, 7, 8) THEN 'Summer' 
                ELSE 'Autumn'                                         
            END AS season                           
        FROM generate_series(
            '2020-01-01'::DATE,                    
            '2027-12-31'::DATE,                   
            '1 day'::INTERVAL                       
        ) AS d;
    """

    with dwh_conn.cursor() as cursor:
        cursor.execute(generate_dates_query)
        dwh_conn.commit()
