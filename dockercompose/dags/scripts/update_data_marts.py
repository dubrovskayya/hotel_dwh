import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


def update_data_marts(conn, dm_schema, dwh_schema):
    # средняя оценка
    update_avg_rating_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.avg_rating;
    
    CREATE TABLE {dm_schema}.avg_rating AS
    SELECT 
        AVG(rating) AS average_rating
    FROM {dwh_schema}.stay_fact sf;
    """

    # процент от общего дохода и количества проживаний,
    # отдельный доход и количество проживаний по типам номеров
    update_revenue_and_stays_prct_by_room_type_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.revenue_and_stays_prct_by_room_type;
    
    CREATE TABLE {dm_schema}.revenue_and_stays_prct_by_room_type AS
    WITH total_revenue AS (
        SELECT SUM(amount) AS total
        FROM {dwh_schema}.revenue_fact rf
    ),
    total_stays AS (
        SELECT COUNT(amount) AS total
        FROM {dwh_schema}.revenue_fact rf
    )
    SELECT 
        rcd.category_name,
        ROUND(SUM(rf.amount) / (SELECT total FROM total_revenue) * 100, 3) AS percentage_of_total_income,
        SUM(rf.amount) AS income,
        ROUND(COUNT(rf.amount)::NUMERIC / (SELECT total FROM total_stays) * 100, 3) AS percentage_of_total_stays,
        COUNT(rf.amount) AS total_stays
    FROM {dwh_schema}.revenue_fact rf
    INNER JOIN {dwh_schema}.room_category_dim rcd 
        ON rcd.room_category_sk = rf.room_category_sk
    GROUP BY rcd.category_name;
    """

    # процент от общего дохода и количества проживаний,
    # отдельный доход и количество проживаний по месяцам
    update_revenue_and_stays_prct_by_month_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.revenue_and_stays_prct_by_month;
    
    CREATE TABLE {dm_schema}.revenue_and_stays_prct_by_month AS
    WITH total_revenue AS (
        SELECT SUM(amount) AS total
        FROM {dwh_schema}.revenue_fact rf
    ),
    stays_count AS (
        SELECT COUNT(amount) AS total
        FROM {dwh_schema}.revenue_fact rf
    )
    SELECT 
        dd.month_name,
        ROUND(SUM(rf.amount) / (SELECT total FROM total_revenue) * 100, 3) AS percentage_of_total_income,
        SUM(rf.amount) AS revenue,
        ROUND(COUNT(rf.amount)::DECIMAL / (SELECT total FROM stays_count) * 100, 3) AS percentage_of_total_stays,
        COUNT(rf.amount) AS stays_count
    FROM {dwh_schema}.revenue_fact rf
    INNER JOIN {dwh_schema}.date_dim dd 
        ON dd.date_id = rf.date_id
    GROUP BY dd.month_name;
    """

    # процент повторных бронирований
    update_repeat_booking_percentage_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.repeat_booking_percentage;

    CREATE TABLE {dm_schema}.repeat_booking_percentage AS
    WITH stays_count AS (
        SELECT 
            gd.guest_id, 
            COUNT(DISTINCT sf.stay_id) AS cnt
        FROM 
            {dwh_schema}.stay_fact sf
        INNER JOIN 
            {dwh_schema}.booking_fact bf 
            ON sf.booking_id = bf.booking_id
        INNER JOIN 
            {dwh_schema}.guests_dim gd 
            ON gd.guest_sk = bf.guest_sk
        GROUP BY 
            gd.guest_id
    )
    SELECT 
        ROUND(
            ((SELECT COUNT(*) FROM stays_count WHERE cnt > 1)::DECIMAL / 
             (SELECT COUNT(*) FROM stays_count) * 100), 2) AS repeat_booking_percentage;
    """

    # процент подтвержденных бронирований
    update_stay_rate_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.stay_rate;
    
    CREATE TABLE {dm_schema}.stay_rate AS
    SELECT 
        ROUND(
            COUNT(bd.booking_id)::DECIMAL / 
            (SELECT COUNT(booking_id) FROM {dwh_schema}.booking_dim) * 100, 3) AS stay_rate_percentage
    FROM {dwh_schema}.booking_dim bd
    WHERE bd.status = 'checked_in';
    """

    # количество активных гостей
    update_active_guests_number_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.active_guests_number;

    CREATE TABLE {dm_schema}.active_guests_number AS
    SELECT COUNT(DISTINCT gd.guest_id) AS active_guests_count
    FROM {dwh_schema}.booking_fact bf
    INNER JOIN {dwh_schema}.guests_dim gd 
        ON gd.guest_sk = bf.guest_sk;
    """

    # общий доход
    update_total_revenue_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.total_revenue;

    CREATE TABLE {dm_schema}.total_revenue AS
    SELECT SUM(amount)
    FROM {dwh_schema}.revenue_fact;
    """

    # общее количество проживаний
    update_total_stays_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.total_stays;
    
    CREATE TABLE {dm_schema}.total_stays AS
    SELECT COUNT(DISTINCT stay_id) FROM {dwh_schema}.stay_fact;
    """

    # доходы и расходы по месяцам
    update_revenue_expenses_by_month_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.revenue_expenses_by_month;

    CREATE TABLE {dm_schema}.revenue_expenses_by_month AS
    WITH revenue AS (
        SELECT SUM(rf.amount) AS month_revenue, dd.month_name
        FROM {dwh_schema}.revenue_fact rf
        INNER JOIN {dwh_schema}.date_dim dd 
            ON rf.date_id = dd.date_id
        GROUP BY dd.month_name
    ),
    expenses AS (
        SELECT SUM(mef.amount) AS month_expenses, dd.month_name
        FROM {dwh_schema}.maintenance_expense_fact mef
        INNER JOIN {dwh_schema}.date_dim dd 
            ON dd.date_id = mef.date_id
        GROUP BY dd.month_name
    )
    SELECT r.month_name, month_revenue, month_expenses
    FROM revenue r
    INNER JOIN expenses e
        ON e.month_name = r.month_name;
    """

    # средняя загруженность в день по сезонам
    update_avg_stays_by_season_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.avg_stays_by_season;

    CREATE TABLE {dm_schema}.avg_stays_by_season AS
    WITH count_by_dates AS (
        SELECT dd.date_id, dd.season, COUNT(bf.booking_id) AS cnt
        FROM {dwh_schema}.stay_fact sf
        INNER JOIN {dwh_schema}.booking_fact bf 
            ON sf.booking_id = bf.booking_id
        INNER JOIN {dwh_schema}.date_dim dd 
            ON dd.date_id BETWEEN bf.checkin_date_id AND bf.checkout_date_id
        GROUP BY dd.date_id, dd.season
    )
    SELECT season, ROUND(AVG(cnt), 2) AS avg_stays
    FROM count_by_dates
    GROUP BY season;
    """

    # средняя загруженность в день по месяцам
    update_avg_stays_by_month_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.avg_stays_by_month;

    CREATE TABLE {dm_schema}.avg_stays_by_month AS
    WITH count_by_dates AS (
        SELECT dd.date_id, dd.month_name,dd.month, COUNT(bf.booking_id) AS cnt
        FROM {dwh_schema}.stay_fact sf
        INNER JOIN {dwh_schema}.booking_fact bf 
            ON sf.booking_id = bf.booking_id
        INNER JOIN {dwh_schema}.date_dim dd 
            ON dd.date_id BETWEEN bf.checkin_date_id AND bf.checkout_date_id
        GROUP BY dd.date_id, dd.month_name, dd.month
        ORDER BY dd.month
    )
    SELECT month_name, ROUND(AVG(cnt), 2) AS avg_stays
    FROM count_by_dates
    GROUP BY month_name;
    """

    # процент дохода и доход по сезонам
    update_revenue_prct_by_season_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.revenue_prct_by_season;

    CREATE TABLE {dm_schema}.revenue_prct_by_season AS
    WITH total_revenue AS (
        SELECT SUM(amount)
        FROM {dwh_schema}.revenue_fact
    )
    SELECT dd.season, SUM(amount) AS revenue, 
           ROUND(SUM(amount) / (SELECT * FROM total_revenue) * 100, 3) AS percentage_of_total_revenue
    FROM {dwh_schema}.revenue_fact rf
    INNER JOIN {dwh_schema}.date_dim dd 
        ON dd.date_id = rf.date_id
    GROUP BY dd.season;
    """

    # средняя загруженность в выходные и будние дни
    update_avg_stays_weekend_and_non_weekend_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.avg_stays_weekend_and_non_weekend;

    CREATE TABLE {dm_schema}.avg_stays_weekend_and_non_weekend AS
    WITH count_by_dates AS (
        SELECT dd.date_id, dd.is_weekend, COUNT(*) AS stays_count
        FROM {dwh_schema}.booking_fact bf
        INNER JOIN {dwh_schema}.date_dim dd 
            ON dd.date_id BETWEEN bf.checkin_date_id AND bf.checkout_date_id
        GROUP BY dd.date_id, dd.is_weekend
    ),
    avg_stays_weekend AS (
        SELECT ROUND(AVG(stays_count), 2) AS avg_weekend
        FROM count_by_dates
        WHERE is_weekend = TRUE
    ),
    avg_stays_not_weekend AS (
        SELECT ROUND(AVG(stays_count), 2) AS avg_not_weekend
        FROM count_by_dates
        WHERE is_weekend = FALSE
    )
    SELECT *
    FROM avg_stays_weekend
    CROSS JOIN avg_stays_not_weekend;
    """

    # доходы и расходы по типам номеров и месяцам
    update_revenue_expenses_by_room_month_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.revenue_expenses_by_room_month;
    
    CREATE TABLE {dm_schema}.revenue_expenses_by_room_month AS
    WITH expenses AS (
        SELECT SUM(mef.amount) AS room_type_expenses, rcd.category_name, dd.month_name
        FROM {dwh_schema}.room_category_dim rcd
        INNER JOIN {dwh_schema}.maintenance_expense_fact mef 
            ON mef.room_category_sk = rcd.room_category_sk
        INNER JOIN {dwh_schema}.date_dim dd 
            ON dd.date_id = mef.date_id
        GROUP BY dd.month_name, rcd.category_name
    ),
    revenue AS (
        SELECT SUM(rf.amount) AS room_type_revenue, rcd.category_name, dd.month_name
        FROM {dwh_schema}.room_category_dim rcd
        INNER JOIN {dwh_schema}.revenue_fact rf 
            ON rcd.room_category_sk = rf.room_category_sk
        INNER JOIN {dwh_schema}.date_dim dd 
            ON rf.date_id = dd.date_id
        GROUP BY dd.month_name, rcd.category_name
    )
    SELECT r.month_name, r.category_name, room_type_revenue, room_type_expenses
    FROM revenue r
    INNER JOIN expenses e 
        ON r.month_name = e.month_name AND r.category_name = e.category_name;
    """

    # незапланированные обслуживания по типу номера и типу обслуживания
    update_unscheduled_maintenance_by_room_mtype_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.unscheduled_maintenance_by_room_mtype;

    CREATE TABLE {dm_schema}.unscheduled_maintenance_by_room_mtype AS
    SELECT rcd.category_name, rmtd.type AS maintenance_type, 
           SUM(mef.amount) AS unscheduled_maintenance_sum, 
           COUNT(mef.amount) AS unscheduled_maintenance_count
    FROM {dwh_schema}.room_category_dim rcd
    INNER JOIN {dwh_schema}.maintenance_expense_fact mef
        ON mef.room_category_sk = rcd.room_category_sk 
        AND mef.is_unscheduled = TRUE
    INNER JOIN {dwh_schema}.room_maintenance_type_dim rmtd
        ON mef.maintenance_type_sk = rmtd.maintenance_type_sk
    GROUP BY rcd.category_name, rmtd.type;
    """

    # незапланированные обслуживания по типу обслуживания
    update_unscheduled_maintenance_by_type_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.unscheduled_maintenance_by_type;

    CREATE TABLE {dm_schema}.unscheduled_maintenance_by_type AS
    SELECT rmtd.type, 
           SUM(mef.amount) AS unscheduled_maintenance_sum, 
           COUNT(mef.amount) AS unscheduled_maintenance_count
    FROM {dwh_schema}.room_maintenance_type_dim rmtd
    INNER JOIN {dwh_schema}.maintenance_expense_fact mef 
        ON mef.maintenance_type_sk = rmtd.maintenance_type_sk
        AND mef.is_unscheduled = TRUE
    GROUP BY rmtd.type;
    """

    # общая сумма обслуживаний по типам
    update_maintenance_sum_by_type_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.maintenance_sum_by_type;

    CREATE TABLE {dm_schema}.maintenance_sum_by_type AS
    SELECT rmtd.type, SUM(amount) AS total_amount
    FROM {dwh_schema}.maintenance_expense_fact mef
    INNER JOIN {dwh_schema}.room_maintenance_type_dim rmtd 
        ON rmtd.maintenance_type_sk = mef.maintenance_type_sk
    GROUP BY rmtd.type;
    """

    # самые популярные и непопулярные номера по месяцам
    update_most_least_popular_room_by_month_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.most_popular_room_by_month;

    CREATE TABLE {dm_schema}.most_popular_room_by_month AS
        SELECT dd.month_name,rcd.category_name,COUNT(sf.booking_id) AS cnt
        FROM {dwh_schema}.stay_fact sf
        INNER JOIN {dwh_schema}.booking_fact bf 
            ON sf.booking_id = bf.booking_id
        INNER JOIN {dwh_schema}.room_category_dim rcd 
            ON rcd.room_category_sk = bf.room_category_sk
        INNER JOIN {dwh_schema}.date_dim dd
            ON dd.date_id BETWEEN bf.checkin_date_id AND bf.checkout_date_id
        GROUP BY dd.month_name, rcd.category_name;
    """

    # предпочтения по типам номеров по национальности
    update_room_preferences_by_nationality_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.room_preferences_by_nationality;

    CREATE TABLE {dm_schema}.room_preferences_by_nationality AS
        SELECT gd.nationality, rcd.category_name, COUNT(*) AS cnt
        FROM {dwh_schema}.guests_dim gd
        INNER JOIN {dwh_schema}.booking_fact bf 
            ON bf.guest_sk = gd.guest_sk
        INNER JOIN {dwh_schema}.room_category_dim rcd 
            ON rcd.room_category_sk = bf.room_category_sk
        GROUP BY gd.nationality, rcd.category_name;
    """

    # средняя продолжительность проживания и счет по странам
    update_avg_stays_and_bill_by_country_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.avg_stays_and_bill_by_country;

    CREATE TABLE {dm_schema}.avg_stays_and_bill_by_country AS
    SELECT gd.country_of_residence,
           ROUND(AVG(bd.expected_length_of_stay + sf.days_extend - sf.days_late), 2) AS avg_stay,
           ROUND(AVG(sf.total_paid), 3) AS avg_bill
    FROM {dwh_schema}.stay_fact sf
    INNER JOIN {dwh_schema}.booking_dim bd 
        ON bd.booking_id = sf.booking_id
    INNER JOIN {dwh_schema}.booking_fact bf 
        ON bf.booking_id = sf.booking_id
    INNER JOIN {dwh_schema}.guests_dim gd 
        ON gd.guest_sk = bf.guest_sk
    GROUP BY gd.country_of_residence;
    """

    # распределение возрастов активных гостей
    update_age_distribution_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.age_distribution;

    CREATE TABLE {dm_schema}.age_distribution AS
    SELECT EXTRACT(YEAR FROM AGE(date_of_birth)) AS age, COUNT(*) AS number
    FROM {dwh_schema}.guests_dim gd
    WHERE is_active = TRUE
    GROUP BY EXTRACT(YEAR FROM AGE(date_of_birth));
    """

    # наиболее распространенная национальность для каждого типа номера
    update_most_common_nationality_for_room_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.most_common_nationality_for_room;

    CREATE TABLE {dm_schema}.most_common_nationality_for_room AS
    WITH stays_count AS (
        SELECT rcd.category_name, gd.nationality, COUNT(*) AS cnt
        FROM {dwh_schema}.stay_fact sf
        INNER JOIN {dwh_schema}.booking_fact bf 
            ON sf.booking_id = bf.booking_id
        INNER JOIN {dwh_schema}.room_category_dim rcd 
            ON bf.room_category_sk = rcd.room_category_sk
        INNER JOIN {dwh_schema}.guests_dim gd 
            ON gd.guest_sk = bf.guest_sk
        GROUP BY rcd.category_name, gd.nationality
    )
    SELECT category_name, nationality
    FROM stays_count sc1
    WHERE cnt = (
        SELECT MAX(cnt)
        FROM stays_count sc2
        WHERE sc1.category_name = sc2.category_name
    );
    """

    # процент низких оценок по типам номеров
    update_bad_rating_percentage_by_room_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.bad_rating_percentage_by_room;

    CREATE TABLE {dm_schema}.bad_rating_percentage_by_room AS
    WITH total_rating_count AS (
        SELECT rcd.category_name, COUNT(sf.rating) AS cnt
        FROM {dwh_schema}.booking_fact bf
        INNER JOIN {dwh_schema}.stay_fact sf 
            ON sf.booking_id = bf.booking_id
        INNER JOIN {dwh_schema}.room_category_dim rcd 
            ON rcd.room_category_sk = bf.room_category_sk
        GROUP BY rcd.category_name
    )
    SELECT rcd.category_name,
           COUNT(sf.rating)::DECIMAL / (
               SELECT cnt
               FROM total_rating_count tr
               WHERE tr.category_name = rcd.category_name
           ) AS bad_rating_percentage
    FROM {dwh_schema}.booking_fact bf
    INNER JOIN {dwh_schema}.stay_fact sf 
        ON sf.booking_id = bf.booking_id AND sf.rating < 3
    INNER JOIN {dwh_schema}.room_category_dim rcd 
        ON rcd.room_category_sk = bf.room_category_sk
    GROUP BY rcd.category_name;
    """

    # выбор номеров по цели визита
    update_room_choice_by_visit_purpose_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.room_choice_by_visit_purpose;

    CREATE TABLE {dm_schema}.room_choice_by_visit_purpose AS

        SELECT bf.visit_purpose, rcd.category_name, COUNT(*) AS cnt
        FROM {dwh_schema}.stay_fact sf
        INNER JOIN {dwh_schema}.booking_fact bf 
            ON sf.booking_id = bf.booking_id
        INNER JOIN {dwh_schema}.room_category_dim rcd 
            ON rcd.room_category_sk = bf.room_category_sk
        GROUP BY bf.visit_purpose, rcd.category_name
    """

    # распределение целей визита по странам
    update_most_common_purpose_for_country_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.most_common_purpose_for_country;

    CREATE TABLE {dm_schema}.most_common_purpose_for_country AS
    SELECT gd.country_of_residence, bf.visit_purpose, COUNT(*) AS cnt
    FROM {dwh_schema}.stay_fact sf
    INNER JOIN {dwh_schema}.booking_fact bf 
        ON sf.booking_id = bf.booking_id
    INNER JOIN {dwh_schema}.guests_dim gd 
        ON gd.guest_sk = bf.guest_sk
    GROUP BY gd.country_of_residence, bf.visit_purpose;
    """

    # средняя продолжительность проживания по целям визита
    update_avg_stay_by_purpose_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.avg_stay_by_purpose;

    CREATE TABLE {dm_schema}.avg_stay_by_purpose AS
    SELECT bf.visit_purpose,
           AVG(bd.expected_length_of_stay + sf.days_extend - sf.days_late) AS avg_stay
    FROM {dwh_schema}.stay_fact sf
    INNER JOIN {dwh_schema}.booking_fact bf 
        ON sf.booking_id = bf.booking_id
    INNER JOIN {dwh_schema}.booking_dim bd 
        ON bd.booking_id = bf.booking_id
    GROUP BY bf.visit_purpose;
    """

    # распространение целей визита по типам номеров
    update_most_common_month_by_purpose_dm_query = f"""
    DROP TABLE IF EXISTS {dm_schema}.most_common_month_by_purpose;

    CREATE TABLE {dm_schema}.most_common_month_by_purpose AS
    SELECT bf.visit_purpose, dd.month_name, COUNT(*) AS cnt
    FROM {dwh_schema}.stay_fact sf
    INNER JOIN {dwh_schema}.booking_fact bf 
        ON sf.booking_id = bf.booking_id
    INNER JOIN {dwh_schema}.date_dim dd 
        ON dd.date_id = bf.checkout_date_id
    GROUP BY dd.month_name, bf.visit_purpose;
    """

    # список всех запросов
    update_queries = [
        update_avg_rating_dm_query,
        update_revenue_and_stays_prct_by_room_type_dm_query,
        update_revenue_and_stays_prct_by_month_dm_query,
        update_stay_rate_dm_query,
        update_total_stays_dm_query,
        update_repeat_booking_percentage_dm_query,
        update_active_guests_number_dm_query,
        update_total_revenue_dm_query,
        update_revenue_expenses_by_month_dm_query,
        update_avg_stays_by_season_dm_query,
        update_avg_stays_by_month_dm_query,
        update_revenue_prct_by_season_dm_query,
        update_avg_stays_weekend_and_non_weekend_dm_query,
        update_revenue_expenses_by_room_month_dm_query,
        update_unscheduled_maintenance_by_room_mtype_dm_query,
        update_unscheduled_maintenance_by_type_dm_query,
        update_maintenance_sum_by_type_dm_query,
        update_most_least_popular_room_by_month_dm_query,
        update_room_preferences_by_nationality_dm_query,
        update_avg_stays_and_bill_by_country_dm_query,
        update_age_distribution_dm_query,
        update_most_common_nationality_for_room_dm_query,
        update_bad_rating_percentage_by_room_dm_query,
        update_room_choice_by_visit_purpose_dm_query,
        update_most_common_purpose_for_country_dm_query,
        update_avg_stay_by_purpose_dm_query,
        update_most_common_month_by_purpose_dm_query
    ]

    try:
        with conn.cursor() as cursor:
            logger.info(f'Data marts update started at {datetime.now()}.')
            for q in update_queries:
                cursor.execute(q)
            conn.commit()
            logger.info('All data marts updated successfully.')
    except Exception as e:
        conn.rollback()
        logger.error(f'{str(e)}. Rollback')
