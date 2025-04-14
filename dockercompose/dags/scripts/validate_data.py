import pandas as pd


def validate_data(data, columns, table_name, logger):
    invalid_rows = []
    masks = []
    df = pd.DataFrame(data=data, columns=columns)
    # удаляются дубликаты
    df.drop_duplicates()

    # проверка на null-значения
    null_mask = df.isnull().any(axis=1)
    if null_mask.any():
        invalid_rows.append(df[null_mask].copy())
    masks.append(null_mask)

    # проверка корректности столбцов с датами
    for i in validation_conditions[table_name]['date_columns']:
        invalid_date_mask = ~pd.to_datetime(df[i], errors="coerce").notnull()
        if invalid_date_mask.any():
            invalid_rows.append(df[invalid_date_mask].copy())
        masks.append(invalid_date_mask)

    # проверка корректности числовых столбцов
    for i in validation_conditions[table_name]['numeric_columns']:
        invalid_numeric_mask = ~pd.to_numeric(df[i], errors="coerce").ge(0)
        if invalid_numeric_mask.any():
            invalid_rows.append(df[invalid_numeric_mask].copy)
        masks.append(invalid_numeric_mask)

    # логирование данных некорректных записей
    if invalid_rows:
        invalid_df = pd.concat(invalid_rows).drop_duplicates()
        logger.info(f"{len(invalid_df)} record were removed. \n{invalid_df.to_string()}")
    else:
        logger.info('All records were valid.')

    # маска для корректных значений (записи, для которых не действует ни одна маска из списка)
    valid_mask = ~pd.concat([pd.Series(mask) for mask in masks], axis=1).any(axis=1)
    # получение индексов корректных значений
    valid_indices = df[valid_mask].index.tolist()

    # возвращаются только корректные значения из исходных данных
    return [data[i] for i in valid_indices]


validation_conditions = {
    'guests_dim': {'date_columns': ['date_of_birth', 'last_update'],
                   'numeric_columns': []},
    'room_category_dim': {'date_columns': ['last_update'],
                          'numeric_columns': ['current_price']},
    'booking_fact': {'date_columns': ['checkin_date', 'checkout_date', 'created_at'],
                     'numeric_columns': []},
    'booking_dim': {'date_columns': ['last_update'],
                    'numeric_columns': ['expected_length_of_stay']},
    'stay_fact': {'date_columns': ['created_at'],
                  'numeric_columns': ['days_late', 'days_extend', 'rating', 'total_paid', 'total_nights']},
    'revenue_fact': {'date_columns': ['date', 'created_at'],
                     'numeric_columns': ['amount']},
    'room_maintenance_type_dim': {'date_columns': ['last_update'],
                                  'numeric_columns': ['current_price']},
    'maintenance_expense_fact': {'date_columns': ['date', 'created_at'],
                                 'numeric_columns': ['amount']}
}
