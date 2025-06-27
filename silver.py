import psycopg2
import pandas as pd
import pyarrow.parquet as pq
from io import StringIO
import time

def create_table(cursor, table_name, columns_with_types):
    """Создает таблицу в PostgreSQL"""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    create_sql = f"CREATE TABLE {table_name} ({', '.join(columns_with_types)})"
    cursor.execute(create_sql)
    print(f"Таблица {table_name} создана")

def load_from_parquet(parquet_path, table_name, cursor, conn, batch_size=100000):
    """Загружает данные из Parquet в PostgreSQL с явным указанием типов колонок"""
    print(f"\nЗагрузка {table_name} из Parquet...")
    start = time.time()
    
    # Чтение Parquet файла
    parquet_file = pq.ParquetFile(parquet_path)
    
    # Схема таблицы с явным указанием типов
    custom_schema = {
        'id': 'BIGINT',
        'transaction_id': 'BIGINT',
        'client_id': 'BIGINT',
        'amount': 'DECIMAL(15, 2)',
        'currency': 'TEXT',
        'transaction_datetime': 'TIMESTAMP',
        'category': 'TEXT',
        'transaction_date': 'DATE',
        'is_suspicious': 'BOOLEAN'
    }
    
    # Получаем список колонок из файла
    first_batch = next(parquet_file.iter_batches(batch_size=1))
    df_sample = first_batch.to_pandas()
    
    # Создаем список колонок с типами, используя кастомную схему или автоматическое определение
    columns_with_types = []
    for col in df_sample.columns:
        if col in custom_schema:
            # Используем указанный тип из схемы
            columns_with_types.append(f"{col} {custom_schema[col]}")
    
    # Создание таблицы
    create_table(cursor, table_name, columns_with_types)
    
    # Загрузка данных батчами с преобразованием типов
    total_rows = 0
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        
        # Преобразование типов согласно схеме
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        if 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
        
        total_rows += len(df)
        
        # Быстрая загрузка через COPY
        buffer = StringIO()
        df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='NULL')
        buffer.seek(0)
        
        cursor.copy_from(buffer, table_name, null="NULL")
        conn.commit()
        
        print(f"Загружено {total_rows:,} строк", end='\r')
    
    print(f"\nВсего загружено {total_rows:,} строк за {time.time()-start:.2f} сек")
    return total_rows

def load_from_csv(csv_path, table_name, cursor, conn):
    """Загружает данные из CSV в PostgreSQL с заданной схемой"""
    print(f"\nЗагрузка {table_name} из CSV...")
    start = time.time()
    
    # Чтение CSV файла с указанием имен столбцов
    df = pd.read_csv(csv_path, header=None, names=[
        'client_id', 'name', 'registration_date', 'tier', 
        'country', 'age', 'client_category'
    ])
    
    # Преобразование типов данных
    df['registration_date'] = pd.to_datetime(df['registration_date']).dt.date
    
    # Определение типов столбцов для PostgreSQL
    columns_with_types = [
        "client_id BIGINT",
        "name TEXT",
        "registration_date DATE",
        "tier TEXT",
        "country TEXT",
        "age BIGINT",
        "client_category TEXT NOT NULL"  # NOT NULL для client_category
    ]
    
    # Создание таблицы
    create_table(cursor, table_name, columns_with_types)
    
    # Загрузка данных
    buffer = StringIO()
    df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='NULL')
    buffer.seek(0)
    
    cursor.copy_from(buffer, table_name, null="NULL")
    conn.commit()
    
    print(f"Загружено {len(df):,} строк за {time.time()-start:.2f} сек")
    return len(df)

def load_currency_rates_from_csv(csv_path, table_name, cursor, conn):
    """Загружает данные currency_rates из CSV в PostgreSQL"""
    print(f"\nЗагрузка {table_name} из CSV...")
    start = time.time()
    
    try:
        # Чтение CSV файла с заголовками
        df = pd.read_csv(csv_path)
        
        # Преобразование типов данных
        df['date'] = pd.to_datetime(df['date']).dt.date
        for col in ['USD', 'EUR', 'CNY']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Определение типов столбцов для PostgreSQL
        columns_with_types = [
            "date DATE PRIMARY KEY",
            "USD DECIMAL(15, 6)",
            "EUR DECIMAL(15, 6)",
            "CNY DECIMAL(15, 6)",
            "USD_change DOUBLE PRECISION",
            "EUR_change DOUBLE PRECISION",
            "CNY_change DOUBLE PRECISION"
        ]
        
        # Создание таблицы
        create_table(cursor, table_name, columns_with_types)
        conn.commit()  # Явная фиксация создания таблицы
        
        # Загрузка данных
        buffer = StringIO()
        df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='NULL')
        buffer.seek(0)
        
        cursor.copy_from(buffer, table_name, null="NULL")
        conn.commit()
        
        print(f"Загружено {len(df):,} строк за {time.time()-start:.2f} сек")
        return len(df)
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке данных: {str(e)}")
        return 0


try:
    # Подключение к PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        database="test_db",
        user="admin",
        password="password"
    )
    cursor = conn.cursor()

    # Пути к файлам
    transactions_parquet = "silver_transactions.snappy.parquet"
    clients_csv = "silver_clients.csv"
    currency_rates_csv = "currency_rates_silver.csv"
    
    # Загрузка данных
    load_from_parquet(transactions_parquet, "transactions", cursor, conn)
    load_from_csv(clients_csv, "clients", cursor, conn)
    load_currency_rates_from_csv(currency_rates_csv, "currency_rates", cursor, conn)
    
    # Проверка
    cursor.execute("SELECT COUNT(*) FROM transactions")
    print(f"\nВсего транзакций в БД: {cursor.fetchone()[0]:,}")
    
    cursor.execute("SELECT COUNT(*) FROM clients")
    print(f"Всего клиентов в БД: {cursor.fetchone()[0]:,}")

    cursor.execute("SELECT COUNT(*) FROM currency_rates")
    print(f"Всего записей курсов валют: {cursor.fetchone()[0]:,}")
    
    # Создание индексов
    print("\nСоздание индексов...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_currency_rates_date ON currency_rates(date)")
    conn.commit()

except Exception as e:
    print(f"\nОшибка: {str(e)}")
    if 'conn' in locals():
        conn.rollback()
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
        print("\nСоединение с PostgreSQL закрыто")