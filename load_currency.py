import psycopg2
import pandas as pd
from io import StringIO
import time

def create_table(cursor, schema, table_name, columns_with_types):
    """Создает таблицу в указанной схеме PostgreSQL"""
    cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    create_sql = f"CREATE TABLE {schema}.{table_name} ({', '.join(columns_with_types)})"
    cursor.execute(create_sql)
    print(f"Таблица {schema}.{table_name} создана")

def load_currency_rates_from_csv(file_path, schema, table_name, cursor, conn):
    """Загружает данные currency_rates из CSV в PostgreSQL"""
    print(f"\nЗагрузка {schema}.{table_name} из CSV...")
    start = time.time()
    
    df = pd.read_csv(file_path)
    
    df['date'] = pd.to_datetime(df['date']).dt.date
    for col in ['USD', 'EUR', 'CNY']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    columns_with_types = [
        "date DATE PRIMARY KEY",
        "USD DECIMAL(15, 6)",
        "EUR DECIMAL(15, 6)",
        "CNY DECIMAL(15, 6)",
        "USD_change DOUBLE PRECISION",
        "EUR_change DOUBLE PRECISION",
        "CNY_change DOUBLE PRECISION"
    ]
    
    create_table(cursor, schema, table_name, columns_with_types)
    
    buffer = StringIO()
    df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='NULL')
    buffer.seek(0)
    
    cursor.copy_from(buffer, f"{schema}.{table_name}", null="NULL")
    conn.commit()
    
    print(f"Загружено {len(df):,} строк за {time.time()-start:.2f} сек")
    return len(df)

try:
    conn = psycopg2.connect(
        host="localhost",
        database="test_db",
        user="admin",
        password="password"
    )
    cursor = conn.cursor()

    file_path = "currency_rates_silver.csv"
    schema_name = "public"
    table_name = "currency_rates"
    
    load_currency_rates_from_csv(file_path, schema_name, table_name, cursor, conn)
    
    cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
    print(f"\nВсего записей курсов валют: {cursor.fetchone()[0]:,}")
    
    print("\nСоздание индексов...")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_currency_rates_date ON {schema_name}.{table_name}(date)")
    conn.commit()
    
    cursor.execute(f"SELECT * FROM {schema_name}.{table_name} ORDER BY date DESC LIMIT 5")
    print("\nПоследние 5 записей:")
    for row in cursor.fetchall():
        print(row)

except Exception as e:
    print(f"\nОшибка: {str(e)}")
    conn.rollback()
finally:
    if conn:
        cursor.close()
        conn.close()
        print("\nСоединение с PostgreSQL закрыто")