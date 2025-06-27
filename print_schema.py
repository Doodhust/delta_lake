import psycopg2
from psycopg2 import sql

def print_table_schema(conn, table_name):
    """Выводит схему указанной таблицы"""
    with conn.cursor() as cursor:
        query = sql.SQL("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """)
        cursor.execute(query, [table_name])
        
        print(f"\nСхема таблицы {table_name}:")
        print("{:<20} {:<20} {:<10} {}".format(
            "Имя столбца", "Тип данных", "Nullable", "Значение по умолчанию"))
        print("-" * 60)
        
        for column in cursor.fetchall():
            print("{:<20} {:<20} {:<10} {}".format(
                column[0], column[1], column[2], column[3] or 'NULL'))

try:
    conn = psycopg2.connect(
        host="localhost",
        database="test_db",
        user="admin",
        password="password"
    )
    
    print_table_schema(conn, "transactions")
    print_table_schema(conn, "clients")
    
except Exception as e:
    print(f"Ошибка: {e}")
finally:
    if conn:
        conn.close()