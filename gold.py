import psycopg2
import pandas as pd
from datetime import datetime
import time

def create_gold_layer(conn):
    cursor = conn.cursor()
    
    cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")

    total_start_time = time.time()

    # 1. Витрина: Клиентская статистика
    start_time = time.time()
    cursor.execute("""
        DROP TABLE IF EXISTS gold.client_stats;
        CREATE TABLE gold.client_stats AS
        SELECT 
            c.client_id,
            c.name,
            c.country,
            c.client_category,
            c.tier,
            SUM(t.amount) AS total_amount,
            AVG(t.amount) AS avg_amount
        FROM 
            clients c
        LEFT JOIN 
            transactions t ON c.client_id = t.client_id
        GROUP BY 
            c.client_id, c.name, c.country, c.client_category, c.tier
        ORDER BY 
            total_amount DESC;
    """)
    print(f"Client stats created in {time.time() - start_time:.2f} sec")
    
    # 2. Витрина: Анализ подозрительных операций
    start_time = time.time()
    cursor.execute("""
        DROP TABLE IF EXISTS gold.fraud_analysis;
        CREATE TABLE gold.fraud_analysis AS
        SELECT 
            t.category,
            c.country,
            COUNT(*) AS fraud_count,
            AVG(t.amount) AS avg_fraud_amount,
            SUM(t.amount) AS total_fraud_amount
        FROM 
            transactions t
        JOIN 
            clients c ON t.client_id = c.client_id
        WHERE 
            t.is_suspicious = TRUE
        GROUP BY 
            t.category, c.country
        ORDER BY 
            total_fraud_amount DESC;
    """)
    print(f"Fraud analysis created in {time.time() - start_time:.2f} sec")

    # Создание таблицы для ежедневных метрик
    start_time = time.time()
    cursor.execute("""
            DROP TABLE IF EXISTS gold.daily_metrics;
            CREATE TABLE gold.daily_metrics AS
            SELECT 
                t.transaction_date AS date,
                SUM(CASE 
                    WHEN t.currency = 'USD' THEN t.amount * cr.USD
                    WHEN t.currency = 'EUR' THEN t.amount * cr.EUR
                    WHEN t.currency = 'CNY' THEN t.amount * cr.CNY
                    ELSE t.amount
                END) AS daily_volume_rub,
                AVG(CASE 
                    WHEN t.currency = 'USD' THEN t.amount * cr.USD
                    WHEN t.currency = 'EUR' THEN t.amount * cr.EUR
                    WHEN t.currency = 'CNY' THEN t.amount * cr.CNY
                    ELSE t.amount
                END) AS avg_transaction_rub,
                COUNT(*) AS transactions_count,
                SUM(CASE WHEN t.is_suspicious THEN 1 ELSE 0 END) AS suspicious_count,
                SUM(CASE 
                    WHEN t.is_suspicious THEN 
                        CASE 
                            WHEN t.currency = 'USD' THEN t.amount * cr.USD
                            WHEN t.currency = 'EUR' THEN t.amount * cr.EUR
                            WHEN t.currency = 'CNY' THEN t.amount * cr.CNY
                            ELSE t.amount
                        END
                    ELSE 0
                END) AS suspicious_volume_rub
            FROM transactions t
            LEFT JOIN currency_rates cr ON t.transaction_date = cr.date
            GROUP BY t.transaction_date
            ORDER BY t.transaction_date;
        """)
    print(f"Client stats created in {time.time() - start_time:.2f} sec")
    
    
    conn.commit()
    cursor.close()

    print(f"total time  {time.time() - total_start_time:.2f} sec")

def analyze_results(conn):
    cursor = conn.cursor()
    
    print("\nТоп клиентов по объему операций:")
    cursor.execute("""
        SELECT * FROM gold.client_stats 
        ORDER BY total_amount DESC 
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(row)
    
    print("\nАнализ подозрительных операций:")
    cursor.execute("""
        SELECT * FROM gold.fraud_analysis 
        ORDER BY total_fraud_amount DESC 
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(row)

    print("\nежедневные метрики:")
    cursor.execute("""
        SELECT * FROM gold.daily_metrics 
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(row)
    
    cursor.close()

def main():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="test_db",
            user="admin",
            password="password"
        )
        
        create_gold_layer(conn)
        
        analyze_results(conn)
        
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if conn:
            conn.close()
            print("\nСоединение с PostgreSQL закрыто")

if __name__ == "__main__":
    main()