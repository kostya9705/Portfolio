"""
Скрипт для импорта данных из CSV файлов в базу данных SQLite
"""

import sqlite3
import csv
import os
from datetime import datetime
from typing import Optional


BATCH_SIZE = 10_000  # размер батча для вставки данных


def create_database(db_path: str, sql_script_path: str) -> None:
    """
    Создает базу данных и таблицы из SQL скрипта.
    
    Args:
        db_path: путь к файлу базы данных.
        sql_script_path: путь к SQL скрипту создания таблиц.
    Returns:
        None
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # читаем и выполняем SQL скрипт
    with open(sql_script_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()
        cursor.executescript(sql_script)
        conn.commit()
    
    print(f"База данных создана: {db_path}")


def import_categories(conn: sqlite3.Connection, csv_path: str) -> None:
    """
    Импорт данных "категорий" покупок/транзакций в БД.

    Args:
        conn: объект подключения к базе данных.
        csv_path: путь к CSV файлу с данными категорий.
    Returns:
        None
    """
    cursor = conn.cursor() 
    count = 0   # счетчик импортированных записей
    batch = []  # храним бачти для вставки
    query  = '''
    INSERT INTO categories (id, name, description, mcc_code)
    VALUES (?, ?, ?, ?)
    '''

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:

            row_to_batch = (int(row['id']), row['name'], row['description'], row['mcc-code'])
            batch.append(row_to_batch)

            # вставка данных батчами 
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(query, batch)
                conn.commit()

                count += len(batch)
                print(f"Импортировано категорий: {count}")
                batch = []  # очистка батча после вставки
        
        # вставка оставшихся записей
        if batch:
            cursor.executemany(query, batch)
            conn.commit()
    
    print(f"Всего импортировано категорий: {count}")


def import_clients(conn: sqlite3.Connection, csv_path: str) -> None:
    """
    Импорт данных клиентов.
    
    Args:
        conn: объект подключения к базе данных.
        csv_path: путь к csv файлу с данными о клиентах.
    Returns:
        None
    """
    cursor = conn.cursor()
    count = 0
    batch = []
    query  = '''
    INSERT INTO clients (
        id, fullname, address, phone_number, email, 
        workplace, birthdate, registration_date, gender, 
        income, expenses, credit, deposit)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # обработка пустых значений
            row['income'] = float(row['income']) if row['income'] else None
            row['expenses'] = float(row['expenses']) if row['expenses'] else None
            row['credit'] = int(row['credit']) if row['credit'] else 0
            row['deposit'] = int(row['deposit']) if row['deposit'] else 0
            
            row_to_batch = (
                int(row['id']),
                row['fullname'],
                row['address'],
                row['phone_number'],
                row['email'],
                row['workplace'],
                row['birthdate'],
                row['registration_date'],
                row['gender'],
                row['income'],
                row['expenses'],
                row['credit'],
                row['deposit']
            )
            
            batch.append(row_to_batch)
            
            # вставка батчами для ускорения
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(query, batch)
                conn.commit()
                count += len(batch)
                print(f"Импортировано клиентов: {count}")
                batch = []
        
        # вставка оставшихся записей
        if batch:
            cursor.executemany(query, batch)
            conn.commit()
    
    print(f"Всего импортировано клиентов: {count}")


def import_subscriptions(conn: sqlite3.Connection, csv_path: str) -> None:
    """
    Импорт данных подписок клиентов.
    
    Args:
        conn: объект подключения к базе данных.
        csv_path: путь к csv файлу с данными о подписках клиентов.
    Returns:
        None
    """
    cursor = conn.cursor()
    count = 0
    batch = []
    query = '''
    INSERT INTO subscriptions (
        id, client_id, product_category, 
        product_company, amount, date_start, date_end)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    '''
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # обработка пустых значений
            row['amount'] = float(row['amount']) if row['amount'] else None
            # TODO: проверить дополнительно row['date_start']
            row['date_end'] = row['date_end'] if row['date_end'] else None
            
            row_to_batch = (
                int(row['id']),
                int(row['client_id']),
                int(row['product_category']),
                row['product_company'],
                row['amount'],
                row['date_start'],
                row['date_end']
            )
            
            batch.append(row_to_batch)
            
            # вставка батчами для ускорения
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(query, batch)
                conn.commit()
                count += len(batch)
                print(f"Импортировано подписок: {count}")
                batch = []
        
        # вставка оставшихся записей
        if batch:
            cursor.executemany(query, batch)
            conn.commit()
    
    print(f"Всего импортировано подписок: {count}")


def import_transactions(conn: sqlite3.Connection, csv_path: str) -> None:
    """
    Импорт данных транзакций клиентов.
    
    Args:
        conn: объект подключения к базе данных.
        csv_path: путь к csv файлу с данными о транзакциях клиентов.
    Returns:
        None
    """
    cursor = conn.cursor()
    count = 00
    batch = []
    query = '''
    INSERT INTO transactions (
        client_id, product_category, 
        product_company, subtype, amount, 
        date, transaction_type)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    '''
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # пропускаем строки без client_id
            if not row['client_id']:
                continue
            
            # обработка пустых значений
            row['amount'] = float(row['amount']) if row['amount'] else None
            
            row_to_batch = (
                int(row['client_id']),
                int(row['product_category']),
                row['product_company'],
                row['subtype'],
                row['amount'],
                row['date'],
                row['transaction_type']
            )
            
            batch.append(row_to_batch)
            
            # вставка батчами для ускорения
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(query, batch)
                conn.commit()
                count += len(batch)
                print(f"Импортировано транзакций: {count}")
                batch = []
        
        # вставка оставшихся записей
        if batch:
            cursor.executemany(query, batch)
            conn.commit()
    
    print(f"Всего импортировано транзакций: {count}")


def main() -> None:
    # пути к файлам
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, 'data')
    db_path = os.path.join(base_dir, 'bank.db')
    sql_script_path = os.path.join(base_dir, 'create_tables.sql')
    
    # удаляем старую БД если существует
    if os.path.exists(db_path):
        os.remove(db_path)
        print("Старая база данных удалена")
    
    # создаем БД и таблицы
    create_database(db_path, sql_script_path)
    
    # подключаемся к БД
    conn = sqlite3.connect(db_path)
    
    try:
        # импортируем данные в правильном порядке (соблюдая внешние ключи)
        print("\n=== Импорт категорий ===")
        import_categories(conn, os.path.join(data_dir, 'categories.csv'))
        
        print("\n=== Импорт клиентов ===")
        import_clients(conn, os.path.join(data_dir, 'clients.csv'))
        
        print("\n=== Импорт подписок ===")
        import_subscriptions(conn, os.path.join(data_dir, 'subscriptions.csv'))
        
        print("\n=== Импорт транзакций ===")
        import_transactions(conn, os.path.join(data_dir, 'transactions.csv'))
        
        print("\n✓ Импорт данных завершен успешно!")
        
        # выводим статистику
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM categories")
        print(f"\nСтатистика:")
        print(f"  Категорий: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM clients")
        print(f"  Клиентов: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM subscriptions")
        print(f"  Подписок: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM transactions")
        print(f"  Транзакций: {cursor.fetchone()[0]}")
        
    except Exception as e:
        print(f"\n✗ Ошибка при импорте: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()


if __name__ == "__main__":
    main()
