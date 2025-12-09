-- создание таблицы категорий
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    mcc_code TEXT
);

-- создание таблицы клиентов
CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY,
    fullname TEXT NOT NULL,
    address TEXT,
    phone_number TEXT,
    email TEXT,
    workplace TEXT,
    birthdate DATE,
    registration_date DATE,
    gender TEXT CHECK(gender IN ('M', 'F')),
    income FLOAT,
    expenses FLOAT,
    credit INTEGER CHECK(credit IN (0, 1)),
    deposit INTEGER CHECK(deposit IN (0, 1))
);

-- создание таблицы подписок
CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY,
    client_id INTEGER NOT NULL,
    product_category INTEGER NOT NULL,
    product_company TEXT,
    amount FLOAT,
    date_start DATE,
    date_end DATE,
    FOREIGN KEY (client_id) REFERENCES clients(id),
    FOREIGN KEY (product_category) REFERENCES categories(id)
);

-- создание таблицы транзакций
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    product_category INTEGER NOT NULL,
    product_company TEXT,
    subtype TEXT,
    amount FLOAT,
    date TIMESTAMP,
    transaction_type TEXT CHECK(transaction_type IN ('Positive', 'Negative')),
    FOREIGN KEY (client_id) REFERENCES clients(id),
    FOREIGN KEY (product_category) REFERENCES categories(id)
);

-- создание индексов для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_subscriptions_client_id ON subscriptions(client_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_product_category ON subscriptions(product_category);
CREATE INDEX IF NOT EXISTS idx_transactions_client_id ON transactions(client_id);
CREATE INDEX IF NOT EXISTS idx_transactions_product_category ON transactions(product_category);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
