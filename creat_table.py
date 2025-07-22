import psycopg2

db_name = 'quant_data'
# 连接参数
conn_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": 5432
}

conn = psycopg2.connect(**conn_params)
conn.autocommit = True  
cursor = conn.cursor()


cursor.execute(f"CREATE DATABASE {db_name};")
print(f"数据库 {db_name} 创建成功！")

cursor.close()
conn.close()

conn_params = {
    "dbname": db_name,
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": 5432
}

# 连接数据库
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()

create_daily_statements_1 = """
CREATE TABLE IF NOT EXISTS market_data_daily (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open NUMERIC(10,4),
    high NUMERIC(10,4),
    low NUMERIC(10,4),
    close NUMERIC(10,4),
    volume BIGINT,
    PRIMARY KEY (time, symbol)
);
"""

create_daily_statements_2 = """
SELECT create_hypertable('market_data_daily', 'time', if_not_exists => TRUE, chunk_time_interval => interval '1 month');
"""

create_minute_statements_1 = """
CREATE TABLE IF NOT EXISTS market_data_minute (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open NUMERIC(10,4),
    high NUMERIC(10,4),
    low NUMERIC(10,4),
    close NUMERIC(10,4),
    volume BIGINT,
    PRIMARY KEY (time, symbol)
);
"""

create_minute_statements_2 = """
SELECT create_hypertable('market_data_minute', 'time', if_not_exists => TRUE, chunk_time_interval => interval '1 day');
"""

create_tick_statements_1 = """
CREATE TABLE IF NOT EXISTS market_data_tick (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC(10,4),
    size NUMERIC(20,4),
    PRIMARY KEY (time, symbol)
);
"""

create_tick_statements_2 = """
SELECT create_hypertable('market_data_tick', 'time', if_not_exists => TRUE, chunk_time_interval => interval '1 day');
"""


try:
    # 执行SQL命令
    cursor.execute('CREATE EXTENSION IF NOT EXISTS timescaledb;')
    cursor.execute(create_daily_statements_1)
    cursor.execute(create_daily_statements_2)
    cursor.execute(create_minute_statements_1)
    cursor.execute(create_minute_statements_2)
    cursor.execute(create_tick_statements_1)
    cursor.execute(create_tick_statements_2)
    conn.commit()
    print("Tables created successfully.")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
