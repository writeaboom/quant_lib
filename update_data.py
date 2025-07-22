from __future__ import print_function, absolute_import
from gm.api import *
import psycopg2

conn_params = {
    "dbname": "quant_lab",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": 5432
}

set_token('133d4049be94e89752f2f629ea519db425027ed6')

symbol = 'SHSE.510300'  
start_time = '2025-07-14 09:30:00'
end_time = '2025-07-18 15:00:00'

ticks = history(symbol=symbol, frequency='tick', start_time=start_time, end_time=end_time)

conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()

insert_sql = """
INSERT INTO market_data_tick (time, symbol, price, size)
VALUES (%s, %s, %s, %s)
ON CONFLICT (time, symbol) DO NOTHING;
"""
BATCH_SIZE = 5000

for i in range(1,len(ticks)):
    data = [ticks[i]['created_at'],ticks[i]['symbol'].split('.')[-1],ticks[i]['price'],ticks[i]['last_volume']]
    cursor.execute(insert_sql, data)
    if i % BATCH_SIZE == 0:
        conn.commit()  # 每个批次提交一次
print(" 更新数据完成")