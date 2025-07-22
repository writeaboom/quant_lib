import pandas as pd
import psycopg2

def load_data(symbol, start_time, end_time, period):
    conn = psycopg2.connect(dbname='quant_data', 
                            user='postgres', 
                            password='123456', 
                            host='localhost',
                            port='5432')

    if period == 'tick':
        table_name = 'market_data_tick'
    elif period == 'minute':
        table_name = 'market_data_minute'
    elif period == 'day':
        table_name = 'market_data_daily'
    else:
        raise ValueError(f"Unsupported period: {period}")
    
    query = f"""
        SELECT symbol,time, price,size
        FROM {table_name}
        WHERE time >= %s AND time <= %s
        AND symbol = %s
        ORDER BY time ASC
    """

    params = [start_time, end_time, symbol]

    try:
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    return df